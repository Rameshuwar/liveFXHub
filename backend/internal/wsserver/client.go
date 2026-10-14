package wsserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

// ClientMessage represents an incoming control message from a WebSocket client.
type ClientMessage struct {
	Action  string   `json:"action"`            // "subscribe" or "unsubscribe"
	Symbols []string `json:"symbols,omitempty"` // instrument list
}

// Client manages a single client WebSocket session with 20ms delta batching and rate limiting.
type Client struct {
	id          string
	conn        *websocket.Conn
	hub         *Hub
	metrics     *metrics.Metrics
	sendQueue   chan []byte

	// Subscription management
	subMu       sync.RWMutex
	subscribed  map[string]struct{}

	// Delta encoding state per symbol
	stateMu     sync.Mutex
	lastSent    map[string]*codec.TickState

	// Pending ticks for current 20ms batch interval
	pendingMu   sync.Mutex
	pending     map[string]*codec.Tick

	// Sequence counter for this client's stream
	streamSeq   uint64

	// Rate limiter (token bucket for 100 msgs/sec)
	tokens      float64
	lastTokenTs time.Time
	rateLimit   int
	rateMu      sync.Mutex

	ctx         context.Context
	cancel      context.CancelFunc
}

// NewClient creates a new WebSocket client session.
func NewClient(id string, conn *websocket.Conn, hub *Hub, m *metrics.Metrics, rateLimit int) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		id:          id,
		conn:        conn,
		hub:         hub,
		metrics:     m,
		sendQueue:   make(chan []byte, 512),
		subscribed:  make(map[string]struct{}),
		lastSent:    make(map[string]*codec.TickState),
		pending:     make(map[string]*codec.Tick),
		tokens:      float64(rateLimit),
		rateLimit:   rateLimit,
		lastTokenTs: time.Now(),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// IsSubscribed checks if the client is subscribed to a symbol.
func (c *Client) IsSubscribed(symbol string) bool {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	_, ok := c.subscribed[symbol]
	return ok
}

// StageTick buffers an incoming market tick into the current 20ms batch window.
func (c *Client) StageTick(t *codec.Tick) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.pending[t.Symbol] = t
}

// StartLifecycle initiates the read, write, and 20ms batching loops.
func (c *Client) StartLifecycle(batchInterval time.Duration) {
	go c.writePump()
	go c.readPump()
	go c.batchPump(batchInterval)
}

// batchPump runs every batchInterval (20ms), evaluates deltas, encodes MessagePack, and pushes to sendQueue.
func (c *Client) batchPump(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.flushBatch()
		}
	}
}

func (c *Client) flushBatch() {
	c.pendingMu.Lock()
	if len(c.pending) == 0 {
		c.pendingMu.Unlock()
		return
	}

	// Drain current pending batch
	batch := make([]*codec.Tick, 0, len(c.pending))
	for _, t := range c.pending {
		batch = append(batch, t)
	}
	c.pending = make(map[string]*codec.Tick)
	c.pendingMu.Unlock()

	nowMs := time.Now().UnixMilli()
	for _, t := range batch {
		t.SentAt = nowMs
		if c.metrics != nil {
			if t.ReceivedAt > 0 && t.ReceivedAt <= nowMs {
				c.metrics.ObserveE2ELatency(float64(nowMs - t.ReceivedAt))
			} else if t.Timestamp > 0 && t.Timestamp <= nowMs {
				c.metrics.ObserveE2ELatency(float64(nowMs - t.Timestamp))
			}
		}
	}

	c.stateMu.Lock()
	deltas := make([]codec.DeltaTick, 0, len(batch))

	for _, t := range batch {
		lastState := c.lastSent[t.Symbol]
		delta, nextState := codec.ComputeDelta(t, lastState)
		if delta != nil {
			deltas = append(deltas, *delta)
			c.lastSent[t.Symbol] = nextState
		}
	}
	c.stateMu.Unlock()

	if len(deltas) == 0 {
		return
	}

	seq := atomic.AddUint64(&c.streamSeq, 1)
	envelope := codec.BatchEnvelope{
		Type:      "batch",
		Seq:       seq,
		Timestamp: nowMs,
		SentAt:    nowMs,
		Deltas:    deltas,
	}

	payload, err := codec.Encode(envelope)
	if err != nil {
		slog.Error("Failed to encode MessagePack batch envelope", "error", err, "client_id", c.id)
		return
	}

	// Deliver to write queue
	select {
	case c.sendQueue <- payload:
		c.metrics.IncBroadcast(len(deltas))
		c.metrics.AddBytesSent(len(payload))
	default:
		// Queue full: drop with warning to prevent backpressure blocking
		c.metrics.IncDropped("client_queue_overflow")
		slog.Warn("Client send queue overflow, dropping batch", "client_id", c.id)
	}
}

// readPump processes incoming client subscription requests and applies rate limiting.
func (c *Client) readPump() {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
		c.cancel()
	}()

	c.conn.SetReadLimit(65536)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		msgType, msgBytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Info("Client disconnected unexpectedly", "client_id", c.id, "error", err)
			}
			break
		}

		// Rate limit enforcement: 100 msgs/sec
		if !c.allowMessage() {
			c.metrics.IncDropped("rate_limit")
			slog.Warn("Client rate limit exceeded (max 100 msgs/sec)", "client_id", c.id)
			c.sendWarning("rate limit exceeded: max 100 msgs/sec, dropping request")
			continue
		}

		c.handleIncomingMessage(msgType, msgBytes)
	}
}

// writePump handles transmitting WebSocket frames (MessagePack binary messages and pings).
func (c *Client) writePump() {
	ticker := time.NewTicker(20 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case <-c.ctx.Done():
			return

		case payload, ok := <-c.sendQueue:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Transmit binary MessagePack with permessage-deflate compression enabled
			if err := c.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
				slog.Info("Failed to write to client WebSocket", "client_id", c.id, "error", err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// allowMessage implements token bucket rate limiting (100 msgs/sec capacity).
func (c *Client) allowMessage() bool {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()

	now := time.Now()
	elapsed := now.Sub(c.lastTokenTs).Seconds()
	c.lastTokenTs = now

	// Refill tokens
	c.tokens += elapsed * float64(c.rateLimit)
	if c.tokens > float64(c.rateLimit) {
		c.tokens = float64(c.rateLimit)
	}

	if c.tokens >= 1.0 {
		c.tokens -= 1.0
		return true
	}

	return false
}

func (c *Client) handleIncomingMessage(msgType int, data []byte) {
	var req ClientMessage

	// Support both JSON and MessagePack client control messages
	if msgType == websocket.BinaryMessage {
		if err := codec.Decode(data, &req); err != nil {
			slog.Warn("Failed to decode binary client request", "client_id", c.id, "error", err)
			return
		}
	} else {
		if err := json.Unmarshal(data, &req); err != nil {
			slog.Warn("Failed to decode JSON client request", "client_id", c.id, "error", err)
			return
		}
	}

	c.subMu.Lock()
	switch req.Action {
	case "subscribe":
		for _, sym := range req.Symbols {
			c.subscribed[sym] = struct{}{}
		}
		slog.Info("Client subscribed to symbols", "client_id", c.id, "count", len(req.Symbols))

		// Immediately push current snapshot for freshly subscribed symbols
		go c.pushInitialSnapshots(req.Symbols)

	case "unsubscribe":
		for _, sym := range req.Symbols {
			delete(c.subscribed, sym)
		}
		slog.Info("Client unsubscribed from symbols", "client_id", c.id, "count", len(req.Symbols))
	}
	c.subMu.Unlock()
}

func (c *Client) pushInitialSnapshots(symbols []string) {
	for _, sym := range symbols {
		snap := c.hub.engine.GetSnapshot(sym)
		if snap != nil && snap.Buy > 0 {
			c.StageTick(snap)
		}
	}
}

func (c *Client) sendWarning(msg string) {
	warnObj := map[string]string{
		"type":    "warning",
		"message": msg,
	}
	data, _ := json.Marshal(warnObj)
	select {
	case c.sendQueue <- data:
	default:
	}
}

// Close gracefully closes the client connection.
func (c *Client) Close() {
	c.cancel()
	c.conn.Close()
}
