package wsserver

import (
	"context"
	"log/slog"
	"sync"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/engine"
	"market-data-system/backend/internal/metrics"
)

// Hub maintains the set of active WebSocket clients and broadcasts incoming symbol ticks.
type Hub struct {
	mu         sync.RWMutex
	clients    map[string]*Client
	engine     *engine.Engine
	metrics    *metrics.Metrics
	tickInput  chan *codec.Tick
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewHub initializes the client connection hub.
func NewHub(eng *engine.Engine, m *metrics.Metrics) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		clients:   make(map[string]*Client),
		engine:    eng,
		metrics:   m,
		tickInput: make(chan *codec.Tick, 10000),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Register with engine output
	eng.RegisterOutput(h.tickInput)
	return h
}

// Register adds a new client to the hub.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	h.clients[c.id] = c
	count := len(h.clients)
	h.mu.Unlock()

	h.metrics.ActiveClients.Set(float64(count))
	slog.Info("Client registered", "client_id", c.id, "active_clients", count)
}

// Unregister removes a client from the hub.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	if _, ok := h.clients[c.id]; ok {
		delete(h.clients, c.id)
	}
	count := len(h.clients)
	h.mu.Unlock()

	h.metrics.ActiveClients.Set(float64(count))
	slog.Info("Client unregistered", "client_id", c.id, "active_clients", count)
}

// StartBroadcastLoop receives calculated ticks from the engine and stages them onto subscribed clients.
func (h *Hub) StartBroadcastLoop() {
	go func() {
		for {
			select {
			case <-h.ctx.Done():
				return

			case t, ok := <-h.tickInput:
				if !ok {
					return
				}

				h.mu.RLock()
				for _, client := range h.clients {
					if client.IsSubscribed(t.Symbol) {
						client.StageTick(t)
					}
				}
				h.mu.RUnlock()
			}
		}
	}()
}

// Close gracefully disconnects all active clients.
func (h *Hub) Close() {
	h.cancel()

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, client := range h.clients {
		client.Close()
	}
	h.clients = make(map[string]*Client)
	h.metrics.ActiveClients.Set(0)
	slog.Info("Hub closed, all clients disconnected")
}
