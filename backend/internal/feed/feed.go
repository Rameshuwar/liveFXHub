package feed

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/config"
	"market-data-system/backend/internal/metrics"
)

// AuthRequest matches the required LiveFXHub authentication payload.
type AuthRequest struct {
	Action string `json:"action"`
	APIKey string `json:"api_key"`
	Secret string `json:"secret"`
}

// SubscribeRequest matches the required LiveFXHub symbol subscription payload.
type SubscribeRequest struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
}

// FeedClient manages the upstream LiveFXHub WebSocket connection with auth,
// heartbeat, auto-reconnection, and fallback simulation.
type FeedClient struct {
	cfg               *config.Config
	outChan           chan<- *codec.Tick
	metrics           *metrics.Metrics
	isConnected       atomic.Bool
	isFallbackActive  atomic.Bool
	connMu            sync.Mutex
	conn              *websocket.Conn

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewFeedClient initializes a new upstream feed client.
func NewFeedClient(cfg *config.Config, outChan chan<- *codec.Tick, m *metrics.Metrics) *FeedClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &FeedClient{
		cfg:     cfg,
		outChan: outChan,
		metrics: m,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins the connection and reconnection lifecycle in the background.
func (f *FeedClient) Start() {
	f.wg.Add(1)
	go f.lifecycleLoop()

	if f.cfg.SimulateFallback {
		f.wg.Add(1)
		go f.fallbackSimulationLoop()
	}
}

// IsConnected returns whether the live feed connection is currently active.
func (f *FeedClient) IsConnected() bool {
	return f.isConnected.Load()
}

// IsFallbackActive returns whether internal fallback tick generation is currently active.
func (f *FeedClient) IsFallbackActive() bool {
	return f.isFallbackActive.Load()
}

// lifecycleLoop maintains persistent connection with exponential backoff.
func (f *FeedClient) lifecycleLoop() {
	defer f.wg.Done()

	backoff := 500 * time.Millisecond
	maxBackoff := 15 * time.Second

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		slog.Info("Connecting to LiveFXHub feed...", "url", f.cfg.FeedURL)
		err := f.connectAndServe()
		f.isConnected.Store(false)
		f.metrics.FeedConnected.Set(0)

		if f.ctx.Err() != nil {
			return
		}

		f.metrics.FeedReconnects.Inc()
		slog.Warn("Feed connection closed or failed", "error", err, "reconnecting_in", backoff)

		// Exponential backoff with 15% jitter
		jitter := time.Duration(float64(backoff) * (0.85 + 0.30*rand.Float64()))
		select {
		case <-f.ctx.Done():
			return
		case <-time.After(jitter):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// connectAndServe handles dialing, authentication, subscription, and tick ingestion.
func (f *FeedClient) connectAndServe() error {
	dialer := websocket.Dialer{
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: f.cfg.FeedInsecureSkipVerify},
		HandshakeTimeout: 10 * time.Second,
	}

	headers := http.Header{}
	conn, resp, err := dialer.DialContext(f.ctx, f.cfg.FeedURL, headers)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial failed with HTTP status %d: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("dial failed: %w", err)
	}

	f.connMu.Lock()
	f.conn = conn
	f.connMu.Unlock()

	defer func() {
		f.connMu.Lock()
		if f.conn != nil {
			f.conn.Close()
			f.conn = nil
		}
		f.connMu.Unlock()
	}()

	// 1. Send Authentication message
	authPayload := AuthRequest{
		Action: "auth",
		APIKey: f.cfg.FeedAPIKey,
		Secret: f.cfg.FeedSecret,
	}

	if err := conn.WriteJSON(authPayload); err != nil {
		f.metrics.FeedAuthFailures.Inc()
		return fmt.Errorf("failed to send auth payload: %w", err)
	}

	// 2. Wait for auth response with a strict 10-second timeout
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, authRespBytes, err := conn.ReadMessage()
	if err != nil {
		f.metrics.FeedAuthFailures.Inc()
		return fmt.Errorf("timeout or error reading auth response: %w", err)
	}

	var authResp map[string]interface{}
	if err := json.Unmarshal(authRespBytes, &authResp); err != nil {
		f.metrics.FeedAuthFailures.Inc()
		return fmt.Errorf("invalid json in auth response: %w", err)
	}

	isSuccess, authErr := isAuthSuccess(authResp)
	if authErr != nil {
		f.metrics.FeedAuthFailures.Inc()
		f.metrics.FeedAuthStatus.WithLabelValues("failed").Set(1)
		return authErr
	}

	if !isSuccess {
		// Response shape is unknown: log full response at INFO and set unknown metric
		slog.Info("Unknown auth response shape received from feed", "response", string(authRespBytes))
		f.metrics.FeedAuthStatus.WithLabelValues("unknown").Set(1)
		f.metrics.FeedAuthFailures.Inc()
		return fmt.Errorf("authentication unconfirmed: unknown response shape: %s", string(authRespBytes))
	}

	f.metrics.FeedAuthStatus.WithLabelValues("authenticated").Set(1)
	f.metrics.FeedAuthStatus.WithLabelValues("unknown").Set(0)
	slog.Info("LiveFXHub authentication confirmed", "response", string(authRespBytes))

	// 3. Send Market Price Subscription message (only after positive auth confirmation)
	subPayload := SubscribeRequest{
		Action:  "subscribeMarketPrice",
		Symbols: f.cfg.Symbols,
	}
	if err := conn.WriteJSON(subPayload); err != nil {
		return fmt.Errorf("failed to send subscribe request: %w", err)
	}

	slog.Info("Subscribed to market prices", "symbols_count", len(f.cfg.Symbols))
	f.isConnected.Store(true)
	f.metrics.FeedConnected.Set(1)
	f.isFallbackActive.Store(false)
	f.metrics.FallbackActive.Set(0)

	// 4. Heartbeat ping loop
	stopPing := make(chan struct{})
	defer close(stopPing)

	go func() {
		pingTicker := time.NewTicker(20 * time.Second)
		defer pingTicker.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-pingTicker.C:
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		return nil
	})

	// 5. Message ingestion loop
	for {
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		_, message, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("connection read error: %w", err)
		}

		f.parseAndDispatch(message)
	}
}

// isAuthSuccess parses and evaluates whether an upstream response represents positive auth confirmation.
func isAuthSuccess(resp map[string]interface{}) (bool, error) {
	// 1. Explicit error checks
	if event, ok := resp["event"].(string); ok && strings.EqualFold(event, "error") {
		msg, _ := resp["message"].(string)
		return false, fmt.Errorf("authentication error: %s", msg)
	}
	if status, ok := resp["status"].(string); ok && (strings.EqualFold(status, "error") || strings.EqualFold(status, "fail") || strings.EqualFold(status, "failed")) {
		msg, _ := resp["message"].(string)
		return false, fmt.Errorf("authentication failed: %s", msg)
	}
	if success, ok := resp["success"].(bool); ok && !success {
		msg, _ := resp["message"].(string)
		return false, fmt.Errorf("authentication unsuccessful: %s", msg)
	}
	if errVal, ok := resp["error"]; ok && errVal != nil && errVal != "" && errVal != false {
		return false, fmt.Errorf("authentication error: %v", errVal)
	}

	// 2. Explicit positive success checks
	if status, ok := resp["status"].(string); ok && (strings.EqualFold(status, "ok") || strings.EqualFold(status, "success") || strings.EqualFold(status, "authenticated")) {
		return true, nil
	}
	if success, ok := resp["success"].(bool); ok && success {
		return true, nil
	}
	if event, ok := resp["event"].(string); ok && (strings.EqualFold(event, "authenticated") || strings.EqualFold(event, "auth_ok") || strings.EqualFold(event, "auth")) {
		return true, nil
	}
	if authBool, ok := resp["authenticated"].(bool); ok && authBool {
		return true, nil
	}
	if token, ok := resp["token"].(string); ok && token != "" {
		return true, nil
	}
	if session, ok := resp["session"].(string); ok && session != "" {
		return true, nil
	}
	if action, ok := resp["action"].(string); ok && strings.EqualFold(action, "auth") && resp["error"] == nil {
		return true, nil
	}

	return false, nil
}

// parseAndDispatch parses various market feed tick schemas.
func (f *FeedClient) parseAndDispatch(data []byte) {
	// 1. Try single tick object
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		// Try array of ticks
		var arr []map[string]interface{}
		if errArr := json.Unmarshal(data, &arr); errArr == nil {
			for _, item := range arr {
				if t := extractTick(item); t != nil {
					f.outChan <- t
				}
			}
		}
		return
	}

	// Check if wrapped in "data" or "tick" key
	if inner, ok := raw["data"].(map[string]interface{}); ok {
		raw = inner
	}

	if t := extractTick(raw); t != nil {
		f.outChan <- t
	}
}

func extractTick(raw map[string]interface{}) *codec.Tick {
	sym := getString(raw, "symbol", "s", "pair")
	if sym == "" {
		return nil
	}

	buy := getFloat(raw, "buy", "bid", "b", "price")
	sell := getFloat(raw, "sell", "ask", "a")
	if sell == 0 && buy > 0 {
		sell = buy * 1.0001
	}
	if buy == 0 && sell > 0 {
		buy = sell * 0.9999
	}
	if buy == 0 && sell == 0 {
		return nil
	}

	ts := getInt64(raw, "timestamp", "time", "ts", "t")
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}

	// FIX 4: Parse feed-provided high_24h, low_24h, change_24h
	high := getFloat(raw, "high_24h", "high", "h")
	low := getFloat(raw, "low_24h", "low", "l")
	change := getFloat(raw, "change_24h", "change", "c", "change_pct")

	return &codec.Tick{
		Symbol:     sym,
		Buy:        buy,
		Sell:       sell,
		High:       high,
		Low:        low,
		ChangePct:  change,
		Timestamp:  ts,
		ReceivedAt: time.Now().UnixMilli(), // FIX 3: Internal middleware arrival time
	}
}

func getString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func getFloat(m map[string]interface{}, keys ...string) float64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch val := v.(type) {
			case float64:
				return val
			case int:
				return float64(val)
			case int64:
				return float64(val)
			}
		}
	}
	return 0
}

func getInt64(m map[string]interface{}, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch val := v.(type) {
			case int64:
				return val
			case float64:
				return int64(val)
			case int:
				return int64(val)
			}
		}
	}
	return 0
}

// fallbackSimulationLoop produces real-time market data ticks when live feed is disconnected.
func (f *FeedClient) fallbackSimulationLoop() {
	defer f.wg.Done()

	// Comprehensive realistic base prices for symbols
	basePrices := map[string]float64{
		"EURUSD": 1.08750, "GBPUSD": 1.27210, "USDJPY": 154.320, "AUDUSD": 0.65420,
		"USDCAD": 1.36850, "USDCHF": 0.90120, "NZDUSD": 0.60150, "AUDCAD": 0.89500,
		"AUDCHF": 0.58900, "AUDJPY": 100.950, "AUDNZD": 1.08700, "AUDSGD": 0.88400,
		"CADCHF": 0.65800, "CADJPY": 112.750, "CHFJPY": 171.200, "CHFSGD": 1.50100,
		"EURAUD": 1.66200, "EURCAD": 1.48800, "EURCHF": 0.97900, "EURGBP": 0.85500,
		"EURJPY": 167.850, "EURNZD": 1.80800, "GBPAUD": 1.94400, "GBPCAD": 1.74100,
		"GBPCHF": 1.14600, "GBPJPY": 196.300, "GBPNZD": 2.11500, "NZDCAD": 0.82300,
		"NZDCHF": 0.54200, "NZDJPY": 92.8000, "USDCNH": 7.23500, "USDMXN": 16.9200,
		"USDNOK": 10.8500, "USDPLN": 3.98500, "USDSEK": 10.7800, "USDUSD": 1.00000,
		"USDTRY": 32.4500, "USDZAR": 18.6500, "EURTRY": 35.2800, "GBPTRY": 41.2800,
		"NOKJPY": 14.2200, "SEKJPY": 14.3100, "SGDJPY": 113.800, "ZARJPY": 8.27000,
		"XAUUSD": 2385.40, "XAGUSD": 28.6500, "BTCUSD": 67450.0, "ETHUSD": 3480.00,
		"BNBUSD": 585.500, "SOLUSD": 178.500, "XRPUSD": 0.52500, "ADAUSD": 0.47500,
		"DOGUSD": 0.15500, "DOTUSD": 7.25000, "LTCUSD": 82.5000, "BCHUSD": 485.000,
		"XLMUSD": 0.11200, "TRXUSD": 0.12100, "UNIUSD": 7.85000, "FILUSD": 5.95000,
		"AVXUSD": 36.8000,
	}

	currentPrices := make(map[string]float64)
	highs := make(map[string]float64)
	lows := make(map[string]float64)
	for s, p := range basePrices {
		currentPrices[s] = p
		highs[s] = p * 1.015
		lows[s] = p * 0.985
	}

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			f.isFallbackActive.Store(false)
			f.metrics.FallbackActive.Set(0)
			return
		case <-ticker.C:
			// If live feed is active, pause simulation
			if f.isConnected.Load() {
				if f.isFallbackActive.Load() {
					f.isFallbackActive.Store(false)
					f.metrics.FallbackActive.Set(0)
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if !f.isFallbackActive.Load() {
				f.isFallbackActive.Store(true)
				f.metrics.FallbackActive.Set(1)
			}

			// Generate 1-3 ticks per burst
			burstCount := 1 + rand.Intn(3)
			for i := 0; i < burstCount; i++ {
				symIdx := rand.Intn(len(f.cfg.Symbols))
				sym := f.cfg.Symbols[symIdx]

				base, ok := currentPrices[sym]
				if !ok {
					base = 100.0
					currentPrices[sym] = base
					highs[sym] = 101.5
					lows[sym] = 98.5
				}

				// Micro price change with mean reversion
				pctChange := (rand.Float64() - 0.499) * 0.0006
				newPrice := base * (1.0 + pctChange)
				currentPrices[sym] = newPrice

				if newPrice > highs[sym] {
					highs[sym] = newPrice
				}
				if newPrice < lows[sym] {
					lows[sym] = newPrice
				}

				// Spread
				spread := newPrice * 0.00015
				if strings.Contains(sym, "BTC") || strings.Contains(sym, "ETH") {
					spread = newPrice * 0.00025
				}

				buy := newPrice - (spread / 2.0)
				sell := newPrice + (spread / 2.0)

				// Introduce occasional realistic out-of-order jitter (within 30ms)
				ts := time.Now().UnixMilli()
				if rand.Float64() < 0.15 {
					// 15% of ticks jitter back by 5ms - 20ms to exercise out-of-order buffer!
					ts -= int64(5 + rand.Intn(15))
				}

				change24h := ((newPrice - basePrices[sym]) / basePrices[sym]) * 100.0

				f.outChan <- &codec.Tick{
					Symbol:     sym,
					Buy:        buy,
					Sell:       sell,
					High:       highs[sym],
					Low:        lows[sym],
					ChangePct:  change24h,
					Timestamp:  ts,
					ReceivedAt: time.Now().UnixMilli(),
				}
			}
		}
	}
}

// Close gracefully terminates the feed client connection and workers.
func (f *FeedClient) Close() {
	f.cancel()
	f.connMu.Lock()
	if f.conn != nil {
		f.conn.Close()
	}
	f.connMu.Unlock()
	f.wg.Wait()
	f.isConnected.Store(false)
	f.isFallbackActive.Store(false)
	f.metrics.FeedConnected.Set(0)
	f.metrics.FallbackActive.Set(0)
	slog.Info("Feed client cleanly stopped")
}
