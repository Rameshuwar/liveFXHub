package engine

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

// Engine orchestrates deduplication, 30ms jitter reordering, 24h metrics calculation, and distribution.
type Engine struct {
	mu             sync.RWMutex
	windows        map[string]*SymbolWindow
	buffer         *DedupeBuffer
	ohlc           *OHLCAggregator
	metrics        *metrics.Metrics
	windowDuration time.Duration

	seq         uint64
	outputChans []chan<- *codec.Tick
	outputMu    sync.RWMutex
}

// NewEngine initializes the core market data engine.
func NewEngine(windowDuration, bufferDuration time.Duration, m *metrics.Metrics) *Engine {
	return &Engine{
		windows:        make(map[string]*SymbolWindow),
		buffer:         NewDedupeBuffer(bufferDuration, m),
		ohlc:           NewOHLCAggregator(),
		metrics:        m,
		windowDuration: windowDuration,
		outputChans:    make([]chan<- *codec.Tick, 0, 4),
	}
}

// RegisterOutput registers a channel to receive fully calculated and sequenced ticks.
func (e *Engine) RegisterOutput(ch chan<- *codec.Tick) {
	e.outputMu.Lock()
	defer e.outputMu.Unlock()
	e.outputChans = append(e.outputChans, ch)
}

// Ingest receives raw ticks from upstream feed, buffers and dedupes them.
func (e *Engine) Ingest(t *codec.Tick) {
	if t == nil || t.Symbol == "" {
		return
	}
	e.metrics.IncReceived()
	e.buffer.Push(t)
}

// StartReorderLoop starts the periodic buffer drain loop that releases sorted ticks.
func (e *Engine) StartReorderLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Flush remaining items on shutdown
				remaining := e.buffer.DrainAll()
				for _, t := range remaining {
					e.processOrderedTick(t)
				}
				slog.Info("Engine reorder loop shutdown cleanly")
				return
			case <-ticker.C:
				ready := e.buffer.DrainReady()
				for _, t := range ready {
					e.processOrderedTick(t)
				}
			}
		}
	}()
}

// processOrderedTick calculates 24h rolling metrics and broadcasts to output consumers.
func (e *Engine) processOrderedTick(t *codec.Tick) {
	e.mu.Lock()
	win, ok := e.windows[t.Symbol]
	if !ok {
		win = NewSymbolWindow(t.Symbol, e.windowDuration, e.metrics)
		e.windows[t.Symbol] = win
	}
	e.mu.Unlock()

	// Assign monotonic sequence number
	t.Seq = atomic.AddUint64(&e.seq, 1)

	// Calculate rolling 24h window metrics
	enriched := win.Update(t)

	// Ingest into OHLC aggregator
	e.ohlc.Ingest(enriched)

	// Measure pipeline latency from arrival at middleware to engine dispatch (FIX 3)
	nowMs := time.Now().UnixMilli()
	if enriched.ReceivedAt > 0 && enriched.ReceivedAt <= nowMs {
		e.metrics.ObservePipelineLatency(float64(nowMs - enriched.ReceivedAt))
	} else if enriched.Timestamp > 0 && enriched.Timestamp <= nowMs {
		e.metrics.ObservePipelineLatency(float64(nowMs - enriched.Timestamp))
	}

	// Dispatch to registered output channels
	e.outputMu.RLock()
	for _, ch := range e.outputChans {
		select {
		case ch <- enriched:
		default:
			// Output channel congested, drop to preserve real-time latency
			e.metrics.IncDropped("engine_congested")
		}
	}
	e.outputMu.RUnlock()
}

// GetSnapshot returns the latest 24h window state for a symbol.
func (e *Engine) GetSnapshot(symbol string) *codec.Tick {
	e.mu.RLock()
	win, ok := e.windows[symbol]
	e.mu.RUnlock()

	if !ok {
		return nil
	}
	return win.Snapshot()
}

// GetAllSnapshots returns current state of all known symbols.
func (e *Engine) GetAllSnapshots() []*codec.Tick {
	e.mu.RLock()
	defer e.mu.RUnlock()

	snapshots := make([]*codec.Tick, 0, len(e.windows))
	for _, win := range e.windows {
		snapshots = append(snapshots, win.Snapshot())
	}
	return snapshots
}

// GetCandles returns OHLC bars for a symbol and interval.
func (e *Engine) GetCandles(symbol, interval string) []Candle {
	return e.ohlc.GetCandles(symbol, interval)
}

// SeedWindow hydrates a symbol's window state from Redis historical data on boot.
func (e *Engine) SeedWindow(symbol string, ticks []*codec.Tick) {
	e.mu.Lock()
	win, ok := e.windows[symbol]
	if !ok {
		win = NewSymbolWindow(symbol, e.windowDuration, e.metrics)
		e.windows[symbol] = win
	}
	e.mu.Unlock()

	win.SeedFromHistory(ticks)
	slog.Info("Hydrated symbol window from Redis", "symbol", symbol, "ticks", len(ticks))
}
