package engine

import (
	"math"
	"sync"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

// timeBucket stores aggregated price metrics for a 10-second time slice.
type timeBucket struct {
	tsStart   int64
	minBuy    float64
	maxBuy    float64
	minSell   float64
	maxSell   float64
	firstBuy  float64
	firstSell float64
	lastBuy   float64
	lastSell  float64
}

// SymbolWindow maintains the state and 24-hour rolling calculations for a specific symbol
// using O(1) time-bucketed aggregation.
type SymbolWindow struct {
	mu             sync.RWMutex
	symbol         string
	windowDuration time.Duration
	metrics        *metrics.Metrics

	// Latest values
	latestBuy       float64
	latestSell      float64
	high24h         float64
	low24h          float64
	changePct       float64
	latestTimestamp int64
	openPrice       float64

	// Locally computed 24h rolling metrics (for fallback and drift validation)
	computedHigh      float64
	computedLow       float64
	computedChangePct float64

	// Bucketed history (10-second granularity, max 8640 buckets, capped by maxHistoryPerSym)
	buckets          []timeBucket
	maxHistoryPerSym int
}

// NewSymbolWindow initializes a 24h time-bucketed sliding window for the given symbol.
func NewSymbolWindow(symbol string, duration time.Duration, m *metrics.Metrics) *SymbolWindow {
	return &SymbolWindow{
		symbol:           symbol,
		windowDuration:   duration,
		metrics:          m,
		buckets:          make([]timeBucket, 0, 1024),
		maxHistoryPerSym: 8640, // 24h = 86400s / 10s = 8640 buckets per symbol (~53MB total across 61 symbols)
		computedHigh:     0,
		computedLow:      0,
	}
}

// Update incorporates a new chronological tick into the rolling 24h window.
// Returns the fully calculated enriched Tick.
func (w *SymbolWindow) Update(t *codec.Tick) *codec.Tick {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.updateLocked(t)
}

func (w *SymbolWindow) updateLocked(t *codec.Tick) *codec.Tick {
	w.latestBuy = t.Buy
	w.latestSell = t.Sell
	w.latestTimestamp = t.Timestamp

	midPrice := (t.Buy + t.Sell) / 2.0
	if midPrice == 0 {
		midPrice = t.Buy
	}

	// 1. Bucket management: 10-second (10000ms) bucket assignment
	bucketStart := t.Timestamp - (t.Timestamp % 10000)
	nBuckets := len(w.buckets)

	if nBuckets == 0 {
		w.buckets = append(w.buckets, timeBucket{
			tsStart:   bucketStart,
			minBuy:    t.Buy,
			maxBuy:    t.Buy,
			minSell:   t.Sell,
			maxSell:   t.Sell,
			firstBuy:  t.Buy,
			firstSell: t.Sell,
			lastBuy:   t.Buy,
			lastSell:  t.Sell,
		})
		w.computedHigh = math.Max(t.Buy, t.Sell)
		w.computedLow = math.Min(t.Buy, t.Sell)
		w.openPrice = midPrice
		w.computedChangePct = 0
	} else {
		lastIdx := nBuckets - 1
		if w.buckets[lastIdx].tsStart == bucketStart {
			// Update current 10-second bucket in O(1)
			b := &w.buckets[lastIdx]
			if t.Buy > b.maxBuy {
				b.maxBuy = t.Buy
			}
			if t.Buy < b.minBuy && t.Buy > 0 {
				b.minBuy = t.Buy
			}
			if t.Sell > b.maxSell {
				b.maxSell = t.Sell
			}
			if t.Sell < b.minSell && t.Sell > 0 {
				b.minSell = t.Sell
			}
			b.lastBuy = t.Buy
			b.lastSell = t.Sell

			// Fast-path incremental extreme updates
			if t.Buy > w.computedHigh {
				w.computedHigh = t.Buy
			}
			if t.Sell > w.computedHigh {
				w.computedHigh = t.Sell
			}
			if t.Buy < w.computedLow && t.Buy > 0 {
				w.computedLow = t.Buy
			}
			if t.Sell < w.computedLow && t.Sell > 0 {
				w.computedLow = t.Sell
			}
		} else if bucketStart > w.buckets[lastIdx].tsStart {
			// Advance to new bucket
			w.buckets = append(w.buckets, timeBucket{
				tsStart:   bucketStart,
				minBuy:    t.Buy,
				maxBuy:    t.Buy,
				minSell:   t.Sell,
				maxSell:   t.Sell,
				firstBuy:  t.Buy,
				firstSell: t.Sell,
				lastBuy:   t.Buy,
				lastSell:  t.Sell,
			})

			// Evict expired buckets older than 24h
			cutoff := t.Timestamp - w.windowDuration.Milliseconds()
			evictCount := 0
			for evictCount < len(w.buckets) && w.buckets[evictCount].tsStart < cutoff {
				evictCount++
			}

			// Apply maxHistoryPerSym cap to bound memory
			remaining := len(w.buckets) - evictCount
			if remaining > w.maxHistoryPerSym {
				evictCount += (remaining - w.maxHistoryPerSym)
			}

			if evictCount > 0 {
				w.buckets = w.buckets[evictCount:]
				w.recalculateExtremes()
			} else {
				// Incremental extreme updates
				if t.Buy > w.computedHigh {
					w.computedHigh = t.Buy
				}
				if t.Sell > w.computedHigh {
					w.computedHigh = t.Sell
				}
				if t.Buy < w.computedLow && t.Buy > 0 {
					w.computedLow = t.Buy
				}
				if t.Sell < w.computedLow && t.Sell > 0 {
					w.computedLow = t.Sell
				}
			}
		}
	}

	// 2. Compute 24h % change from oldest active bucket's opening quote
	if len(w.buckets) > 0 {
		oldest := &w.buckets[0]
		oldestMid := (oldest.firstBuy + oldest.firstSell) / 2.0
		if oldestMid == 0 {
			oldestMid = oldest.firstBuy
		}
		w.openPrice = oldestMid

		if oldestMid > 0 {
			w.computedChangePct = ((midPrice - oldestMid) / oldestMid) * 100.0
		} else {
			w.computedChangePct = 0.0
		}
	}

	// 3. FIX 4: Trust feed-provided 24h aggregates when non-zero; use computed window as fallback & drift validator
	hasFeedAggregates := t.High > 0 && t.Low > 0
	if hasFeedAggregates {
		w.high24h = t.High
		w.low24h = t.Low
		w.changePct = t.ChangePct

		// Measure drift between feed aggregates and locally computed window
		if w.computedHigh > 0 && w.metrics != nil {
			driftPct := math.Abs(t.High-w.computedHigh) / t.High * 100.0
			w.metrics.FeedVsComputedDrift.Set(driftPct)
		}
	} else {
		// Fallback to locally computed 24h rolling metrics
		w.high24h = w.computedHigh
		w.low24h = w.computedLow
		w.changePct = w.computedChangePct
	}

	// 4. Return enriched tick
	enriched := &codec.Tick{
		Symbol:     w.symbol,
		Buy:        w.latestBuy,
		Sell:       w.latestSell,
		High:       w.high24h,
		Low:        w.low24h,
		ChangePct:  w.changePct,
		Timestamp:  w.latestTimestamp,
		ReceivedAt: t.ReceivedAt,
		Seq:        t.Seq,
	}

	return enriched
}

// recalculateExtremes scans the heads of time buckets to find 24h high/low.
// Because buckets are aggregated per 10 seconds, scanning is bounded to maxHistoryPerSym entries.
func (w *SymbolWindow) recalculateExtremes() {
	if len(w.buckets) == 0 {
		w.computedHigh = 0
		w.computedLow = 0
		return
	}

	high := -math.MaxFloat64
	low := math.MaxFloat64

	for i := range w.buckets {
		b := &w.buckets[i]
		if b.maxBuy > high {
			high = b.maxBuy
		}
		if b.maxSell > high {
			high = b.maxSell
		}
		if b.minBuy < low && b.minBuy > 0 {
			low = b.minBuy
		}
		if b.minSell < low && b.minSell > 0 {
			low = b.minSell
		}
	}

	if high == -math.MaxFloat64 {
		high = 0
	}
	if low == math.MaxFloat64 {
		low = 0
	}

	w.computedHigh = high
	w.computedLow = low
}

// Snapshot returns the current computed state of this symbol's window.
func (w *SymbolWindow) Snapshot() *codec.Tick {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return &codec.Tick{
		Symbol:    w.symbol,
		Buy:       w.latestBuy,
		Sell:      w.latestSell,
		High:      w.high24h,
		Low:       w.low24h,
		ChangePct: w.changePct,
		Timestamp: w.latestTimestamp,
	}
}

// SeedFromHistory populates the rolling window with restored ticks from Redis on boot.
func (w *SymbolWindow) SeedFromHistory(ticks []*codec.Tick) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buckets = make([]timeBucket, 0, len(ticks))
	for _, t := range ticks {
		w.updateLocked(t)
	}
}
