package engine

import (
	"container/heap"
	"sync"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

// bufferItem wraps a tick with its middleware arrival timestamp for min-heap sorting.
type bufferItem struct {
	tick       *codec.Tick
	enqueuedAt time.Time
	index      int
}

// tickHeap implements heap.Interface for min-heap priority queue sorted by tick timestamp.
type tickHeap []*bufferItem

func (h tickHeap) Len() int           { return len(h) }
func (h tickHeap) Less(i, j int) bool { return h[i].tick.Timestamp < h[j].tick.Timestamp }
func (h tickHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *tickHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*bufferItem)
	item.index = n
	*h = append(*h, item)
}
func (h *tickHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// DedupeBuffer manages out-of-order 50ms reordering and timestamp deduplication.
type DedupeBuffer struct {
	mu             sync.Mutex
	h              tickHeap
	bufferDuration time.Duration
	metrics        *metrics.Metrics

	// Per-symbol deduplication set with rolling eviction
	seenTimestamps map[string]map[int64]struct{}
	symbolWatermark map[string]int64 // highest emitted timestamp per symbol
	maxHistoryPerSym int
}

// NewDedupeBuffer initializes the out-of-order reordering buffer.
func NewDedupeBuffer(duration time.Duration, m *metrics.Metrics) *DedupeBuffer {
	b := &DedupeBuffer{
		bufferDuration:   duration,
		metrics:          m,
		seenTimestamps:   make(map[string]map[int64]struct{}),
		symbolWatermark:  make(map[string]int64),
		maxHistoryPerSym: 10000,
	}
	heap.Init(&b.h)
	return b
}

// Push ingests a tick, checks for duplicates, checks late arrival, and queues for reordering.
// Returns true if queued, false if dropped.
func (b *DedupeBuffer) Push(t *codec.Tick) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	sym := t.Symbol
	timestamps, ok := b.seenTimestamps[sym]
	if !ok {
		timestamps = make(map[int64]struct{}, 2048)
		b.seenTimestamps[sym] = timestamps
	}

	// 1. Deduplication check
	if _, seen := timestamps[t.Timestamp]; seen {
		b.metrics.IncDropped("dedupe")
		return false
	}

	// 2. Late arrival check against watermark (if older than watermark by more than buffer window)
	watermark := b.symbolWatermark[sym]
	if watermark > 0 && t.Timestamp < (watermark-b.bufferDuration.Milliseconds()) {
		b.metrics.IncDropped("late")
		return false
	}

	// Record timestamp in seen set
	timestamps[t.Timestamp] = struct{}{}
	if len(timestamps) > b.maxHistoryPerSym {
		// Prune timestamps older than 24 hours or lowest half
		cutoff := t.Timestamp - (24 * 3600 * 1000)
		for ts := range timestamps {
			if ts < cutoff {
				delete(timestamps, ts)
			}
		}
	}

	// 3. Push to min-heap for reordering
	item := &bufferItem{
		tick:       t,
		enqueuedAt: time.Now(),
	}
	heap.Push(&b.h, item)
	return true
}

// DrainReady pops all ticks that have spent at least bufferDuration in the buffer,
// ordered by ascending tick timestamp.
func (b *DedupeBuffer) DrainReady() []*codec.Tick {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ready []*codec.Tick
	now := time.Now()

	for b.h.Len() > 0 {
		oldest := b.h[0]
		// Has the oldest tick spent >= bufferDuration in the buffer?
		if now.Sub(oldest.enqueuedAt) < b.bufferDuration {
			break
		}

		item := heap.Pop(&b.h).(*bufferItem)
		sym := item.tick.Symbol

		// Update watermark for this symbol
		if item.tick.Timestamp > b.symbolWatermark[sym] {
			b.symbolWatermark[sym] = item.tick.Timestamp
		}

		ready = append(ready, item.tick)
	}

	return ready
}

// DrainAll forces all remaining buffered items out (used during graceful shutdown).
func (b *DedupeBuffer) DrainAll() []*codec.Tick {
	b.mu.Lock()
	defer b.mu.Unlock()

	var all []*codec.Tick
	for b.h.Len() > 0 {
		item := heap.Pop(&b.h).(*bufferItem)
		all = append(all, item.tick)
	}
	return all
}
