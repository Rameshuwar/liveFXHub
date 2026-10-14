package engine

import (
	"testing"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

func TestDedupe(t *testing.T) {
	m := metrics.New()
	buf := NewDedupeBuffer(50*time.Millisecond, m)

	tick1 := &codec.Tick{Symbol: "EURUSD", Timestamp: 1000, Buy: 1.0850, Sell: 1.0852}
	tick2 := &codec.Tick{Symbol: "EURUSD", Timestamp: 1000, Buy: 1.0851, Sell: 1.0853}

	if !buf.Push(tick1) {
		t.Fatal("expected first tick to be accepted")
	}

	if buf.Push(tick2) {
		t.Fatal("expected duplicate tick with same timestamp to be rejected")
	}
}

func TestLateDrop(t *testing.T) {
	m := metrics.New()
	buf := NewDedupeBuffer(50*time.Millisecond, m)

	// Ingest tick at T=1000 and drain it to advance watermark
	buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 1000})
	time.Sleep(60 * time.Millisecond)
	drained := buf.DrainReady()
	if len(drained) != 1 {
		t.Fatalf("expected 1 tick drained, got %d", len(drained))
	}

	// Watermark is now 1000.
	// Tick at T=960 is within watermark - 50ms (1000 - 50 = 950), so accepted
	if !buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 960}) {
		t.Fatal("expected tick at T=960 to be accepted within watermark tolerance")
	}

	// Tick at T=940 is older than watermark - 50ms (950), so dropped as late
	if buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 940}) {
		t.Fatal("expected tick at T=940 to be dropped as late")
	}
}

func TestReorder(t *testing.T) {
	m := metrics.New()
	buf := NewDedupeBuffer(30*time.Millisecond, m)

	// Push out-of-order ticks
	buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 300, Buy: 1.3})
	buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 100, Buy: 1.1})
	buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 200, Buy: 1.2})

	// Before buffer duration elapsed, ready should be empty
	if len(buf.DrainReady()) != 0 {
		t.Fatal("expected 0 ticks drained before buffer duration elapsed")
	}

	// Wait for buffer duration to expire
	time.Sleep(45 * time.Millisecond)

	ready := buf.DrainReady()
	if len(ready) != 3 {
		t.Fatalf("expected 3 ticks drained, got %d", len(ready))
	}

	// Verify ascending chronological order: 100, 200, 300
	if ready[0].Timestamp != 100 || ready[1].Timestamp != 200 || ready[2].Timestamp != 300 {
		t.Fatalf("expected ticks in chronological order [100, 200, 300], got [%d, %d, %d]",
			ready[0].Timestamp, ready[1].Timestamp, ready[2].Timestamp)
	}
}

func TestDrainAll(t *testing.T) {
	m := metrics.New()
	buf := NewDedupeBuffer(500*time.Millisecond, m)

	buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 100})
	buf.Push(&codec.Tick{Symbol: "EURUSD", Timestamp: 200})

	// DrainAll forces all remaining items out without waiting
	remaining := buf.DrainAll()
	if len(remaining) != 2 {
		t.Fatalf("expected 2 items from DrainAll, got %d", len(remaining))
	}

	if buf.h.Len() != 0 {
		t.Fatalf("expected heap to be empty after DrainAll, got len %d", buf.h.Len())
	}
}
