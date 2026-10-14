package engine

import (
	"context"
	"testing"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

func TestEngineLifecycleAndDispatch(t *testing.T) {
	m := metrics.New()
	eng := NewEngine(24*time.Hour, 20*time.Millisecond, m)

	outCh := make(chan *codec.Tick, 100)
	eng.RegisterOutput(outCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eng.StartReorderLoop(ctx)

	// Ingest valid tick
	tick1 := &codec.Tick{
		Symbol:     "EURUSD",
		Buy:        1.0850,
		Sell:       1.0852,
		Timestamp:  time.Now().UnixMilli(),
		ReceivedAt: time.Now().UnixMilli(),
	}
	eng.Ingest(tick1)

	// Ingest nil or empty symbol tick (should be ignored)
	eng.Ingest(nil)
	eng.Ingest(&codec.Tick{Symbol: ""})

	// Wait for reorder loop to drain
	var received *codec.Tick
	select {
	case received = <-outCh:
		if received.Symbol != "EURUSD" {
			t.Fatalf("expected symbol EURUSD, got %s", received.Symbol)
		}
		if received.Seq == 0 {
			t.Fatal("expected non-zero sequence number")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for dispatched tick")
	}

	// Test snapshots
	snap := eng.GetSnapshot("EURUSD")
	if snap == nil || snap.Buy != 1.0850 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	nilSnap := eng.GetSnapshot("NONEXISTENT")
	if nilSnap != nil {
		t.Fatalf("expected nil for nonexistent symbol, got %+v", nilSnap)
	}

	allSnaps := eng.GetAllSnapshots()
	if len(allSnaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(allSnaps))
	}

	// Test candles
	candles := eng.GetCandles("EURUSD", "1m")
	if len(candles) == 0 {
		t.Fatal("expected at least 1 candle bar after tick ingestion")
	}

	// Test SeedWindow
	eng.SeedWindow("GBPUSD", []*codec.Tick{
		{Symbol: "GBPUSD", Buy: 1.2500, Sell: 1.2502, Timestamp: time.Now().UnixMilli()},
	})
	gbpSnap := eng.GetSnapshot("GBPUSD")
	if gbpSnap == nil || gbpSnap.Buy != 1.2500 {
		t.Fatalf("unexpected seeded snapshot: %+v", gbpSnap)
	}
}

func TestWindowEdgeCases(t *testing.T) {
	m := metrics.New()
	win := NewSymbolWindow("USDJPY", 24*time.Hour, m)

	// Empty window recalculate extremes
	win.recalculateExtremes()
	if win.computedHigh != 0 || win.computedLow != 0 {
		t.Fatal("expected 0 computedHigh/Low on empty window")
	}

	// Tick with feed-provided aggregates
	t1 := &codec.Tick{
		Symbol:     "USDJPY",
		Buy:        155.0,
		Sell:       155.02,
		High:       156.0,
		Low:        154.0,
		ChangePct:  0.45,
		Timestamp:  time.Now().UnixMilli(),
		ReceivedAt: time.Now().UnixMilli(),
	}
	enriched := win.Update(t1)
	if enriched.High != 156.0 || enriched.Low != 154.0 || enriched.ChangePct != 0.45 {
		t.Fatalf("expected feed aggregates to be trusted: %+v", enriched)
	}

	// Mid price fallback when Buy/Sell average is 0
	winZero := NewSymbolWindow("ZERO", 24*time.Hour, m)
	enrichedZero := winZero.Update(&codec.Tick{
		Symbol:    "ZERO",
		Buy:       10.0,
		Sell:      -10.0, // (10 + -10) / 2 = 0 -> fallback to Buy
		Timestamp: time.Now().UnixMilli(),
	})
	if enrichedZero == nil {
		t.Fatal("expected non-nil enriched tick")
	}
}
