package engine

import (
	"math"
	"testing"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/metrics"
)

func TestUpdateBucketRollover(t *testing.T) {
	m := metrics.New()
	win := NewSymbolWindow("EURUSD", 24*time.Hour, m)

	baseTime := int64(1700000000000) // Divisible by 10000

	// Tick 1 in bucket 1
	t1 := &codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0850,
		Sell:      1.0852,
		Timestamp: baseTime + 1000,
	}
	enriched1 := win.Update(t1)
	if enriched1.Buy != 1.0850 || enriched1.Sell != 1.0852 {
		t.Fatalf("unexpected tick 1 values: %+v", enriched1)
	}
	if len(win.buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(win.buckets))
	}

	// Tick 2 in same 10-second bucket (baseTime + 4000)
	t2 := &codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0860,
		Sell:      1.0862,
		Timestamp: baseTime + 4000,
	}
	win.Update(t2)
	if len(win.buckets) != 1 {
		t.Fatalf("expected still 1 bucket, got %d", len(win.buckets))
	}
	if win.computedHigh != 1.0862 {
		t.Fatalf("expected computedHigh 1.0862, got %f", win.computedHigh)
	}

	// Tick 3 in next 10-second bucket (baseTime + 12000)
	t3 := &codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0870,
		Sell:      1.0872,
		Timestamp: baseTime + 12000,
	}
	win.Update(t3)
	if len(win.buckets) != 2 {
		t.Fatalf("expected 2 buckets after rollover, got %d", len(win.buckets))
	}
	if win.buckets[1].tsStart != baseTime+10000 {
		t.Fatalf("expected second bucket start %d, got %d", baseTime+10000, win.buckets[1].tsStart)
	}
	if win.computedHigh != 1.0872 {
		t.Fatalf("expected computedHigh 1.0872, got %f", win.computedHigh)
	}
}

func TestEviction24h(t *testing.T) {
	m := metrics.New()
	duration := 1 * time.Hour
	win := NewSymbolWindow("EURUSD", duration, m)

	baseTime := int64(1700000000000)

	// Old high price
	win.Update(&codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.1000,
		Sell:      1.1002,
		Timestamp: baseTime,
	})

	// Subsequent lower price 30 mins later
	win.Update(&codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0800,
		Sell:      1.0802,
		Timestamp: baseTime + 30*60*1000,
	})

	if win.computedHigh != 1.1002 {
		t.Fatalf("expected computedHigh 1.1002, got %f", win.computedHigh)
	}

	// Advance beyond windowDuration (65 mins later), evicting the first bucket
	win.Update(&codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0850,
		Sell:      1.0852,
		Timestamp: baseTime + 65*60*1000,
	})

	// Extremes should be recalculated without the evicted 1.1002
	if win.computedHigh > 1.0852 {
		t.Fatalf("expected computedHigh to drop after eviction, got %f", win.computedHigh)
	}
}

func TestSeedFromHistoryNoDeadlock(t *testing.T) {
	m := metrics.New()
	win := NewSymbolWindow("BTCUSD", 24*time.Hour, m)

	ticks := []*codec.Tick{
		{Symbol: "BTCUSD", Buy: 60000, Sell: 60010, Timestamp: 1700000000000},
		{Symbol: "BTCUSD", Buy: 60100, Sell: 60110, Timestamp: 1700000010000},
		{Symbol: "BTCUSD", Buy: 59900, Sell: 59910, Timestamp: 1700000020000},
	}

	done := make(chan struct{})
	go func() {
		win.SeedFromHistory(ticks)
		close(done)
	}()

	select {
	case <-done:
		// Success: no deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("SeedFromHistory deadlocked!")
	}

	snap := win.Snapshot()
	if snap.Buy != 59900 || snap.Sell != 59910 {
		t.Fatalf("unexpected snapshot after seed: %+v", snap)
	}
	if snap.High < 60110 {
		t.Fatalf("expected high >= 60110, got %f", snap.High)
	}
}

func TestChangePctCalculation(t *testing.T) {
	m := metrics.New()
	win := NewSymbolWindow("EURUSD", 24*time.Hour, m)

	baseTime := int64(1700000000000)

	// Initial opening quote: mid = (1.0000 + 1.0000) / 2 = 1.0000
	win.Update(&codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0000,
		Sell:      1.0000,
		Timestamp: baseTime,
	})

	// Price advances 5% to 1.0500 in new bucket
	t2 := win.Update(&codec.Tick{
		Symbol:    "EURUSD",
		Buy:       1.0500,
		Sell:      1.0500,
		Timestamp: baseTime + 15000,
	})

	expectedPct := 5.0
	if math.Abs(t2.ChangePct-expectedPct) > 1e-4 {
		t.Fatalf("expected changePct ~5.0%%, got %f%%", t2.ChangePct)
	}
}
