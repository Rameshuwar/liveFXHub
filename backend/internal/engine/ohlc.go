package engine

import (
	"sync"

	"market-data-system/backend/internal/codec"
)

// Candle represents an aggregated Open-High-Low-Close bar over a specific time interval.
type Candle struct {
	Symbol    string  `json:"symbol"`
	Interval  string  `json:"interval"`   // "1m" or "5m"
	StartTime int64   `json:"start_time"` // Bucket start epoch in milliseconds
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int64   `json:"volume"`     // Number of ticks aggregated
}

// OHLCAggregator maintains real-time 1m and 5m candle bars per symbol.
type OHLCAggregator struct {
	mu         sync.RWMutex
	maxCandles int
	candles1m  map[string][]Candle
	candles5m  map[string][]Candle
}

// NewOHLCAggregator initializes the candle accumulator.
func NewOHLCAggregator() *OHLCAggregator {
	return &OHLCAggregator{
		maxCandles: 200,
		candles1m:  make(map[string][]Candle),
		candles5m:  make(map[string][]Candle),
	}
}

// Ingest updates current 1m and 5m candle bars with the new tick.
func (a *OHLCAggregator) Ingest(t *codec.Tick) {
	a.mu.Lock()
	defer a.mu.Unlock()

	midPrice := (t.Buy + t.Sell) / 2.0
	if midPrice == 0 {
		midPrice = t.Buy
	}

	a.updateBucket(a.candles1m, t.Symbol, "1m", 60*1000, t.Timestamp, midPrice)
	a.updateBucket(a.candles5m, t.Symbol, "5m", 300*1000, t.Timestamp, midPrice)
}

func (a *OHLCAggregator) updateBucket(m map[string][]Candle, symbol, interval string, bucketMs int64, ts int64, price float64) {
	bucketStart := (ts / bucketMs) * bucketMs
	list := m[symbol]

	if len(list) == 0 || list[len(list)-1].StartTime != bucketStart {
		// New candle period started
		newCandle := Candle{
			Symbol:    symbol,
			Interval:  interval,
			StartTime: bucketStart,
			Open:      price,
			High:      price,
			Low:       price,
			Close:     price,
			Volume:    1,
		}
		list = append(list, newCandle)
		if len(list) > a.maxCandles {
			list = list[len(list)-a.maxCandles:]
		}
		m[symbol] = list
		return
	}

	// Update existing open candle
	idx := len(list) - 1
	c := &list[idx]
	if price > c.High {
		c.High = price
	}
	if price < c.Low && price > 0 {
		c.Low = price
	}
	c.Close = price
	c.Volume++
}

// GetCandles returns historical and active candles for symbol and interval.
func (a *OHLCAggregator) GetCandles(symbol, interval string) []Candle {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var list []Candle
	if interval == "5m" {
		list = a.candles5m[symbol]
	} else {
		list = a.candles1m[symbol]
	}

	res := make([]Candle, len(list))
	copy(res, list)
	return res
}
