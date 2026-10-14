package codec

import (
	"math"

	"github.com/vmihailenco/msgpack/v5"
)

// Tick represents the normalized market data point processed by the engine.
type Tick struct {
	Symbol     string  `json:"symbol" msgpack:"s"`
	Buy        float64 `json:"buy" msgpack:"b"`       // Bid price
	Sell       float64 `json:"sell" msgpack:"a"`      // Ask price
	High       float64 `json:"high" msgpack:"h"`      // 24h High
	Low        float64 `json:"low" msgpack:"l"`       // 24h Low
	ChangePct  float64 `json:"change" msgpack:"c"`    // 24h % change
	Timestamp  int64   `json:"timestamp" msgpack:"t"` // Milliseconds epoch
	ReceivedAt int64   `json:"received_at,omitempty" msgpack:"-"` // Internal arrival-time (unix ms)
	SentAt     int64   `json:"sent_at,omitempty" msgpack:"sent_at,omitempty"` // Unix ms when client flush happens
	Seq        uint64  `json:"seq" msgpack:"seq"`     // Engine global sequence
}

// TickState records the last transmitted values per symbol for a specific client.
type TickState struct {
	Buy       float64
	Sell      float64
	High      float64
	Low       float64
	ChangePct float64
	Timestamp int64
	Seq       uint64
}

// DeltaTick represents a bandwidth-optimized delta message.
// Fields that have not changed since last client transmission are omitted (nil).
type DeltaTick struct {
	Symbol    string   `json:"s" msgpack:"s"`
	Seq       uint64   `json:"seq" msgpack:"seq"`
	Buy       *float64 `json:"b,omitempty" msgpack:"b,omitempty"`
	Sell      *float64 `json:"a,omitempty" msgpack:"a,omitempty"`
	High      *float64 `json:"h,omitempty" msgpack:"h,omitempty"`
	Low       *float64 `json:"l,omitempty" msgpack:"l,omitempty"`
	ChangePct *float64 `json:"c,omitempty" msgpack:"c,omitempty"`
	Timestamp int64    `json:"t" msgpack:"t"`
}

// BatchEnvelope encapsulates a collection of delta updates batched within a 20ms window.
type BatchEnvelope struct {
	Type      string      `json:"type" msgpack:"type"` // "batch"
	Seq       uint64      `json:"seq" msgpack:"seq"`   // Per-client transmission sequence
	Timestamp int64       `json:"t" msgpack:"t"`       // Server batch timestamp
	SentAt    int64       `json:"sent_at,omitempty" msgpack:"sent_at,omitempty"` // Timestamp when flush happens
	Deltas    []DeltaTick `json:"d" msgpack:"d"`
}

const priceEpsilon = 1e-7

func floatEquals(a, b float64) bool {
	return math.Abs(a-b) < priceEpsilon
}

// ComputeDelta checks the current tick against the client's last sent state.
// If any value changed, returns a DeltaTick with only modified fields populated.
// If nothing changed, returns nil.
func ComputeDelta(curr *Tick, last *TickState) (*DeltaTick, *TickState) {
	if curr == nil {
		return nil, last
	}

	if last == nil {
		// First transmission: send all fields
		b := curr.Buy
		s := curr.Sell
		h := curr.High
		l := curr.Low
		c := curr.ChangePct

		delta := &DeltaTick{
			Symbol:    curr.Symbol,
			Seq:       curr.Seq,
			Buy:       &b,
			Sell:      &s,
			High:      &h,
			Low:       &l,
			ChangePct: &c,
			Timestamp: curr.Timestamp,
		}

		nextState := &TickState{
			Buy:       curr.Buy,
			Sell:      curr.Sell,
			High:      curr.High,
			Low:       curr.Low,
			ChangePct: curr.ChangePct,
			Timestamp: curr.Timestamp,
			Seq:       curr.Seq,
		}
		return delta, nextState
	}

	var (
		buyChanged    = !floatEquals(curr.Buy, last.Buy)
		sellChanged   = !floatEquals(curr.Sell, last.Sell)
		highChanged   = !floatEquals(curr.High, last.High)
		lowChanged    = !floatEquals(curr.Low, last.Low)
		changeChanged = !floatEquals(curr.ChangePct, last.ChangePct)
	)

	// If no price metrics changed, do not emit a delta
	if !buyChanged && !sellChanged && !highChanged && !lowChanged && !changeChanged {
		return nil, last
	}

	delta := &DeltaTick{
		Symbol:    curr.Symbol,
		Seq:       curr.Seq,
		Timestamp: curr.Timestamp,
	}

	nextState := &TickState{
		Buy:       last.Buy,
		Sell:      last.Sell,
		High:      last.High,
		Low:       last.Low,
		ChangePct: last.ChangePct,
		Timestamp: curr.Timestamp,
		Seq:       curr.Seq,
	}

	if buyChanged {
		b := curr.Buy
		delta.Buy = &b
		nextState.Buy = b
	}
	if sellChanged {
		s := curr.Sell
		delta.Sell = &s
		nextState.Sell = s
	}
	if highChanged {
		h := curr.High
		delta.High = &h
		nextState.High = h
	}
	if lowChanged {
		l := curr.Low
		delta.Low = &l
		nextState.Low = l
	}
	if changeChanged {
		c := curr.ChangePct
		delta.ChangePct = &c
		nextState.ChangePct = c
	}

	return delta, nextState
}

// Encode marshals any payload using MessagePack.
func Encode(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Decode unmarshals MessagePack binary data into the target struct.
func Decode(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
