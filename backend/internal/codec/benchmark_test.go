package codec

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var testSymbols = []string{
	"AUDUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDSGD", "CADCHF", "CADJPY", "CHFJPY", "CHFSGD",
	"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF",
	"GBPJPY", "GBPNZD", "GBPUSD", "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD", "USDCAD", "USDCHF", "USDCNH",
	"USDJPY", "USDMXN", "USDNOK", "USDPLN", "USDSEK", "USDUSD", "USDTRY", "USDZAR", "EURTRY", "GBPTRY",
	"NOKJPY", "SEKJPY", "SGDJPY", "ZARJPY", "XAUUSD", "XAGUSD", "BTCUSD", "ETHUSD", "BNBUSD", "SOLUSD",
	"XRPUSD", "ADAUSD", "DOGUSD", "DOTUSD", "LTCUSD", "BCHUSD", "XLMUSD", "TRXUSD", "UNIUSD", "FILUSD",
	"AVXUSD",
}

// deflateCompress compresses data using DEFLATE (permessage-deflate equivalent).
func deflateCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestSimulateBandwidth(t *testing.T) {
	for _, changeProb := range []float64{0.10, 0.15, 0.20, 0.25, 0.50, 1.0} {
		r := rand.New(rand.NewSource(42))
		basePrices := make(map[string]float64)
		highs := make(map[string]float64)
		lows := make(map[string]float64)
		for _, s := range testSymbols {
			basePrices[s] = 1.0 + r.Float64()*100.0
			highs[s] = basePrices[s] * 1.02
			lows[s] = basePrices[s] * 0.98
		}

		totalTicks := 1220
		numBatches := 50
		ticksPerBatch := totalTicks / numBatches

		var rawJSONBytes int
		var msgpackNoDeltaBytes int
		var msgpackDeltaBytes int
		var msgpackDeltaDeflateBytes int

		clientLastSent := make(map[string]*TickState)
		var streamSeq uint64
		now := time.Now().UnixMilli()

		for b := 0; b < numBatches; b++ {
			batchTicks := make([]*Tick, 0, ticksPerBatch)
			pending := make(map[string]*Tick)
			for i := 0; i < ticksPerBatch; i++ {
				sym := testSymbols[r.Intn(len(testSymbols))]
				if r.Float64() < changeProb {
					basePrices[sym] += (r.Float64() - 0.499) * 0.0005
				}
				p := basePrices[sym]
				if p > highs[sym] {
					highs[sym] = p
				}
				if p < lows[sym] {
					lows[sym] = p
				}
				tick := &Tick{
					Symbol:    sym,
					Buy:       p,
					Sell:      p + 0.0002,
					High:      highs[sym],
					Low:       lows[sym],
					ChangePct: 0.15,
					Timestamp: now + int64(b*20),
					Seq:       streamSeq + 1,
				}
				streamSeq++
				pending[sym] = tick

				jb, _ := json.Marshal(tick)
				rawJSONBytes += len(jb) + 46
			}

			for _, tick := range pending {
				batchTicks = append(batchTicks, tick)
			}

			noDeltaEnv := map[string]interface{}{
				"type": "batch",
				"seq":  b + 1,
				"t":    now + int64(b*20),
				"d":    batchTicks,
			}
			ndb, _ := Encode(noDeltaEnv)
			msgpackNoDeltaBytes += len(ndb)

			deltas := make([]DeltaTick, 0, len(batchTicks))
			for _, tick := range batchTicks {
				last := clientLastSent[tick.Symbol]
				delta, next := ComputeDelta(tick, last)
				if delta != nil {
					deltas = append(deltas, *delta)
					clientLastSent[tick.Symbol] = next
				}
			}

			env := BatchEnvelope{
				Type:      "batch",
				Seq:       uint64(b + 1),
				Timestamp: now + int64(b*20),
				Deltas:    deltas,
			}

			db, _ := Encode(env)
			msgpackDeltaBytes += len(db)

			defb, _ := deflateCompress(db)
			msgpackDeltaDeflateBytes += len(defb)
		}

		fmt.Printf("Prob=%.2f -> RawJSON: %d (%.2f KB/s) | MsgPack no-delta: %d (%.2f KB/s) | MsgPack+delta: %d (%.2f KB/s) | MsgPack+delta+deflate: %d (%.2f KB/s)\n",
			changeProb, rawJSONBytes, float64(rawJSONBytes)/1024.0,
			msgpackNoDeltaBytes, float64(msgpackNoDeltaBytes)/1024.0,
			msgpackDeltaBytes, float64(msgpackDeltaBytes)/1024.0,
			msgpackDeltaDeflateBytes, float64(msgpackDeltaDeflateBytes)/1024.0)
	}
}
