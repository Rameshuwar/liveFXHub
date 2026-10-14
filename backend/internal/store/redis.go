package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/config"
	"market-data-system/backend/internal/engine"
)

// AggregatedBucket stores downsampled 10-second OHLC and quote extremes for Redis persistence.
type AggregatedBucket struct {
	TS        int64   `json:"ts"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int64   `json:"volume"`
	FirstBuy  float64 `json:"firstBuy"`
	FirstSell float64 `json:"firstSell"`
	LastBuy   float64 `json:"lastBuy"`
	LastSell  float64 `json:"lastSell"`
}

// RedisStore handles Redis persistence and boot state restoration.
type RedisStore struct {
	client        *redis.Client
	cfg           *config.Config
	inChan        chan *codec.Tick
	activeBuckets map[string]*AggregatedBucket
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewRedisStore creates a new RedisStore connected to Redis 7.
func NewRedisStore(cfg *config.Config) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithCancel(context.Background())

	return &RedisStore{
		client:        rdb,
		cfg:           cfg,
		inChan:        make(chan *codec.Tick, 20000),
		activeBuckets: make(map[string]*AggregatedBucket),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// IngestChannel returns the channel where the engine sends ticks for async persistence.
func (s *RedisStore) IngestChannel() chan<- *codec.Tick {
	return s.inChan
}

// Ping checks if Redis is reachable.
func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// StartAsyncWorker starts the background batch pipeline flusher.
func (s *RedisStore) StartAsyncWorker() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		flushTicker := time.NewTicker(s.cfg.RedisFlushInterval)
		cleanupTicker := time.NewTicker(30 * time.Second)
		defer flushTicker.Stop()
		defer cleanupTicker.Stop()

		batch := make([]*codec.Tick, 0, s.cfg.RedisBatchSize)

		for {
			select {
			case <-s.ctx.Done():
				// Drain remaining ticks
				for len(s.inChan) > 0 {
					batch = append(batch, <-s.inChan)
					if len(batch) >= s.cfg.RedisBatchSize {
						s.flushBatch(batch)
						batch = batch[:0]
					}
				}
				if len(batch) > 0 {
					s.flushBatch(batch)
				}
				// Flush any active pending buckets on shutdown
				s.flushActiveBuckets()
				slog.Info("Redis async persistence worker stopped")
				return

			case t := <-s.inChan:
				batch = append(batch, t)
				if len(batch) >= s.cfg.RedisBatchSize {
					s.flushBatch(batch)
					batch = batch[:0]
				}

			case <-flushTicker.C:
				if len(batch) > 0 {
					s.flushBatch(batch)
					batch = batch[:0]
				}

			case <-cleanupTicker.C:
				s.pruneHistoricalData()
			}
		}
	}()
}

// flushBatch downsamples ticks into 10-second buckets per symbol and writes to Redis via pipelining.
func (s *RedisStore) flushBatch(batch []*codec.Tick) {
	if len(batch) == 0 {
		return
	}

	pipe := s.client.Pipeline()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	bucketsToPersist := make(map[string]*AggregatedBucket)

	for _, t := range batch {
		if t == nil || t.Symbol == "" {
			continue
		}

		// 1. tick:{symbol}:latest snapshot
		tickData, err := json.Marshal(t)
		if err == nil {
			latestKey := fmt.Sprintf("tick:%s:latest", t.Symbol)
			pipe.Set(ctx, latestKey, tickData, 0)
		}

		// 2. Downsample into 10-second buckets per symbol
		bucketStart := t.Timestamp - (t.Timestamp % 10000)
		mid := (t.Buy + t.Sell) / 2.0
		if mid == 0 {
			mid = t.Buy
		}

		active, exists := s.activeBuckets[t.Symbol]
		if !exists || bucketStart > active.TS {
			if exists && active != nil {
				// Completed previous bucket, queue for persistence
				bucketsToPersist[t.Symbol] = active
			}
			high := t.Buy
			if t.Sell > high {
				high = t.Sell
			}
			low := t.Buy
			if t.Sell < low && t.Sell > 0 {
				low = t.Sell
			}
			newBucket := &AggregatedBucket{
				TS:        bucketStart,
				Open:      mid,
				High:      high,
				Low:       low,
				Close:     mid,
				Volume:    1,
				FirstBuy:  t.Buy,
				FirstSell: t.Sell,
				LastBuy:   t.Buy,
				LastSell:  t.Sell,
			}
			s.activeBuckets[t.Symbol] = newBucket
			bucketsToPersist[t.Symbol] = newBucket
		} else if active.TS == bucketStart {
			if t.Buy > active.High {
				active.High = t.Buy
			}
			if t.Sell > active.High {
				active.High = t.Sell
			}
			if t.Buy < active.Low && t.Buy > 0 {
				active.Low = t.Buy
			}
			if t.Sell < active.Low && t.Sell > 0 {
				active.Low = t.Sell
			}
			active.Close = mid
			active.LastBuy = t.Buy
			active.LastSell = t.Sell
			active.Volume++
			bucketsToPersist[t.Symbol] = active
		}
	}

	// 3. Write downsampled buckets to window:{symbol} sorted set
	for sym, b := range bucketsToPersist {
		data, err := json.Marshal(b)
		if err != nil {
			continue
		}
		windowKey := fmt.Sprintf("window:%s", sym)
		pipe.ZRemRangeByScore(ctx, windowKey, fmt.Sprintf("%d", b.TS), fmt.Sprintf("%d", b.TS))
		pipe.ZAdd(ctx, windowKey, redis.Z{
			Score:  float64(b.TS),
			Member: string(data),
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("Failed to execute Redis flush pipeline", "error", err, "batch_size", len(batch))
	}
}

// flushActiveBuckets writes all in-flight 10s buckets to Redis upon shutdown.
func (s *RedisStore) flushActiveBuckets() {
	if len(s.activeBuckets) == 0 {
		return
	}
	pipe := s.client.Pipeline()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for sym, b := range s.activeBuckets {
		data, err := json.Marshal(b)
		if err != nil {
			continue
		}
		windowKey := fmt.Sprintf("window:%s", sym)
		pipe.ZRemRangeByScore(ctx, windowKey, fmt.Sprintf("%d", b.TS), fmt.Sprintf("%d", b.TS))
		pipe.ZAdd(ctx, windowKey, redis.Z{
			Score:  float64(b.TS),
			Member: string(data),
		})
	}
	pipe.Exec(ctx)
}

// pruneHistoricalData removes ticks older than 24h from all symbol sorted sets.
func (s *RedisStore) pruneHistoricalData() {
	cutoff := time.Now().Add(-s.cfg.WindowDuration).UnixMilli()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipe := s.client.Pipeline()
	for _, sym := range s.cfg.Symbols {
		windowKey := fmt.Sprintf("window:%s", sym)
		pipe.ZRemRangeByScore(ctx, windowKey, "-inf", fmt.Sprintf("%d", cutoff))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("Failed to prune historical Redis sorted sets", "error", err)
	}
}

// RestoreAll restores all symbols' 24h windows from Redis on boot.
func (s *RedisStore) RestoreAll(ctx context.Context, eng *engine.Engine) error {
	slog.Info("Attempting to restore symbol windows from Redis...")
	if err := s.Ping(ctx); err != nil {
		slog.Warn("Redis ping failed during boot restore, starting with fresh state", "error", err)
		return err
	}

	cutoff := time.Now().Add(-s.cfg.WindowDuration).UnixMilli()
	restoredSymbols := 0
	totalTicks := 0

	for _, sym := range s.cfg.Symbols {
		windowKey := fmt.Sprintf("window:%s", sym)
		// Fetch ticks from (now - 24h) to +inf
		vals, err := s.client.ZRangeByScore(ctx, windowKey, &redis.ZRangeBy{
			Min: fmt.Sprintf("%d", cutoff),
			Max: "+inf",
		}).Result()

		if err != nil || len(vals) == 0 {
			// Also check latest key
			latestKey := fmt.Sprintf("tick:%s:latest", sym)
			latestVal, err := s.client.Get(ctx, latestKey).Result()
			if err == nil && latestVal != "" {
				var t codec.Tick
				if json.Unmarshal([]byte(latestVal), &t) == nil {
					eng.SeedWindow(sym, []*codec.Tick{&t})
					restoredSymbols++
					totalTicks++
				}
			}
			continue
		}

		ticks := make([]*codec.Tick, 0, len(vals))
		for _, v := range vals {
			var b AggregatedBucket
			if err := json.Unmarshal([]byte(v), &b); err == nil && b.TS > 0 {
				ticks = append(ticks, &codec.Tick{
					Symbol:    sym,
					Buy:       b.LastBuy,
					Sell:      b.LastSell,
					High:      b.High,
					Low:       b.Low,
					Timestamp: b.TS,
				})
			} else {
				var t codec.Tick
				if err := json.Unmarshal([]byte(v), &t); err == nil {
					ticks = append(ticks, &t)
				}
			}
		}

		if len(ticks) > 0 {
			eng.SeedWindow(sym, ticks)
			restoredSymbols++
			totalTicks += len(ticks)
		}
	}

	slog.Info("Completed Redis restore on boot", "restored_symbols", restoredSymbols, "total_ticks", totalTicks)
	return nil
}

// Close gracefully stops the background persistence worker and closes the Redis client.
func (s *RedisStore) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.client.Close()
}
