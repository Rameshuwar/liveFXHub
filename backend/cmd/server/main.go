package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"market-data-system/backend/internal/codec"
	"market-data-system/backend/internal/config"
	"market-data-system/backend/internal/engine"
	"market-data-system/backend/internal/feed"
	"market-data-system/backend/internal/metrics"
	"market-data-system/backend/internal/store"
	"market-data-system/backend/internal/wsserver"
)

func main() {
	// Initialize structured slog logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting Real-Time Market Data Middleware System...")

	// Root context with graceful termination trap
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Load configuration
	cfg := config.Load()
	slog.Info("Configuration loaded",
		"feed_url", cfg.FeedURL,
		"redis_addr", cfg.RedisAddr,
		"http_port", cfg.HTTPPort,
		"symbols_count", len(cfg.Symbols),
		"batch_interval", cfg.BatchInterval,
		"reorder_buffer", cfg.OutOfOrderBuffer,
	)

	// 2. Initialize Prometheus metrics
	m := metrics.New()
	m.StartRateWorker(ctx)

	// 3. Initialize core Engine
	eng := engine.NewEngine(cfg.WindowDuration, cfg.OutOfOrderBuffer, m)

	// 4. Initialize Redis persistence store
	redisStore := store.NewRedisStore(cfg)

	// Connect engine output to Redis async persistence queue
	eng.RegisterOutput(redisStore.IngestChannel())

	// Start Redis async batch flusher
	redisStore.StartAsyncWorker()

	// Boot hydration: restore 24h rolling windows from Redis
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := redisStore.RestoreAll(bootCtx, eng); err != nil {
		slog.Warn("Proceeding with fresh engine state (no Redis history restored)", "error", err)
	}
	bootCancel()

	// 5. Start engine out-of-order reorder loop
	eng.StartReorderLoop(ctx)

	// 6. Initialize upstream Feed client
	feedChan := make(chan *codec.Tick, 10000)
	feedClient := feed.NewFeedClient(cfg, feedChan, m)
	feedClient.Start()

	// Pipe incoming feed ticks into the engine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-feedChan:
				if !ok {
					return
				}
				eng.Ingest(t)
			}
		}
	}()

	// 7. Initialize HTTP and WebSocket server
	server := wsserver.NewServer(cfg, eng, m)

	// Start HTTP server in background
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP/WS server encountered fatal error", "error", err)
			stop()
		}
	}()

	slog.Info("Market Data Middleware is fully operational", "endpoint", "ws://0.0.0.0:"+cfg.HTTPPort+"/ws")

	// 8. Wait for OS termination signal
	<-ctx.Done()
	slog.Info("Shutdown signal received. Initiating graceful shutdown...")

	// 9. Orderly teardown sequence
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Close feed client
	feedClient.Close()

	// Shutdown WebSocket and HTTP server (disconnects active clients cleanly)
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Error during server shutdown", "error", err)
	}

	// Close and flush Redis persistence store
	if err := redisStore.Close(); err != nil {
		slog.Error("Error closing Redis store", "error", err)
	}

	slog.Info("Market Data Middleware exited gracefully")
}
