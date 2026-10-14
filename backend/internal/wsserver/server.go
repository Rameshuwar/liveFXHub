package wsserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"market-data-system/backend/internal/config"
	"market-data-system/backend/internal/engine"
	"market-data-system/backend/internal/metrics"
)

// Server encapsulates the HTTP server, WebSocket endpoints, and Prometheus metrics.
type Server struct {
	httpServer *http.Server
	hub        *Hub
	engine     *engine.Engine
	cfg        *config.Config
	metrics    *metrics.Metrics
	upgrader   websocket.Upgrader
}

// NewServer initializes the HTTP and WebSocket transport server.
func NewServer(cfg *config.Config, eng *engine.Engine, m *metrics.Metrics) *Server {
	hub := NewHub(eng, m)

	s := &Server{
		hub:     hub,
		engine:  eng,
		cfg:     cfg,
		metrics: m,
		upgrader: websocket.Upgrader{
			ReadBufferSize:    65536,
			WriteBufferSize:   65536,
			EnableCompression: true, // Enables permessage-deflate
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for dev/docker
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/", s.handleWSFallback)
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/api/symbols", s.handleSymbols)
	mux.HandleFunc("/api/candles", s.handleCandles)
	mux.HandleFunc("/api/snapshots", s.handleSnapshots)
	mux.HandleFunc("/health", s.handleHealth)

	// Wrap with CORS and logging
	handler := s.corsMiddleware(mux)

	s.httpServer = &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// Start launches the hub broadcaster and begins serving HTTP/WebSocket requests.
func (s *Server) Start() error {
	s.hub.StartBroadcastLoop()
	slog.Info("Starting HTTP & WebSocket server", "port", s.cfg.HTTPPort, "compression", "permessage-deflate")
	return s.httpServer.ListenAndServe()
}

// Shutdown initiates graceful termination of HTTP server and active WebSocket connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.hub.Close()
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}

	clientID := generateID()
	client := NewClient(clientID, conn, s.hub, s.metrics, s.cfg.ClientRateLimit)
	s.hub.Register(client)

	client.StartLifecycle(s.cfg.BatchInterval)
}

func (s *Server) handleWSFallback(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		s.handleWS(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"service":   "market-data-middleware",
		"version":   "1.0.0",
		"websocket": "/ws",
		"metrics":   "/metrics",
		"health":    "/health",
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleCandles(w http.ResponseWriter, r *http.Request) {
	sym := r.URL.Query().Get("symbol")
	if sym == "" {
		sym = "EURUSD"
	}
	interval := r.URL.Query().Get("interval")
	if interval != "5m" {
		interval = "1m"
	}

	candles := s.engine.GetCandles(sym, interval)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(candles)
}

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config.DefaultSymbols)
}

func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	snapshots := s.engine.GetAllSnapshots()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshots)
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func generateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
