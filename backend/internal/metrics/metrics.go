package metrics

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics encapsulates all Prometheus instrumentation for the market data middleware.
type Metrics struct {
	ActiveClients       prometheus.Gauge
	TicksReceived       prometheus.Counter
	TicksBroadcast      prometheus.Counter
	TicksDropped        *prometheus.CounterVec
	LatencySummary      prometheus.Summary
	LatencyHistogram    prometheus.Histogram
	PipelineLatency     prometheus.Histogram
	E2ELatency          prometheus.Histogram
	TickRate            prometheus.Gauge
	BytesSent           prometheus.Counter

	// Feed connectivity, fallback, and validation metrics
	FeedConnected       prometheus.Gauge
	FallbackActive      prometheus.Gauge
	FeedReconnects      prometheus.Counter
	FeedAuthFailures    prometheus.Counter
	FeedVsComputedDrift prometheus.Gauge
	FeedAuthStatus      *prometheus.GaugeVec

	// Internal rate calculation counter and total counter
	recentReceivedTicks uint64
	totalReceivedTicks  uint64
}

var (
	dropReasons = []string{"reason"}
	authStatuses = []string{"status"}
)

// New initializes and registers all Prometheus collectors.
func New() *Metrics {
	m := &Metrics{
		ActiveClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "market_data",
			Name:      "active_clients",
			Help:      "Current number of connected WebSocket clients.",
		}),
		TicksReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "market_data",
			Name:      "ticks_received_total",
			Help:      "Total count of incoming ticks received from upstream feed.",
		}),
		TicksBroadcast: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "market_data",
			Name:      "ticks_broadcast_total",
			Help:      "Total count of tick deltas delivered to client queues.",
		}),
		TicksDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "market_data",
			Name:      "ticks_dropped_total",
			Help:      "Total count of dropped ticks segmented by cause.",
		}, dropReasons),
		LatencySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  "market_data",
			Name:       "latency_ms_summary",
			Help:       "End-to-end processing and dispatch latency in milliseconds (p50, p95, p99).",
			Objectives: map[float64]float64{0.5: 0.05, 0.95: 0.01, 0.99: 0.001},
			MaxAge:     time.Minute,
		}),
		LatencyHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "market_data",
			Name:      "latency_ms_bucket",
			Help:      "Histogram of processing and dispatch latency in milliseconds.",
			Buckets:   []float64{0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 35.0, 40.0, 50.0, 75.0, 100.0},
		}),
		PipelineLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "market_data",
			Name:      "pipeline_latency_ms",
			Help:      "Histogram of feed-to-engine pipeline latency in milliseconds.",
			Buckets:   []float64{0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 35.0, 40.0, 50.0, 75.0, 100.0},
		}),
		E2ELatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "market_data",
			Name:      "e2e_latency_ms",
			Help:      "Histogram of end-to-end latency from feed receipt to client broadcast in milliseconds.",
			Buckets:   []float64{5, 10, 20, 30, 40, 50, 75, 100, 200},
		}),
		TickRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "market_data",
			Name:      "tick_rate",
			Help:      "Current incoming tick ingestion rate per second.",
		}),
		BytesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "market_data",
			Name:      "bytes_sent_total",
			Help:      "Total compressed MessagePack bytes transmitted to WebSocket clients.",
		}),
		FeedConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "market_data",
			Name:      "feed_connected",
			Help:      "Whether the live upstream feed connection is active (1) or down (0).",
		}),
		FallbackActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "market_data",
			Name:      "fallback_active",
			Help:      "Whether internal fallback simulation is actively generating market data (1) or inactive (0).",
		}),
		FeedReconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "market_data",
			Name:      "feed_reconnects_total",
			Help:      "Total count of feed reconnection attempts.",
		}),
		FeedAuthFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "market_data",
			Name:      "feed_auth_failures_total",
			Help:      "Total count of feed authentication rejections.",
		}),
		FeedVsComputedDrift: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "market_data",
			Name:      "feed_vs_computed_drift_pct",
			Help:      "Percentage drift between feed-provided aggregates and locally computed rolling window.",
		}),
		FeedAuthStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "market_data",
			Name:      "feed_auth_status",
			Help:      "Status gauge for feed authentication handshake outcome.",
		}, authStatuses),
	}

	collectors := []prometheus.Collector{
		m.ActiveClients,
		m.TicksReceived,
		m.TicksBroadcast,
		m.TicksDropped,
		m.LatencySummary,
		m.LatencyHistogram,
		m.PipelineLatency,
		m.E2ELatency,
		m.TickRate,
		m.BytesSent,
		m.FeedConnected,
		m.FallbackActive,
		m.FeedReconnects,
		m.FeedAuthFailures,
		m.FeedVsComputedDrift,
		m.FeedAuthStatus,
	}

	for _, c := range collectors {
		if err := prometheus.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				panic(err)
			}
		}
	}

	return m
}

// Handler returns the HTTP handler exposing the /metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.Handler()
}

// IncReceived increments received ticks and tracks the rolling tick rate.
func (m *Metrics) IncReceived() {
	m.TicksReceived.Inc()
	atomic.AddUint64(&m.recentReceivedTicks, 1)
	atomic.AddUint64(&m.totalReceivedTicks, 1)
}

// TotalReceived returns the all-time count of ticks received.
func (m *Metrics) TotalReceived() uint64 {
	return atomic.LoadUint64(&m.totalReceivedTicks)
}

// IncBroadcast increments transmitted ticks.
func (m *Metrics) IncBroadcast(count int) {
	m.TicksBroadcast.Add(float64(count))
}

// IncDropped records a dropped tick for the specified reason.
func (m *Metrics) IncDropped(reason string) {
	m.TicksDropped.WithLabelValues(reason).Inc()
}

// ObserveLatency records processing latency in milliseconds.
func (m *Metrics) ObserveLatency(latencyMs float64) {
	if latencyMs < 0 {
		latencyMs = 0
	}
	m.LatencySummary.Observe(latencyMs)
	m.LatencyHistogram.Observe(latencyMs)
}

// ObservePipelineLatency records feed-to-engine pipeline latency in milliseconds.
func (m *Metrics) ObservePipelineLatency(latencyMs float64) {
	if latencyMs < 0 {
		latencyMs = 0
	}
	m.PipelineLatency.Observe(latencyMs)
	m.ObserveLatency(latencyMs)
}

// ObserveE2ELatency records end-to-end latency in milliseconds.
func (m *Metrics) ObserveE2ELatency(latencyMs float64) {
	if latencyMs < 0 {
		latencyMs = 0
	}
	m.E2ELatency.Observe(latencyMs)
}

// AddBytesSent records transmitted payload volume.
func (m *Metrics) AddBytesSent(n int) {
	m.BytesSent.Add(float64(n))
}

// StartRateWorker runs a periodic 1-second ticker to update the current tick rate gauge.
func (m *Metrics) StartRateWorker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := atomic.SwapUint64(&m.recentReceivedTicks, 0)
				m.TickRate.Set(float64(count))
			}
		}
	}()
}
