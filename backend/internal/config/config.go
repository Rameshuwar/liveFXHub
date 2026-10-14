package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration parameters for the market data middleware.
type Config struct {
	// Feed configuration
	FeedURL                string
	FeedAPIKey             string
	FeedSecret             string
	Symbols                []string
	SimulateFallback       bool
	FeedInsecureSkipVerify bool

	// Engine configuration
	OutOfOrderBuffer time.Duration
	WindowDuration   time.Duration
	DedupeCacheSize  int

	// Store configuration
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	RedisFlushInterval time.Duration
	RedisBatchSize     int

	// Server configuration
	HTTPPort           string
	BatchInterval      time.Duration
	ClientRateLimit    int // max msgs per second per client
	MaxClientQueueSize int
}

const (
	DefaultFeedURL = "wss://v3-prod.livefxhub.com/ws/algo"
)

// DefaultSymbols contains exactly the 61 required instruments specified in the assignment.
var DefaultSymbols = []string{
	"AUDUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDSGD", "CADCHF", "CADJPY", "CHFJPY", "CHFSGD",
	"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF",
	"GBPJPY", "GBPNZD", "GBPUSD", "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD", "USDCAD", "USDCHF", "USDCNH",
	"USDJPY", "USDMXN", "USDNOK", "USDPLN", "USDSEK", "USDUSD", "USDTRY", "USDZAR", "EURTRY", "GBPTRY",
	"NOKJPY", "SEKJPY", "SGDJPY", "ZARJPY", "XAUUSD", "XAGUSD", "BTCUSD", "ETHUSD", "BNBUSD", "SOLUSD",
	"XRPUSD", "ADAUSD", "DOGUSD", "DOTUSD", "LTCUSD", "BCHUSD", "XLMUSD", "TRXUSD", "UNIUSD", "FILUSD",
	"AVXUSD",
}

// Load reads configuration from environment variables or falls back to defaults.
func Load() *Config {
	cfg := &Config{
		FeedURL:                getEnv("FEED_URL", DefaultFeedURL),
		FeedAPIKey:             getEnv("FEED_API_KEY", ""),
		FeedSecret:             getEnv("FEED_SECRET", ""),
		Symbols:                getEnvSlice("FEED_SYMBOLS", DefaultSymbols),
		SimulateFallback:       getEnvBool("SIMULATE_FALLBACK", true),
		FeedInsecureSkipVerify: getEnvBool("FEED_INSECURE_SKIP_VERIFY", false),

		OutOfOrderBuffer: getEnvDuration("OUT_OF_ORDER_BUFFER", 50*time.Millisecond),
		WindowDuration:   getEnvDuration("WINDOW_DURATION", 24*time.Hour),
		DedupeCacheSize:  getEnvInt("DEDUPE_CACHE_SIZE", 10000),

		RedisAddr:          getEnv("REDIS_ADDR", "redis:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvInt("REDIS_DB", 0),
		RedisFlushInterval: getEnvDuration("REDIS_FLUSH_INTERVAL", 25*time.Millisecond),
		RedisBatchSize:     getEnvInt("REDIS_BATCH_SIZE", 200),

		HTTPPort:           getEnv("PORT", "8080"),
		BatchInterval:      getEnvDuration("BATCH_INTERVAL", 20*time.Millisecond),
		ClientRateLimit:    getEnvInt("CLIENT_RATE_LIMIT", 100),
		MaxClientQueueSize: getEnvInt("MAX_CLIENT_QUEUE_SIZE", 512),
	}

	return cfg
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvSlice(key string, defaultVal []string) []string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	parts := strings.Split(val, ",")
	var result []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return defaultVal
	}
	return result
}

func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return i
}

func getEnvBool(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return defaultVal
	}
	return b
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return defaultVal
	}
	return d
}
