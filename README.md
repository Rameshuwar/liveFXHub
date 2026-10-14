# Real-Time Market Data Middleware System

A high-performance, low-latency financial market data middleware built with **Go 1.22**, **Redis 7**, and **React + TypeScript + Vite**. 

Ingests real-time market prices from **LiveFXHub**, performs deduplication, corrects out-of-order packet arrival via a 50ms jitter buffer, maintains sliding 24-hour rolling high/low/%change metrics, persists state to Redis AOF, and broadcasts delta-encoded **MessagePack** batches to WebSocket clients every 20ms with **permessage-deflate** compression.

---

## ⚡ Quick Start

### 1. Setup & Credentials
Copy the example environment file and configure your credentials:
```bash
cp .env.example .env
```
Fill in your `FEED_API_KEY` and `FEED_SECRET` in `.env`.

> [!WARNING]
> Rotate keys if previously committed. Never commit `.env` or production credentials to version control.

### 2. Launch with Single Command
From the project root:
```bash
docker compose up --build
```
*(or `docker-compose up --build`)*

This spins up:
- **Redis 7 Alpine**: In-memory state store with AOF persistence enabled (`port 6379`).
- **Backend Middleware (Go 1.22)**: Feed ingestion, 50ms reorder buffer, 24h metrics engine, and WebSocket server (`port 8080`).
- **Frontend UI (React + TypeScript)**: Dark-mode trading terminal UI (`port 3000`).

---

## 🌐 Endpoints & Ports

| Service | Port | Endpoint / URL | Description |
| :--- | :--- | :--- | :--- |
| **Frontend Web App** | `3000` | [http://localhost:3000](http://localhost:3000) | Live trading terminal with real-time price updates & latency monitoring |
| **Client WebSocket** | `8080` | `ws://localhost:8080/ws` | Client streaming endpoint (20ms MessagePack batches + permessage-deflate) |
| **Prometheus Metrics**| `8080` | [http://localhost:8080/metrics](http://localhost:8080/metrics) | Latency p50/p95/p99, tick rate, client count, drop rate counters |
| **OHLC Candles API** | `8080` | [http://localhost:8080/api/candles?symbol=EURUSD&interval=1m](http://localhost:8080/api/candles?symbol=EURUSD&interval=1m) | 1-minute and 5-minute aggregated OHLC candle bars |
| **Latest Snapshots** | `8080` | [http://localhost:8080/api/snapshots](http://localhost:8080/api/snapshots) | Instant snapshot of 24h high/low/%change for all symbols |
| **Health Check** | `8080` | [http://localhost:8080/health](http://localhost:8080/health) | System health probe |
| **Redis Store** | `6379` | `localhost:6379` | Redis 7 persistence layer |

---

## 📦 Project Structure

```
liveFXHub/
├── docker-compose.yml
├── README.md
├── backend/
│   ├── go.mod
│   ├── go.sum
│   ├── Dockerfile
│   ├── cmd/server/main.go
│   └── internal/
│       ├── config/        (Feed credentials, symbols, timeouts)
│       ├── feed/          (LiveFXHub WS client, auth, reconnect & fallback)
│       ├── engine/        (50ms buffer, timestamp dedupe, 24h rolling window, OHLC)
│       ├── store/         (Redis persistence, async flusher & boot restore)
│       ├── wsserver/      (Client WS server, 20ms batch flusher, rate limiter)
│       ├── codec/         (MessagePack serialization, delta encoding)
│       └── metrics/       (Prometheus latency histograms, tick rate gauges)
├── frontend/
│   ├── Dockerfile
│   ├── package.json
│   ├── vite.config.ts
│   ├── nginx.conf
│   └── src/
│       ├── App.tsx
│       ├── App.css        (Plain dark-theme CSS, no Tailwind, tabular-nums)
│       ├── types.ts
│       ├── hooks/useMarketFeed.ts
│       └── components/
│           ├── PriceTable.tsx
│           ├── Header.tsx
│           └── SymbolSelector.tsx
└── docs/
    ├── ARCHITECTURE.md    (ASCII diagram & component responsibilities)
    ├── DESIGN.md          (Why MessagePack, Redis, 20ms batching, trade-offs)
    └── CHALLENGES.md      (Dedupe, out-of-order buffer, reconnect, compression math)
```

---

## ⚙️ Configuration Reference

All settings can be configured via environment variables:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `FEED_URL` | `wss://v3-prod.livefxhub.com/ws/algo` | LiveFXHub WebSocket endpoint |
| `FEED_API_KEY` | *(from `.env`)* | LiveFXHub API Key |
| `FEED_SECRET` | *(from `.env`)* | LiveFXHub API Secret |
| `FEED_INSECURE_SKIP_VERIFY` | `false` | Allow insecure TLS certificates (local dev only) |
| `FEED_SYMBOLS` | `AUDUSD,EURUSD,BTCUSD,XAUUSD,...` | Comma-delimited list of subscribed instruments |
| `REDIS_ADDR` | `redis:6379` | Host:Port of Redis 7 |
| `PORT` | `8080` | Port for Go HTTP & WebSocket server |
| `BATCH_INTERVAL` | `20ms` | Client WebSocket batch emission cadence |
| `OUT_OF_ORDER_BUFFER` | `50ms` | Duration to buffer ticks for time reordering |
| `WINDOW_DURATION` | `24h` | Rolling window length for high/low/%change metrics |
| `CLIENT_RATE_LIMIT`| `100` | Inbound client message rate limit (msgs/sec) |
| `SIMULATE_FALLBACK`| `true` | Runs fallback generator when remote API key has active session conflict |

---

## 🔬 Testing & Verification

### Verify Prometheus Metrics
```bash
curl -s http://localhost:8080/metrics | grep market_data
```
Expected output includes:
- `market_data_active_clients`
- `market_data_ticks_received_total`
- `market_data_ticks_broadcast_total`
- `market_data_tick_rate`
- `market_data_latency_ms_summary{quantile="0.5"}`
- `market_data_latency_ms_summary{quantile="0.95"}`
- `market_data_latency_ms_summary{quantile="0.99"}`

### Verify Redis State & AOF Persistence
Connect to the running Redis container:
```bash
docker exec -it market-data-redis redis-cli
```
Inspect persisted keys:
```text
127.0.0.1:6379> KEYS *
1) "tick:EURUSD:latest"
2) "window:EURUSD"
3) "tick:BTCUSD:latest"
4) "window:BTCUSD"

127.0.0.1:6379> GET tick:EURUSD:latest
127.0.0.1:6379> ZCARD window:EURUSD
```

---

## 📚 Technical Documentation
Detailed technical deep dives are located in the `docs/` folder:
- [**Architecture & Pipeline Flow**](docs/ARCHITECTURE.md)
- [**Design Decisions & Trade-Offs**](docs/DESIGN.md)
- [**Technical Challenges & Mathematical Proofs**](docs/CHALLENGES.md)
