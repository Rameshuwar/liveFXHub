# Submission Checklist

## Repository
- [ ] GitHub repo is public (or shared with interviewer)
- [ ] LICENSE file added
- [ ] .gitignore protects .env, bin/, node_modules/, dist/
- [ ] .env.example in repo (no real credentials)
- [ ] .env NOT in repo
- [ ] Commit history is clean (no "wip", "asdf")
- [ ] README.md at repo root

## Documentation
- [ ] README has setup, endpoints, config
- [ ] docs/ARCHITECTURE.md (diagram + Mermaid)
- [ ] docs/DESIGN.md (trade-offs)
- [ ] docs/CHALLENGES.md (problems + solutions)
- [ ] docs/INTERVIEW_NOTES.md (optional)

## Screenshots
- [ ] docs/screenshot-ui.png (frontend live)
- [ ] docs/screenshot-restore.png (boot restore logs)
- [ ] docs/screenshot-metrics.png (Prometheus)
- [ ] Screenshots embedded in README

## Code Quality
- [ ] `go build ./...` passes
- [ ] `go test ./...` passes
- [ ] `go vet ./...` passes
- [ ] `npm run build` passes
- [ ] GitHub Actions CI is green

## Deployment
- [ ] `docker compose down && docker compose up --build` works
- [ ] Fresh clone → `cp .env.example .env` → fill credentials → `docker compose up --build` works
- [ ] No `version` warning in docker-compose.yml
- [ ] All 3 containers healthy

## Functional
- [ ] Feed authenticates with LiveFXHub
- [ ] 61 symbols subscribed
- [ ] Frontend shows live prices
- [ ] /api/symbols returns 61
- [ ] /api/snapshots returns 61 symbols
- [ ] /api/candles returns OHLC
- [ ] /metrics has feed_connected=1
- [ ] Boot restore works (restored_symbols=61)

## Known Gaps (Declare openly)
- [ ] Live/fallback badge in UI
- [ ] OHLCAggregator.maxCandles 200 → 1440
- [ ] Tests for wsserver and store
- [ ] App.tsx "100ms" text → "50ms"