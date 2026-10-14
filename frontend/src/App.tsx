import React, { useState } from 'react';
import { useMarketFeed } from './hooks/useMarketFeed';
import { Header } from './components/Header';
import { SymbolSelector } from './components/SymbolSelector';
import { PriceTable } from './components/PriceTable';
import './App.css';

export const App: React.FC = () => {
  const {
    status,
    prices,
    subscribedSymbols,
    stats,
    toggleSymbol,
    selectAll,
    deselectAll,
    symbols,
  } = useMarketFeed();

  const [showDiagnostics, setShowDiagnostics] = useState(false);

  return (
    <div className="app-layout">
      <Header
        status={status}
        stats={stats}
        activeCount={subscribedSymbols.size}
        totalCount={symbols.length}
      />

      <main className="main-content">
        <SymbolSelector
          symbols={symbols}
          subscribedSymbols={subscribedSymbols}
          onToggle={toggleSymbol}
          onSelectAll={selectAll}
          onDeselectAll={deselectAll}
        />

        <div className="section-toolbar">
          <div className="toolbar-title">
            <h2>Real-Time Streaming Quotes</h2>
            <span className="live-indicator">
              <span className="live-circle"></span> LIVE FEED
            </span>
          </div>

          <div className="toolbar-actions">
            <button
              className="btn btn-diagnostic"
              onClick={() => setShowDiagnostics(!showDiagnostics)}
            >
              {showDiagnostics ? 'Hide Pipeline Architecture' : 'View Pipeline Architecture'}
            </button>
            <a
              href="http://localhost:8080/metrics"
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-outline"
            >
              Prometheus Metrics ↗
            </a>
          </div>
        </div>

        {showDiagnostics && (
          <div className="diagnostics-panel">
            <div className="diag-header">
              <h4>System Architecture & Flow Guarantees</h4>
              <button
                className="close-btn"
                onClick={() => setShowDiagnostics(false)}
              >
                ×
              </button>
            </div>
            <div className="diag-grid">
              <div className="diag-card">
                <h5>1. Ingestion & Deduplication</h5>
                <p>
                  Incoming ticks from LiveFXHub are deduplicated against per-symbol
                  timestamp sets and pushed into a 100ms jitter buffer (min-heap)
                  to correct out-of-order packet arrival.
                </p>
              </div>
              <div className="diag-card">
                <h5>2. Rolling 24h Window & State</h5>
                <p>
                  High, Low, and % Change are computed across a dynamic 24-hour sliding
                  window in memory and asynchronously persisted to Redis 7 (AOF enabled).
                </p>
              </div>
              <div className="diag-card">
                <h5>3. 20ms Delta Batching</h5>
                <p>
                  Ticks for subscribed symbols are buffered per-client into 20ms batches.
                  Only modified price metrics are delta-encoded and compressed via
                  MessagePack and permessage-deflate.
                </p>
              </div>
            </div>
          </div>
        )}

        <PriceTable
          prices={prices}
          subscribedSymbols={subscribedSymbols}
        />
      </main>

      <footer className="app-footer">
        <div className="footer-left">
          <span>Middleware Engine: <strong>Go 1.22</strong></span>
          <span>Transport: <strong>Gorilla WebSocket + Permessage-Deflate</strong></span>
          <span>Codec: <strong>MessagePack (Delta Encoded)</strong></span>
          <span>Persistence: <strong>Redis 7 (AOF)</strong></span>
        </div>
        <div className="footer-right">
          <span>Batch Cadence: <strong>20ms</strong></span>
          <span>Client Sequence: <strong>#{stats.lastSeq}</strong></span>
        </div>
      </footer>
    </div>
  );
};

export default App;
