import React from 'react';
import { ConnectionStatus, FeedStats } from '../types';

interface HeaderProps {
  status: ConnectionStatus;
  stats: FeedStats;
  activeCount: number;
  totalCount: number;
}

export const Header: React.FC<HeaderProps> = ({
  status,
  stats,
  activeCount,
  totalCount,
}) => {
  const getStatusBadge = () => {
    switch (status) {
      case 'CONNECTED':
        return <span className="status-badge status-connected"><span className="pulse-dot"></span> CONNECTED</span>;
      case 'CONNECTING':
        return <span className="status-badge status-connecting"><span className="pulse-dot-yellow"></span> CONNECTING</span>;
      case 'DISCONNECTED':
        return <span className="status-badge status-disconnected"><span className="pulse-dot-red"></span> DISCONNECTED</span>;
      case 'ERROR':
        return <span className="status-badge status-error"><span className="pulse-dot-red"></span> ERROR</span>;
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(2) + ' MB';
  };

  return (
    <header className="app-header">
      <div className="header-brand">
        <div className="brand-logo">⚡</div>
        <div className="brand-text">
          <h1>LiveFXHub Market Data Middleware</h1>
          <p className="subtitle">Real-Time Ingestion Engine • 20ms Delta Batching • MessagePack + Deflate</p>
        </div>
      </div>

      <div className="header-meta">
        <div className="meta-card">
          <span className="meta-label">TRANSPORT STATUS</span>
          <div className="meta-value">{getStatusBadge()}</div>
        </div>

        <div className="meta-card">
          <span className="meta-label">CLIENT TICK RATE</span>
          <div className="meta-value text-accent">
            <strong>{stats.ticksPerSec}</strong> <small>ticks/s</small>
          </div>
        </div>

        <div className="meta-card">
          <span className="meta-label">E2E LATENCY (p50 / p95 / p99)</span>
          <div className="meta-value">
            <strong className={stats.p50Latency < 30 ? 'text-green' : 'text-yellow'}>
              {stats.p50Latency}
            </strong>
            <small> / {stats.p95Latency} / {stats.p99Latency} ms</small>
          </div>
        </div>

        <div className="meta-card">
          <span className="meta-label">SUBSCRIPTIONS</span>
          <div className="meta-value">
            <strong>{activeCount}</strong> <small>/ {totalCount}</small>
          </div>
        </div>

        <div className="meta-card">
          <span className="meta-label">PAYLOAD RECEIVED</span>
          <div className="meta-value">
            <strong>{formatBytes(stats.bytesReceived)}</strong>
            <span className="compression-badge">DEFLATE</span>
          </div>
        </div>
      </div>
    </header>
  );
};
