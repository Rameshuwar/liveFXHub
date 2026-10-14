import React from 'react';
import { MarketPrice } from '../types';

interface PriceTableProps {
  prices: Record<string, MarketPrice>;
  subscribedSymbols: Set<string>;
}

export const PriceTable: React.FC<PriceTableProps> = ({
  prices,
  subscribedSymbols,
}) => {
  const activeList = Array.from(subscribedSymbols).map((sym) => {
    return (
      prices[sym] || {
        symbol: sym,
        buy: 0,
        sell: 0,
        high: 0,
        low: 0,
        changePct: 0,
        timestamp: 0,
        seq: 0,
        latency: 0,
        lastDirection: 'neutral' as const,
        lastUpdated: 0,
      }
    );
  });

  if (activeList.length === 0) {
    return (
      <div className="empty-state">
        <div className="empty-icon">📊</div>
        <h3>No Instruments Subscribed</h3>
        <p>Select one or more symbols from the panel above to begin streaming live market data.</p>
      </div>
    );
  }

  const formatPrice = (symbol: string, val: number): string => {
    if (!val || val === 0) return '—';
    if (symbol.includes('JPY')) return val.toFixed(3);
    if (symbol.includes('BTC') || symbol.includes('ETH')) return val.toFixed(2);
    if (symbol.includes('XAU')) return val.toFixed(2);
    if (symbol.includes('XAG')) return val.toFixed(3);
    if (symbol.includes('SOL')) return val.toFixed(2);
    return val.toFixed(5);
  };

  const formatSpread = (buy: number, sell: number, symbol: string): string => {
    if (!buy || !sell || buy === 0 || sell === 0) return '—';
    const spread = Math.abs(sell - buy);
    if (symbol.includes('JPY')) return (spread * 100).toFixed(1) + ' pips';
    if (symbol.includes('BTC') || symbol.includes('ETH')) return '$' + spread.toFixed(2);
    return (spread * 10000).toFixed(1) + ' pips';
  };

  const formatTime = (ts: number): string => {
    if (!ts || ts === 0) return '—';
    const d = new Date(ts);
    return d.toTimeString().split(' ')[0] + '.' + String(d.getMilliseconds()).padStart(3, '0');
  };

  return (
    <div className="table-container">
      <table className="price-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th className="align-right">Buy (Bid)</th>
            <th className="align-right">Sell (Ask)</th>
            <th className="align-right">Spread</th>
            <th className="align-right">24h High</th>
            <th className="align-right">24h Low</th>
            <th className="align-right">24h % Change</th>
            <th className="align-right">Client Latency</th>
            <th className="align-right">Last Tick Time</th>
          </tr>
        </thead>
        <tbody>
          {activeList.map((item) => {
            const isUp = item.lastDirection === 'up';
            const isDown = item.lastDirection === 'down';
            const changePositive = item.changePct >= 0;
            const flashClass = isUp ? 'flash-green' : isDown ? 'flash-red' : '';

            return (
              <tr key={item.symbol} className={`table-row ${flashClass}`}>
                <td className="symbol-cell">
                  <span className="symbol-name">{item.symbol}</span>
                  <span className="seq-badge">#{item.seq}</span>
                </td>
                <td className={`align-right price-cell buy-cell ${isUp ? 'text-green' : isDown ? 'text-red' : ''}`}>
                  {formatPrice(item.symbol, item.buy)}
                  <span className="tick-arrow">{isUp ? '▲' : isDown ? '▼' : ''}</span>
                </td>
                <td className={`align-right price-cell sell-cell ${isUp ? 'text-green' : isDown ? 'text-red' : ''}`}>
                  {formatPrice(item.symbol, item.sell)}
                </td>
                <td className="align-right spread-cell">
                  {formatSpread(item.buy, item.sell, item.symbol)}
                </td>
                <td className="align-right high-cell">
                  {formatPrice(item.symbol, item.high)}
                </td>
                <td className="align-right low-cell">
                  {formatPrice(item.symbol, item.low)}
                </td>
                <td className="align-right change-cell">
                  <span className={`change-pill ${changePositive ? 'pill-green' : 'pill-red'}`}>
                    {changePositive ? '+' : ''}
                    {item.changePct.toFixed(2)}%
                  </span>
                </td>
                <td className="align-right latency-cell">
                  <span className={`latency-tag ${item.latency < 25 ? 'latency-low' : item.latency < 80 ? 'latency-med' : 'latency-high'}`}>
                    {item.latency} ms
                  </span>
                </td>
                <td className="align-right time-cell">
                  {formatTime(item.timestamp)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};
