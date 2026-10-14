import { useState, useEffect, useRef, useCallback } from 'react';
import { decode } from '@msgpack/msgpack';
import {
  MarketPrice,
  BatchEnvelope,
  ConnectionStatus,
  FeedStats,
  ALL_SYMBOLS,
} from '../types';

interface UseMarketFeedOptions {
  url?: string;
  initialSymbols?: string[];
}

export function useMarketFeed({
  url,
  initialSymbols = ALL_SYMBOLS,
}: UseMarketFeedOptions = {}) {
  const [status, setStatus] = useState<ConnectionStatus>('CONNECTING');
  const [prices, setPrices] = useState<Record<string, MarketPrice>>({});
  const [symbols, setSymbols] = useState<string[]>(ALL_SYMBOLS);
  const [subscribedSymbols, setSubscribedSymbols] = useState<Set<string>>(
    new Set(initialSymbols)
  );
  const [stats, setStats] = useState<FeedStats>({
    ticksPerSec: 0,
    avgLatency: 0,
    p50Latency: 0,
    p95Latency: 0,
    p99Latency: 0,
    lastSeq: 0,
    totalBatches: 0,
    bytesReceived: 0,
  });

  const wsRef = useRef<WebSocket | null>(null);
  const pricesRef = useRef<Record<string, MarketPrice>>({});
  const tickCountRef = useRef<number>(0);
  const latencySumRef = useRef<number>(0);
  const latencyCountRef = useRef<number>(0);
  const recentLatenciesRef = useRef<number[]>([]);
  const totalBatchesRef = useRef<number>(0);
  const totalBytesRef = useRef<number>(0);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const reconnectDelayRef = useRef<number>(1000);

  // Fetch symbol list dynamically from backend with offline fallback
  useEffect(() => {
    let isMounted = true;
    const apiUrl = (() => {
      const envApi = typeof import.meta !== 'undefined' && (import.meta as any).env ? (import.meta as any).env.VITE_API_URL : undefined;
      if (envApi) return `${envApi}/api/symbols`;
      const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
      const host = window.location.port === '3000' ? `${window.location.hostname}:8080` : window.location.host;
      return `${protocol}//${host}/api/symbols`;
    })();

    fetch(apiUrl)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: string[]) => {
        if (isMounted && Array.isArray(data) && data.length > 0) {
          setSymbols(data);
        }
      })
      .catch((err) => {
        console.warn('Failed to fetch /api/symbols, falling back to ALL_SYMBOLS:', err);
        if (isMounted) {
          setSymbols(ALL_SYMBOLS);
        }
      });

    return () => {
      isMounted = false;
    };
  }, []);

  // Determine WebSocket target URL
  const getWSUrl = useCallback(() => {
    if (url) return url;
    const envWs = typeof import.meta !== 'undefined' && (import.meta as any).env ? (import.meta as any).env.VITE_WS_URL : undefined;
    if (envWs) return envWs;
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}/ws`;
  }, [url]);

  // Connect and manage WebSocket lifecycle
  useEffect(() => {
    let isMounted = true;

    function connect() {
      if (wsRef.current) {
        wsRef.current.close();
      }

      const targetUrl = getWSUrl();
      console.log(`Connecting to WebSocket: ${targetUrl}`);
      setStatus('CONNECTING');

      try {
        const ws = new WebSocket(targetUrl);
        ws.binaryType = 'arraybuffer';
        wsRef.current = ws;

        ws.onopen = () => {
          if (!isMounted) return;
          console.log('Connected to Market Data WebSocket server');
          setStatus('CONNECTED');
          reconnectDelayRef.current = 1000;

          // Send subscription request for current active symbols
          const symbols = Array.from(subscribedSymbols);
          if (symbols.length > 0) {
            ws.send(JSON.stringify({ action: 'subscribe', symbols }));
          }
        };

        ws.onmessage = (event: MessageEvent) => {
          if (!isMounted) return;

          let envelope: BatchEnvelope;
          let byteLen = 0;

          if (event.data instanceof ArrayBuffer) {
            byteLen = event.data.byteLength;
            totalBytesRef.current += byteLen;
            try {
              const u8 = new Uint8Array(event.data);
              envelope = decode(u8) as BatchEnvelope;
            } catch (err) {
              console.error('Failed to decode MessagePack payload:', err);
              return;
            }
          } else if (typeof event.data === 'string') {
            byteLen = event.data.length;
            totalBytesRef.current += byteLen;
            try {
              envelope = JSON.parse(event.data);
            } catch {
              return;
            }
          } else {
            return;
          }

          if (!envelope || !envelope.d || !Array.isArray(envelope.d)) {
            return;
          }

          totalBatchesRef.current += 1;
          const now = Date.now();
          const batchE2ELatency = envelope.t > 0 ? Math.max(0, now - envelope.t) : 0;
          recentLatenciesRef.current.push(batchE2ELatency);
          const nextPrices = { ...pricesRef.current };

          for (const delta of envelope.d) {
            const sym = delta.s;
            if (!sym) continue;

            const existing = nextPrices[sym] || {
              symbol: sym,
              buy: 0,
              sell: 0,
              high: 0,
              low: 0,
              changePct: 0,
              timestamp: delta.t || now,
              seq: delta.seq || 0,
              latency: 0,
              lastDirection: 'neutral' as const,
              lastUpdated: now,
            };

            const oldBuy = existing.buy;
            const newBuy = delta.b !== undefined && delta.b !== null ? delta.b : existing.buy;
            const newSell = delta.a !== undefined && delta.a !== null ? delta.a : existing.sell;
            const newHigh = delta.h !== undefined && delta.h !== null ? delta.h : existing.high;
            const newLow = delta.l !== undefined && delta.l !== null ? delta.l : existing.low;
            const newChange = delta.c !== undefined && delta.c !== null ? delta.c : existing.changePct;

            let direction: 'up' | 'down' | 'neutral' = existing.lastDirection;
            if (delta.b !== undefined && delta.b !== null && oldBuy > 0) {
              if (newBuy > oldBuy) direction = 'up';
              else if (newBuy < oldBuy) direction = 'down';
            }

            // Client-side measured latency (time from server timestamp to client unpack)
            const latency = delta.t > 0 ? Math.max(0, now - delta.t) : 0;
            latencySumRef.current += latency;
            latencyCountRef.current += 1;
            tickCountRef.current += 1;

            nextPrices[sym] = {
              symbol: sym,
              buy: newBuy,
              sell: newSell,
              high: newHigh,
              low: newLow,
              changePct: newChange,
              timestamp: delta.t || existing.timestamp,
              seq: delta.seq || existing.seq,
              latency: latency,
              lastDirection: direction,
              lastUpdated: now,
            };
          }

          pricesRef.current = nextPrices;
          setPrices(nextPrices);
        };

        ws.onerror = (err) => {
          console.warn('WebSocket error encountered:', err);
          if (isMounted) setStatus('ERROR');
        };

        ws.onclose = () => {
          if (!isMounted) return;
          console.warn('WebSocket connection closed. Scheduling reconnect...');
          setStatus('DISCONNECTED');
          wsRef.current = null;

          // Exponential backoff reconnect
          const delay = reconnectDelayRef.current;
          reconnectDelayRef.current = Math.min(delay * 1.5, 10000);
          reconnectTimeoutRef.current = window.setTimeout(() => {
            if (isMounted) connect();
          }, delay);
        };
      } catch (err) {
        console.error('WebSocket instantiation error:', err);
        setStatus('ERROR');
      }
    }

    connect();

    // 1-second rolling stats timer
    const statsInterval = setInterval(() => {
      const ticks = tickCountRef.current;
      tickCountRef.current = 0;

      const avgLat =
        latencyCountRef.current > 0
          ? Math.round(latencySumRef.current / latencyCountRef.current)
          : 0;
      latencySumRef.current = 0;
      latencyCountRef.current = 0;

      const lats = recentLatenciesRef.current;
      recentLatenciesRef.current = [];
      let p50 = 0, p95 = 0, p99 = 0;
      if (lats.length > 0) {
        lats.sort((a, b) => a - b);
        p50 = lats[Math.floor(lats.length * 0.50)];
        p95 = lats[Math.floor(lats.length * 0.95)];
        p99 = lats[Math.floor(lats.length * 0.99)];
      }

      setStats({
        ticksPerSec: ticks,
        avgLatency: avgLat,
        p50Latency: p50,
        p95Latency: p95,
        p99Latency: p99,
        lastSeq: Object.values(pricesRef.current).reduce(
          (max, p) => Math.max(max, p.seq),
          0
        ),
        totalBatches: totalBatchesRef.current,
        bytesReceived: totalBytesRef.current,
      });
    }, 1000);

    return () => {
      isMounted = false;
      clearInterval(statsInterval);
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, [getWSUrl]);

  // Subscribe / unsubscribe action
  const toggleSymbol = useCallback((symbol: string) => {
    setSubscribedSymbols((prev) => {
      const next = new Set(prev);
      const isSubscribing = !next.has(symbol);

      if (isSubscribing) {
        next.add(symbol);
      } else {
        next.delete(symbol);
      }

      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        const action = isSubscribing ? 'subscribe' : 'unsubscribe';
        wsRef.current.send(JSON.stringify({ action, symbols: [symbol] }));
      }

      return next;
    });
  }, []);

  const selectAll = useCallback(() => {
    const all = new Set(symbols);
    setSubscribedSymbols(all);
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(
        JSON.stringify({ action: 'subscribe', symbols })
      );
    }
  }, [symbols]);

  const deselectAll = useCallback(() => {
    const syms = Array.from(subscribedSymbols);
    setSubscribedSymbols(new Set());
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ action: 'unsubscribe', symbols: syms }));
    }
  }, [subscribedSymbols]);

  return {
    status,
    prices,
    subscribedSymbols,
    stats,
    toggleSymbol,
    selectAll,
    deselectAll,
    symbols,
  };
}
