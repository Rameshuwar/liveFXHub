export interface RawDeltaTick {
  s: string;       // Symbol
  seq: number;     // Sequence number
  b?: number;      // Buy / Bid (delta)
  a?: number;      // Sell / Ask (delta)
  h?: number;      // 24h High (delta)
  l?: number;      // 24h Low (delta)
  c?: number;      // 24h % Change (delta)
  t: number;       // Milliseconds timestamp
}

export interface BatchEnvelope {
  type: string;
  seq: number;
  t: number;
  sent_at?: number;
  d: RawDeltaTick[];
}

export interface MarketPrice {
  symbol: string;
  buy: number;
  sell: number;
  high: number;
  low: number;
  changePct: number;
  timestamp: number;
  seq: number;
  latency: number;
  lastDirection: 'up' | 'down' | 'neutral';
  lastUpdated: number;
}

export type ConnectionStatus = 'CONNECTING' | 'CONNECTED' | 'DISCONNECTED' | 'ERROR';

export interface FeedStats {
  ticksPerSec: number;
  avgLatency: number;
  p50Latency: number;
  p95Latency: number;
  p99Latency: number;
  lastSeq: number;
  totalBatches: number;
  bytesReceived: number;
}

export const ALL_SYMBOLS = [
  'AUDUSD', 'AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'AUDSGD', 'CADCHF', 'CADJPY', 'CHFJPY', 'CHFSGD',
  'EURAUD', 'EURCAD', 'EURCHF', 'EURGBP', 'EURJPY', 'EURNZD', 'EURUSD', 'GBPAUD', 'GBPCAD', 'GBPCHF',
  'GBPJPY', 'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDCNH',
  'USDJPY', 'USDMXN', 'USDNOK', 'USDPLN', 'USDSEK', 'USDUSD', 'USDTRY', 'USDZAR', 'EURTRY', 'GBPTRY',
  'NOKJPY', 'SEKJPY', 'SGDJPY', 'ZARJPY', 'XAUUSD', 'XAGUSD', 'BTCUSD', 'ETHUSD', 'BNBUSD', 'SOLUSD',
  'XRPUSD', 'ADAUSD', 'DOGUSD', 'DOTUSD', 'LTCUSD', 'BCHUSD', 'XLMUSD', 'TRXUSD', 'UNIUSD', 'FILUSD',
  'AVXUSD',
];
