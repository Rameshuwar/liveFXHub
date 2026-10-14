import React from 'react';
import { ALL_SYMBOLS } from '../types';

interface SymbolSelectorProps {
  symbols?: string[];
  subscribedSymbols: Set<string>;
  onToggle: (symbol: string) => void;
  onSelectAll: () => void;
  onDeselectAll: () => void;
}

export const SymbolSelector: React.FC<SymbolSelectorProps> = ({
  symbols = ALL_SYMBOLS,
  subscribedSymbols,
  onToggle,
  onSelectAll,
  onDeselectAll,
}) => {
  const getCategory = (sym: string): string => {
    if (
      sym.startsWith('BTC') || sym.startsWith('ETH') || sym.startsWith('SOL') ||
      sym.startsWith('BNB') || sym.startsWith('XRP') || sym.startsWith('ADA') ||
      sym.startsWith('DOG') || sym.startsWith('DOT') || sym.startsWith('LTC') ||
      sym.startsWith('BCH') || sym.startsWith('XLM') || sym.startsWith('TRX') ||
      sym.startsWith('UNI') || sym.startsWith('FIL') || sym.startsWith('AVX')
    ) return 'CRYPTO';
    if (sym.startsWith('XAU') || sym.startsWith('XAG')) return 'METAL';
    return 'FOREX';
  };

  return (
    <div className="selector-panel">
      <div className="selector-header">
        <div className="selector-title">
          <h3>Market Subscriptions</h3>
          <span className="selector-hint">Toggle symbols to dynamically manage backend client subscriptions</span>
        </div>
        <div className="selector-actions">
          <button className="btn btn-secondary" onClick={onSelectAll}>
            Select All
          </button>
          <button className="btn btn-outline" onClick={onDeselectAll}>
            Deselect All
          </button>
        </div>
      </div>

      <div className="symbol-chips">
        {symbols.map((sym) => {
          const isSelected = subscribedSymbols.has(sym);
          const category = getCategory(sym);

          return (
            <label
              key={sym}
              className={`symbol-chip ${isSelected ? 'chip-active' : ''}`}
            >
              <input
                type="checkbox"
                checked={isSelected}
                onChange={() => onToggle(sym)}
                className="chip-checkbox"
              />
              <span className="chip-label">{sym}</span>
              <span className={`category-tag tag-${category.toLowerCase()}`}>
                {category}
              </span>
            </label>
          );
        })}
      </div>
    </div>
  );
};
