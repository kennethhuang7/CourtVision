import { X } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { TrendFilters, TrendType } from '@/types/trends';
import type { StatType } from '@/types/nba';

interface TrendsFiltersProps {
  filters: TrendFilters;
  updateFilter: <K extends keyof TrendFilters>(key: K, value: TrendFilters[K]) => void;
  onClose: () => void;
}

export function TrendsFilters({ filters, updateFilter, onClose }: TrendsFiltersProps) {
  const statTypes: { value: StatType | 'all'; label: string }[] = [
    { value: 'all', label: 'All Stats' },
    { value: 'points', label: 'Points' },
    { value: 'rebounds', label: 'Rebounds' },
    { value: 'assists', label: 'Assists' },
    { value: 'steals', label: 'Steals' },
    { value: 'blocks', label: 'Blocks' },
    { value: 'turnovers', label: 'Turnovers' },
    { value: 'threePointersMade', label: '3-Pointers' },
  ];

  const trendTypes: { value: TrendType; label: string }[] = [
    { value: 'recent-form', label: 'Recent Form' },
    { value: 'h2h', label: 'Head to Head' },
    { value: 'home-away', label: 'Home/Away Splits' },
  ];

  const toggleTrendType = (type: TrendType) => {
    const current = filters.trendTypes || [];
    const newTypes = current.includes(type)
      ? current.filter(t => t !== type)
      : [...current, type];
    updateFilter('trendTypes', newTypes);
  };

  const updateCustomModifier = (statType: string, value: number) => {
    const current = filters.customModifiers || {};
    updateFilter('customModifiers', {
      ...current,
      [statType]: value,
    });
  };

  return (
    <div className="bg-zinc-900 border-b border-zinc-800 p-6">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-lg font-semibold text-white">Filters</h3>
          <button
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
          >
            <X className="h-5 w-5 text-zinc-400" />
          </button>
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-2">
                Stat Type
              </label>
              <select
                value={filters.statType}
                onChange={(e) => updateFilter('statType', e.target.value as StatType | 'all')}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {statTypes.map(type => (
                  <option key={type.value} value={type.value}>
                    {type.label}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-2">
                Direction
              </label>
              <div className="flex gap-2">
                {(['over', 'under', 'both'] as const).map(direction => (
                  <button
                    key={direction}
                    onClick={() => updateFilter('overUnder', direction)}
                    className={cn(
                      'flex-1 px-2 py-2 rounded-lg text-sm font-medium transition-all',
                      filters.overUnder === direction
                        ? 'bg-blue-600 text-white'
                        : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                    )}
                  >
                    {direction === 'over' ? 'Over' : direction === 'under' ? 'Under' : 'Both'}
                  </button>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-2">
                Min Streak
              </label>
              <select
                value={filters.minStreak}
                onChange={(e) => updateFilter('minStreak', parseInt(e.target.value) as 3 | 4 | 5)}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value={3}>3+ games</option>
                <option value={4}>4+ games</option>
                <option value={5}>5+ games</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-2">
                Player Search
              </label>
              <input
                type="text"
                value={filters.playerSearch ?? ''}
                onChange={(e) => updateFilter('playerSearch', e.target.value || undefined)}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                placeholder="Search..."
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-2">
                Line Method
              </label>
              <select
                value={filters.lineMethod}
                onChange={(e) => updateFilter('lineMethod', e.target.value as 'player-average' | 'ai-prediction')}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="player-average">Player Average</option>
                <option value="ai-prediction">AI Prediction</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-2">
                Line Adjustment
              </label>
              <select
                value={filters.lineAdjustment}
                onChange={(e) => updateFilter('lineAdjustment', e.target.value as 'standard' | 'favorable' | 'custom')}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="standard">Standard</option>
                <option value="favorable">Favorable</option>
                <option value="custom">Custom</option>
              </select>
            </div>

            <div className="flex items-end pb-2">
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={filters.requireAiAgreement ?? false}
                  onChange={(e) => updateFilter('requireAiAgreement', e.target.checked)}
                  className="w-4 h-4 bg-zinc-800 border-zinc-700 rounded text-blue-600 focus:ring-2 focus:ring-blue-500"
                />
                <span className="text-sm font-medium text-zinc-400">AI Agreement</span>
              </label>
            </div>
          </div>
        </div>

        {filters.lineAdjustment === 'custom' && (
          <div className="mt-6">
            <label className="block text-sm font-medium text-zinc-400 mb-3">
              Custom Adjustments (buffer amount for each stat)
            </label>
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
              {statTypes.slice(1).map(type => ( 
                <div key={type.value}>
                  <label className="block text-xs text-zinc-500 mb-1">
                    {type.label}
                  </label>
                  <input
                    type="number"
                    step="0.5"
                    value={filters.customModifiers?.[type.value] ?? 0}
                    onChange={(e) => updateCustomModifier(type.value, parseFloat(e.target.value) || 0)}
                    className="w-full bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="0.0"
                  />
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="mt-6">
          <label className="block text-sm font-medium text-zinc-400 mb-3">
            Trend Types (select multiple or none for all)
          </label>
          <div className="flex flex-wrap gap-2">
            {trendTypes.map(type => {
              const isSelected = filters.trendTypes?.includes(type.value) ?? false;
              return (
                <button
                  key={type.value}
                  onClick={() => toggleTrendType(type.value)}
                  className={cn(
                    'px-4 py-2 rounded-lg font-medium transition-all',
                    isSelected
                      ? 'bg-blue-600 text-white'
                      : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                  )}
                >
                  {type.label}
                </button>
              );
            })}
          </div>
          <p className="text-xs text-zinc-500 mt-2">
            {filters.trendTypes?.length === 0 || !filters.trendTypes
              ? 'Showing all trend types'
              : `Showing ${filters.trendTypes.length} trend type(s)`}
          </p>
        </div>
      </div>
    </div>
  );
}
