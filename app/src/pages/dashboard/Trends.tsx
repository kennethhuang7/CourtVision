import { useState, useEffect } from 'react';
import { Flame, Filter, Loader2 } from 'lucide-react';
import { useTrends } from '@/hooks/useTrends';
import { useEnsemble } from '@/contexts/EnsembleContext';
import type { TrendFilters, Trend } from '@/types/trends';
import type { StatType } from '@/types/nba';
import { TrendsList } from '@/components/trends/TrendsList';
import { TrendDetail } from '@/components/trends/TrendDetail';
import { TrendsFilters } from '@/components/trends/TrendsFilters';

const defaultFilters: TrendFilters = {
  statType: 'all',
  overUnder: 'both',
  trendTypes: ['recent-form', 'h2h', 'home-away'], 
  minStreak: 3, 
  lineMethod: 'player-average',
  lineAdjustment: 'standard',
  requireAiAgreement: false, 
};

function Trends() {
  const { findTrends, isLoading } = useTrends();
  const { selectedModels } = useEnsemble();

  const [filters, setFilters] = useState<TrendFilters>(defaultFilters);
  const [trends, setTrends] = useState<Trend[]>([]);
  const [selectedTrend, setSelectedTrend] = useState<Trend | null>(null);
  const [showFilters, setShowFilters] = useState(false);

  
  useEffect(() => {
    loadTrends();
  }, [filters, selectedModels]);

  const loadTrends = async () => {
    const results = await findTrends(filters, selectedModels);
    setTrends(results);

    
    if (results.length > 0 && !selectedTrend) {
      setSelectedTrend(results[0]);
    }
  };

  const updateFilter = <K extends keyof TrendFilters>(
    key: K,
    value: TrendFilters[K]
  ) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  return (
    <div className="flex flex-col h-screen bg-zinc-950">
      <div className="flex items-center justify-between p-6 border-b border-zinc-800">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-gradient-to-br from-orange-500 to-red-600 rounded-lg">
            <Flame className="h-6 w-6 text-white shrink-0" />
          </div>
          <div className="min-w-0 flex-1">
            <h1 className="text-2xl font-bold text-white leading-tight truncate">Trends</h1>
            <p className="text-sm text-zinc-400 leading-tight truncate">
              Discover hot picks and trending performances
            </p>
          </div>
        </div>

        <button
          onClick={() => setShowFilters(!showFilters)}
          className="flex items-center gap-2 px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-white rounded-lg transition-colors"
        >
          <Filter className="h-4 w-4" />
          Filters
        </button>
      </div>

      {showFilters && (
        <TrendsFilters
          filters={filters}
          updateFilter={updateFilter}
          onClose={() => setShowFilters(false)}
        />
      )}

      <div className="flex-1 flex overflow-hidden">
        {isLoading && (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-center">
              <Loader2 className="h-12 w-12 text-blue-500 animate-spin mx-auto mb-4" />
              <p className="text-zinc-400">Finding trending picks...</p>
            </div>
          </div>
        )}

        {!isLoading && trends.length === 0 && (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-center max-w-md">
              <Flame className="h-16 w-16 text-zinc-600 mx-auto mb-4" />
              <h3 className="text-xl font-semibold text-white mb-2">
                No Trends Found
              </h3>
              <p className="text-zinc-400">
                Try adjusting your filters or checking back later when games are scheduled.
              </p>
            </div>
          </div>
        )}

        {!isLoading && trends.length > 0 && (
          <>
            <div className="w-96 border-r border-zinc-800 overflow-y-auto">
              <TrendsList
                trends={trends}
                selectedTrend={selectedTrend}
                onSelectTrend={setSelectedTrend}
              />
            </div>

            <div className="flex-1 overflow-y-auto">
              {selectedTrend ? (
                <TrendDetail trend={selectedTrend} />
              ) : (
                <div className="flex items-center justify-center h-full">
                  <p className="text-zinc-400">Select a trend to view details</p>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default Trends;
