import { useState, useEffect } from 'react';
import { Filter, Loader2, TrendingUp, RefreshCw } from 'lucide-react';
import { useTrends } from '@/hooks/useTrends';
import { useEnsemble } from '@/contexts/EnsembleContext';
import { RateLimitError } from '@/components/ui/RateLimitError';
import type { TrendFilters, Trend } from '@/types/trends';
import type { StatType } from '@/types/nba';
import { TrendsList } from '@/components/trends/TrendsList';
import { TrendDetail } from '@/components/trends/TrendDetail';
import { TrendsFilters } from '@/components/trends/TrendsFilters';
import { Button } from '@/components/ui/button';

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
  const { findTrends, isLoading, error, clearCache } = useTrends();
  const { selectedModels } = useEnsemble();
  const [isRefreshing, setIsRefreshing] = useState(false);

  const [filters, setFilters] = useState<TrendFilters>(() => {
    const stored = localStorage.getItem('trends-filters');
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        return { ...defaultFilters, ...parsed };
      } catch {
        return defaultFilters;
      }
    }
    return defaultFilters;
  });
  const [trends, setTrends] = useState<Trend[]>([]);
  const [selectedTrend, setSelectedTrend] = useState<Trend | null>(null);
  const [showFilters, setShowFilters] = useState(false);
  const [loadingStage, setLoadingStage] = useState<string>('');
  const [loadingProgress, setLoadingProgress] = useState<number>(0);

  
  useEffect(() => {
    loadTrends(false);
  }, [filters, selectedModels]);

  const loadTrends = async (forceRefresh: boolean = false) => {
    try {
      if (forceRefresh) {
        setIsRefreshing(true);
        clearCache();
      }
      const results = await findTrends(filters, selectedModels, (stage, progress) => {
        setLoadingStage(stage);
        setLoadingProgress(progress);
      }, forceRefresh);
      setTrends(results);

      
      if (results.length > 0 && !selectedTrend) {
        setSelectedTrend(results[0]);
      }
    } catch (err) {
      
    } finally {
      setIsRefreshing(false);
    }
  };

  const handleRefresh = () => {
    loadTrends(true);
  };

  const updateFilter = <K extends keyof TrendFilters>(
    key: K,
    value: TrendFilters[K]
  ) => {
    setFilters(prev => {
      const updated = { ...prev, [key]: value };
      localStorage.setItem('trends-filters', JSON.stringify(updated));
      return updated;
    });
  };

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0 flex-1">
          <h1 className="text-3xl font-bold text-foreground leading-tight truncate">Trends</h1>
          <p className="text-muted-foreground leading-tight truncate">
            Discover trending picks and performances
          </p>
        </div>

        <div className="flex gap-2 shrink-0">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={handleRefresh}
            disabled={isRefreshing || isLoading}
            className="gap-2"
          >
            <RefreshCw className={`h-4 w-4 shrink-0 ${isRefreshing ? 'animate-spin' : ''}`} />
            <span className="whitespace-nowrap">Refresh</span>
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => setShowFilters(!showFilters)}
            className="gap-2"
          >
            <Filter className="h-4 w-4 shrink-0" />
            <span className="whitespace-nowrap">Filters</span>
          </Button>
        </div>
      </div>

      {showFilters && (
        <TrendsFilters
          filters={filters}
          updateFilter={updateFilter}
          onClose={() => setShowFilters(false)}
        />
      )}

      <div className="flex overflow-hidden">
        {error && (error.message?.includes('Rate limited') || error.message?.includes('429')) ? (
          <div className="flex-1 flex items-center justify-center p-6">
            <RateLimitError 
              error={error} 
              onRetry={loadTrends}
              showRetry={true}
            />
          </div>
        ) : isLoading && (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-center space-y-4 max-w-md">
              <div className="relative w-20 h-20 mx-auto">
                <div className="absolute inset-0 rounded-full bg-primary/10 animate-pulse" />
                <div className="absolute inset-0 flex items-center justify-center">
                  <Loader2 className="h-10 w-10 text-primary animate-spin shrink-0" />
                </div>
              </div>
              <div className="space-y-2">
                <p className="text-foreground font-medium">Finding trends...</p>
                {loadingStage && (
                  <p className="text-muted-foreground text-sm">{loadingStage}</p>
                )}
                {loadingProgress > 0 && (
                  <div className="w-full max-w-xs mx-auto">
                    <div className="w-full bg-secondary rounded-full h-2">
                      <div 
                        className="bg-primary h-2 rounded-full transition-all duration-300"
                        style={{ width: `${loadingProgress}%` }}
                      />
                    </div>
                    <p className="text-muted-foreground text-xs mt-2 text-center">{Math.round(loadingProgress)}%</p>
                  </div>
                )}
              </div>
              <p className="text-muted-foreground text-xs max-w-sm mx-auto">
                This may take a moment while we analyze player performance data
              </p>
            </div>
          </div>
        )}

        {!isLoading && trends.length === 0 && (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-center max-w-md">
              <div className="relative mx-auto mb-4 w-fit">
                <div className="absolute inset-0 rounded-full bg-gradient-to-br from-primary/20 via-primary/10 to-transparent blur-xl" />
                <div className="relative w-20 h-20 rounded-full bg-gradient-to-br from-muted/80 to-muted/40 flex items-center justify-center">
                  <TrendingUp className="h-10 w-10 text-muted-foreground" />
                </div>
              </div>
              <h3 className="text-xl font-semibold text-foreground mb-2">
                No Trends Found
              </h3>
              <p className="text-muted-foreground">
                Try adjusting your filters or checking back later when games are scheduled.
              </p>
            </div>
          </div>
        )}

        {!isLoading && trends.length > 0 && (
          <>
            <div className="w-96 border-r border-border overflow-y-auto">
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
                  <p className="text-muted-foreground">Select a trend to view details</p>
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
