import { useState, useMemo, useEffect, useCallback } from 'react';
import { Calendar, Target, Trophy, TrendingUp, BarChart3, Search, Filter, Brain, RefreshCw, Clock, Loader2 } from 'lucide-react';
import { formatUserDate } from '@/lib/dateUtils';
import { useTheme } from '@/contexts/ThemeContext';
import { useCache } from '@/contexts/CacheContext';
import { Button } from '@/components/ui/button';
import { OfflineState } from '@/components/ui/offline-state';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar as CalendarComponent } from '@/components/ui/calendar';
import { SummaryCard } from '@/components/predictions/SummaryCard';
import { GameSection } from '@/components/predictions/GameSection';
import { useSupabasePredictions } from '@/hooks/useSupabasePredictions';
import { useEnsemble } from '@/contexts/EnsembleContext';
import { cn } from '@/lib/utils';
import { cleanNameForMatching, normalizeName, extractTeamName } from '@/lib/nameUtils';
import { debounce } from '@/lib/rateLimiter';
import { TEAM_SEARCH_MAP } from '@/constants/teams';

export default function Predictions() {
  const { dateFormat } = useTheme();
  
  const [selectedDate, setSelectedDate] = useState<Date>(() => {
    const stored = sessionStorage.getItem('shared-selected-date');
    if (stored) {
      const parsed = new Date(stored);
      if (!isNaN(parsed.getTime())) {
        return parsed;
      }
    }
    return new Date();
  });

  
  useEffect(() => {
    sessionStorage.setItem('shared-selected-date', selectedDate.toISOString());
  }, [selectedDate]);
  const [confidenceFilter, setConfidenceFilter] = useState<string>('all');
  const [playerSearch, setPlayerSearch] = useState('');
  const [playerSearchInput, setPlayerSearchInput] = useState('');
  const [positionFilter, setPositionFilter] = useState<string>('all');
  
  
  const debouncedSetPlayerSearch = useCallback(
    debounce((value: string) => {
      setPlayerSearch(value);
    }, 300),
    []
  );
  
  useEffect(() => {
    debouncedSetPlayerSearch(playerSearchInput);
    
    
    return () => {
      debouncedSetPlayerSearch.cancel();
    };
  }, [playerSearchInput, debouncedSetPlayerSearch]);

  const { selectedModels } = useEnsemble();
  const { isOnline, cacheCounts } = useCache();
  const {
    data: games = [],
    isLoading,
    isError,
    error,
    refetch,
  } = useSupabasePredictions(selectedDate, selectedModels);

  
  const [retryCountdown, setRetryCountdown] = useState<number | null>(null);
  const isRateLimitError = error?.message?.includes('Rate limited') || error?.message?.includes('429');
  const isOfflineError = !isOnline && error?.message?.includes('offline');

  useEffect(() => {
    if (!isRateLimitError || !error) {
      setRetryCountdown(null);
      return;
    }

    
    const match = error.message.match(/Try again in (\d+)s/);
    const retrySeconds = match ? parseInt(match[1], 10) : 5; 

    setRetryCountdown(retrySeconds);

    const countdownInterval = setInterval(() => {
      setRetryCountdown(prev => {
        if (prev === null || prev <= 1) {
          clearInterval(countdownInterval);
          refetch(); 
          return null;
        }
        return prev - 1;
      });
    }, 1000);

    return () => clearInterval(countdownInterval);
  }, [isRateLimitError, error, refetch]);


  const matchesTeamSearch = (teamAbbr: string, teamFullName: string, normalizedSearch: string) => {
    
    if (cleanNameForMatching(teamAbbr.toLowerCase()).includes(normalizedSearch) || 
        normalizedSearch.includes(cleanNameForMatching(teamAbbr.toLowerCase()))) {
      return true;
    }
    
    
    const aliases = TEAM_SEARCH_MAP[teamAbbr] || [];
    for (const alias of aliases) {
      const normalizedAlias = cleanNameForMatching(alias);
      if (normalizedAlias.includes(normalizedSearch) || normalizedSearch.includes(normalizedAlias)) {
        return true;
      }
    }
    
    
    const normalizedFullName = cleanNameForMatching(teamFullName.toLowerCase());
    if (normalizedFullName.includes(normalizedSearch) || normalizedSearch.includes(normalizedFullName)) {
      return true;
    }
    
    
    const teamName = extractTeamName(teamFullName);
    const normalizedTeamName = cleanNameForMatching(teamName.toLowerCase());
    if (normalizedTeamName.includes(normalizedSearch) || normalizedSearch.includes(normalizedTeamName)) {
      return true;
    }
    
    return false;
  };

  
  const filteredGames = useMemo(() => {
    return games.map(game => ({
      ...game,
      predictions: game.predictions.filter(p => {
        
        if (confidenceFilter === 'high' && p.confidence < 80) return false;
        if (confidenceFilter === 'medium' && (p.confidence < 60 || p.confidence >= 80)) return false;
        if (confidenceFilter === 'low' && p.confidence >= 60) return false;

        
        if (playerSearch) {
          const normalizedSearch = cleanNameForMatching(playerSearch.toLowerCase());
          
          
          const normalizedPlayerName = cleanNameForMatching(p.player.name.toLowerCase());
          const playerMatch = normalizedPlayerName.includes(normalizedSearch) || 
                             normalizedSearch.includes(normalizedPlayerName);
          
          
          const playerTeamMatch = matchesTeamSearch(p.player.teamAbbr, p.player.team, normalizedSearch);
          
          
          const opponentAbbr = p.isHome ? game.awayTeamAbbr : game.homeTeamAbbr;
          const opponentName = p.isHome ? game.awayTeam : game.homeTeam;
          const opponentTeamMatch = matchesTeamSearch(opponentAbbr, opponentName, normalizedSearch);
          
          if (!playerMatch && !playerTeamMatch && !opponentTeamMatch) return false;
        }

        
        if (positionFilter !== 'all') {
          const positionMap: Record<string, string[]> = {
            guard: ['Guard', 'PG', 'SG'],
            forward: ['Forward', 'SF', 'PF'],
            center: ['Center', 'C'],
          };
          if (!positionMap[positionFilter]?.some(pos => p.player.position.includes(pos))) return false;
        }

        return true;
      }),
    })).filter(game => game.predictions.length > 0);
  }, [games, confidenceFilter, playerSearch, positionFilter]);

  
  const totalPredictions = filteredGames.reduce((sum, game) => sum + game.predictions.length, 0);
  const totalGames = filteredGames.length;
  const avgConfidence = totalPredictions > 0
    ? Math.round(filteredGames.flatMap(g => g.predictions).reduce((sum, p) => sum + p.confidence, 0) / totalPredictions)
    : 0;
  const highConfidenceCount = filteredGames.flatMap(g => g.predictions).filter(p => p.confidence >= 80).length;

  const isPastDate = selectedDate < new Date(new Date().setHours(0, 0, 0, 0));

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0 flex-1">
          <h1 className="text-3xl font-bold text-foreground leading-tight truncate">Predictions</h1>
          <p className="text-muted-foreground leading-tight truncate">AI-powered player performance predictions</p>
        </div>

        <Popover>
          <PopoverTrigger asChild>
            <Button
              variant="outline"
              className="gap-2 relative group overflow-hidden transition-all duration-300 hover:border-primary/50 shrink-0"
            >
              <div className="absolute inset-0 bg-gradient-to-r from-primary/0 via-primary/10 to-primary/0 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300 blur-xl bg-primary/20" />
              <Calendar className="h-4 w-4 relative z-10 shrink-0" />
              <span className="relative z-10 whitespace-nowrap">{formatUserDate(selectedDate, dateFormat, true)}</span>
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-auto p-0" align="end">
            <CalendarComponent
              mode="single"
              selected={selectedDate}
              onSelect={(date) => date && setSelectedDate(date)}
              disabled={(date) => date > new Date()}
              initialFocus
              className="pointer-events-auto"
            />
          </PopoverContent>
        </Popover>
      </div>

      <div className="flex flex-wrap density-gap rounded-xl bg-card/50 density-padding border border-border">
        <div className="flex-1 min-w-[200px]">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 h-4 w-4 shrink-0 -translate-y-1/2 text-muted-foreground pointer-events-none" />
            <Input
              placeholder="Search players or teams..."
              value={playerSearchInput}
              onChange={(e) => setPlayerSearchInput(e.target.value)}
              className="pl-9"
            />
          </div>
        </div>

        <Select value={confidenceFilter} onValueChange={setConfidenceFilter}>
          <SelectTrigger className="w-[180px] shrink-0">
            <Filter className="h-4 w-4 mr-2 shrink-0" />
            <SelectValue placeholder="Confidence" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Predictions</SelectItem>
            <SelectItem value="high">High (80+)</SelectItem>
            <SelectItem value="medium">Medium (60-79)</SelectItem>
            <SelectItem value="low">Low (&lt;60)</SelectItem>
          </SelectContent>
        </Select>

        <Select value={positionFilter} onValueChange={setPositionFilter}>
          <SelectTrigger className="w-[150px] shrink-0">
            <SelectValue placeholder="Position" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Positions</SelectItem>
            <SelectItem value="guard">Guard</SelectItem>
            <SelectItem value="forward">Forward</SelectItem>
            <SelectItem value="center">Center</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="grid density-gap sm:grid-cols-2 lg:grid-cols-4">
        <SummaryCard
          title="Total Predictions"
          value={totalPredictions}
          icon={Brain}
        />
        <SummaryCard
          title="Total Games"
          value={totalGames}
          icon={Trophy}
        />
        <SummaryCard
          title="Avg Confidence"
          value={`${avgConfidence}%`}
          icon={TrendingUp}
        />
        <SummaryCard
          title="High Confidence"
          value={highConfidenceCount}
          icon={BarChart3}
        />
      </div>

      <div className="space-y-4">
        {isLoading ? (
          <>
            {[...Array(2)].map((_, i) => (
              <div key={i} className="section-gradient">
                <div className="flex items-center gap-3 p-3 mb-4">
                  <div className="h-8 w-8 bg-muted animate-pulse rounded-lg" />
                  <div className="h-5 w-32 bg-muted animate-pulse rounded" />
                  <div className="h-5 w-20 bg-muted/50 animate-pulse rounded-full" />
                </div>

                <div className="mb-4 flex gap-2 p-1 bg-muted/20 rounded-lg w-fit">
                  {[...Array(3)].map((_, k) => (
                    <div key={k} className="h-8 w-16 bg-muted animate-pulse rounded" />
                  ))}
                </div>

                <div className="flex flex-col gap-3">
                  {[...Array(4)].map((_, j) => (
                    <div key={j} className="stat-card p-4">
                      <div className="flex items-start gap-4">
                        <div className="h-20 w-20 bg-muted animate-pulse rounded-lg flex-shrink-0" />
                        <div className="flex-1 space-y-2">
                          <div className="h-5 w-40 bg-muted animate-pulse rounded" />
                          <div className="h-4 w-28 bg-muted/70 animate-pulse rounded" />
                          <div className="flex gap-2 mt-2">
                            <div className="h-6 w-20 bg-muted animate-pulse rounded" />
                            <div className="h-6 w-20 bg-muted animate-pulse rounded" />
                            <div className="h-6 w-24 bg-muted animate-pulse rounded" />
                          </div>
                        </div>
                        <div className="h-8 w-12 bg-muted animate-pulse rounded-full flex-shrink-0" />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </>
        ) : isError ? (
          <>
            {isOfflineError ? (
              <OfflineState
                context="predictions for this date"
                availableOffline={
                  cacheCounts.predictions > 0
                    ? [`${cacheCounts.predictions} cached prediction dates available`]
                    : []
                }
                onRetry={() => refetch()}
                showRetry={true}
              />
            ) : isRateLimitError ? (
              <div className="rounded-xl border border-border bg-card p-12 text-center">
              <div className="max-w-xl mx-auto">
                <div className="rounded-2xl bg-muted/30 border border-border/50 p-10 space-y-10">
                  <div className="text-center space-y-5">
                    <div className="relative w-20 h-20 mx-auto">
                      <div className="absolute inset-0 rounded-full bg-primary/10 animate-pulse" />
                      <div className="absolute inset-0 flex items-center justify-center">
                        <Loader2 className="h-10 w-10 text-primary animate-spin shrink-0" />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <h3 className="text-2xl font-semibold text-foreground tracking-tight">
                        Loading Predictions
                      </h3>
                      <p className="text-muted-foreground">
                        Fetching latest data - this may take a moment
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center justify-center">
                    <div className="relative">
                      <div className="absolute inset-0 rounded-2xl bg-primary/5 blur-xl" />
                      <div className="relative rounded-2xl bg-gradient-to-br from-primary/10 to-primary/5 border border-primary/20 px-10 py-6">
                        <div className="flex items-baseline justify-center gap-3">
                          <span className="text-6xl font-bold text-primary tabular-nums leading-none tracking-tight">
                            {retryCountdown !== null ? retryCountdown : '...'}
                          </span>
                          <span className="text-lg text-primary/60 font-medium pb-1">sec</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="text-center space-y-4">
                    <p className="text-xs text-muted-foreground/80 max-w-sm mx-auto leading-relaxed">
                      Future loads will be instant - data is cached locally after first fetch
                    </p>
                    <Button
                      onClick={() => refetch()}
                      variant="ghost"
                      size="sm"
                      className="gap-2 text-muted-foreground hover:text-foreground transition-colors"
                    >
                      <RefreshCw className="h-3.5 w-3.5 shrink-0" />
                      <span className="whitespace-nowrap">Retry Now</span>
                    </Button>
                  </div>
                </div>
              </div>
              </div>
            ) : (
              <div className="rounded-xl border border-border bg-card p-12 text-center">
                <Brain className="mx-auto h-12 w-12 text-muted-foreground mb-4 shrink-0" />
                <h3 className="text-lg font-semibold text-foreground mb-2">Error loading predictions</h3>
                <p className="text-muted-foreground mb-4">
                  There was a problem fetching predictions. Please try again or select a different date.
                </p>
                <Button
                  onClick={() => refetch()}
                  variant="outline"
                  className="gap-2"
                >
                  <RefreshCw className="h-4 w-4 shrink-0" />
                  <span className="whitespace-nowrap">Try Again</span>
                </Button>
              </div>
            )}
          </>
        ) : filteredGames.length > 0 ? (
          filteredGames.map(game => (
            <GameSection key={game.id} game={game} showCompare={isPastDate} />
          ))
        ) : (
          <div className="rounded-xl border border-border bg-card p-12 text-center">
            <Brain className="mx-auto h-12 w-12 text-muted-foreground mb-4 shrink-0" />
            <h3 className="text-lg font-semibold text-foreground mb-2">No Predictions Found</h3>
            <p className="text-muted-foreground">
              Try adjusting your filters or selecting a different date.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
