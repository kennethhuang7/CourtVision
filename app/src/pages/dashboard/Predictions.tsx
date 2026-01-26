import { useState, useMemo, useEffect, useCallback, useRef } from 'react';
import { useLocation } from 'react-router-dom';
import { Search, Filter, Brain, RefreshCw, Loader2, ArrowUpDown, ArrowUp, ArrowDown, Grid3x3, ChevronDown, ChevronUp } from 'lucide-react';
import { parseStoredDate, toDateOnlyString } from '@/lib/dateUtils';
import { useTheme } from '@/contexts/ThemeContext';
import { useCache } from '@/contexts/CacheContext';
import { Button } from '@/components/ui/button';
import { OfflineState } from '@/components/ui/offline-state';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { DatePicker } from '@/components/ui/date-picker';
import { PlayerCard } from '@/components/predictions/PlayerCard';
import { useSupabasePredictions } from '@/hooks/useSupabasePredictions';
import { useEnsemble } from '@/contexts/EnsembleContext';
import { useCountUp } from '@/hooks/useCountUp';
import { cn } from '@/lib/utils';
import { cleanNameForMatching, normalizeName, extractTeamName } from '@/lib/nameUtils';
import { debounce } from '@/lib/rateLimiter';
import { TEAM_SEARCH_MAP } from '@/constants/teams';
import { Prediction } from '@/types/nba';
import { getTeamLogoUrl } from '@/utils/teamLogos';
import { motion, AnimatePresence } from 'framer-motion';

export default function Predictions() {
  const { dateFormat } = useTheme();
  
  const location = useLocation();
  const [selectedDate, setSelectedDate] = useState<Date>(new Date());
  const [isInitializing, setIsInitializing] = useState(true);
  const locationKeyRef = useRef<string | undefined>(undefined);

  useEffect(() => {
    if (locationKeyRef.current === location.key) return;
    locationKeyRef.current = location.key;

    const initializeDate = async () => {
      const stored = sessionStorage.getItem('shared-selected-date');
      if (stored) {
        const parsed = parseStoredDate(stored);
        if (parsed) {
          setSelectedDate(parsed);
          setIsInitializing(false);
          return;
        }
      }

      const { getBestInitialDate } = await import('@/lib/dateUtils');
      const bestDate = await getBestInitialDate();
      setSelectedDate(bestDate);
      sessionStorage.setItem('shared-selected-date', toDateOnlyString(bestDate));
      setIsInitializing(false);
    };

    initializeDate();
  }, [location.key]);

  useEffect(() => {
    if (!isInitializing) {
      sessionStorage.setItem('shared-selected-date', toDateOnlyString(selectedDate));
    }
  }, [selectedDate, isInitializing]);
  const [confidenceFilter, setConfidenceFilter] = useState<string>('all');
  const [playerSearch, setPlayerSearch] = useState('');
  const [playerSearchInput, setPlayerSearchInput] = useState('');
  const [positionFilter, setPositionFilter] = useState<string>('all');
  const [sortBy, setSortBy] = useState<'confidence' | 'points' | 'rebounds' | 'assists' | 'name'>('confidence');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const [groupBy, setGroupBy] = useState<'none' | 'game' | 'team' | 'position'>('none');
  const [selectedGameId, setSelectedGameId] = useState<string | null>(null);
  const [showAllGames, setShowAllGames] = useState(false);
  const gamesContainerRef = useRef<HTMLDivElement>(null);
  const [cardsPerRow, setCardsPerRow] = useState<number | null>(null);
  
  
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
    isFetching,
    isError,
    error,
    refetch,
  } = useSupabasePredictions(selectedDate, selectedModels, { enabled: !isInitializing });

  
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

  
  type PredictionWithContext = Prediction & {
    gameContext: {
      homeTeamAbbr: string;
      awayTeamAbbr: string;
      gameId: string;
    };
  };

  const allPredictionsWithContext = useMemo<PredictionWithContext[]>(() => {
    return games.flatMap(game => 
      game.predictions.map(prediction => ({
        ...prediction,
        gameContext: {
          homeTeamAbbr: game.homeTeamAbbr,
          awayTeamAbbr: game.awayTeamAbbr,
          gameId: game.id,
        }
      }))
    );
  }, [games]);

  const filteredPredictions = useMemo<PredictionWithContext[]>(() => {
    return allPredictionsWithContext.filter(p => {
      if (selectedGameId && p.gameContext.gameId !== selectedGameId) return false;
        
        if (confidenceFilter === 'high' && p.confidence < 80) return false;
        if (confidenceFilter === 'medium' && (p.confidence < 60 || p.confidence >= 80)) return false;
        if (confidenceFilter === 'low' && p.confidence >= 60) return false;

        
        if (playerSearch) {
          const normalizedSearch = cleanNameForMatching(playerSearch.toLowerCase());
          
        if (p.player?.name) {
          const normalizedPlayerName = cleanNameForMatching(p.player.name.toLowerCase());
          const playerMatch = normalizedPlayerName.includes(normalizedSearch) || 
                             normalizedSearch.includes(normalizedPlayerName);
          if (playerMatch) return true;
        }
          
        if (p.player?.teamAbbr && p.player?.team) {
          const playerTeamMatch = matchesTeamSearch(p.player.teamAbbr, p.player.team, normalizedSearch);
          if (playerTeamMatch) return true;
        }
        
        const opponentAbbr = p.isHome ? p.gameContext.awayTeamAbbr : p.gameContext.homeTeamAbbr;
        const opponentName = p.opponent;
        if (opponentAbbr && opponentName) {
          const opponentTeamMatch = matchesTeamSearch(opponentAbbr, opponentName, normalizedSearch);
          if (opponentTeamMatch) return true;
        }
        
        return false;
      }

      
      if (positionFilter !== 'all' && p.player?.position) {
          const positionMap: Record<string, string[]> = {
            guard: ['Guard', 'PG', 'SG'],
            forward: ['Forward', 'SF', 'PF'],
            center: ['Center', 'C'],
          };
          if (!positionMap[positionFilter]?.some(pos => p.player.position.includes(pos))) return false;
        }

        return true;
    });
  }, [allPredictionsWithContext, confidenceFilter, playerSearch, positionFilter, selectedGameId]);

  const sortedPredictions = useMemo<PredictionWithContext[]>(() => {
    const sorted = [...filteredPredictions];
    sorted.sort((a, b) => {
      let aValue: number | string;
      let bValue: number | string;

      switch (sortBy) {
        case 'confidence':
          aValue = a.confidence;
          bValue = b.confidence;
          break;
        case 'points':
          aValue = a.predictedStats.points;
          bValue = b.predictedStats.points;
          break;
        case 'rebounds':
          aValue = a.predictedStats.rebounds;
          bValue = b.predictedStats.rebounds;
          break;
        case 'assists':
          aValue = a.predictedStats.assists;
          bValue = b.predictedStats.assists;
          break;
        case 'name':
          aValue = a.player.name.toLowerCase();
          bValue = b.player.name.toLowerCase();
          break;
        default:
          return 0;
      }

      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDirection === 'desc' ? bValue - aValue : aValue - bValue;
      } else {
        return sortDirection === 'desc' 
          ? String(bValue).localeCompare(String(aValue))
          : String(aValue).localeCompare(String(bValue));
      }
    });
    return sorted;
  }, [filteredPredictions, sortBy, sortDirection]);

  const groupedPredictions = useMemo<Record<string, PredictionWithContext[]>>(() => {
    if (groupBy === 'none') {
      return { 'All Predictions': sortedPredictions };
    }

    const groups: Record<string, PredictionWithContext[]> = {};

    sortedPredictions.forEach(prediction => {
      let key: string;
      
      if (groupBy === 'game') {
        key = `${prediction.gameContext.awayTeamAbbr} @ ${prediction.gameContext.homeTeamAbbr}`;
      } else if (groupBy === 'team') {
        key = prediction.player.teamAbbr;
      } else if (groupBy === 'position') {
        key = prediction.player.position || 'Unknown';
      } else {
        key = 'All Predictions';
      }

      if (!groups[key]) {
        groups[key] = [];
      }
      groups[key].push(prediction);
    });

    return groups;
  }, [sortedPredictions, groupBy]);

  const filteredGames = useMemo(() => {
    const gameMap = new Map<string, typeof games[0]>();
    filteredPredictions.forEach(p => {
      const gameId = p.gameContext.gameId;
      if (!gameMap.has(gameId)) {
        const game = games.find(g => g.id === gameId);
        if (game) {
          gameMap.set(gameId, { ...game, predictions: [] });
        }
      }
    });
    return Array.from(gameMap.values());
  }, [filteredPredictions, games]);

  
  const totalPredictionsAllGames = allPredictionsWithContext.length;

  const totalPredictions = filteredPredictions.length;
  const totalGames = new Set(filteredPredictions.map(p => p.gameContext.gameId)).size;
  const avgConfidence = totalPredictions > 0
    ? Math.round(filteredPredictions.reduce((sum, p) => sum + p.confidence, 0) / totalPredictions)
    : 0;
  const highConfidenceCount = filteredPredictions.filter(p => p.confidence >= 80).length;

  const animatedTotalPredictions = useCountUp(totalPredictions, { duration: 400 });
  const animatedTotalGames = useCountUp(totalGames, { duration: 400 });
  const animatedAvgConfidence = useCountUp(avgConfidence, { duration: 500 });
  const animatedHighConfidence = useCountUp(highConfidenceCount, { duration: 400 });

  const isPastDate = selectedDate < new Date(new Date().setHours(0, 0, 0, 0));

  useEffect(() => {
    if (!gamesContainerRef.current || games.length === 0) {
      setCardsPerRow(null);
      return;
    }

    const calculateCardsPerRow = () => {
      const container = gamesContainerRef.current;
      if (!container) return;

      const computedStyle = window.getComputedStyle(container);
      const gap = parseFloat(computedStyle.gap) || 12;
      const containerWidth = container.offsetWidth;
      
      const minCardWidth = 140;
      
      const estimatedCards = Math.floor((containerWidth + gap) / (minCardWidth + gap));
      
      const cardsThatFit = Math.max(1, estimatedCards - 1);
      setCardsPerRow(cardsThatFit);
    };

    calculateCardsPerRow();
    
    const resizeObserver = new ResizeObserver(() => {
      setTimeout(calculateCardsPerRow, 0);
    });
    resizeObserver.observe(gamesContainerRef.current);

    const handleResize = () => {
      setTimeout(calculateCardsPerRow, 0);
    };
    window.addEventListener('resize', handleResize);

    return () => {
      resizeObserver.disconnect();
      window.removeEventListener('resize', handleResize);
    };
  }, [games.length]);

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0 flex-1">
          <h1 className="text-3xl font-bold text-foreground leading-tight truncate">Predictions</h1>
          <p className="text-muted-foreground leading-tight truncate">AI-powered player performance predictions</p>
        </div>

        <DatePicker
          selectedDate={selectedDate}
          onDateChange={setSelectedDate}
          disabled={(date) => date > new Date()}
        />
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

      {/* Upcoming Games Section */}
      {games.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-lg font-semibold text-foreground">Upcoming Games</h2>
          <div 
            ref={gamesContainerRef}
            className="grid grid-cols-[repeat(auto-fit,minmax(min(140px,100%),1fr))] density-gap auto-rows-fr"
          >
            {/* All Games Card */}
            <motion.button
              onClick={() => setSelectedGameId(null)}
              whileHover={{ 
                scale: 1.03,
                y: -2,
              }}
              whileTap={{ 
                scale: 0.97,
                y: 0,
              }}
              animate={{
                scale: selectedGameId === null ? 1.02 : 1,
                y: selectedGameId === null ? -1 : 0,
              }}
              transition={{ 
                duration: 0.25, 
                ease: [0.4, 0, 0.2, 1],
                type: "spring",
                stiffness: 300,
                damping: 20
              }}
              className={cn(
                'relative group flex flex-col items-center justify-center p-4 rounded-xl border-2 transition-all duration-300 min-w-[140px] overflow-hidden',
                selectedGameId === null
                  ? 'bg-gradient-to-br from-primary/20 via-primary/10 to-accent/10 border-primary/50 shadow-lg shadow-primary/10'
                  : 'bg-card/50 border-border hover:border-primary/30 hover:bg-card/70'
              )}
            >
              {/* Animated background gradient */}
              <motion.div
                className={cn(
                  'absolute inset-0 rounded-xl bg-gradient-to-br from-primary/0 via-primary/5 to-accent/5',
                  selectedGameId === null ? 'opacity-100' : 'opacity-0'
                )}
                animate={{
                  opacity: selectedGameId === null ? 1 : 0,
                }}
                transition={{ duration: 0.3 }}
              />
              
              {/* Hover ripple effect */}
              <motion.div
                className="absolute inset-0 rounded-xl pointer-events-none overflow-hidden"
                initial={false}
              >
                <motion.div
                  className="absolute top-1/2 left-1/2 w-0 h-0 rounded-full bg-primary/20"
                  style={{ x: "-50%", y: "-50%" }}
                  whileHover={{
                    width: "200%",
                    height: "200%",
                    opacity: [0.3, 0.1, 0.3],
                  }}
                  transition={{
                    duration: 1.5,
                    ease: "easeOut",
                    repeat: Infinity,
                  }}
                />
              </motion.div>
              
              {/* Ripple effect on click */}
              <motion.div
                className="absolute inset-0 rounded-xl bg-primary/30"
                initial={{ scale: 0, opacity: 0.5 }}
                whileTap={{ scale: 2, opacity: 0 }}
                transition={{ duration: 0.4 }}
              />
              
              <div className="relative z-10 flex flex-col items-center gap-2">
                <motion.div 
                  className="text-2xl font-bold text-foreground"
                  whileHover={{ scale: 1.1 }}
                  transition={{ duration: 0.2 }}
                >
                  {games.length}
                </motion.div>
                <div className="text-xs font-medium text-muted-foreground uppercase tracking-wide">All Games</div>
                <div className="text-xs text-muted-foreground/60 mt-1">{totalPredictionsAllGames} predictions</div>
              </div>
            </motion.button>

            {/* Individual Game Cards */}
            {((showAllGames || !cardsPerRow || games.length <= cardsPerRow) 
              ? games 
              : games.slice(0, cardsPerRow)
            ).map(game => {
              const gamePredictions = allPredictionsWithContext.filter(p => p.gameContext.gameId === game.id);
              const isSelected = selectedGameId === game.id;
              const awayLogo = getTeamLogoUrl(game.awayTeamAbbr);
              const homeLogo = getTeamLogoUrl(game.homeTeamAbbr);
              
              return (
                <motion.button
                  key={game.id}
                  onClick={() => setSelectedGameId(isSelected ? null : game.id)}
                  whileHover={{ 
                    scale: 1.03,
                    y: -2,
                  }}
                  whileTap={{ 
                    scale: 0.97,
                    y: 0,
                  }}
                  animate={{
                    scale: isSelected ? 1.02 : 1,
                    y: isSelected ? -1 : 0,
                  }}
                  transition={{ 
                    duration: 0.25, 
                    ease: [0.4, 0, 0.2, 1],
                    type: "spring",
                    stiffness: 300,
                    damping: 20
                  }}
                  className={cn(
                    'relative group flex flex-col items-center justify-center p-4 rounded-xl border-2 transition-all duration-300 overflow-hidden',
                    isSelected
                      ? 'bg-gradient-to-br from-primary/20 via-primary/10 to-accent/10 border-primary/50 shadow-lg shadow-primary/10'
                      : 'bg-card/50 border-border hover:border-primary/30 hover:bg-card/70'
                  )}
                >
                  {/* Animated background gradient */}
                  <motion.div
                    className="absolute inset-0 rounded-xl bg-gradient-to-br from-primary/0 via-primary/5 to-accent/5"
                    animate={{
                      opacity: isSelected ? 1 : 0,
                    }}
                    transition={{ duration: 0.3 }}
                  />
                  
                  {/* Hover ripple effect */}
                  <motion.div
                    className="absolute inset-0 rounded-xl pointer-events-none overflow-hidden"
                    initial={false}
                  >
                    <motion.div
                      className="absolute top-1/2 left-1/2 w-0 h-0 rounded-full bg-primary/20"
                      style={{ x: "-50%", y: "-50%" }}
                      whileHover={{
                        width: "200%",
                        height: "200%",
                        opacity: [0.3, 0.1, 0.3],
                      }}
                      transition={{
                        duration: 1.5,
                        ease: "easeOut",
                        repeat: Infinity,
                      }}
                    />
                  </motion.div>
                  
                  {/* Ripple effect on click */}
                  <motion.div
                    className="absolute inset-0 rounded-xl bg-primary/30"
                    initial={{ scale: 0, opacity: 0.5 }}
                    whileTap={{ scale: 2, opacity: 0 }}
                    transition={{ duration: 0.4 }}
                  />
                  
                  <div className="relative z-10 flex items-center gap-3 mb-2 pointer-events-none">
                    {/* Away Team */}
                    <div className="flex flex-col items-center gap-1">
                      <div className="w-10 h-10 rounded-full overflow-hidden bg-muted/50 border-2 border-border flex items-center justify-center">
                        <img
                          src={awayLogo.primary}
                          alt={game.awayTeamAbbr}
                          className="w-full h-full object-cover pointer-events-none"
                          onError={(e) => {
                            if (awayLogo.fallback) {
                              e.currentTarget.src = awayLogo.fallback;
                            } else {
                              e.currentTarget.style.display = 'none';
                            }
                          }}
                        />
                      </div>
                      <span className="text-xs font-semibold text-foreground">{game.awayTeamAbbr}</span>
                    </div>

                    {/* VS */}
                    <div className="text-muted-foreground/40 font-bold text-lg">
                      @
                    </div>

                    {/* Home Team */}
                    <div className="flex flex-col items-center gap-1">
                      <div className="w-10 h-10 rounded-full overflow-hidden bg-muted/50 border-2 border-border flex items-center justify-center">
                        <img
                          src={homeLogo.primary}
                          alt={game.homeTeamAbbr}
                          className="w-full h-full object-cover pointer-events-none"
                          onError={(e) => {
                            if (homeLogo.fallback) {
                              e.currentTarget.src = homeLogo.fallback;
                            } else {
                              e.currentTarget.style.display = 'none';
                            }
                          }}
                        />
                      </div>
                      <span className="text-xs font-semibold text-foreground">{game.homeTeamAbbr}</span>
                    </div>
                  </div>

                  {/* Prediction Count */}
                  <div className="relative z-10 mt-1 pointer-events-none">
                    <span className="text-sm font-semibold text-foreground">{gamePredictions.length}</span>
                    <span className="text-xs text-muted-foreground ml-1">predictions</span>
                  </div>
                </motion.button>
              );
            })}
          </div>
          
          {/* Show More / Show Less Toggle - Only if there's a second row */}
          {cardsPerRow && games.length > cardsPerRow && (
            <div className="flex justify-center">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowAllGames(!showAllGames)}
                className="text-xs text-muted-foreground hover:text-foreground gap-1.5"
              >
                {showAllGames ? (
                  <>
                    <span>Show Less</span>
                    <ChevronUp className="h-3.5 w-3.5" />
                  </>
                ) : (
                  <>
                    <span>Show {games.length - cardsPerRow} More</span>
                    <ChevronDown className="h-3.5 w-3.5" />
                  </>
                )}
              </Button>
            </div>
          )}
        </div>
      )}

      {/* Summary Context Bar with Sort/Group Controls */}
      <div className="rounded-lg border border-border/50 bg-card/30 backdrop-blur-sm density-padding">
        <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3 text-sm">
          {/* Metrics Section - Left */}
          <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
            <div className="flex items-center gap-2">
              <span className="font-semibold text-foreground tabular-nums">{animatedTotalPredictions}</span>
              <span className="text-muted-foreground">predictions</span>
            </div>
            <div className="hidden sm:block w-px h-4 bg-border/50" />
            <div className="flex items-center gap-2">
              <span className="font-semibold text-foreground tabular-nums">{animatedTotalGames}</span>
              <span className="text-muted-foreground">games</span>
            </div>
            <div className="hidden sm:block w-px h-4 bg-border/50" />
            <div className="flex items-center gap-3">
              <span className="text-muted-foreground">Avg confidence</span>
              <div className="flex items-center gap-2">
                <div className="relative w-20 h-2 bg-muted/50 rounded-full overflow-hidden">
                  <motion.div
                    className="absolute inset-y-0 left-0 bg-gradient-to-r from-primary/60 to-primary rounded-full"
                    initial={{ width: 0 }}
                    animate={{ width: `${Math.min(avgConfidence, 100)}%` }}
                    transition={{ duration: 0.5, ease: "easeOut" }}
                  />
                </div>
                <span className="font-semibold text-foreground tabular-nums min-w-[2.5rem]">{animatedAvgConfidence}%</span>
              </div>
            </div>
            <div className="hidden sm:block w-px h-4 bg-border/50" />
            <div className="flex items-center gap-2">
              <span className="text-muted-foreground">High confidence</span>
              <span className={cn(
                "px-2 py-0.5 rounded-md font-semibold text-xs tabular-nums",
                highConfidenceCount > 0
                  ? "bg-primary/10 text-primary"
                  : "bg-muted/30 text-muted-foreground"
              )}>
                {animatedHighConfidence}
              </span>
            </div>
          </div>

          {/* Sort/Group Controls Section - Right */}
          <div className="hidden lg:flex items-center gap-x-4 gap-y-2 ml-auto">
            <div className="flex items-center gap-2">
              <ArrowUpDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
              <span className="text-xs text-muted-foreground whitespace-nowrap">Sort:</span>
              <Select value={sortBy} onValueChange={(value) => setSortBy(value as typeof sortBy)}>
                <SelectTrigger className="w-[130px] h-7 text-xs shrink-0">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="confidence">Confidence</SelectItem>
                  <SelectItem value="points">Points</SelectItem>
                  <SelectItem value="rebounds">Rebounds</SelectItem>
                  <SelectItem value="assists">Assists</SelectItem>
                  <SelectItem value="name">Name</SelectItem>
                </SelectContent>
              </Select>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 w-7 p-0 shrink-0 relative overflow-hidden group/btn"
                onClick={() => setSortDirection(sortDirection === 'desc' ? 'asc' : 'desc')}
              >
                <div className="absolute inset-0 flex items-center justify-center">
                  <ArrowDown 
                    className={cn(
                      "h-3.5 w-3.5 absolute transition-all duration-300",
                      sortDirection === 'desc' 
                        ? "opacity-100 rotate-0 scale-100" 
                        : "opacity-0 rotate-180 scale-75"
                    )} 
                  />
                  <ArrowUp 
                    className={cn(
                      "h-3.5 w-3.5 absolute transition-all duration-300",
                      sortDirection === 'asc' 
                        ? "opacity-100 rotate-0 scale-100" 
                        : "opacity-0 -rotate-180 scale-75"
                    )} 
                  />
                </div>
              </Button>
            </div>
            <div className="hidden sm:block w-px h-4 bg-border/50" />
            <div className="flex items-center gap-2">
              <Grid3x3 className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
              <span className="text-xs text-muted-foreground whitespace-nowrap">Group:</span>
              <Select value={groupBy} onValueChange={(value) => setGroupBy(value as typeof groupBy)}>
                <SelectTrigger className="w-[110px] h-7 text-xs shrink-0">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">None</SelectItem>
                  <SelectItem value="game">Game</SelectItem>
                  <SelectItem value="team">Team</SelectItem>
                  <SelectItem value="position">Position</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>
      </div>

      {/* Sort/Group Controls - Mobile/Tablet (below summary bar) */}
      <div className="lg:hidden flex flex-wrap density-gap items-center rounded-xl bg-card/50 density-padding border border-border">
        <div className="flex items-center gap-2">
          <ArrowUpDown className="h-4 w-4 text-muted-foreground shrink-0" />
          <span className="text-sm text-muted-foreground whitespace-nowrap">Sort by:</span>
          <Select value={sortBy} onValueChange={(value) => setSortBy(value as typeof sortBy)}>
            <SelectTrigger className="w-[140px] shrink-0">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="confidence">Confidence</SelectItem>
              <SelectItem value="points">Points</SelectItem>
              <SelectItem value="rebounds">Rebounds</SelectItem>
              <SelectItem value="assists">Assists</SelectItem>
              <SelectItem value="name">Name</SelectItem>
            </SelectContent>
          </Select>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0 shrink-0 relative overflow-hidden"
            onClick={() => setSortDirection(sortDirection === 'desc' ? 'asc' : 'desc')}
          >
            <div className="absolute inset-0 flex items-center justify-center">
              <ArrowDown 
                className={cn(
                  "h-4 w-4 absolute transition-all duration-300",
                  sortDirection === 'desc' 
                    ? "opacity-100 rotate-0 scale-100" 
                    : "opacity-0 rotate-180 scale-75"
                )} 
              />
              <ArrowUp 
                className={cn(
                  "h-4 w-4 absolute transition-all duration-300",
                  sortDirection === 'asc' 
                    ? "opacity-100 rotate-0 scale-100" 
                    : "opacity-0 -rotate-180 scale-75"
                )} 
              />
            </div>
          </Button>
        </div>
        <div className="hidden sm:block w-px h-6 bg-border/50" />
        <div className="flex items-center gap-2">
          <Grid3x3 className="h-4 w-4 text-muted-foreground shrink-0" />
          <span className="text-sm text-muted-foreground whitespace-nowrap">Group by:</span>
          <Select value={groupBy} onValueChange={(value) => setGroupBy(value as typeof groupBy)}>
            <SelectTrigger className="w-[140px] shrink-0">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="none">None</SelectItem>
              <SelectItem value="game">Game</SelectItem>
              <SelectItem value="team">Team</SelectItem>
              <SelectItem value="position">Position</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="space-y-4">
        {isLoading && games.length === 0 ? (
          <>
            {/* Game Cards Skeleton */}
            <div className="space-y-3">
              <div className="h-6 w-32 bg-muted animate-pulse rounded" />
              <div className="grid grid-cols-[repeat(auto-fit,minmax(min(140px,100%),1fr))] density-gap">
                {[...Array(4)].map((_, i) => (
                  <div key={i} className="min-w-[140px] h-32 bg-muted/50 animate-pulse rounded-xl border-2 border-border" />
                ))}
              </div>
            </div>

            {/* Summary Bar Skeleton */}
            <div className="rounded-lg border border-border/50 bg-card/30 backdrop-blur-sm density-padding">
              <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
                <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
                  <div className="h-5 w-24 bg-muted animate-pulse rounded" />
                  <div className="hidden sm:block w-px h-4 bg-border/50" />
                  <div className="h-5 w-20 bg-muted animate-pulse rounded" />
                  <div className="hidden sm:block w-px h-4 bg-border/50" />
                  <div className="h-5 w-32 bg-muted animate-pulse rounded" />
                  <div className="hidden sm:block w-px h-4 bg-border/50" />
                  <div className="h-5 w-28 bg-muted animate-pulse rounded" />
                </div>
                <div className="hidden lg:flex items-center gap-x-4 gap-y-2">
                  <div className="h-7 w-24 bg-muted animate-pulse rounded" />
                  <div className="hidden sm:block w-px h-4 bg-border/50" />
                  <div className="h-7 w-20 bg-muted animate-pulse rounded" />
                </div>
              </div>
            </div>

            {/* Sort/Group Controls Skeleton (Mobile) */}
            <div className="lg:hidden flex flex-wrap density-gap items-center rounded-xl bg-card/50 density-padding border border-border">
              <div className="h-8 w-32 bg-muted animate-pulse rounded" />
              <div className="hidden sm:block w-px h-6 bg-border/50" />
              <div className="h-8 w-28 bg-muted animate-pulse rounded" />
            </div>

            {/* Player Cards Skeleton */}
            <div className="flex flex-col density-gap">
              {[...Array(6)].map((_, j) => (
                <div key={j} className="player-card-horizontal p-4">
                  <div className="flex items-start gap-4 w-full">
                    <div className="h-16 w-16 md:h-20 md:w-20 bg-muted animate-pulse rounded-full flex-shrink-0" />
                    <div className="flex-1 space-y-2 min-w-0">
                      <div className="h-5 w-40 bg-muted animate-pulse rounded" />
                      <div className="flex gap-2">
                        <div className="h-6 w-16 bg-muted/70 animate-pulse rounded-full" />
                        <div className="h-6 w-20 bg-muted/70 animate-pulse rounded-full" />
                      </div>
                      <div className="h-4 w-28 bg-muted/50 animate-pulse rounded" />
                    </div>
                    <div className="flex-1 grid grid-cols-7 gap-2 md:gap-4">
                      {[...Array(7)].map((_, k) => (
                        <div key={k} className="flex flex-col items-center gap-1">
                          <div className="h-3 w-8 bg-muted/50 animate-pulse rounded" />
                          <div className="h-5 w-10 bg-muted animate-pulse rounded" />
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </>
        ) : isError && games.length === 0 ? (
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
                <div className="relative mx-auto mb-4 w-fit">
                  <div className="absolute inset-0 rounded-full bg-gradient-to-br from-destructive/20 via-destructive/10 to-transparent blur-xl" />
                  <div className="relative w-20 h-20 rounded-full bg-gradient-to-br from-destructive/20 to-destructive/10 flex items-center justify-center">
                    <Brain className="h-10 w-10 text-destructive" />
                  </div>
                </div>
                <h3 className="text-xl font-semibold text-foreground mb-2">Error Loading Predictions</h3>
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
        ) : (
          <>
            {sortedPredictions.length > 0 ? (
              <AnimatePresence mode="wait">
                <motion.div
                  key={`${selectedGameId || 'all'}-${groupBy}-${sortBy}-${sortDirection}-${positionFilter}-${confidenceFilter}-${playerSearch}`}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.15, ease: "easeOut" }}
                  className="space-y-6"
                >
                  {Object.entries(groupedPredictions).map(([groupKey, groupPredictions]) => (
                    <motion.div key={groupKey} className="space-y-4">
                      {groupBy !== 'none' && (
                        <motion.div
                          initial={{ opacity: 0 }}
                          animate={{ opacity: 1 }}
                          transition={{ delay: 0.05, duration: 0.15 }}
                          className="flex items-center gap-3"
                        >
                          <div className="h-px flex-1 bg-gradient-to-r from-transparent via-border to-transparent" />
                          <h2 className="text-lg font-semibold text-foreground px-3">
                            {groupKey}
                            <span className="ml-2 text-sm font-normal text-muted-foreground">
                              ({groupPredictions.length})
                            </span>
                          </h2>
                          <div className="h-px flex-1 bg-gradient-to-r from-transparent via-border to-transparent" />
                        </motion.div>
                      )}
                      <div className="flex flex-col density-gap">
                        {groupPredictions.map((prediction, index) => (
                          <motion.div
                            key={prediction.id}
                            initial={{ opacity: 0, y: 4 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0 }}
                            transition={{
                              duration: 0.15,
                              delay: Math.min(index * 0.02, 0.1),
                              ease: "easeOut"
                            }}
                          >
                              <PlayerCard
                                prediction={prediction}
                                showCompare={isPastDate}
                                gameContext={prediction.gameContext}
                              />
                            </motion.div>
                          ))}
                      </div>
                    </motion.div>
                  ))}
                </motion.div>
              </AnimatePresence>
            ) : (
          <div className="rounded-xl border border-border bg-card p-12 text-center">
            <div className="relative mx-auto mb-4 w-fit">
              <div className="absolute inset-0 rounded-full bg-gradient-to-br from-primary/20 via-primary/10 to-transparent blur-xl" />
              <div className="relative w-20 h-20 rounded-full bg-gradient-to-br from-muted/80 to-muted/40 flex items-center justify-center">
                <Brain className="h-10 w-10 text-muted-foreground" />
              </div>
            </div>
            <h3 className="text-xl font-semibold text-foreground mb-2">No Predictions Found</h3>
            <p className="text-muted-foreground">
              Try adjusting your filters or selecting a different date.
            </p>
          </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
