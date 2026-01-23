import { useState, useRef, useCallback } from 'react';
import { supabase } from '@/lib/supabase';
import { logger } from '@/lib/logger';
import { cacheManager } from '@/lib/cache';
import { withBackoff } from '@/lib/backoff';
import { getMostRecentDateWithPredictions } from '@/lib/dateUtils';
import type { TrendFilters, Trend } from '@/types/trends';
import type { StatType } from '@/types/nba';
import type { ModelId } from '@/contexts/EnsembleContext';

// Cached raw data structure
interface CachedTrendData {
  targetDateStr: string;
  games: any[];
  teamsMap: Map<number, any>;
  players: any[];
  predictionsMap: Map<number, any>;
  playerHistoryMap: Map<number, any[]>;
  gamesMap: Map<number, any>;
  targetGameType: string;
  targetSeason: string;
  selectedModels: ModelId[];
  lastFetched: number;
}

function getStatColumn(statType: StatType): string {
  const mapping: Record<StatType, string> = {
    points: 'points',
    rebounds: 'rebounds_total',
    assists: 'assists',
    steals: 'steals',
    blocks: 'blocks',
    turnovers: 'turnovers',
    threePointersMade: 'three_pointers_made',
  };
  return mapping[statType];
}


function getPredictionColumn(statType: StatType): string {
  const mapping: Record<StatType, string> = {
    points: 'predicted_points',
    rebounds: 'predicted_rebounds',
    assists: 'predicted_assists',
    steals: 'predicted_steals',
    blocks: 'predicted_blocks',
    turnovers: 'predicted_turnovers',
    threePointersMade: 'predicted_three_pointers_made',
  };
  return mapping[statType];
}


function getBufferForStatType(statType: StatType): number {
  const buffers: Record<StatType, number> = {
    points: 2.0,
    rebounds: 1.0,
    assists: 1.0,
    steals: 0.5,
    blocks: 0.5,
    turnovers: 0.5,
    threePointersMade: 0.5,
  };
  return buffers[statType];
}


function calculateLine(
  seasonAvg: number,
  aiPrediction: number,
  lineMethod: 'player-average' | 'ai-prediction',
  lineAdjustment: 'standard' | 'favorable' | 'custom',
  overUnder: 'over' | 'under',
  statType: StatType,
  customModifiers?: { [key: string]: number }
): number {
  
  let rawValue = lineMethod === 'player-average' ? seasonAvg : aiPrediction;

  
  if (lineAdjustment === 'favorable') {
    const buffer = getBufferForStatType(statType);
    if (overUnder === 'over') {
      rawValue -= buffer; 
    } else {
      rawValue += buffer; 
    }
  } else if (lineAdjustment === 'custom' && customModifiers) {
    const buffer = customModifiers[statType] ?? 0;
    if (overUnder === 'over') {
      rawValue -= buffer; 
    } else {
      rawValue += buffer; 
    }
  }

  
  let finalLine: number;
  if (lineAdjustment === 'standard') {
    
    finalLine = Math.round(rawValue * 2) / 2;
  } else {
    
    if (overUnder === 'over') {
      finalLine = Math.floor(rawValue * 2) / 2; 
    } else {
      finalLine = Math.ceil(rawValue * 2) / 2; 
    }
  }

  
  return Math.max(0.5, finalLine);
}


function calculateHitRate(
  stats: number[],
  line: number,
  overUnder: 'over' | 'under'
): { hitRate: number; hitCount: number; totalGames: number } {
  if (stats.length === 0) {
    return { hitRate: 0, hitCount: 0, totalGames: 0 };
  }

  const hits = stats.filter(stat =>
    overUnder === 'over' ? stat > line : stat < line
  ).length;

  return {
    hitRate: (hits / stats.length) * 100,
    hitCount: hits,
    totalGames: stats.length,
  };
}


function calculateConsecutiveHits(
  stats: number[],
  line: number,
  overUnder: 'over' | 'under'
): number {
  let consecutive = 0;
  // Iterate from most recent (index 0) to oldest (end of array)
  // since stats are sorted most recent first
  for (let i = 0; i < stats.length; i++) {
    const stat = stats[i];
    const hit = overUnder === 'over' ? stat > line : stat < line;
    if (hit) {
      consecutive++;
    } else {
      break;
    }
  }
  return consecutive;
}


function calculateTrendScore(
  hitRate: number,
  consecutiveHits: number,
  timeWindow: number
): number {
  
  const hitRateScore = hitRate * 0.6;

  
  const streakScore = (consecutiveHits / timeWindow) * 40;

  return Math.min(Math.round(hitRateScore + streakScore), 100);
}

const TRENDS_DATA_KEY = 'trends-cached-data';

export function useTrends() {
  const [isLoading, setIsLoading] = useState(false);
  const [isFetchingData, setIsFetchingData] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  // Load data from localStorage
  const loadCachedData = useCallback((): CachedTrendData | null => {
    try {
      const stored = localStorage.getItem(TRENDS_DATA_KEY);
      if (!stored) return null;
      
      const parsed = JSON.parse(stored);
      // Convert Maps back from arrays/objects
      const teamsMap = new Map(parsed.teamsMap || []);
      const predictionsMap = new Map(parsed.predictionsMap || []);
      const playerHistoryMap = new Map(
        Object.entries(parsed.playerHistoryMap || {}).map(([k, v]: [string, any]) => [
          Number(k),
          v
        ])
      );
      const gamesMap = new Map(
        Object.entries(parsed.gamesMap || {}).map(([k, v]: [string, any]) => [
          Number(k),
          v
        ])
      );
      
      return {
        ...parsed,
        teamsMap,
        predictionsMap,
        playerHistoryMap,
        gamesMap,
      };
    } catch (err) {
      logger.warn('Error loading cached trends data', err as Error);
      localStorage.removeItem(TRENDS_DATA_KEY); // Clear corrupted data
      return null;
    }
  }, []);

  // Save data to localStorage
  const saveCachedData = useCallback((data: CachedTrendData) => {
    try {
      // Convert Maps to arrays/objects for JSON serialization
      const serializable = {
        targetDateStr: data.targetDateStr,
        games: data.games,
        teamsMap: Array.from(data.teamsMap.entries()),
        players: data.players,
        predictionsMap: Array.from(data.predictionsMap.entries()),
        playerHistoryMap: Object.fromEntries(
          Array.from(data.playerHistoryMap.entries()).map(([k, v]) => [String(k), v])
        ),
        gamesMap: Object.fromEntries(
          Array.from(data.gamesMap.entries()).map(([k, v]) => [String(k), v])
        ),
        targetGameType: data.targetGameType,
        targetSeason: data.targetSeason,
        selectedModels: data.selectedModels,
        lastFetched: data.lastFetched,
      };
      localStorage.setItem(TRENDS_DATA_KEY, JSON.stringify(serializable));
    } catch (err) {
      logger.warn('Error saving cached trends data', err as Error);
    }
  }, []);

  // Clear cached data
  const clearCachedData = useCallback(() => {
    localStorage.removeItem(TRENDS_DATA_KEY);
  }, []);

  // Fetch and cache all raw data
  const fetchTrendData = useCallback(async (
    selectedModels: ModelId[],
    onProgress?: (stage: string, progress: number) => void,
    forceRefresh: boolean = false
  ): Promise<CachedTrendData | null> => {
    setIsFetchingData(true);
    setError(null);

    try {
      // Check localStorage first (unless forcing refresh)
      if (!forceRefresh) {
        const cached = loadCachedData();
        if (cached && cached.selectedModels.join(',') === selectedModels.join(',')) {
          return cached;
        }
      }

      if (onProgress) onProgress('Fetching upcoming games...', 5);

      const { data: upcomingGames, error: upcomingError } = await withBackoff(
        () => supabase
          .from('games')
          .select('game_id, game_date, home_team_id, away_team_id, season, game_status, game_type')
          .in('game_status', ['scheduled', 'upcoming', 'live'])
          .order('game_date', { ascending: true })
          .limit(100)
          .then(result => {
            if (result.error) throw result.error;
            return result;
          }),
        { initialDelay: 2000, maxDelay: 15000, maxRetries: 3 },
        (attempt, delay) => {
          logger.warn(`Trends games query retry ${attempt} after ${Math.ceil(delay / 1000)}s`);
        }
      );

      if (upcomingError) throw upcomingError;
      if (!upcomingGames || upcomingGames.length === 0) return null;

      // Determine target date
      const today = new Date();
      const todayStr = today.toISOString().split('T')[0];
      
      let gamesToday = upcomingGames.filter(g => {
        const gameDateStr = g.game_date.split('T')[0];
        return gameDateStr === todayStr;
      });
      
      let targetDateStr: string;
      let gamesToUse: typeof upcomingGames;
      
      if (gamesToday.length > 0) {
        targetDateStr = todayStr;
        gamesToUse = gamesToday;
      } else {
        const mostRecentDate = await getMostRecentDateWithPredictions();
        
        if (mostRecentDate) {
          const mostRecentStr = mostRecentDate.toISOString().split('T')[0];
          gamesToUse = upcomingGames.filter(g => {
            const gameDateStr = g.game_date.split('T')[0];
            return gameDateStr === mostRecentStr;
          });
          
          if (gamesToUse.length > 0) {
            targetDateStr = mostRecentStr;
          } else {
            targetDateStr = upcomingGames[0].game_date.split('T')[0];
            gamesToUse = upcomingGames.filter(g => g.game_date.startsWith(targetDateStr));
          }
        } else {
          targetDateStr = upcomingGames[0].game_date.split('T')[0];
          gamesToUse = upcomingGames.filter(g => g.game_date.startsWith(targetDateStr));
        }
      }
      
      if (gamesToUse.length === 0) return null;

      const gameIds = gamesToUse.map(g => g.game_id);
      const teamIds = Array.from(new Set(gamesToUse.flatMap(g => [g.home_team_id, g.away_team_id])));

      if (onProgress) onProgress('Fetching teams and players...', 15);

      const [teamsResult, playersResult] = await Promise.all([
        withBackoff(
          () => supabase
            .from('teams')
            .select('team_id, full_name, abbreviation')
            .in('team_id', teamIds)
            .then(result => {
              if (result.error) throw result.error;
              return result;
            }),
          { initialDelay: 2000, maxDelay: 15000, maxRetries: 3 },
          (attempt, delay) => {
            logger.warn(`Trends teams query retry ${attempt} after ${Math.ceil(delay / 1000)}s`);
          }
        ),
        withBackoff(
          () => supabase
            .from('players')
            .select('player_id, full_name, position, team_id')
            .in('team_id', teamIds)
            .eq('is_active', true)
            .then(result => {
              if (result.error) throw result.error;
              return result;
            }),
          { initialDelay: 2000, maxDelay: 15000, maxRetries: 3 },
          (attempt, delay) => {
            logger.warn(`Trends players query retry ${attempt} after ${Math.ceil(delay / 1000)}s`);
          }
        )
      ]);

      const { data: teams, error: teamsError } = teamsResult;
      if (teamsError) throw teamsError;
      const teamsMap = new Map(teams?.map(t => [t.team_id, t]) || []);

      const { data: players, error: playersError } = playersResult;
      if (playersError) throw playersError;
      if (!players || players.length === 0) return null;

      if (onProgress) onProgress('Fetching predictions...', 30);

      const { data: allPredictions, error: predError } = await withBackoff(
        () => supabase
          .from('predictions')
          .select('player_id, game_id, model_version, predicted_points, predicted_rebounds, predicted_assists, predicted_steals, predicted_blocks, predicted_turnovers, predicted_three_pointers_made, confidence_score')
          .in('game_id', gameIds)
          .in('model_version', selectedModels)
          .then(result => {
            if (result.error) throw result.error;
            return result;
          }),
        { initialDelay: 2000, maxDelay: 15000, maxRetries: 3 },
        (attempt, delay) => {
          logger.warn(`Trends predictions query retry ${attempt} after ${Math.ceil(delay / 1000)}s`);
        }
      );

      if (predError) throw predError;

      const predictionsMap = new Map<number, any>();
      if (allPredictions) {
        const aggMap = new Map<number, { sum: any; count: number }>();

        for (const pred of allPredictions) {
          const existing = aggMap.get(pred.player_id);
          const values = {
            predicted_points: Number(pred.predicted_points ?? 0),
            predicted_rebounds: Number(pred.predicted_rebounds ?? 0),
            predicted_assists: Number(pred.predicted_assists ?? 0),
            predicted_steals: Number(pred.predicted_steals ?? 0),
            predicted_blocks: Number(pred.predicted_blocks ?? 0),
            predicted_turnovers: Number(pred.predicted_turnovers ?? 0),
            predicted_three_pointers_made: Number(pred.predicted_three_pointers_made ?? 0),
            confidence_score: Number(pred.confidence_score ?? 0),
          };

          if (!existing) {
            aggMap.set(pred.player_id, { sum: { ...values }, count: 1 });
          } else {
            Object.keys(values).forEach(key => {
              existing.sum[key] += values[key];
            });
            existing.count += 1;
          }
        }

        for (const [playerId, agg] of aggMap.entries()) {
          const avgPred: any = {};
          Object.keys(agg.sum).forEach(key => {
            avgPred[key] = agg.sum[key] / agg.count;
          });
          predictionsMap.set(playerId, avgPred);
        }
      }

      if (onProgress) onProgress('Fetching player stats...', 50);

      const playerIds = players.map(p => p.player_id);
      const batchSize = 50;
      const allStatsRows: any[] = [];

      // Fetch stats for all stat types at once
      const allStatColumns = ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made'];
      const statColumnsStr = allStatColumns.join(', ');

      for (let i = 0; i < playerIds.length; i += batchSize) {
        const batch = playerIds.slice(i, i + batchSize);
        
        try {
          const { data: statsRows, error: statsError } = await withBackoff(
            () => supabase
              .from('player_game_stats')
              .select(`player_id, game_id, ${statColumnsStr}, minutes_played, team_id`)
              .in('player_id', batch)
              .limit(5000)
              .then(result => {
                if (result.error) throw result.error;
                return result;
              }),
            { initialDelay: 2000, maxDelay: 15000, maxRetries: 3 },
            (attempt, delay) => {
              logger.warn(`Trends stats batch ${i / batchSize + 1} retry ${attempt} after ${Math.ceil(delay / 1000)}s`);
            }
          );

          if (statsError) throw statsError;
          if (statsRows) allStatsRows.push(...statsRows);
        } catch (err) {
          const error = err as Error;
          const isRateLimit = error.message?.includes('Rate limited') || error.message?.includes('429');
          if (isRateLimit) {
            throw error;
          }
          logger.warn(`Error fetching trends stats batch ${i / batchSize + 1}, continuing...`, error);
        }
      }

      if (onProgress) onProgress('Fetching historical games...', 75);

      const historicalGameIds = Array.from(new Set(allStatsRows.map(s => s.game_id)));
      const allGameRows: any[] = [];
      const gameBatchSize = 1000;

      for (let i = 0; i < historicalGameIds.length; i += gameBatchSize) {
        const batch = historicalGameIds.slice(i, i + gameBatchSize);
        
        try {
          const { data: gameRows, error: gameError } = await withBackoff(
            () => supabase
              .from('games')
              .select('game_id, game_date, home_team_id, away_team_id, game_status, season, game_type')
              .in('game_id', batch)
              .then(result => {
                if (result.error) throw result.error;
                return result;
              }),
            { initialDelay: 2000, maxDelay: 15000, maxRetries: 3 },
            (attempt, delay) => {
              logger.warn(`Trends games batch ${i / gameBatchSize + 1} retry ${attempt} after ${Math.ceil(delay / 1000)}s`);
            }
          );

          if (gameError) throw gameError;
          if (gameRows) allGameRows.push(...gameRows);
        } catch (err) {
          const error = err as Error;
          const isRateLimit = error.message?.includes('Rate limited') || error.message?.includes('429');
          if (isRateLimit) {
            throw error;
          }
          logger.warn(`Error fetching trends games batch ${i / gameBatchSize + 1}, continuing...`, error);
        }
      }

      const gamesMap = new Map(allGameRows.map(g => [g.game_id, g]));
      const targetGameType = gamesToUse[0]?.game_type || 'regular_season';
      const targetSeason = gamesToUse[0]?.season;

      // Filter and organize player history
      const filteredGames = allStatsRows
        .map(stat => ({ ...stat, games: gamesMap.get(stat.game_id) }))
        .filter(game => {
          if (!game.games) return false;
          if (game.games.game_status !== 'completed') return false;
          if (game.games.game_date.split('T')[0] >= targetDateStr) return false;
          
          if (game.games.game_type !== targetGameType) return false;
          
          if (targetSeason && game.games.season !== targetSeason) return false;
          
          return true;
        });

      const playerHistoryMap = new Map<number, any[]>();
      for (const game of filteredGames) {
        if (!playerHistoryMap.has(game.player_id)) {
          playerHistoryMap.set(game.player_id, []);
        }
        playerHistoryMap.get(game.player_id)!.push(game);
      }

      for (const [playerId, games] of playerHistoryMap.entries()) {
        games.sort((a, b) => {
          const dateA = new Date(a.games.game_date).getTime();
          const dateB = new Date(b.games.game_date).getTime();
          return dateB - dateA;
        });
      }

      if (onProgress) onProgress('Data loaded', 100);

      const cachedData: CachedTrendData = {
        targetDateStr,
        games: gamesToUse,
        teamsMap,
        players,
        predictionsMap,
        playerHistoryMap,
        gamesMap,
        targetGameType,
        targetSeason,
        selectedModels: [...selectedModels],
        lastFetched: Date.now(),
      };

      // Save to localStorage
      saveCachedData(cachedData);
      return cachedData;
    } catch (err) {
      const error = err as Error;
      logger.error('Error fetching trend data', error);
      setError(error);
      return null;
    } finally {
      setIsFetchingData(false);
    }
  }, []);

  // Calculate trends from cached data
  const calculateTrends = useCallback((
    cachedData: CachedTrendData,
    filters: TrendFilters,
    onProgress?: (stage: string, progress: number) => void
  ): Trend[] => {
    const allStatTypes: StatType[] = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'threePointersMade'];
    const statTypes = filters.statType === 'all' ? allStatTypes : [filters.statType];

    const allTrends: Trend[] = [];
    const totalStats = statTypes.length;

    for (let i = 0; i < statTypes.length; i++) {
      const statType = statTypes[i];
      if (onProgress) {
        onProgress(`Calculating trends for ${statType}...`, (i / totalStats) * 100);
      }

      try {
        const trends = calculateTrendsForStat(statType, cachedData, filters);
        allTrends.push(...trends);
      } catch (err) {
        logger.error(`Error calculating trends for ${statType}`, err as Error);
      }
    }

    // Sort trends
    allTrends.sort((a, b) => {
      if (b.consecutiveHits !== a.consecutiveHits) {
        return b.consecutiveHits - a.consecutiveHits;
      }
      if (b.totalGames !== a.totalGames) {
        return b.totalGames - a.totalGames;
      }
      return b.trendScore - a.trendScore;
    });

    if (onProgress) {
      onProgress('Completed', 100);
    }

    return allTrends;
  }, []);

  // Main function that loads from localStorage or fetches, then calculates trends
  const findTrends = useCallback(async (
    filters: TrendFilters,
    selectedModels: ModelId[],
    onProgress?: (stage: string, progress: number) => void,
    forceRefresh: boolean = false
  ): Promise<Trend[]> => {
    setIsLoading(true);
    setError(null);

    try {
      // Load from localStorage or fetch new data
      let cachedData = loadCachedData();
      
      // Check if we need to refetch (different models or force refresh)
      const needsRefetch = forceRefresh || 
        !cachedData || 
        cachedData.selectedModels.join(',') !== selectedModels.join(',');
      
      if (needsRefetch) {
        cachedData = await fetchTrendData(selectedModels, onProgress, forceRefresh);
        if (!cachedData) return [];
      }

      // Calculate trends from cached data
      const trends = calculateTrends(cachedData!, filters, onProgress);
      return trends;
    } catch (err) {
      const error = err as Error;
      logger.error('Error finding trends', error);
      setError(error);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, [fetchTrendData, calculateTrends, loadCachedData]);

  return { 
    findTrends, 
    isLoading: isLoading || isFetchingData, 
    error,
    clearCache: clearCachedData,
  };
}

// Calculate trends for a single stat type from cached data
function calculateTrendsForStat(
  statType: StatType,
  cachedData: CachedTrendData,
  filters: TrendFilters
): Trend[] {
  const { games, teamsMap, players, predictionsMap, playerHistoryMap, targetGameType, targetSeason } = cachedData;
  const statColumn = getStatColumn(statType);
  const trends: Trend[] = [];

  for (const game of games) {
    const gamePlayers = players.filter(
      p => p.team_id === game.home_team_id || p.team_id === game.away_team_id
    );

    for (const player of gamePlayers) {
      const playerHistory = playerHistoryMap.get(player.player_id);
      if (!playerHistory || playerHistory.length < filters.minStreak) continue;

      const prediction = predictionsMap.get(player.player_id);

      if (!prediction || !prediction.confidence_score || Number(prediction.confidence_score) <= 0) {
        continue;
      }

      const directions: ('over' | 'under')[] =
        filters.overUnder === 'both' ? ['over', 'under'] : [filters.overUnder];

      for (const direction of directions) {
        const trend = evaluateTrend(
          player,
          game,
          teamsMap,
          playerHistory,
          prediction,
          statType,
          direction,
          filters
        );

        if (trend) trends.push(trend);
      }
    }
  }

  return trends;
}

function evaluateTrend(
  player: any,
  game: any,
  teamsMap: Map<number, any>,
  playerHistory: any[],
  prediction: any | undefined,
  statType: StatType,
  overUnder: 'over' | 'under',
  filters: TrendFilters
): Trend | null {
  const isHome = player.team_id === game.home_team_id;
  const opponentTeamId = isHome ? game.away_team_id : game.home_team_id;

  const playerTeam = teamsMap.get(player.team_id);
  const opponentTeam = teamsMap.get(opponentTeamId);

  if (!playerTeam || !opponentTeam) return null;

  
  if (filters.playerSearch && !player.full_name.toLowerCase().includes(filters.playerSearch.toLowerCase())) {
    return null;
  }
  if (filters.teams && filters.teams.length > 0 && !filters.teams.includes(playerTeam.abbreviation)) {
    return null;
  }
  if (filters.opponents && filters.opponents.length > 0 && !filters.opponents.includes(opponentTeam.abbreviation)) {
    return null;
  }

  const statColumn = getStatColumn(statType);

  
  const seasonStats = playerHistory.map(g => Number(g[statColumn]) || 0);
  const seasonAvg = seasonStats.reduce((sum, val) => sum + val, 0) / seasonStats.length;

  
  const predictionColumn = getPredictionColumn(statType);
  const aiPredictionValue = prediction ? Number(prediction[predictionColumn]) || seasonAvg : seasonAvg;

  
  const line = calculateLine(
    seasonAvg,
    aiPredictionValue,
    filters.lineMethod,
    filters.lineAdjustment,
    overUnder,
    statType,
    filters.customModifiers
  );

  
  const allStats = seasonStats; 
  const consecutiveHits = calculateConsecutiveHits(allStats, line, overUnder);

  
  if (consecutiveHits < filters.minStreak) return null;

  
  const streakStats = allStats.slice(0, consecutiveHits);
  const { hitRate, hitCount, totalGames } = calculateHitRate(streakStats, line, overUnder);

  
  // Require at least 80% hit rate on the streak (less strict than 100%)
  if (hitRate < 80) return null;

  
  if (filters.requireAiAgreement) {
    if (overUnder === 'over' && aiPredictionValue <= line) {
      return null; 
    }
    if (overUnder === 'under' && aiPredictionValue >= line) {
      return null; 
    }
  }

  
  const hasTrendTypeFilter = filters.trendTypes && filters.trendTypes.length > 0;

  
  let contextHitRate: number | undefined;
  let contextHitCount: number | undefined;
  let contextTotalGames: number | undefined;
  let passesHomeAway = false;
  let passesH2H = false;
  
  const recentFormStats = allStats.slice(0, consecutiveHits);
  const recentFormResult = calculateHitRate(recentFormStats, line, overUnder);
  const passesRecentForm = recentFormResult.hitRate >= 80 && consecutiveHits >= filters.minStreak;
  
  let trendContext: 'h2h' | 'home-away' | 'recent-form' = 'recent-form'; 

  if (!hasTrendTypeFilter || filters.trendTypes.includes('home-away')) {
    const contextGames = playerHistory
      .filter(g => (g.team_id === g.games.home_team_id) === isHome);

    if (contextGames.length >= filters.minStreak) {
      const contextStats = contextGames.map(g => Number(g[statColumn]) || 0);
      const contextConsecutive = calculateConsecutiveHits(contextStats, line, overUnder);

      if (contextConsecutive >= filters.minStreak) {
        const contextStreakStats = contextStats.slice(0, contextConsecutive);
        const contextResult = calculateHitRate(contextStreakStats, line, overUnder);
        
        if (contextResult.hitRate >= 80) {
          passesHomeAway = true;
          if (!passesH2H) {
            trendContext = 'home-away';
            contextHitRate = contextResult.hitRate;
            contextHitCount = contextResult.hitCount;
            contextTotalGames = contextResult.totalGames;
          }
        }
      }
    }
  }

  
  if (!hasTrendTypeFilter || filters.trendTypes.includes('h2h')) {
    const h2hGames = playerHistory
      .filter(g => {
        const gameOpponentId = g.team_id === g.games.home_team_id ? g.games.away_team_id : g.games.home_team_id;
        return gameOpponentId === opponentTeamId;
      });

    if (h2hGames.length >= filters.minStreak) {
      const h2hStats = h2hGames.map(g => Number(g[statColumn]) || 0);
      const h2hConsecutive = calculateConsecutiveHits(h2hStats, line, overUnder);

      if (h2hConsecutive >= filters.minStreak) {
        const h2hStreakStats = h2hStats.slice(0, h2hConsecutive);
        const h2hResult = calculateHitRate(h2hStreakStats, line, overUnder);

        if (h2hResult.hitRate >= 80) {
          passesH2H = true;
          trendContext = 'h2h';
          contextHitRate = h2hResult.hitRate;
          contextHitCount = h2hResult.hitCount;
          contextTotalGames = h2hResult.totalGames;
        }
      }
    }
  }

  
  if (hasTrendTypeFilter) {
    const passes =
      (filters.trendTypes.includes('recent-form') && passesRecentForm) ||
      (filters.trendTypes.includes('h2h') && passesH2H) ||
      (filters.trendTypes.includes('home-away') && passesHomeAway);

    if (!passes) return null;
  }

  
  const trendScore = calculateTrendScore(hitRate, consecutiveHits, consecutiveHits);

  
  const direction = overUnder === 'over' ? 'Over' : 'Under';
  let trendLabel: string;

  if (trendContext === 'h2h') {
    trendLabel = `${direction} in last ${consecutiveHits} games vs ${opponentTeam.abbreviation}`;
  } else if (trendContext === 'home-away') {
    const location = isHome ? 'home' : 'away';
    trendLabel = `${direction} in last ${consecutiveHits} ${location} games`;
  } else {
    trendLabel = `${direction} in last ${consecutiveHits} games`;
  }

  
  const confidence = prediction ? Number(prediction.confidence_score) || undefined : undefined;

  return {
    playerId: player.player_id.toString(),
    playerName: player.full_name,
    playerPhotoUrl: `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${player.player_id}.png`,
    position: player.position || 'N/A',
    team: playerTeam.full_name,
    teamAbbr: playerTeam.abbreviation,
    gameId: game.game_id,
    opponent: opponentTeam.full_name,
    opponentAbbr: opponentTeam.abbreviation,
    isHome,
    gameDate: game.game_date,
    statType,
    overUnder,
    line,
    lineMethod: filters.lineMethod,
    lineAdjustment: filters.lineAdjustment,
    aiPrediction: aiPredictionValue,
    confidence,
    hitRate,
    hitCount,
    totalGames,
    consecutiveHits,
    contextHitRate,
    contextHitCount,
    contextTotalGames,
    trendScore,
    trendLabel,
    gameType: game.game_type,
    season: game.season,
  };
}
