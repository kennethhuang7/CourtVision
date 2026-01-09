import { useState } from 'react';
import { supabase } from '@/lib/supabase';
import { logger } from '@/lib/logger';
import { cacheManager } from '@/lib/cache';
import type { TrendFilters, Trend } from '@/types/trends';
import type { StatType } from '@/types/nba';
import type { ModelId } from '@/contexts/EnsembleContext';


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
  for (let i = stats.length - 1; i >= 0; i--) {
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

export function useTrends() {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const findTrends = async (
    filters: TrendFilters,
    selectedModels: ModelId[]
  ): Promise<Trend[]> => {
    setIsLoading(true);
    setError(null);

    try {
      const allStatTypes: StatType[] = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'threePointersMade'];
      const statTypes = filters.statType === 'all' ? allStatTypes : [filters.statType];

      const allTrends: Trend[] = [];

      
      for (const statType of statTypes) {
        const trends = await findTrendsForStat(statType, filters, selectedModels);
        allTrends.push(...trends);
      }

      
      allTrends.sort((a, b) => {
        if (b.consecutiveHits !== a.consecutiveHits) {
          return b.consecutiveHits - a.consecutiveHits;
        }
        if (b.totalGames !== a.totalGames) {
          return b.totalGames - a.totalGames;
        }
        return b.trendScore - a.trendScore;
      });

      return allTrends;
    } catch (err) {
      logger.error('Error finding trends', err as Error);
      setError(err as Error);
      return [];
    } finally {
      setIsLoading(false);
    }
  };

  return { findTrends, isLoading, error };
}


async function findTrendsForStat(
  statType: StatType,
  filters: TrendFilters,
  selectedModels: ModelId[]
): Promise<Trend[]> {
  try {
    
    const { data: upcomingGames, error: upcomingError } = await supabase
      .from('games')
      .select('game_id, game_date, home_team_id, away_team_id, season, game_status, game_type')
      .in('game_status', ['scheduled', 'upcoming', 'live'])
      .order('game_date', { ascending: true })
      .limit(100);

    if (upcomingError) throw upcomingError;
    if (!upcomingGames || upcomingGames.length === 0) return [];

    
    const earliestGameDate = upcomingGames[0].game_date;
    const targetDateStr = earliestGameDate.split('T')[0];

    
    const games = upcomingGames.filter(g => g.game_date.startsWith(targetDateStr));
    if (games.length === 0) return [];

    const gameIds = games.map(g => g.game_id);
    const teamIds = Array.from(new Set(games.flatMap(g => [g.home_team_id, g.away_team_id])));

    
    const { data: teams, error: teamsError } = await supabase
      .from('teams')
      .select('team_id, full_name, abbreviation')
      .in('team_id', teamIds);

    if (teamsError) throw teamsError;
    const teamsMap = new Map(teams?.map(t => [t.team_id, t]) || []);

    
    const { data: players, error: playersError } = await supabase
      .from('players')
      .select('player_id, full_name, position, team_id')
      .in('team_id', teamIds)
      .eq('is_active', true);

    if (playersError) throw playersError;
    if (!players || players.length === 0) return [];

    
    const { data: allPredictions, error: predError } = await supabase
      .from('predictions')
      .select('player_id, game_id, model_version, predicted_points, predicted_rebounds, predicted_assists, predicted_steals, predicted_blocks, predicted_turnovers, predicted_three_pointers_made, confidence_score')
      .in('game_id', gameIds)
      .in('model_version', selectedModels);

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

    
    const statColumn = getStatColumn(statType);
    const playerIds = players.map(p => p.player_id);

    const batchSize = 50;
    const allStatsRows: any[] = [];

    for (let i = 0; i < playerIds.length; i += batchSize) {
      const batch = playerIds.slice(i, i + batchSize);
      const { data: statsRows, error: statsError } = await supabase
        .from('player_game_stats')
        .select(`player_id, game_id, ${statColumn}, minutes_played, team_id`)
        .in('player_id', batch)
        .limit(5000);

      if (statsError) throw statsError;
      if (statsRows) allStatsRows.push(...statsRows);
    }

    
    const historicalGameIds = Array.from(new Set(allStatsRows.map(s => s.game_id)));
    const allGameRows: any[] = [];
    const gameBatchSize = 1000;

    for (let i = 0; i < historicalGameIds.length; i += gameBatchSize) {
      const batch = historicalGameIds.slice(i, i + gameBatchSize);
      const { data: gameRows, error: gameError } = await supabase
        .from('games')
        .select('game_id, game_date, home_team_id, away_team_id, game_status, season, game_type')
        .in('game_id', batch);

      if (gameError) throw gameError;
      if (gameRows) allGameRows.push(...gameRows);
    }

    const gamesMap = new Map(allGameRows.map(g => [g.game_id, g]));
    const targetGameType = games[0]?.game_type || 'regular_season';

    
    const filteredGames = allStatsRows
      .map(stat => ({ ...stat, games: gamesMap.get(stat.game_id) }))
      .filter(game => {
        if (!game.games) return false;
        if (game.games.game_status !== 'completed') return false;
        if (game.games.game_date.split('T')[0] >= targetDateStr) return false;
        
        if (game.games.game_type !== targetGameType) return false;
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
  } catch (err) {
    logger.error('Error finding trends for stat', err as Error);
    return [];
  }
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

  
  if (hitRate !== 100) return null;

  
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
  let passesRecentForm = true; 
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
        contextHitRate = contextResult.hitRate;
        contextHitCount = contextResult.hitCount;
        contextTotalGames = contextResult.totalGames;

        if (contextHitRate === 100) {
          passesHomeAway = true;
          trendContext = 'home-away'; 
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
        const h2hHitRate = h2hResult.hitRate;
        const h2hHitCount = h2hResult.hitCount;
        const h2hTotalGames = h2hResult.totalGames;

        if (h2hHitRate === 100) {
          passesH2H = true;
          trendContext = 'h2h'; 
          
          contextHitRate = h2hHitRate;
          contextHitCount = h2hHitCount;
          contextTotalGames = h2hTotalGames;
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
    playerPhotoUrl: `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${topPlayer.player_id}.png`,
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
  };
}
