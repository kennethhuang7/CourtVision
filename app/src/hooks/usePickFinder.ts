import { useState } from 'react';
import { supabase } from '@/lib/supabase';
import { logger } from '@/lib/logger';
import type { PickFinderFilters, PickResult } from '@/types/pickFinder';
import type { StatType } from '@/types/nba';
import type { ModelId } from '@/contexts/EnsembleContext';


function mapPositionToDefensePosition(position: string | null | undefined): string {
  if (!position) return 'G';
  const posStr = position.toUpperCase();
  if (posStr.includes('C') && !posStr.includes('G') && !posStr.includes('F')) {
    return 'C';
  } else if (posStr.includes('F') && !posStr.includes('G')) {
    return 'F';
  }
  return 'G';
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


function calculateStrengthScore(
  hitRate: number,
  contextHitRate: number | undefined,
  aiMargin: number,
  confidence: number,
  defenseRank: number | undefined,
  paceBonus: number | undefined
): { score: number; breakdown: any } {
  
  const hitRateScore = Math.max(0, ((hitRate - 50) / 50) * 30);

  
  const contextScore = contextHitRate !== undefined
    ? Math.max(0, ((contextHitRate - 50) / 50) * 15)
    : 0;

  
  const aiMarginScore = Math.min(Math.abs(aiMargin) / 8.0, 1.0) * 25;

  
  const confidenceScore = (confidence / 100) * 15;

  
  const defenseScore = defenseRank !== undefined
    ? ((31 - defenseRank) / 30) * 10
    : 0;

  
  const paceScore = paceBonus !== undefined
    ? Math.min(Math.max(paceBonus, 0) / 10, 1.0) * 5
    : 0;

  const totalScore = Math.min(
    Math.round(hitRateScore + contextScore + aiMarginScore + confidenceScore + defenseScore + paceScore),
    100
  );

  return {
    score: totalScore,
    breakdown: {
      hitRate: Math.round(hitRateScore),
      contextHitRate: contextScore > 0 ? Math.round(contextScore) : undefined,
      aiMargin: Math.round(aiMarginScore),
      confidence: Math.round(confidenceScore),
      defenseRank: Math.round(defenseScore),
      pace: paceScore > 0 ? Math.round(paceScore) : undefined,
    },
  };
}

export function usePickFinder() {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const findPicks = async (
    filters: PickFinderFilters,
    selectedModels: ModelId[],
    onProgress?: (stage: string, progress: number) => void
  ): Promise<PickResult[]> => {
    setIsLoading(true);
    setError(null);

    try {
      const allStatTypes: StatType[] = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'threePointersMade'];

      
      if (filters.statType === 'all') {
        const allResults: PickResult[] = [];

        
        if (filters.overUnder === 'both') {
          const totalIterations = allStatTypes.length * 2;
          let currentIteration = 0;

          for (const statType of allStatTypes) {
            const iterationSize = 90 / totalIterations;

            const overResults = await findPicksForDirection(
              { ...filters, statType, overUnder: 'over' },
              selectedModels,
              (stage, stageProgress) => {
                
                const baseProgress = currentIteration * iterationSize;
                const scaledProgress = baseProgress + (stageProgress / 100) * iterationSize;
                onProgress?.(stage, scaledProgress);
              },
              currentIteration === 0 
            );
            allResults.push(...overResults);
            currentIteration++;

            const underResults = await findPicksForDirection(
              { ...filters, statType, overUnder: 'under' },
              selectedModels,
              (stage, stageProgress) => {
                const baseProgress = currentIteration * iterationSize;
                const scaledProgress = baseProgress + (stageProgress / 100) * iterationSize;
                onProgress?.(stage, scaledProgress);
              },
              false 
            );
            allResults.push(...underResults);
            currentIteration++;
          }
        } else {
          const totalIterations = allStatTypes.length;
          let currentIteration = 0;

          for (const statType of allStatTypes) {
            const iterationSize = 90 / totalIterations;

            const results = await findPicksForDirection(
              { ...filters, statType },
              selectedModels,
              (stage, stageProgress) => {
                
                const baseProgress = currentIteration * iterationSize;
                const scaledProgress = baseProgress + (stageProgress / 100) * iterationSize;
                onProgress?.(stage, scaledProgress);
              },
              currentIteration === 0 
            );
            allResults.push(...results);
            currentIteration++;
          }
        }

        onProgress?.('scoring', 90);
        allResults.sort((a, b) => b.strengthScore - a.strengthScore);

        
        onProgress?.('scoring', 100);
        await new Promise(resolve => setTimeout(resolve, 400));

        return allResults;
      }

      
      if (filters.overUnder === 'both') {
        const overResults = await findPicksForDirection(
          { ...filters, overUnder: 'over' },
          selectedModels,
          (stage, stageProgress) => {
            
            const scaledProgress = (stageProgress / 100) * 45;
            onProgress?.(stage, scaledProgress);
          },
          true 
        );

        const underResults = await findPicksForDirection(
          { ...filters, overUnder: 'under' },
          selectedModels,
          (stage, stageProgress) => {
            
            const scaledProgress = 45 + (stageProgress / 100) * 45;
            onProgress?.(stage, scaledProgress);
          },
          false 
        );

        onProgress?.('scoring', 90);
        const combined = [...overResults, ...underResults];
        combined.sort((a, b) => b.strengthScore - a.strengthScore);

        
        onProgress?.('scoring', 100);
        await new Promise(resolve => setTimeout(resolve, 400));

        return combined;
      }

      const results = await findPicksForDirection(filters, selectedModels, onProgress, true);

      
      onProgress?.('scoring', 100);
      await new Promise(resolve => setTimeout(resolve, 400));

      return results;
    } catch (err) {
      logger.error('Error finding picks', err as Error);
      setError(err as Error);
      return [];
    } finally {
      setIsLoading(false);
    }
  };

  return { findPicks, isLoading, error };
}


async function findPicksForDirection(
  filters: PickFinderFilters,
  selectedModels: ModelId[],
  onProgress?: (stage: string, progress: number) => void,
  isFirstIteration: boolean = true
): Promise<PickResult[]> {
  try {
      const { statType, overUnder, lineMethod, lineAdjustment } = filters;

      
      const reportProgress = (actualStage: string, progress: number) => {
        const stage = isFirstIteration ? actualStage : 'filtering';
        onProgress?.(stage, progress);
      };

      
      reportProgress('games', 5);
      
      const { data: upcomingGames, error: upcomingError } = await supabase
        .from('games')
        .select('game_id, game_date, home_team_id, away_team_id, season, game_status')
        .in('game_status', ['scheduled', 'upcoming', 'live']) 
        .order('game_date', { ascending: true })
        .limit(100); 

      if (upcomingError) throw upcomingError;
      if (!upcomingGames || upcomingGames.length === 0) {
        return [];
      }

      
      const earliestGameDate = upcomingGames[0].game_date;
      const targetDateStr = earliestGameDate.split('T')[0]; 

      
      const games = upcomingGames.filter(g => g.game_date.startsWith(targetDateStr));

      if (games.length === 0) {
        return [];
      }

      const gameIds = games.map(g => g.game_id);

      
      reportProgress('players', 10);
      const { data: predictionRows, error: predError } = await supabase
        .from('predictions')
        .select('game_id, player_id')
        .in('game_id', gameIds)
        .limit(5000);

      if (predError) throw predError;
      if (!predictionRows || predictionRows.length === 0) {
        return [];
      }

      const season = games[0].season;

      
      const teamIds = Array.from(new Set(games.flatMap(g => [g.home_team_id, g.away_team_id])));
      const { data: teams, error: teamsError } = await supabase
        .from('teams')
        .select('team_id, full_name, abbreviation')
        .in('team_id', teamIds);

      if (teamsError) throw teamsError;

      
      const { data: teamRatings, error: ratingsError } = await supabase
        .from('team_ratings')
        .select('team_id, pace, def_rating')
        .in('team_id', teamIds)
        .eq('season', season);

      if (ratingsError) {
        logger.error('Error fetching team ratings', ratingsError);
        
      }

      
      
      const ratingsWithRanks = (teamRatings || [])
        .filter(r => r.def_rating !== null && r.def_rating !== undefined)
        .sort((a, b) => (a.def_rating || 999) - (b.def_rating || 999))
        .map((r, index) => ({
          ...r,
          defense_rank: index + 1
        }));

      
      const teamRatingsMap = new Map(
        ratingsWithRanks.map(r => [r.team_id, r])
      );

      
      const teamsMap = new Map(
        (teams || []).map(t => {
          const ratings = teamRatingsMap.get(t.team_id);
          return [
            t.team_id,
            {
              ...t,
              pace: ratings?.pace,
              defense_rank: ratings?.defense_rank,
              def_rating: ratings?.def_rating
            }
          ];
        })
      );

      
      const { data: players, error: playersError } = await supabase
        .from('players')
        .select('player_id, full_name, position, team_id')
        .in('team_id', teamIds)
        .eq('is_active', true);

      if (playersError) throw playersError;
      if (!players || players.length === 0) {
        return [];
      }

      
      
      reportProgress('predictions', 15);
      const { data: allPredictions, error: allPredictionsError } = await supabase
        .from('predictions')
        .select('player_id, game_id, model_version, predicted_points, predicted_rebounds, predicted_assists, predicted_steals, predicted_blocks, predicted_turnovers, predicted_three_pointers_made, confidence_score')
        .in('game_id', gameIds)
        .in('model_version', selectedModels); 

      if (allPredictionsError) throw allPredictionsError;

      
      const playerIdsWithPredictions = Array.from(new Set((allPredictions || []).map(p => p.player_id)));

      
      reportProgress('history', 20);

      
      
      
      const statColumn = getStatColumn(statType as StatType);

      
      
      
      
      
      const batchSize = 25; 
      const maxGamesPerBatch = 5000; 
      const allStatsRows: any[] = [];

      for (let i = 0; i < playerIdsWithPredictions.length; i += batchSize) {
        const batch = playerIdsWithPredictions.slice(i, i + batchSize);
        const { data: statsRows, error: statsError } = await supabase
          .from('player_game_stats')
          .select(`player_id, game_id, ${statColumn}, minutes_played, team_id`)
          .in('player_id', batch)
          .limit(maxGamesPerBatch); 

        if (statsError) {
          logger.error('Error fetching player_game_stats', statsError);
          throw statsError;
        }

        if (statsRows) {
          allStatsRows.push(...statsRows);
        }

        
        const batchProgress = 20 + (5 * ((i + batchSize) / playerIdsWithPredictions.length));
        reportProgress('history', Math.min(batchProgress, 25));
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

        if (gameError) {
          logger.error('Error fetching games', gameError);
          throw gameError;
        }

        if (gameRows) {
          allGameRows.push(...gameRows);
        }
      }

      
      const gamesMap = new Map(allGameRows.map(g => [g.game_id, g]));

      
      reportProgress('history', 30);

      
      const targetGameType = games[0]?.game_type || 'regular_season';

      
      const filteredGames = allStatsRows
        .map(stat => ({
          ...stat,
          games: gamesMap.get(stat.game_id),
        }))
        .filter(game => {
          if (!game.games) return false;
          
          if (game.games.game_status !== 'completed') return false;
          
          const gameDateStr = game.games.game_date.split('T')[0];
          if (gameDateStr >= targetDateStr) return false;

          
          if (filters.separatePlayoffStats) {
            
            
            if (game.games.game_type !== targetGameType) return false;
          }

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

      
      
      const aggMap = new Map<number, {
        sum: {
          predicted_points: number;
          predicted_rebounds: number;
          predicted_assists: number;
          predicted_steals: number;
          predicted_blocks: number;
          predicted_turnovers: number;
          predicted_three_pointers_made: number;
          confidence_score: number;
        };
        count: number;
      }>();

      if (allPredictions) {
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
            aggMap.set(pred.player_id, {
              sum: { ...values },
              count: 1,
            });
          } else {
            existing.sum.predicted_points += values.predicted_points;
            existing.sum.predicted_rebounds += values.predicted_rebounds;
            existing.sum.predicted_assists += values.predicted_assists;
            existing.sum.predicted_steals += values.predicted_steals;
            existing.sum.predicted_blocks += values.predicted_blocks;
            existing.sum.predicted_turnovers += values.predicted_turnovers;
            existing.sum.predicted_three_pointers_made += values.predicted_three_pointers_made;
            existing.sum.confidence_score += values.confidence_score;
            existing.count += 1;
          }
        }
      }

      
      const predictionsMap = new Map<number, any>();
      for (const [playerId, agg] of aggMap.entries()) {
        predictionsMap.set(playerId, {
          player_id: playerId,
          predicted_points: agg.sum.predicted_points / agg.count,
          predicted_rebounds: agg.sum.predicted_rebounds / agg.count,
          predicted_assists: agg.sum.predicted_assists / agg.count,
          predicted_steals: agg.sum.predicted_steals / agg.count,
          predicted_blocks: agg.sum.predicted_blocks / agg.count,
          predicted_turnovers: agg.sum.predicted_turnovers / agg.count,
          predicted_three_pointers_made: agg.sum.predicted_three_pointers_made / agg.count,
          confidence_score: agg.sum.confidence_score / agg.count,
        });
      }

      
      reportProgress('filtering', 35);

      
      const results: PickResult[] = [];
      let totalEvaluated = 0;
      let skippedNoHistory = 0;
      let skippedNoPrediction = 0;
      let skippedFilters = 0;

      
      const filterReasons = {
        notEnoughGames: 0,
        notEnoughContextGames: 0,
        notEnoughH2HGames: 0,
        hitRateTooLow: 0,
        contextHitRateTooLow: 0,
        h2hHitRateTooLow: 0,
        consecutiveHitsFailed: 0,
        contextConsecutiveHitsFailed: 0,
        h2hConsecutiveHitsFailed: 0,
        confidenceTooLow: 0,
        minutesTooLow: 0,
        aiAgreementFailed: 0,
      };

      const totalPlayersToEvaluate = games.reduce((sum, game) => {
        const gamePlayers = players.filter(
          p => p.team_id === game.home_team_id || p.team_id === game.away_team_id
        );
        return sum + gamePlayers.length;
      }, 0);

      for (const game of games) {
        const gamePlayers = players.filter(
          p => p.team_id === game.home_team_id || p.team_id === game.away_team_id
        );

        for (const player of gamePlayers) {
          try {
            totalEvaluated++;

            
            if (totalEvaluated % 10 === 0 || totalEvaluated === totalPlayersToEvaluate) {
              const filterProgress = 35 + (35 * (totalEvaluated / totalPlayersToEvaluate));
              reportProgress('filtering', Math.min(filterProgress, 70));
            }

            
            const playerHistory = playerHistoryMap.get(player.player_id);
            const prediction = predictionsMap.get(player.player_id);

            if (!playerHistory || playerHistory.length === 0) {
              skippedNoHistory++;
              continue;
            }

            
            if (!prediction || !prediction.confidence_score || Number(prediction.confidence_score) <= 0) {
              skippedNoPrediction++; 
              continue;
            }

            
            const result = evaluatePlayerPickOptimized(
              player,
              game,
              teamsMap,
              filters,
              playerHistory || [],
              prediction,
              statType as StatType,
              filterReasons 
            );

            if (result) {
              results.push(result);
            } else {
              skippedFilters++;
            }
          } catch (err) {
            logger.error('Error evaluating player pick', err as Error, {
              playerId: player.player_id,
              gameId: game.game_id,
            });
            
          }
        }
      }

      
      reportProgress('scoring', 75);
      results.sort((a, b) => b.strengthScore - a.strengthScore);

      
      reportProgress('scoring', 90);

      return results;
  } catch (err) {
    logger.error('Error finding picks for direction', err as Error);
    return [];
  }
}


function evaluatePlayerPickOptimized(
  player: any,
  game: any,
  teamsMap: Map<number, any>,
  filters: PickFinderFilters,
  playerHistory: any[],
  prediction: any | undefined,
  statType: StatType,
  filterReasons?: {
    notEnoughGames: number;
    notEnoughContextGames: number;
    notEnoughH2HGames: number;
    hitRateTooLow: number;
    contextHitRateTooLow: number;
    h2hHitRateTooLow: number;
    consecutiveHitsFailed: number;
    contextConsecutiveHitsFailed: number;
    h2hConsecutiveHitsFailed: number;
    confidenceTooLow: number;
    minutesTooLow: number;
    aiAgreementFailed: number;
  }
): PickResult | null {
  const { overUnder, lineMethod, lineAdjustment } = filters;

  
  
  const minRequiredGames = Math.min(3, filters.timeWindow);
  if (!playerHistory || playerHistory.length < minRequiredGames) {
    if (filterReasons) filterReasons.notEnoughGames++;
    return null;
  }

  
  const isHome = player.team_id === game.home_team_id;
  const opponentTeamId = isHome ? game.away_team_id : game.home_team_id;

  const playerTeam = teamsMap.get(player.team_id);
  const opponentTeam = teamsMap.get(opponentTeamId);

  if (!playerTeam || !opponentTeam) return null;

  
  const statColumn = getStatColumn(statType);

  
  const seasonStats = playerHistory.map(g => Number(g[statColumn]) || 0);
  const seasonAvg = seasonStats.reduce((sum, val) => sum + val, 0) / seasonStats.length;

  
  
  let recentGames;
  let contextGames: any[] = [];

  if (filters.enableContextSplit) {
    
    contextGames = playerHistory.filter(g => {
      const gameIsHome = g.team_id === g.games.home_team_id;
      return gameIsHome === isHome;
    });

    
    recentGames = contextGames.slice(0, filters.contextTimeWindow);

    
    const minRequiredContextGames = Math.min(3, filters.contextTimeWindow);

    if (recentGames.length < minRequiredContextGames) {
      if (filterReasons) filterReasons.notEnoughContextGames++;
      return null;
    }
  } else {
    
    recentGames = playerHistory.slice(0, filters.timeWindow);
  }

  const stats = recentGames.map(g => Number(g[statColumn]) || 0);

  
  
  let h2hGames: any[] = [];
  let h2hHitRate: number | undefined;
  let h2hHitCount: number | undefined;
  let h2hTotalGames: number | undefined;

  if (filters.enableH2H) {
    
    h2hGames = playerHistory.filter(g => {
      const gameOpponentId = g.team_id === g.games.home_team_id ? g.games.away_team_id : g.games.home_team_id;
      return gameOpponentId === opponentTeamId;
    });

    
    const recentH2HGames = h2hGames.slice(0, filters.h2hTimeWindow);

    
    if (recentH2HGames.length === 0) {
      if (filterReasons) filterReasons.notEnoughH2HGames++;
      return null;
    }

    
    h2hTotalGames = recentH2HGames.length;
  }

  
  if (filters.enableMinMinutes) {
    
    const minMinutesGames = filters.minMinutesWindow === 999
      ? playerHistory
      : playerHistory.slice(0, filters.minMinutesWindow);
    const avgMinutes = minMinutesGames.reduce((sum, g) => sum + (Number(g.minutes_played) || 0), 0) / minMinutesGames.length;
    if (avgMinutes < filters.minMinutes) {
      if (filterReasons) filterReasons.minutesTooLow++;
      return null;
    }
  }

  
  const aiPrediction = prediction ? Number(prediction[getPredictionColumn(statType)]) || 0 : 0;
  const confidence = prediction ? Number(prediction.confidence_score) || 0 : 0;

  
  if (filters.enableMinConfidence && confidence < filters.minConfidence) {
    if (filterReasons) filterReasons.confidenceTooLow++;
    return null;
  }

  
  const line = calculateLine(seasonAvg, aiPrediction, lineMethod, lineAdjustment, overUnder, statType, filters.customModifiers);

  
  const { hitRate, hitCount, totalGames } = calculateHitRate(stats, line, overUnder);

  
  if (filters.enableHitRateThreshold) {
    if (filters.hitRateMode === 'percentage') {
      if (hitRate < filters.hitRateThreshold) {
        if (filterReasons) filterReasons.hitRateTooLow++;
        return null;
      }
    } else {
      
      if (hitCount < filters.hitRateCount) {
        if (filterReasons) filterReasons.hitRateTooLow++;
        return null;
      }
    }
  }

  
  
  let contextHitRate: number | undefined;
  let contextHitCount: number | undefined;
  let contextTotalGames: number | undefined;

  if (filters.enableContextSplit) {
    
    contextHitRate = hitRate;
    contextHitCount = hitCount;
    contextTotalGames = totalGames;

    
    if (filters.enableContextHitRate) {
      if (filters.contextHitRateMode === 'percentage') {
        if (contextHitRate < filters.contextHitRateThreshold) {
          if (filterReasons) filterReasons.contextHitRateTooLow++;
          return null;
        }
      } else {
        
        if (contextHitCount < filters.contextHitRateCount) {
          if (filterReasons) filterReasons.contextHitRateTooLow++;
          return null;
        }
      }
    }

    
    if (filters.enableContextConsecutiveHits && filters.contextConsecutiveHits > 0) {
      const contextConsecutiveHits = calculateConsecutiveHits(stats, line, overUnder);
      if (contextConsecutiveHits < filters.contextConsecutiveHits) {
        if (filterReasons) filterReasons.contextConsecutiveHitsFailed++;
        return null;
      }
    }
  }

  
  if (filters.enableH2H && h2hTotalGames) {
    const recentH2HGames = h2hGames.slice(0, filters.h2hTimeWindow);
    const h2hStats = recentH2HGames.map(g => Number(g[statColumn]) || 0);
    const h2hResult = calculateHitRate(h2hStats, line, overUnder);
    h2hHitRate = h2hResult.hitRate;
    h2hHitCount = h2hResult.hitCount;

    
    if (filters.enableH2hHitRate) {
      if (filters.h2hHitRateMode === 'percentage') {
        if (h2hHitRate < filters.h2hHitRateThreshold) {
          if (filterReasons) filterReasons.h2hHitRateTooLow++;
          return null;
        }
      } else {
        
        if (h2hHitCount < filters.h2hHitRateCount) {
          if (filterReasons) filterReasons.h2hHitRateTooLow++;
          return null;
        }
      }
    }

    
    if (filters.enableH2hConsecutiveHits && filters.h2hConsecutiveHits > 0) {
      const h2hConsecutiveHits = calculateConsecutiveHits(h2hStats, line, overUnder);
      if (h2hConsecutiveHits < filters.h2hConsecutiveHits) {
        if (filterReasons) filterReasons.h2hConsecutiveHitsFailed++;
        return null;
      }
    }
  }

  
  const consecutiveHits = calculateConsecutiveHits(stats, line, overUnder);
  if (filters.enableConsecutiveHits && filters.consecutiveHits > 0 && consecutiveHits < filters.consecutiveHits) {
    if (filterReasons) filterReasons.consecutiveHitsFailed++;
    return null;
  }

  
  if (filters.aiAgreement !== 'disabled') {
    const margin = overUnder === 'over' ? aiPrediction - line : line - aiPrediction;

    if (filters.aiAgreement === 'simple' && margin <= 0) {
      if (filterReasons) filterReasons.aiAgreementFailed++;
      return null;
    }
    if (filters.aiAgreement === 'strong' && margin < 2.0) {
      if (filterReasons) filterReasons.aiAgreementFailed++;
      return null;
    }
    if (filters.aiAgreement === 'very-strong' && margin < 4.0) {
      if (filterReasons) filterReasons.aiAgreementFailed++;
      return null;
    }
  }

  
  const aiMargin = overUnder === 'over' ? aiPrediction - line : line - aiPrediction;

  
  
  
  const defenseRank = opponentTeam.defense_rank || undefined;

  
  
  
  
  const playerTeamPace = playerTeam.pace;
  const opponentPace = opponentTeam.pace;
  let paceBonus: number | undefined = undefined;

  if (playerTeamPace !== undefined && playerTeamPace !== null &&
      opponentPace !== undefined && opponentPace !== null) {
    
    const gamePace = (Number(playerTeamPace) + Number(opponentPace)) / 2;
    const leagueAvgPace = 100; 
    
    paceBonus = gamePace - leagueAvgPace;
  }

  
  const { score: strengthScore, breakdown } = calculateStrengthScore(
    hitRate,
    contextHitRate,
    aiMargin,
    confidence,
    defenseRank,
    paceBonus
  );

  
  const reasons: string[] = [];
  reasons.push(`Hit in ${hitCount} of last ${totalGames} games (${hitRate.toFixed(0)}%)`);

  if (contextHitRate !== undefined && contextTotalGames !== undefined) {
    reasons.push(`Hit in ${contextHitCount} of last ${contextTotalGames} ${isHome ? 'home' : 'away'} games (${contextHitRate.toFixed(0)}%)`);
  }

  if (h2hHitRate !== undefined && h2hTotalGames !== undefined) {
    reasons.push(`Hit in ${h2hHitCount} of last ${h2hTotalGames} H2H games vs ${opponentTeam?.abbreviation || 'opponent'} (${h2hHitRate.toFixed(0)}%)`);
  }

  if (consecutiveHits >= 2) {
    reasons.push(`Hit in last ${consecutiveHits} consecutive games`);
  }

  if (aiMargin > 0) {
    reasons.push(`AI predicts ${aiPrediction.toFixed(1)} (${Math.abs(aiMargin).toFixed(1)} ${overUnder === 'over' ? 'above' : 'below'} line)`);
  }

  reasons.push(`${confidence}% confidence score`);

  
  const result: PickResult = {
    playerId: player.player_id.toString(),
    playerName: player.full_name,
    playerPhotoUrl: `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${prediction.player_id}.png`,
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
    aiPrediction,
    confidence,
    hitRate,
    hitCount,
    totalGames,
    contextHitRate,
    contextHitCount,
    contextTotalGames,
    h2hHitRate,
    h2hHitCount,
    h2hTotalGames,
    consecutiveHits,
    strengthScore,
    strengthBreakdown: breakdown,
    reasons,
  };

  return result;
}


async function evaluatePlayerPick(
  player: any,
  game: any,
  teamsMap: Map<number, any>,
  filters: PickFinderFilters,
  todayStr: string
): Promise<PickResult | null> {
  const { statType, overUnder, lineMethod, lineAdjustment } = filters;

  
  const isHome = player.team_id === game.home_team_id;
  const opponentTeamId = isHome ? game.away_team_id : game.home_team_id;

  const playerTeam = teamsMap.get(player.team_id);
  const opponentTeam = teamsMap.get(opponentTeamId);

  if (!playerTeam || !opponentTeam) return null;

  
  const statColumn = getStatColumn(statType);

  
  const { data: allSeasonGames, error: histError } = await supabase
    .from('player_game_stats')
    .select(`game_id, ${statColumn}, minutes_played, team_id, games!inner(game_date, home_team_id, away_team_id, game_status, season)`)
    .eq('player_id', player.player_id)
    .eq('games.game_status', 'completed')
    .lt('games.game_date', todayStr)
    .eq('games.season', game.season);

  if (histError) {
    logger.error('Error fetching historical games', histError);
    return null;
  }

  if (!allSeasonGames || allSeasonGames.length < filters.timeWindow) {
    
    return null;
  }

  
  allSeasonGames.sort((a, b) => {
    const dateA = new Date(a.games.game_date).getTime();
    const dateB = new Date(b.games.game_date).getTime();
    return dateB - dateA;
  });

  
  const seasonStats = allSeasonGames.map(g => Number(g[statColumn]) || 0);
  const seasonAvg = seasonStats.reduce((sum, val) => sum + val, 0) / seasonStats.length;

  
  const recentGames = allSeasonGames.slice(0, filters.timeWindow);
  const stats = recentGames.map(g => Number(g[statColumn]) || 0);

  
  
  const minMinutesGames = filters.minMinutesWindow === 999
    ? allSeasonGames
    : allSeasonGames.slice(0, filters.minMinutesWindow);
  const avgMinutes = minMinutesGames.reduce((sum, g) => sum + (Number(g.minutes_played) || 0), 0) / minMinutesGames.length;
  if (avgMinutes < filters.minMinutes) return null;

  
  const { data: predictions, error: predError } = await supabase
    .from('predictions')
    .select('predicted_points, predicted_rebounds, predicted_assists, predicted_steals, predicted_blocks, predicted_turnovers, predicted_three_pointers_made, confidence_score')
    .eq('player_id', player.player_id)
    .eq('game_id', game.game_id)
    .limit(1)
    .maybeSingle();

  if (predError) {
    logger.error('Error fetching prediction', predError);
    return null;
  }

  const aiPrediction = predictions ? Number(predictions[getPredictionColumn(statType)]) || 0 : 0;
  const confidence = predictions ? Number(predictions.confidence_score) || 0 : 0;

  
  if (confidence < filters.minConfidence) return null;

  
  const line = calculateLine(seasonAvg, aiPrediction, lineMethod, lineAdjustment, overUnder, statType, filters.customModifiers);

  
  const { hitRate, hitCount, totalGames } = calculateHitRate(stats, line, overUnder);

  
  if (filters.enableHitRateThreshold) {
    if (filters.hitRateMode === 'percentage') {
      if (hitRate < filters.hitRateThreshold) return null;
    } else {
      if (hitCount < filters.hitRateCount) return null;
    }
  }

  
  let contextHitRate: number | undefined;
  let contextHitCount: number | undefined;
  let contextTotalGames: number | undefined;

  if (filters.enableContextSplit) {
    const contextGames = recentGames.filter(g => {
      const gameIsHome = g.team_id === g.games.home_team_id;
      return gameIsHome === isHome;
    });

    
    if (contextGames.length > 0) {
      const contextStats = contextGames.map(g => Number(g[statColumn]) || 0);
      const contextResult = calculateHitRate(contextStats, line, overUnder);
      contextHitRate = contextResult.hitRate;
      contextHitCount = contextResult.hitCount;
      contextTotalGames = contextResult.totalGames;

      
      if (filters.enableContextHitRate) {
        if (filters.contextHitRateMode === 'percentage') {
          if (contextHitRate < filters.contextHitRateThreshold) return null;
        } else {
          if (contextHitCount < filters.contextHitRateCount) return null;
        }
      }
    } else {
      
      return null;
    }
  }

  
  const consecutiveHits = calculateConsecutiveHits(stats, line, overUnder);
  if (filters.enableConsecutiveHits && filters.consecutiveHits > 0 && consecutiveHits < filters.consecutiveHits) {
    if (filterReasons) filterReasons.consecutiveHitsFailed++;
    return null;
  }

  
  if (filters.aiAgreement !== 'disabled') {
    const margin = overUnder === 'over' ? aiPrediction - line : line - aiPrediction;

    if (filters.aiAgreement === 'simple' && margin <= 0) {
      if (filterReasons) filterReasons.aiAgreementFailed++;
      return null;
    }
    if (filters.aiAgreement === 'strong' && margin < 2.0) {
      if (filterReasons) filterReasons.aiAgreementFailed++;
      return null;
    }
    if (filters.aiAgreement === 'very-strong' && margin < 4.0) {
      if (filterReasons) filterReasons.aiAgreementFailed++;
      return null;
    }
  }

  
  const aiMargin = overUnder === 'over' ? aiPrediction - line : line - aiPrediction;

  
  
  const defenseRank = opponentTeam.defense_rank || undefined;

  
  const playerTeamPace = playerTeam.pace;
  const opponentPace = opponentTeam.pace;
  let paceBonus: number | undefined = undefined;

  if (playerTeamPace !== undefined && playerTeamPace !== null &&
      opponentPace !== undefined && opponentPace !== null) {
    const gamePace = (Number(playerTeamPace) + Number(opponentPace)) / 2;
    const leagueAvgPace = 100;
    paceBonus = gamePace - leagueAvgPace;
  }

  
  const { score: strengthScore, breakdown } = calculateStrengthScore(
    hitRate,
    contextHitRate,
    aiMargin,
    confidence,
    defenseRank,
    paceBonus
  );

  
  const reasons: string[] = [];
  reasons.push(`Hit in ${hitCount} of last ${totalGames} games (${hitRate.toFixed(0)}%)`);

  if (contextHitRate !== undefined && contextTotalGames !== undefined) {
    reasons.push(`Hit in ${contextHitCount} of last ${contextTotalGames} ${isHome ? 'home' : 'away'} games (${contextHitRate.toFixed(0)}%)`);
  }

  if (consecutiveHits >= 2) {
    reasons.push(`Hit in last ${consecutiveHits} consecutive games`);
  }

  if (aiMargin > 0) {
    reasons.push(`AI predicts ${aiPrediction.toFixed(1)} (${Math.abs(aiMargin).toFixed(1)} ${overUnder === 'over' ? 'above' : 'below'} line)`);
  }

  reasons.push(`${confidence}% confidence score`);

  
  const result: PickResult = {
    playerId: player.player_id.toString(),
    playerName: player.full_name,
    playerPhotoUrl: `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${prediction.player_id}.png`,
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
    aiPrediction,
    confidence,
    hitRate,
    hitCount,
    totalGames,
    contextHitRate,
    contextHitCount,
    contextTotalGames,
    consecutiveHits,
    strengthScore,
    strengthBreakdown: breakdown,
    reasons,
  };

  return result;
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
