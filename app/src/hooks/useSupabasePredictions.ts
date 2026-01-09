import { useQuery } from '@tanstack/react-query';
import { format } from 'date-fns';
import { supabase, queryWithTimeout } from '@/lib/supabase';
import { cacheManager } from '@/lib/cache';
import { useCache } from '@/contexts/CacheContext';
import type { ModelId } from '@/contexts/EnsembleContext';
import { Game, Prediction, Player, PlayerStats, FeatureExplanations } from '@/types/nba';
import { getRefreshIntervalMs } from './useAutoRefresh';
import { logger } from '@/lib/logger';

type PredictionRow = {
  prediction_id: number;
  player_id: number;
  game_id: string;
  prediction_date: string;
  predicted_points: number | null;
  predicted_rebounds: number | null;
  predicted_assists: number | null;
  predicted_steals: number | null;
  predicted_blocks: number | null;
  predicted_turnovers: number | null;
  predicted_three_pointers_made: number | null;
  actual_points: number | null;
  actual_rebounds: number | null;
  actual_assists: number | null;
  actual_steals: number | null;
  actual_blocks: number | null;
  actual_turnovers: number | null;
  actual_three_pointers_made: number | null;
  prediction_error: number | null;
  confidence_score: number | null;
  model_version: string | null;
  feature_explanations: any | null; 
};

interface PlayerRow {
  player_id: number;
  full_name: string;
  team_id: number | null;
  position: string | null;
}

interface GameRow {
  game_id: string;
  game_date: string;
  home_team_id: number;
  away_team_id: number;
}

interface TeamRow {
  team_id: number;
  full_name: string;
  abbreviation: string;
  city: string;
}

function toStats(row: PredictionRow): PlayerStats {
  return {
    points: Number(row.predicted_points ?? 0),
    rebounds: Number(row.predicted_rebounds ?? 0),
    assists: Number(row.predicted_assists ?? 0),
    steals: Number(row.predicted_steals ?? 0),
    blocks: Number(row.predicted_blocks ?? 0),
    turnovers: Number(row.predicted_turnovers ?? 0),
    threePointersMade: Number(row.predicted_three_pointers_made ?? 0),
  };
}

function toActualStats(row: PredictionRow): PlayerStats | undefined {
  
  if (row.actual_points === null && row.actual_rebounds === null && row.actual_assists === null &&
      row.actual_steals === null && row.actual_blocks === null && row.actual_turnovers === null &&
      row.actual_three_pointers_made === null) {
    return undefined;
  }
  
  return {
    points: Number(row.actual_points ?? 0),
    rebounds: Number(row.actual_rebounds ?? 0),
    assists: Number(row.actual_assists ?? 0),
    steals: Number(row.actual_steals ?? 0),
    blocks: Number(row.actual_blocks ?? 0),
    turnovers: Number(row.actual_turnovers ?? 0),
    threePointersMade: Number(row.actual_three_pointers_made ?? 0),
  };
}

export function useSupabasePredictions(selectedDate: Date, selectedModels: ModelId[]) {
  const dateStr = format(selectedDate, 'yyyy-MM-dd');
  const { isOnline } = useCache();

  return useQuery<Game[], Error>({
    queryKey: ['predictions', dateStr, selectedModels.slice().sort()],
    queryFn: async () => {
      
      
      const sortedModels = selectedModels.slice().sort();
      const cachedData = await cacheManager.getPredictions(dateStr, sortedModels);

      if (cachedData) {
        logger.info(`Using cached predictions for ${dateStr} with models ${sortedModels.join(',')}`);
        return cachedData as Game[];
      }

      
      if (!isOnline) {
        throw new Error('No cached data available and device is offline');
      }

      
      const { data: predictionRows, error: predError } = await queryWithTimeout(
        supabase
          .from('predictions')
          .select(
            'prediction_id, player_id, game_id, prediction_date, predicted_points, predicted_rebounds, predicted_assists, predicted_steals, predicted_blocks, predicted_turnovers, predicted_three_pointers_made, actual_points, actual_rebounds, actual_assists, actual_steals, actual_blocks, actual_turnovers, actual_three_pointers_made, prediction_error, confidence_score, model_version, feature_explanations'
          )
          .gte('prediction_date', `${dateStr}T00:00:00`)
          .lt('prediction_date', `${dateStr}T23:59:59`)
      );

      if (predError) throw predError;
      if (!predictionRows || predictionRows.length === 0) {
        return [];
      }

      const rows = predictionRows as unknown as PredictionRow[];

      
      const aggMap = new Map<
        string,
        {
          player_id: number;
          game_id: string;
          prediction_date: string;
          sample_prediction_id: number | null; 
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
          actual_points: number | null;
          actual_rebounds: number | null;
          actual_assists: number | null;
          actual_steals: number | null;
          actual_blocks: number | null;
          actual_turnovers: number | null;
          actual_three_pointers_made: number | null;
          prediction_error: number | null;
          count: number;
          feature_explanations: any | null; 
        }
      >();

      
      const modelFilter = selectedModels.length > 0
        ? new Set<string>(selectedModels)
        : null;

      for (const row of rows) {
        if (modelFilter && row.model_version && !modelFilter.has(row.model_version)) {
          continue;
        }
        const key = `${row.player_id}-${row.game_id}`;
        const existing = aggMap.get(key);
        const values = {
          predicted_points: Number(row.predicted_points ?? 0),
          predicted_rebounds: Number(row.predicted_rebounds ?? 0),
          predicted_assists: Number(row.predicted_assists ?? 0),
          predicted_steals: Number(row.predicted_steals ?? 0),
          predicted_blocks: Number(row.predicted_blocks ?? 0),
          predicted_turnovers: Number(row.predicted_turnovers ?? 0),
          predicted_three_pointers_made: Number(row.predicted_three_pointers_made ?? 0),
          confidence_score: Number(row.confidence_score ?? 0),
        };

        if (!existing) {
          aggMap.set(key, {
            player_id: row.player_id,
            game_id: row.game_id,
            prediction_date: row.prediction_date,
            sample_prediction_id: row.prediction_id, 
            sum: { ...values },
            actual_points: row.actual_points,
            actual_rebounds: row.actual_rebounds,
            actual_assists: row.actual_assists,
            actual_steals: row.actual_steals,
            actual_blocks: row.actual_blocks,
            actual_turnovers: row.actual_turnovers,
            actual_three_pointers_made: row.actual_three_pointers_made,
            prediction_error: row.prediction_error,
            count: 1,
            feature_explanations: row.feature_explanations, 
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
          
          if (existing.actual_points === null && row.actual_points !== null) {
            existing.actual_points = row.actual_points;
            existing.actual_rebounds = row.actual_rebounds;
            existing.actual_assists = row.actual_assists;
            existing.actual_steals = row.actual_steals;
            existing.actual_blocks = row.actual_blocks;
            existing.actual_turnovers = row.actual_turnovers;
            existing.actual_three_pointers_made = row.actual_three_pointers_made;
            existing.prediction_error = row.prediction_error;
          }
          existing.count += 1;
        }
      }

      const aggregatedRows: PredictionRow[] = Array.from(aggMap.values()).map((agg, idx) => ({
        prediction_id: agg.sample_prediction_id ?? idx, 
        player_id: agg.player_id,
        game_id: agg.game_id,
        prediction_date: agg.prediction_date,
        predicted_points: agg.sum.predicted_points / agg.count,
        predicted_rebounds: agg.sum.predicted_rebounds / agg.count,
        predicted_assists: agg.sum.predicted_assists / agg.count,
        predicted_steals: agg.sum.predicted_steals / agg.count,
        predicted_blocks: agg.sum.predicted_blocks / agg.count,
        predicted_turnovers: agg.sum.predicted_turnovers / agg.count,
        predicted_three_pointers_made: agg.sum.predicted_three_pointers_made / agg.count,
        actual_points: agg.actual_points,
        actual_rebounds: agg.actual_rebounds,
        actual_assists: agg.actual_assists,
        actual_steals: agg.actual_steals,
        actual_blocks: agg.actual_blocks,
        actual_turnovers: agg.actual_turnovers,
        actual_three_pointers_made: agg.actual_three_pointers_made,
        prediction_error: agg.prediction_error,
        confidence_score: agg.sum.confidence_score / agg.count,
        model_version: 'ensemble',
        feature_explanations: agg.feature_explanations,
      }));

      const playerIds = Array.from(new Set(aggregatedRows.map((r) => r.player_id)));
      const gameIds = Array.from(new Set(aggregatedRows.map((r) => r.game_id)));

      
      const [{ data: playerRows, error: playerError }, { data: gameRows, error: gameError }] =
        await Promise.all([
          supabase
            .from('players')
            .select('player_id, full_name, team_id, position')
            .in('player_id', playerIds),
          supabase
            .from('games')
            .select('game_id, game_date, home_team_id, away_team_id')
            .in('game_id', gameIds),
        ]);

      if (playerError) throw playerError;
      if (gameError) throw gameError;

      const teamIds = Array.from(
        new Set(
          (gameRows as GameRow[]).flatMap((g) => [g.home_team_id, g.away_team_id])
        )
      );

      const { data: teamRows, error: teamError } = await supabase
        .from('teams')
        .select('team_id, full_name, abbreviation, city')
        .in('team_id', teamIds);

      if (teamError) throw teamError;

      const playersById = new Map<number, PlayerRow>(
        (playerRows as PlayerRow[]).map((p) => [p.player_id, p])
      );
      const gamesById = new Map<string, GameRow>(
        (gameRows as GameRow[]).map((g) => [g.game_id, g])
      );
      const teamsById = new Map<number, TeamRow>(
        (teamRows as TeamRow[]).map((t) => [t.team_id, t])
      );

      
      const gamesMap = new Map<string, Game>();

      for (const row of aggregatedRows) {
        const gameRow = gamesById.get(row.game_id);
        const playerRow = playersById.get(row.player_id);
        if (!gameRow || !playerRow) continue;

        const homeTeam = teamsById.get(gameRow.home_team_id);
        const awayTeam = teamsById.get(gameRow.away_team_id);
        if (!homeTeam || !awayTeam) continue;

        const playerTeam = teamsById.get(playerRow.team_id ?? homeTeam.team_id);

        const isHome = playerRow.team_id === gameRow.home_team_id;
        const opponentTeam = isHome ? awayTeam : homeTeam;

        const player: Player = {
          id: String(playerRow.player_id),
          name: playerRow.full_name,
          team: playerTeam?.full_name ?? '',
          teamAbbr: playerTeam?.abbreviation ?? '',
          position: playerRow.position ?? '',
          photoUrl: `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${playerRow.player_id}.png`,
        };

        
        let featureExplanations: FeatureExplanations | undefined;
        if (row.feature_explanations) {
          try {
            
            featureExplanations = row.feature_explanations as FeatureExplanations;
          } catch (e) {
            logger.warn('Failed to parse feature_explanations', { error: e });
          }
        }

        const actualStats = toActualStats(row);
        const prediction: Prediction = {
          id: String(row.prediction_id),
          predictionId: row.prediction_id, 
          playerId: String(playerRow.player_id),
          player,
          gameId: row.game_id,
          gameDate: gameRow.game_date,
          opponent: opponentTeam.full_name,
          opponentAbbr: opponentTeam.abbreviation,
          isHome,
          confidence: Number(row.confidence_score ?? 0),
          predictedStats: toStats(row),
          actualStats,
          predictionError: row.prediction_error !== null ? Number(row.prediction_error) : undefined,
          featureExplanations,
        };

        const existingGame = gamesMap.get(row.game_id);
        if (!existingGame) {
          gamesMap.set(row.game_id, {
            id: row.game_id,
            date: gameRow.game_date,
            homeTeam: homeTeam.full_name,
            homeTeamAbbr: homeTeam.abbreviation,
            homeTeamCity: homeTeam.city,
            awayTeam: awayTeam.full_name,
            awayTeamAbbr: awayTeam.abbreviation,
            awayTeamCity: awayTeam.city,
            predictions: [prediction],
          });
        } else {
          existingGame.predictions.push(prediction);
        }
      }

      const games = Array.from(gamesMap.values());

      
      
      await cacheManager.savePredictions(dateStr, games, sortedModels);

      return games;
    },
    
    
    
    refetchInterval: (query) => {
      if (typeof window === 'undefined') return false;
      const stored = localStorage.getItem('courtvision-auto-refresh-interval');
      return getRefreshIntervalMs(stored || 'never');
    },
    refetchIntervalInBackground: false, 
    staleTime: 60000, 
  });
}


