import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import type { ModelId } from '@/contexts/EnsembleContext';
import { getRefreshIntervalMs } from './useAutoRefresh';
import { logger } from '@/lib/logger';
import { cacheManager } from '@/lib/cache';
import { useCache } from '@/contexts/CacheContext';

interface ModelPerformanceData {
  stat: string;
  mae: number;
  predictions: number;
}

interface TimeSeriesDataPoint {
  date: string;
  fullDate?: string;
  error: number;
  predictions?: number;
}

interface ScatterDataPoint {
  actual: number;
  predicted: number;
  error: number;
}

interface ErrorDistributionPoint {
  error: number;
  frequency: number;
}

interface ModelPerformanceResult {
  overallMetrics: ModelPerformanceData[];
  timeSeriesData: TimeSeriesDataPoint[];
  scatterData: ScatterDataPoint[];
  errorDistribution: ErrorDistributionPoint[];
}

const statFields = {
  points: { predicted: 'predicted_points', actual: 'actual_points' },
  rebounds: { predicted: 'predicted_rebounds', actual: 'actual_rebounds' },
  assists: { predicted: 'predicted_assists', actual: 'actual_assists' },
  steals: { predicted: 'predicted_steals', actual: 'actual_steals' },
  blocks: { predicted: 'predicted_blocks', actual: 'actual_blocks' },
  turnovers: { predicted: 'predicted_turnovers', actual: 'actual_turnovers' },
  threePointers: { predicted: 'predicted_three_pointers_made', actual: 'actual_three_pointers_made' },
};

const VALID_TIME_PERIODS = ['all', '7', '30', '90', '180', '365'];
const VALID_STATS = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'threePointers', 'overall'];
const VALID_MODEL_IDS: ModelId[] = ['xgboost', 'lightgbm', 'catboost', 'random_forest'];

export function useModelPerformance(
  timePeriod: string,
  selectedStat: string,
  selectedModels: ModelId[],
  options?: { enabled?: boolean }
) {
  const { modelPerfRetentionDays } = useCache();

  return useQuery<ModelPerformanceResult, Error>({
    queryKey: ['model-performance', timePeriod, selectedStat, selectedModels.slice().sort()],
    enabled: options?.enabled !== false,
    queryFn: async () => {
      try {

      const validatedTimePeriod = VALID_TIME_PERIODS.includes(timePeriod) ? timePeriod : 'all';
      const validatedStat = VALID_STATS.includes(selectedStat) ? selectedStat : 'points';
      const validatedModels = selectedModels.filter(model => VALID_MODEL_IDS.includes(model));

      if (validatedModels.length === 0) {
        throw new Error('No valid models selected');
      }

      const cacheKey = `${validatedTimePeriod}|${validatedStat}|${validatedModels.join(',')}`;

      if (modelPerfRetentionDays !== 'off') {
        const cachedData = await cacheManager.getModelPerformance(cacheKey);
        if (cachedData) {
          logger.info(`Using cached model performance for ${cacheKey}`);
          return cachedData as ModelPerformanceResult;
        }
      }

      
      const endDate = new Date();
      endDate.setHours(23, 59, 59, 999);
      const startDate = new Date();
      
      if (validatedTimePeriod === 'all') {
        startDate.setFullYear(2020, 0, 1);
        startDate.setHours(0, 0, 0, 0);
      } else {
        const days = parseInt(validatedTimePeriod);
        if (isNaN(days) || days <= 0) {
          throw new Error(`Invalid time period: ${timePeriod}`);
        }
        startDate.setDate(endDate.getDate() - days);
        startDate.setHours(0, 0, 0, 0);
      }

      
      
      
      
      interface PredictionRow {
        prediction_id: number;
        player_id: number;
        game_id: string;
        prediction_date: string;
        model_version: string;
        predicted_points: number;
        predicted_rebounds: number;
        predicted_assists: number;
        predicted_steals: number;
        predicted_blocks: number;
        predicted_turnovers: number;
        predicted_three_pointers_made: number;
        actual_points: number | null;
        actual_rebounds: number | null;
        actual_assists: number | null;
        actual_steals: number | null;
        actual_blocks: number | null;
        actual_turnovers: number | null;
        actual_three_pointers_made: number | null;
        prediction_error: number | null;
      }

      let allRows: PredictionRow[] = [];
      let hasMore = true;
      let offset = 0;
      const pageSize = 1000;
      const maxIterations = 100;
      let iterationCount = 0;
      
      while (hasMore && iterationCount < maxIterations) {
        iterationCount++;
        let query = supabase
          .from('predictions')
          .select(
            'prediction_id, player_id, game_id, prediction_date, model_version, ' +
            'predicted_points, predicted_rebounds, predicted_assists, predicted_steals, predicted_blocks, predicted_turnovers, predicted_three_pointers_made, ' +
            'actual_points, actual_rebounds, actual_assists, actual_steals, actual_blocks, actual_turnovers, actual_three_pointers_made, ' +
            'prediction_error'
          )
          .not('actual_points', 'is', null)
          .range(offset, offset + pageSize - 1);
        
        if (validatedModels.length > 0) {
          query = query.in('model_version', validatedModels);
        }

        const { data: pageRows, error } = await query;
        
        if (error) throw error;
        
        if (!pageRows || pageRows.length === 0) {
          hasMore = false;
        } else {
          allRows = allRows.concat(pageRows as PredictionRow[]);
          hasMore = pageRows.length === pageSize;
          offset += pageSize;
        }
      }

      if (iterationCount >= maxIterations) {
        logger.warn('Pagination reached max iterations limit. Some data may be missing.');
      }
      
      const rows = allRows;

      if (!rows || rows.length === 0) {
        
        return {
          overallMetrics: [],
          timeSeriesData: [],
          scatterData: [],
          errorDistribution: [],
        };
      }

      
      const gameIds = Array.from(new Set(rows.map(r => r.game_id)));
      const { data: gameRows, error: gameError } = await supabase
        .from('games')
        .select('game_id, game_date')
        .in('game_id', gameIds)
        .order('game_date', { ascending: false }) 
        .limit(10000); 
      
      
      
      

      if (gameError) throw gameError;
      
      const gamesByGameId = new Map<string, string>();
      if (gameRows) {
        for (const game of gameRows) {
          gamesByGameId.set(game.game_id, game.game_date);
        }
      }
      
      
      
      
      
      
      rows.sort((a, b) => {
        const dateA = gamesByGameId.get(a.game_id);
        const dateB = gamesByGameId.get(b.game_id);
        if (!dateA || !dateB) {
          return 0; 
        }
        
        const dateAStr = typeof dateA === 'string' ? dateA : dateA.toISOString();
        const dateBStr = typeof dateB === 'string' ? dateB : dateB.toISOString();
        const dateAOnly = dateAStr.split('T')[0];
        const dateBOnly = dateBStr.split('T')[0];
        
        const dateCompare = dateBOnly.localeCompare(dateAOnly); 
        if (dateCompare !== 0) return dateCompare;
        
        
        
        const idA = (a as PredictionRow).prediction_id || 0;
        const idB = (b as PredictionRow).prediction_id || 0;
        return idA - idB; 
      });
      
      
      
      
      const startDateOnly = new Date(startDate);
      startDateOnly.setHours(0, 0, 0, 0);
      const endDateOnly = new Date(endDate);
      endDateOnly.setHours(23, 59, 59, 999);
      
      const filteredRows = rows.filter(row => {
        const gameDate = gamesByGameId.get(row.game_id);
        if (!gameDate) return false;
        
        const gameDateStr = typeof gameDate === 'string' ? gameDate : gameDate.toISOString();
        const gameDateOnlyStr = gameDateStr.split('T')[0]; 
        
        const [year, month, day] = gameDateOnlyStr.split('-').map(Number);
        const gameDateOnly = new Date(year, month - 1, day);
        gameDateOnly.setHours(0, 0, 0, 0);
        
        return gameDateOnly.getTime() >= startDateOnly.getTime() && gameDateOnly.getTime() <= endDateOnly.getTime();
      });
      
      
      const processedRows = filteredRows;

      const shouldAggregate = validatedModels.length > 1;
      
      
      if (process.env.NODE_ENV === 'development') {
        
      }
      
      const playerGameMap = new Map<string, {
        player_id: number;
        game_id: string;
        game_date?: string;
        predicted: Record<string, number[]>;
        actual: Record<string, number>;
        prediction_error?: number;
        count: number;
      }>();

      let loggedMissingGameDate = false;

      for (const row of processedRows) {
        const gameDate = gamesByGameId.get(row.game_id);
        if (!gameDate) {
          if (!loggedMissingGameDate) {
            logger.warn('Skipping predictions with missing game_date mapping', { gameId: row.game_id });
            loggedMissingGameDate = true;
          }
          continue; 
        }
        
        
        
        
        const gameDateStr = typeof gameDate === 'string' ? gameDate : gameDate.toISOString();
        const gameDateOnly = gameDateStr.split('T')[0]; 
        const key = `${row.player_id}-${row.game_id}-${gameDateOnly}`;
        const existing = playerGameMap.get(key);
        
        const predicted: Record<string, number[]> = {};
        const actual: Record<string, number> = {};
        
        
        for (const statKey of Object.keys(statFields) as Array<keyof typeof statFields>) {
          const { predicted: predField, actual: actField } = statFields[statKey];
          const pred = Number(row[predField as keyof typeof row] ?? 0);
          const act = Number(row[actField as keyof typeof row] ?? 0);
          
        
        
        if (act !== null && act !== undefined && !isNaN(act)) {
          if (!predicted[statKey]) predicted[statKey] = [];
          predicted[statKey].push(pred);
          actual[statKey] = act; 
        }
        }
        
        
        
        const predError = (row as PredictionRow).prediction_error;
        const numPredError = predError !== null && predError !== undefined && !isNaN(Number(predError)) 
          ? Number(predError) 
          : undefined;
        
        
        
        
        const hasAnyActualValue = Object.values(actual).some(act => act !== null && act !== undefined && !isNaN(act));
        const hasPredictionError = numPredError !== undefined;
        
        if (!hasAnyActualValue && !hasPredictionError) {
          continue;
        }
        
        if (existing) {
          if (shouldAggregate) {
            
            for (const statKey of Object.keys(predicted)) {
              if (!existing.predicted[statKey]) existing.predicted[statKey] = [];
              existing.predicted[statKey].push(...predicted[statKey]);
            }
            
            
            
            
            if (existing.prediction_error === undefined && numPredError !== undefined) {
              existing.prediction_error = numPredError;
            }
            existing.count++;
          }
          
          
        } else {
          playerGameMap.set(key, {
            player_id: row.player_id,
            game_id: row.game_id,
            game_date: gameDateOnly, 
            predicted,
            actual,
            prediction_error: numPredError, 
            prediction_error_candidates: [], 
            count: 1,
          });
        }
      }

      
      
      const overallMetrics: ModelPerformanceData[] = [];
      const statKeys = Object.keys(statFields) as Array<keyof typeof statFields>;
      
      
      const statErrors: Record<string, number[]> = {};
      for (const statKey of statKeys) {
        statErrors[statKey] = [];
      }

      
      
      for (const aggregated of playerGameMap.values()) {
        for (const statKey of statKeys) {
          const predValues = aggregated.predicted[statKey];
          const act = aggregated.actual[statKey];
          
          if (predValues && predValues.length > 0 && act !== null && act !== undefined && !isNaN(act)) {
            let avgPred = predValues.length > 0 
              ? predValues.reduce((sum, p) => sum + p, 0) / predValues.length 
              : 0;
            
            
            avgPred = Math.round(avgPred * 10) / 10;
            
            if (!isNaN(avgPred) && isFinite(avgPred)) {
              statErrors[statKey].push(Math.abs(avgPred - act));
            }
          }
        }
      }

      for (const statKey of statKeys) {
        const errors = statErrors[statKey];
        const mae = errors.length > 0 
          ? errors.reduce((sum, e) => sum + e, 0) / errors.length 
          : 0;
        const statLabel = statKey === 'threePointers' ? '3PM' : 
                         statKey.charAt(0).toUpperCase() + statKey.slice(1);
        
        overallMetrics.push({
          stat: statLabel,
          mae,
          predictions: errors.length,
        });
      }

      
      
      
      const predictionErrors: number[] = [];
      for (const aggregated of playerGameMap.values()) {
        if (aggregated.prediction_error !== undefined && aggregated.prediction_error !== null && !isNaN(aggregated.prediction_error)) {
          predictionErrors.push(aggregated.prediction_error);
        }
      }
      
      const hasPredictionErrorColumn = predictionErrors.length > 0;
      const overallMae = hasPredictionErrorColumn
        ? predictionErrors.length > 0
          ? predictionErrors.reduce((sum, e) => sum + e, 0) / predictionErrors.length
          : 0
        : (overallMetrics.length > 0
          ? overallMetrics.reduce((sum, m) => sum + m.mae, 0) / overallMetrics.length
          : 0);
      
      overallMetrics.push({
        stat: 'Overall',
        mae: overallMae,
        predictions: playerGameMap.size,
      });

      
      
      const dateMap = new Map<string, { 
        predictionErrors: number[];
        count: number;
      }>();
      
      for (const aggregated of playerGameMap.values()) {
        
        
        const gameDate = aggregated.game_date;
        if (!gameDate) continue; 
        
        
        const dateStr = gameDate;
        const existing = dateMap.get(dateStr);
        
        
        
        
        
        let errorToUse: number | undefined = undefined;
        let totalError = 0;
        let statCount = 0;
        
        const statKeyMap: Record<string, keyof typeof statFields> = {
          'points': 'points',
          'rebounds': 'rebounds',
          'assists': 'assists',
          'steals': 'steals',
          'blocks': 'blocks',
          'turnovers': 'turnovers',
          'threePointers': 'threePointers',
        };
        
        const statsToInclude: Array<keyof typeof statFields> = validatedStat === 'overall' || !statKeyMap[validatedStat]
          ? (Object.keys(statFields) as Array<keyof typeof statFields>)
          : [statKeyMap[validatedStat]];
        
        for (const statKey of statsToInclude) {
          const predValues = aggregated.predicted[statKey];
          const act = aggregated.actual[statKey];
          
            if (predValues && predValues.length > 0 && act !== null && act !== undefined && !isNaN(act)) {
              let avgPred = predValues.length > 0
                ? predValues.reduce((sum, p) => sum + p, 0) / predValues.length
                : 0;
              
              
              if (!isNaN(avgPred) && isFinite(avgPred)) {
                totalError += Math.abs(avgPred - act);
                statCount++;
              }
            }
        }
        
        if (statCount > 0) {
          errorToUse = totalError / statCount;
        }
        
        if (errorToUse !== undefined) {
          if (existing) {
            existing.predictionErrors.push(errorToUse);
            existing.count++;
          } else {
            dateMap.set(dateStr, { 
              predictionErrors: [errorToUse],
              count: 1,
            });
          }
        }
      }




      const sortedEntries = Array.from(dateMap.entries())
        .map(([dateStr, data]) => {
          const [year, month, day] = dateStr.split('-').map(Number);
          const dateObj = new Date(year, month - 1, day);
          const avgError = data.predictionErrors.length > 0
            ? data.predictionErrors.reduce((sum, e) => sum + e, 0) / data.predictionErrors.length
            : 0;
          const preciseError = Number(avgError);

          return {
            fullDate: dateStr,
            dateSort: dateObj.getTime(),
            error: preciseError,
            predictions: data.count,
          };
        })
        .sort((a, b) => a.dateSort - b.dateSort);

      const years = new Set(sortedEntries.map(d => d.fullDate.split('-')[0]));
      const spansMultipleYears = years.size > 1;

      const timeSeriesData: TimeSeriesDataPoint[] = sortedEntries.map(point => {
        const [year, month, day] = point.fullDate.split('-').map(Number);
        const dateObj = new Date(year, month - 1, day);

        return {
          date: spansMultipleYears
            ? dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
            : dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
          fullDate: point.fullDate,
          error: point.error,
          predictions: point.predictions,
        };
      }); 

      
      const statKeyMap: Record<string, keyof typeof statFields> = {
        'points': 'points',
        'rebounds': 'rebounds',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'threePointers': 'threePointers',
      };
      const statKey = statKeyMap[validatedStat] || 'points';
      const scatterData: ScatterDataPoint[] = [];
      
      for (const aggregated of playerGameMap.values()) {
        const predValues = aggregated.predicted[statKey];
        const act = aggregated.actual[statKey];
        
        if (predValues && predValues.length > 0 && act !== null && act !== undefined && !isNaN(act)) {
          const avgPred = predValues.length > 0
            ? predValues.reduce((sum, p) => sum + p, 0) / predValues.length
            : 0;
          if (!isNaN(avgPred)) {
            scatterData.push({
              actual: act,
              predicted: avgPred,
              error: avgPred - act,
            });
          }
        }
      }
      
      
      const limitedScatterData = scatterData.slice(0, 100);

      
      const errorBins = new Map<number, number>();
      for (const point of limitedScatterData) {
        const bin = Math.round(point.error);
        errorBins.set(bin, (errorBins.get(bin) || 0) + 1);
      }

      const errorDistribution: ErrorDistributionPoint[] = Array.from({ length: 11 }, (_, i) => i - 5)
        .map(error => ({
          error,
          frequency: errorBins.get(error) || 0,
        }));

      const result = {
        overallMetrics,
        timeSeriesData,
        scatterData: limitedScatterData,
        errorDistribution,
      };

      if (modelPerfRetentionDays !== 'off') {
        await cacheManager.saveModelPerformance(cacheKey, result);
      }

      return result;
      } catch (error) {
        logger.error('Error in useModelPerformance', error as Error, {
          timePeriod,
          selectedStat,
          selectedModels,
        });
        throw error;
      }
    },
    
    
    
    refetchInterval: (query) => {
      if (typeof window === 'undefined') return false;
      const stored = localStorage.getItem('courtvision-auto-refresh-interval');
      return getRefreshIntervalMs(stored || 'never');
    },
    refetchIntervalInBackground: false, 
    staleTime: 300000, 
  });
}

