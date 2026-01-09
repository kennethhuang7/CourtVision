import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import type { ModelId } from '@/contexts/EnsembleContext';

export interface ConfidenceComponent {
  component_id: number;
  prediction_id: number;
  player_id: number;
  game_id: number;
  prediction_date: string;
  model_version: string;
  stat_name: string;
  ensemble_score: number;
  variance_score: number;
  feature_score: number;
  experience_score: number;
  transaction_score: number;
  opponent_adj: number;
  injury_adj: number;
  playoff_adj: number;
  back_to_back_adj: number;
  raw_score: number;
  calibrated_score: number;
  n_models: number;
  created_at: string;
}


export function useConfidenceComponents(
  playerId: string | undefined,
  gameId: string | undefined,
  selectedModels: ModelId[]
) {
  return useQuery<ConfidenceComponent[], Error>({
    queryKey: ['confidence-components', playerId, gameId, selectedModels.slice().sort()],
    queryFn: async () => {
      if (!playerId || !gameId || selectedModels.length === 0) {
        return [];
      }

      const numericPlayerId = Number(playerId);
      if (Number.isNaN(numericPlayerId)) {
        return [];
      }

      
      const { data: predictions, error: predError } = await supabase
        .from('predictions')
        .select('prediction_id, model_version')
        .eq('player_id', numericPlayerId)
        .eq('game_id', gameId)
        .in('model_version', selectedModels);

      if (predError) throw predError;
      if (!predictions || predictions.length === 0) {
        return [];
      }

      const predictionIds = predictions.map(p => p.prediction_id);

      
      const { data, error } = await supabase
        .from('confidence_components')
        .select('*')
        .in('prediction_id', predictionIds)
        .order('stat_name', { ascending: true })
        .order('model_version', { ascending: true });

      if (error) throw error;
      if (!data || data.length === 0) {
        return [];
      }

      
      const componentsByStat = new Map<string, ConfidenceComponent[]>();
      
      for (const component of data as ConfidenceComponent[]) {
        const key = component.stat_name;
        if (!componentsByStat.has(key)) {
          componentsByStat.set(key, []);
        }
        componentsByStat.get(key)!.push(component);
      }

      
      const averagedComponents: ConfidenceComponent[] = [];
      
      for (const [statName, components] of componentsByStat.entries()) {
        
        const filteredComponents = components.filter(c => 
          selectedModels.includes(c.model_version as ModelId)
        );

        if (filteredComponents.length === 0) continue;

        const avg = (arr: number[]) => arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
        const nModels = selectedModels.length;
        
        
        
        const ensembleScore = nModels === 1 ? 0 : avg(filteredComponents.map(c => c.ensemble_score));
        
        
        const varianceScore = avg(filteredComponents.map(c => c.variance_score));
        
        const averaged: ConfidenceComponent = {
          ...filteredComponents[0], 
          component_id: filteredComponents[0].component_id, 
          prediction_id: filteredComponents[0].prediction_id, 
          model_version: nModels === 1 ? selectedModels[0] : 'ensemble', 
          ensemble_score: ensembleScore,
          variance_score: varianceScore,
          feature_score: avg(filteredComponents.map(c => c.feature_score)),
          experience_score: avg(filteredComponents.map(c => c.experience_score)),
          transaction_score: avg(filteredComponents.map(c => c.transaction_score)),
          opponent_adj: avg(filteredComponents.map(c => c.opponent_adj)),
          injury_adj: avg(filteredComponents.map(c => c.injury_adj)),
          playoff_adj: avg(filteredComponents.map(c => c.playoff_adj)),
          back_to_back_adj: avg(filteredComponents.map(c => c.back_to_back_adj)),
          raw_score: avg(filteredComponents.map(c => c.raw_score)),
          calibrated_score: avg(filteredComponents.map(c => c.calibrated_score)),
          n_models: nModels, 
        };

        averagedComponents.push(averaged);
      }

      return averagedComponents;
    },
    enabled: !!playerId && !!gameId && selectedModels.length > 0,
  });
}

