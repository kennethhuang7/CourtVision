import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId, validateUUID } from '@/lib/security';

interface SavePickInput {
  playerId: string;
  gameId: string;
  statName: string;
  lineValue: number;
  overUnder: 'over' | 'under';
  tailedFromPickId?: string; 
}

export function useSavePick() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (input: SavePickInput) => {
      if (!user) {
        throw new Error('You must be logged in to save a pick.');
      }

      
      const validatedUserId = validateUserId(user.id);

      
      const playerId = Number(input.playerId);
      if (isNaN(playerId) || playerId <= 0) {
        throw new Error('Invalid player ID');
      }

      
      if (!input.gameId || typeof input.gameId !== 'string') {
        throw new Error('Invalid game ID');
      }
      const trimmedGameId = input.gameId.trim();
      const gameIdPattern = /^\d{10}$/;
      if (!gameIdPattern.test(trimmedGameId)) {
        throw new Error('Invalid game ID format');
      }

      
      const validStats = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'threePointersMade'];
      if (!validStats.includes(input.statName)) {
        throw new Error('Invalid stat name');
      }

      
      if (typeof input.lineValue !== 'number' || isNaN(input.lineValue) || input.lineValue < 0) {
        throw new Error('Invalid line value');
      }

      
      if (input.overUnder !== 'over' && input.overUnder !== 'under') {
        throw new Error('Invalid over/under value');
      }

      
      let validatedTailedFromPickId: string | null = null;
      if (input.tailedFromPickId) {
        try {
          validatedTailedFromPickId = validateUUID(input.tailedFromPickId, 'Tailed from pick ID');
        } catch (error) {
          
          validatedTailedFromPickId = null;
        }
      }

      interface UserPickInsert {
        owner_id: string;
        player_id: number;
        game_id: string;
        stat_name: string;
        line_value: number;
        over_under: 'over' | 'under';
        tailed_from_pick_id?: string;
      }

      const insertData: UserPickInsert = {
        owner_id: validatedUserId,
        player_id: playerId,
        game_id: trimmedGameId,
        stat_name: input.statName,
        line_value: input.lineValue,
        over_under: input.overUnder,
      };

      if (validatedTailedFromPickId) {
        insertData.tailed_from_pick_id = validatedTailedFromPickId;
      }

      const { data, error } = await supabase.from('user_picks').insert(insertData).select().single();

      if (error) {
        throw error;
      }

      return data;
    },
    onSuccess: (data, variables) => {
      
      queryClient.invalidateQueries({ queryKey: ['userPicks', user?.id] });
      
      
      
    },
  });
}


