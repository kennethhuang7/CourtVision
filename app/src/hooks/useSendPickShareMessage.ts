import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';
import { logger } from '@/lib/logger';

interface SendPickShareMessageInput {
  conversationType: 'dm' | 'group';
  conversationId: string;
  pickId: string;
  playerId: number;
  gameId: string;
  statName: string;
  lineValue: number;
  overUnder: 'over' | 'under';
}

export function useSendPickShareMessage() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (input: SendPickShareMessageInput) => {
      if (!user) {
        throw new Error('You must be logged in to share picks.');
      }

      if (input.conversationType !== 'dm' && input.conversationType !== 'group') {
        throw new Error('Invalid conversation type');
      }

      const validatedConversationId = validateUUID(input.conversationId, 'Conversation ID');
      const validatedPickId = validateUUID(input.pickId, 'Pick ID');

      const { data: rpcData, error: rpcError } = await supabase.rpc('send_pick_share_message', {
        p_conversation_type: input.conversationType,
        p_conversation_id: validatedConversationId,
        p_pick_id: validatedPickId,
        p_player_id: input.playerId,
        p_game_id: input.gameId,
        p_stat_name: input.statName,
        p_line_value: input.lineValue,
        p_over_under: input.overUnder,
      });

      if (!rpcError) {
        return rpcData;
      }

      if (rpcError.code === '42883') {
        const { data, error } = await supabase
          .from('user_messages')
          .insert({
            sender_id: user.id,
            conversation_type: input.conversationType,
            conversation_id: validatedConversationId,
            content: '',
            message_type: 'pick_share',
            metadata: {
              pick_id: validatedPickId,
              player_id: input.playerId,
              game_id: input.gameId,
              stat_name: input.statName,
              line_value: input.lineValue,
              over_under: input.overUnder,
            },
          })
          .select()
          .single();

        if (error) {
          logger.error('Error sending pick share message via insert', error as Error);
          throw error;
        }

        return data;
      }

      logger.error('Error sending pick share message via RPC', rpcError as Error);
      throw rpcError;
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({
        queryKey: ['messages', variables.conversationType, variables.conversationId],
        exact: false
      });
      queryClient.invalidateQueries({ queryKey: ['conversations'], exact: false });
    },
    onError: (error: any) => {
      logger.error('Failed to send pick share message', error as Error);
    },
  });
}
