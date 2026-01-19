import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

interface AddReactionInput {
  messageId: string;
  emoji: string;
}

export function useAddReaction() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async ({ messageId, emoji }: AddReactionInput) => {
      if (!user) {
        throw new Error('You must be logged in to react to messages.');
      }

      if (!messageId || !emoji) {
        throw new Error('Message ID and emoji are required.');
      }

      
      const { data, error } = await supabase
        .from('message_reactions')
        .insert({
          message_id: messageId,
          user_id: user.id,
          emoji,
        })
        .select()
        .single();

      if (error) {
        
        if (error.code === '23505') {
          throw new Error('You already reacted with this emoji');
        }

        logger.error('Failed to add reaction', error as Error, { messageId, emoji });
        throw error;
      }

      return data;
    },
    onSuccess: (_, variables) => {
      
      queryClient.invalidateQueries({
        queryKey: ['messageReactions', variables.messageId],
      });

      
      queryClient.invalidateQueries({
        queryKey: ['batchMessageReactions'],
      });
    },
    onError: (error: any) => {
      if (error.message !== 'You already reacted with this emoji') {
        toast.error(error?.message || 'Failed to add reaction');
      }
    },
  });
}
