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
        const isConflict = error.code === '23505' || 
                          (error as any).status === 409 || 
                          (error as any).statusCode === 409 ||
                          (error as any).code === '23505';
        
        if (isConflict) {
          const { error: deleteError } = await supabase
            .from('message_reactions')
            .delete()
            .eq('message_id', messageId)
            .eq('user_id', user.id)
            .eq('emoji', emoji);
          
          if (deleteError) {
            logger.error('Failed to remove existing reaction', deleteError as Error, { messageId, emoji });
            throw new Error('You already reacted with this emoji');
          }
          
          return { removed: true, messageId, emoji };
        }

        logger.error('Failed to add reaction', error as Error, { messageId, emoji, errorCode: error.code, status: (error as any).status });
        throw error;
      }

      return data;
    },
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({
        queryKey: ['messageReactions', variables.messageId],
      });

      queryClient.invalidateQueries({
        predicate: (query) => {
          return query.queryKey[0] === 'batchMessageReactions';
        },
      });
      
      queryClient.refetchQueries({
        predicate: (query) => {
          return query.queryKey[0] === 'batchMessageReactions';
        },
      });
    },
    onError: (error: any) => {
      if (error.message !== 'You already reacted with this emoji') {
        toast.error(error?.message || 'Failed to add reaction');
      }
    },
  });
}
