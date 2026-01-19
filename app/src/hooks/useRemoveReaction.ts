import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

interface RemoveReactionInput {
  messageId: string;
  emoji: string;
}

export function useRemoveReaction() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async ({ messageId, emoji }: RemoveReactionInput) => {
      if (!user) {
        throw new Error('You must be logged in to remove reactions.');
      }

      if (!messageId || !emoji) {
        throw new Error('Message ID and emoji are required.');
      }

      
      const { error } = await supabase
        .from('message_reactions')
        .delete()
        .eq('message_id', messageId)
        .eq('user_id', user.id)
        .eq('emoji', emoji);

      if (error) {
        logger.error('Failed to remove reaction', error as Error, { messageId, emoji });
        throw error;
      }

      return { messageId, emoji };
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
      toast.error(error?.message || 'Failed to remove reaction');
    },
  });
}
