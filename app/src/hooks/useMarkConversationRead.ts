import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';

interface MarkReadInput {
  conversationType: 'dm' | 'group';
  conversationId: string;
}

export function useMarkConversationRead() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (input: MarkReadInput) => {
      if (!user) {
        throw new Error('You must be logged in.');
      }

      
      if (input.conversationType !== 'dm' && input.conversationType !== 'group') {
        throw new Error('Invalid conversation type');
      }

      
      const validatedConversationId = validateUUID(input.conversationId, 'Conversation ID');

      const { error } = await supabase.rpc('mark_conversation_read', {
        p_conversation_type: input.conversationType,
        p_conversation_id: validatedConversationId,
      });

      if (error) {
        logger.error('Error marking conversation as read', error as Error);
        throw error;
      }
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['conversations', user?.id] });
    },
  });
}

