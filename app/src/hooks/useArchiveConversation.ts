import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';
import { logger } from '@/lib/logger';

interface ArchiveInput {
  conversationType: 'dm' | 'group';
  conversationId: string;
  archive: boolean;
}

export function useArchiveConversation() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (input: ArchiveInput) => {
      if (!user) {
        throw new Error('You must be logged in.');
      }

      
      if (input.conversationType !== 'dm' && input.conversationType !== 'group') {
        throw new Error('Invalid conversation type');
      }

      
      const validatedConversationId = validateUUID(input.conversationId, 'Conversation ID');

      const { error } = await supabase.rpc('toggle_conversation_archive', {
        p_conversation_type: input.conversationType,
        p_conversation_id: validatedConversationId,
        p_archive: Boolean(input.archive),
      });

      if (error) {
        logger.error('Error archiving conversation', error as Error);
        throw error;
      }
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['conversations', user?.id] });
    },
  });
}

