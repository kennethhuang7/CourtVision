import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

interface SendMessageInput {
  conversationType: 'dm' | 'group';
  conversationId: string;
  content: string;
}

export function useSendMessage() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (input: SendMessageInput) => {
      if (!user) {
        throw new Error('You must be logged in to send messages.');
      }

      if (input.content.trim().length === 0) {
        throw new Error('Message cannot be empty.');
      }

      if (input.content.length > 2000) {
        throw new Error('Message exceeds 2000 characters.');
      }
      
      
      
      

      
      if (input.conversationType !== 'dm' && input.conversationType !== 'group') {
        throw new Error('Invalid conversation type');
      }

      
      const validatedConversationId = validateUUID(input.conversationId, 'Conversation ID');

      const { data, error } = await supabase.rpc('send_message', {
        p_conversation_type: input.conversationType,
        p_conversation_id: validatedConversationId,
        p_content: input.content.trim(),
      });

      if (error) {
        logger.error('Error sending message', error as Error, {
          conversationType: input.conversationType,
          conversationId: input.conversationId.substring(0, 8) + '...',
        });
        throw error;
      }

      return data;
    },
    onSuccess: (_, variables) => {
      
      queryClient.invalidateQueries({ 
        queryKey: ['messages', variables.conversationType, variables.conversationId] 
      });
      
      queryClient.invalidateQueries({ queryKey: ['conversations', user?.id] });
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to send message');
    },
  });
}

