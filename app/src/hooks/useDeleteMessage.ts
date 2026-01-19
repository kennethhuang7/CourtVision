import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

export function useDeleteMessage() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (messageId: string) => {
      if (!user) {
        throw new Error('You must be logged in to delete messages.');
      }

      
      const validatedMessageId = validateUUID(messageId, 'Message ID');

      const { data, error } = await supabase.rpc('delete_message', {
        p_message_id: validatedMessageId,
      });

      if (error) {
        logger.error('Error deleting message', error as Error, {
          messageId: messageId.substring(0, 8) + '...',
        });
        throw error;
      }

      return data;
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['messages'] });
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to delete message');
    },
  });
}

