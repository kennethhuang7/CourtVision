import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { logger } from '@/lib/logger';
import { toast } from 'sonner';

async function findExistingConversation(groupId: string): Promise<string | null> {
  const { data: existingConv } = await supabase
    .from('user_conversations')
    .select('conversation_id')
    .eq('conversation_type', 'group')
    .eq('group_id', groupId)
    .single();

  return existingConv?.conversation_id || null;
}

export function useEnsureGroupConversation() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (groupId: string) => {
      if (!user) {
        throw new Error('You must be logged in.');
      }

      const { data, error } = await supabase.rpc('ensure_group_conversation', {
        p_group_id: groupId,
      });

      if (error) {
        logger.error('Error ensuring group conversation', error as Error);

        const existingId = await findExistingConversation(groupId);
        if (existingId) {
          return existingId;
        }

        throw error;
      }

      if (data) {
        return data as string;
      }

      const existingId = await findExistingConversation(groupId);
      if (existingId) {
        return existingId;
      }

      throw new Error('Failed to create or find group conversation');
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conversations'] });
    },
    onError: (error: any) => {
      logger.error('Failed to ensure group conversation', error as Error);
      toast.error(error?.message || 'Failed to get group conversation');
    },
  });
}

