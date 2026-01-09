import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { logger } from '@/lib/logger';

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
        
        if (error.code === 'P0001' || error.message?.includes('already exists')) {
          
          return groupId;
        }
        throw error;
      }

      return data as string; 
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['conversations'] });
    },
    onError: (error: any) => {
      logger.error('Failed to ensure group conversation', error as Error);
      
    },
  });
}

