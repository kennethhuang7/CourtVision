import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId, validateUUID } from '@/lib/security';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

export function useCreateDM() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (otherUserId: string) => {
      if (!user) {
        throw new Error('You must be logged in to create a DM.');
      }

      
      const validatedUserId = validateUserId(user.id);
      const validatedOtherUserId = validateUUID(otherUserId, 'Other user ID');

      if (validatedUserId === validatedOtherUserId) {
        throw new Error('Cannot create a DM with yourself.');
      }

      const { data, error } = await supabase.rpc('get_or_create_dm_conversation', {
        p_user1_id: validatedUserId,
        p_user2_id: validatedOtherUserId,
      });

      if (error) {
        logger.error('Error creating DM', error as Error);
        throw error;
      }

      return data as string; 
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['conversations', user?.id] });
    },
    onError: (error: any) => {
      const message = typeof error?.message === 'string' ? error.message : '';
      const code = typeof error?.code === 'string' ? error.code : '';
      if (code === '42883') {
        if (message.includes('uuid_generate_v5') || message.includes('uuid_nil')) {
          toast.error(
            'Messaging setup is incomplete. Your DB function is calling uuid_generate_v5/uuid_nil but can’t see it. In Supabase SQL editor: (1) create extension if not exists "uuid-ossp"; (2) ensure BOTH functions have search_path: alter function public.get_or_create_dm_conversation(uuid, uuid) set search_path = public, extensions; alter function public.get_dm_conversation_id(uuid, uuid) set search_path = public, extensions;'
          );
          return;
        }
        toast.error(message || 'Failed to create DM (missing database function)');
        return;
      }

      if (message.includes('uuid_generate_v5') || message.includes('uuid_nil')) {
        toast.error(
          'Messaging setup is incomplete. Your DB function is calling uuid_generate_v5/uuid_nil but can’t see it. In Supabase SQL editor: (1) create extension if not exists "uuid-ossp"; (2) ensure BOTH functions have search_path: alter function public.get_or_create_dm_conversation(uuid, uuid) set search_path = public, extensions; alter function public.get_dm_conversation_id(uuid, uuid) set search_path = public, extensions;'
        );
        return;
      }

      toast.error(message || 'Failed to create DM');
    },
  });
}

