import { useMutation } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';
import { logger } from '@/lib/logger';

export function useSendPickTailNotification() {
  const { user } = useAuth();

  return useMutation({
    mutationFn: async ({
      originalPickId,
      tailedPickId,
    }: {
      originalPickId: string; 
      tailedPickId: string; 
    }) => {
      if (!user) {
        throw new Error('You must be logged in to tail picks.');
      }

      
      const validatedOriginalPickId = validateUUID(originalPickId, 'Original Pick ID');
      const validatedTailedPickId = validateUUID(tailedPickId, 'Tailed Pick ID');

      
      
      const { data, error } = await supabase.rpc('send_pick_tail_notification', {
        p_original_pick_id: validatedOriginalPickId,
        p_tailed_pick_id: validatedTailedPickId,
      }).catch(() => {
        
        return { data: null, error: { message: 'Notification RPC function not available' } };
      });

      if (error) {
        logger.error('Error sending pick tail notification', error as Error);
        
        return null;
      }

      return data;
    },
  });
}

