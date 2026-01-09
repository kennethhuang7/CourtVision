import { useMutation } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID } from '@/lib/security';
import { logger } from '@/lib/logger';

export function useSendPickShareNotification() {
  const { user } = useAuth();

  return useMutation({
    mutationFn: async ({
      pickId,
      visibility,
      groupId,
    }: {
      pickId: string;
      visibility: 'friends' | 'group' | 'public';
      groupId?: string;
    }) => {
      if (!user) {
        throw new Error('You must be logged in to share picks.');
      }

      
      if (visibility === 'public') {
        return null; 
      }

      
      const validVisibilities = ['friends', 'group', 'public'];
      if (!validVisibilities.includes(visibility)) {
        throw new Error('Invalid visibility value');
      }

      
      const validatedPickId = validateUUID(pickId, 'Pick ID');
      const validatedGroupId = groupId ? validateUUID(groupId, 'Group ID') : null;

      const { data, error } = await supabase.rpc('send_pick_share_notification', {
        p_pick_id: validatedPickId,
        p_visibility: visibility,
        p_group_id: validatedGroupId,
      });

      if (error) {
        logger.error('Error sending pick share notification', error as Error);
        
        return null;
      }

      return data;
    },
  });
}

