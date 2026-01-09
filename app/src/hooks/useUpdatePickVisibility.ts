import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId, validateUUID } from '@/lib/security';
import { toast } from 'sonner';
import { useSendPickShareNotification } from './useSendPickShareNotification';
import { logger } from '@/lib/logger';

export type PickVisibility = 'private' | 'friends' | 'public' | 'group' | 'custom';

export function useUpdatePickVisibility() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const sendNotification = useSendPickShareNotification();

  return useMutation({
    mutationFn: async ({
      pickId,
      visibility,
      groupId,
      groupIds,
      friendIds,
      previousVisibility
    }: {
      pickId: string;
      visibility: PickVisibility;
      groupId?: string; 
      groupIds?: string[];
      friendIds?: string[];
      previousVisibility?: PickVisibility | null;
    }) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedPickId = validateUUID(pickId, 'Pick ID');

      
      const finalGroupIds = groupIds || (groupId ? [groupId] : []);
      const validatedGroupIds = finalGroupIds.map(id => validateUUID(id, 'Group ID'));

      
      const validVisibilities: PickVisibility[] = ['private', 'friends', 'public', 'group', 'custom'];
      if (!validVisibilities.includes(visibility)) {
        throw new Error('Invalid visibility value');
      }

      
      const updateData: any = {
        updated_at: new Date().toISOString(),
      };

      if (visibility === 'private') {
        updateData.visibility = null;
        updateData.shared_group_id = null;
        updateData.shared_group_ids = [];
        updateData.shared_friend_ids = [];
      } else if (visibility === 'custom') {
        
        updateData.visibility = 'custom';
        
        updateData.shared_group_id = validatedGroupIds.length > 0 ? validatedGroupIds[0] : null;
        
        updateData.shared_group_ids = validatedGroupIds;
        updateData.shared_friend_ids = friendIds || [];
      } else if (visibility === 'group' && validatedGroupIds.length > 0) {
        updateData.visibility = 'group';
        
        updateData.shared_group_id = validatedGroupIds[0];
        
        updateData.shared_group_ids = validatedGroupIds;
        updateData.shared_friend_ids = [];
      } else if (visibility === 'friends') {
        updateData.visibility = 'friends';
        updateData.shared_group_id = null;
        updateData.shared_group_ids = [];
        
        updateData.shared_friend_ids = friendIds || [];
      } else {
        updateData.visibility = visibility;
        updateData.shared_group_id = null;
        updateData.shared_group_ids = [];
        updateData.shared_friend_ids = [];
      }

      const { data, error } = await supabase
        .from('user_picks')
        .update(updateData)
        .eq('id', validatedPickId)
        .eq('owner_id', validatedUserId) 
        .select()
        .single();

      if (error) throw error;

      const wasPrivate = !previousVisibility || previousVisibility === 'private';
      const isNowShared = visibility === 'friends' || visibility === 'group' || visibility === 'custom';

      if (wasPrivate && isNowShared) {
        if (visibility === 'group' || (visibility === 'custom' && finalGroupIds.length > 0)) {
          finalGroupIds.forEach(gId => {
            sendNotification.mutate({
              pickId,
              visibility: 'group',
              groupId: gId,
            });
          });
        }

        if (visibility === 'friends' || (visibility === 'custom' && friendIds && friendIds.length > 0)) {
          const targetFriendIds = friendIds || [];
          if (targetFriendIds.length > 0) {
            targetFriendIds.forEach(fId => {
              sendNotification.mutate({
                pickId,
                visibility: 'friends',
                groupId: undefined,
              });
            });
          } else {
            sendNotification.mutate({
              pickId,
              visibility: 'friends',
              groupId: undefined,
            });
          }
        }
      }

      return data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['userPicks', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['communityPicks'] });
      queryClient.invalidateQueries({ queryKey: ['conversations', user?.id] });
      toast.success('Pick sharing updated');
    },
    onError: (error: any) => {
      logger.error('Error updating pick visibility', error as Error);
      toast.error('Failed to update pick sharing');
    },
  });
}

