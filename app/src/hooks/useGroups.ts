import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId, validateUUID } from '@/lib/security';
import { toast } from 'sonner';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { useRef, useEffect } from 'react';
import { logger } from '@/lib/logger';

export interface Group {
  id: string;
  owner_id: string;
  name: string;
  description: string | null;
  profile_picture_url: string | null;
  members_can_invite: boolean;
  created_at: string;
  updated_at: string;
  
  member_count?: number;
  is_owner?: boolean;
}

export interface GroupMember {
  id: string;
  group_id: string;
  user_id: string;
  joined_at: string;
  
  user_profile?: {
    user_id: string;
    username: string;
    display_name: string | null;
    profile_picture_url: string | null;
  };
}

export interface GroupInvite {
  id: string;
  group_id: string;
  inviter_id: string;
  invitee_id: string;
  status: 'pending' | 'accepted' | 'declined';
  created_at: string;
  updated_at: string;
  
  group?: Group;
  inviter_profile?: {
    user_id: string;
    username: string;
    display_name: string | null;
    profile_picture_url: string | null;
  };
  invitee_profile?: {
    user_id: string;
    username: string;
    display_name: string | null;
    profile_picture_url: string | null;
  };
}


export function useGroups() {
  const { user } = useAuth();

  return useQuery<Group[], Error>({
    queryKey: ['groups', user?.id],
    queryFn: async () => {
      if (!user?.id) return [];

      
      const { data: allGroups, error: rpcError } = await supabase
        .rpc('get_user_accessible_groups');

      if (rpcError) {
        logger.error('Error fetching groups via RPC', rpcError as Error);
        throw rpcError; 
      }

      if (!allGroups || allGroups.length === 0) {
        return [];
      }

      
      const countMap = new Map<string, number>();
      for (const group of allGroups) {
        try {
          const { data: members } = await supabase.rpc('get_group_members', { 
            p_group_id: group.id 
          });
          
          countMap.set(group.id, (members || []).length);
        } catch (err) {
          
          countMap.set(group.id, 1);
        }
      }

      
      const data = allGroups.map((group: any) => {
        return {
          ...group,
          member_count: countMap.get(group.id) || 1, 
          is_owner: group.owner_id === user.id,
        };
      });

      return data.sort((a: any, b: any) => 
        new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      ) as Group[];

    },
    enabled: !!user?.id,
    staleTime: 30000,
    refetchOnWindowFocus: false,
  });
}


export function useGroup(groupId: string | null) {
  const { user } = useAuth();

  return useQuery<Group & { members: GroupMember[] }, Error>({
    queryKey: ['group', groupId],
    queryFn: async () => {
      if (!groupId || !user?.id) throw new Error('Group ID or user ID missing');

      
      const validatedUserId = validateUserId(user.id);
      const validatedGroupId = validateUUID(groupId, 'Group ID');

      
      let group: any;
      const { data: accessibleGroups } = await supabase.rpc('get_user_accessible_groups');
      if (accessibleGroups) {
        group = accessibleGroups.find((g: any) => g.id === validatedGroupId);
      }
      
      
      if (!group) {
        throw new Error('Group not found or access denied');
      }

      
      const { data: membersData, error: membersError } = await supabase
        .rpc('get_group_members', { p_group_id: validatedGroupId });

      if (membersError) {
        logger.error('Error fetching group members', membersError as Error, { groupId });
        throw membersError;
      }

      const members = (membersData || []).map((m: any) => ({
        id: m.member_id,
        group_id: m.member_group_id,
        user_id: m.member_user_id,
        joined_at: m.member_joined_at,
        user_profile: m.member_user_profile,
      }));

      
      
      return {
        ...group,
        is_owner: group.owner_id === validatedUserId,
        members: (members || []) as GroupMember[],
      } as Group & { members: GroupMember[] };
    },
    enabled: !!groupId && !!user?.id,
  });
}


export function useCreateGroup() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (data: {
      name: string;
      description?: string;
      inviteeIds?: string[];
    }) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);

      
      if (!data.name || typeof data.name !== 'string' || data.name.trim().length === 0) {
        throw new Error('Group name is required');
      }
      if (data.name.length > 100) {
        throw new Error('Group name is too long');
      }
      
      
      if (data.description !== undefined && data.description !== null) {
        if (typeof data.description !== 'string') {
          throw new Error('Group description must be a string');
        }
        if (data.description.length > 500) {
          throw new Error('Group description cannot exceed 500 characters');
        }
      }

      
      const { data: groupId, error: groupError } = await supabase
        .rpc('create_group', {
          p_name: data.name.trim(),
          p_description: data.description?.trim() || null,
        });

      if (groupError) throw groupError;
      if (!groupId) throw new Error('Failed to create group');

      
      const validatedGroupId = validateUUID(groupId, 'Group ID');

      
      if (data.inviteeIds && data.inviteeIds.length > 0) {
        
        const validatedInviteeIds = Array.from(
          new Set(data.inviteeIds.map(id => validateUUID(id, 'Invitee ID')))
        );
        
        try {
          for (const inviteeId of validatedInviteeIds) {
            if (inviteeId === validatedUserId) {
              throw new Error('Cannot invite yourself to a group');
            }
            const { error: inviteError } = await supabase
              .rpc('send_group_invite', {
                p_group_id: validatedGroupId,
                p_invitee_id: inviteeId,
              });
            
            if (inviteError) {
              throw inviteError;
            }
          }
        } catch (inviteError) {
          
          const { error: deleteError } = await supabase.rpc('delete_group', { p_group_id: validatedGroupId });
          if (deleteError) {
            logger.error('Failed to roll back group after invite error', deleteError as Error, {
              groupId: validatedGroupId,
            });
          }
          throw inviteError as Error;
        }
      }

      
      const { data: accessibleGroups } = await supabase.rpc('get_user_accessible_groups');
      const group = accessibleGroups?.find((g: any) => g.id === validatedGroupId);
      
      if (!group) throw new Error('Failed to fetch created group');
      
      return {
        ...group,
        is_owner: true,
      } as Group;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['groups'] });
      queryClient.invalidateQueries({ queryKey: ['groupInvites'] });
      toast.success('Group created successfully');
    },
    onError: (error: any) => {
      logger.error('Error creating group', error as Error);
      toast.error(error.message || 'Failed to create group');
    },
  });
}


export function useUpdateGroup() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (data: {
      groupId: string;
      name?: string;
      description?: string;
      profile_picture_url?: string | null;
      members_can_invite?: boolean;
    }) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedGroupId = validateUUID(data.groupId, 'Group ID');

      
      if (data.name !== undefined) {
        if (typeof data.name !== 'string' || data.name.trim().length === 0) {
          throw new Error('Group name cannot be empty');
        }
        if (data.name.length > 100) {
          throw new Error('Group name is too long');
        }
      }
      
      
      if (data.description !== undefined && data.description !== null) {
        if (typeof data.description !== 'string') {
          throw new Error('Group description must be a string');
        }
        if (data.description.length > 500) {
          throw new Error('Group description cannot exceed 500 characters');
        }
      }

      
      const rpcParams: any = {
        p_group_id: validatedGroupId,
        p_name: data.name?.trim() || '',
      };
      
      if (data.description !== undefined) {
        rpcParams.p_description = data.description;
      }
      
      
      if (data.profile_picture_url !== undefined) {
        rpcParams.p_profile_picture_url = data.profile_picture_url;
      } else {
        
        
      }
      
      if (data.members_can_invite !== undefined) {
        rpcParams.p_members_can_invite = data.members_can_invite;
      }

      const { error: updateError } = await supabase.rpc('update_group', rpcParams);

      if (updateError) {
        logger.error('RPC update_group error', updateError as Error);
        throw updateError;
      }

      
      
    },
    onSuccess: (_, variables) => {
      
      queryClient.invalidateQueries({ queryKey: ['groups'] });
      queryClient.invalidateQueries({ queryKey: ['group', variables.groupId] });
      queryClient.invalidateQueries({ queryKey: ['group'] });
      toast.success('Group updated successfully');
    },
    onError: (error: any) => {
      logger.error('Error updating group', error as Error);
      toast.error(error.message || 'Failed to update group');
    },
  });
}


export function useDeleteGroup() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (groupId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedGroupId = validateUUID(groupId, 'Group ID');

      const { error } = await supabase.rpc('delete_group', {
        p_group_id: validatedGroupId,
      });

      if (error) throw error;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['groups'] });
      toast.success('Group deleted successfully');
    },
    onError: (error: any) => {
      logger.error('Error deleting group', error as Error);
      toast.error(error.message || 'Failed to delete group');
    },
  });
}


export function useGroupInvites(groupId?: string | null) {
  const { user } = useAuth();
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousInviteIdsRef = useRef<Set<string>>(new Set());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined' && user?.id) {
      try {
        const stored = localStorage.getItem(`courtvision-notified-group-invites-${user.id}`);
        if (stored) {
          previousInviteIdsRef.current = new Set(JSON.parse(stored));
        }
      } catch (e) {
        logger.warn('Error loading group invite notification state from localStorage', { error: e });
      }
      hasInitializedRef.current = true;
    }
  }, [user?.id]);

  const query = useQuery<GroupInvite[], Error>({
    queryKey: ['groupInvites', groupId, user?.id],
    queryFn: async () => {
      if (!user?.id) return [];

      
      const { data: invitesData, error } = await supabase.rpc('get_user_group_invites');

      if (error) throw error;

      
      let invites = invitesData || [];
      if (groupId) {
        invites = invites.filter((inv: any) => inv.group_id === groupId);
      }

      
      const mappedInvites = invites.map((inv: any) => ({
        id: inv.id,
        group_id: inv.group_id,
        inviter_id: inv.inviter_id,
        invitee_id: inv.invitee_id,
        status: inv.status,
        created_at: inv.created_at,
        updated_at: inv.updated_at,
        group: inv.group_data ? {
          id: inv.group_data.id,
          owner_id: inv.group_data.owner_id,
          name: inv.group_data.name,
          description: inv.group_data.description,
          created_at: inv.group_data.created_at,
          updated_at: inv.group_data.updated_at,
        } : undefined,
        inviter_profile: inv.inviter_profile,
        invitee_profile: inv.invitee_profile,
      })) as GroupInvite[];

      
      const currentUserInvites = mappedInvites.filter(inv => inv.invitee_id === user?.id && inv.status === 'pending');
      const currentInviteIds = new Set(currentUserInvites.map(inv => inv.id));
      
      
      
      if (previousInviteIdsRef.current.size === 0 && currentInviteIds.size > 0) {
        
        previousInviteIdsRef.current = new Set(currentInviteIds);
        
        if (typeof window !== 'undefined' && user?.id) {
          localStorage.setItem(
            `courtvision-notified-group-invites-${user.id}`,
            JSON.stringify(Array.from(currentInviteIds))
          );
        }
      } else {
        currentUserInvites.forEach(invite => {
          if (!previousInviteIdsRef.current.has(invite.id)) {
            const groupName = invite.group?.name || 'a group';
            const inviterName = invite.inviter_profile?.display_name || 
                                invite.inviter_profile?.username || 
                                'Someone';
            notify('invites', 'Group Invite', `${inviterName} invited you to join ${groupName}`, {
              tag: `group-invite-${invite.id}`,
            });
          }
        });
        previousInviteIdsRef.current = new Set(currentInviteIds);
      }

      
      if (typeof window !== 'undefined' && user?.id) {
        localStorage.setItem(
          `courtvision-notified-group-invites-${user.id}`,
          JSON.stringify(Array.from(currentInviteIds))
        );
      }

      return mappedInvites;
    },
    enabled: !!user?.id && !doNotDisturb, 
    refetchInterval: 300000, 
    refetchIntervalInBackground: true, 
    staleTime: 30000,
    refetchOnWindowFocus: false,
  });
  
  return query;
}


export function useSendGroupInvite() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (data: { groupId: string; inviteeId: string }) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedGroupId = validateUUID(data.groupId, 'Group ID');
      const validatedInviteeId = validateUUID(data.inviteeId, 'Invitee ID');

      if (validatedInviteeId === validatedUserId) {
        throw new Error('Cannot invite yourself to a group');
      }

      const { error } = await supabase.rpc('send_group_invite', {
        p_group_id: validatedGroupId,
        p_invitee_id: validatedInviteeId,
      });

      if (error) throw error;
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['groupInvites'] });
      queryClient.invalidateQueries({ queryKey: ['group', variables.groupId] });
      toast.success('Invite sent successfully');
    },
    onError: (error: any) => {
      logger.error('Error sending invite', error as Error);
      toast.error(error.message || 'Failed to send invite');
    },
  });
}


export function useAcceptGroupInvite() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const { notify } = useNotifications();

  return useMutation({
    mutationFn: async (inviteId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedInviteId = validateUUID(inviteId, 'Invite ID');

      const { error } = await supabase.rpc('accept_group_invite', {
        p_invite_id: validatedInviteId,
      });
      
      if (error) throw error;
      
      
      const { data: inviteData } = await supabase
        .from('group_invites')
        .select('group_id')
        .eq('id', validatedInviteId)
        .single();
      
      if (inviteData?.group_id) {
        const { data: groupData } = await supabase
          .from('groups')
          .select('name')
          .eq('id', inviteData.group_id)
          .single();
        
        return { groupId: inviteData.group_id, groupName: groupData?.name };
      }
      
      return { groupId: null, groupName: null };
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['groupInvites'] });
      queryClient.invalidateQueries({ queryKey: ['groups'] });
      
      if (data?.groupName) {
        notify('groupUpdates', 'Joined Group', `You joined ${data.groupName}`, {
          tag: `group-joined-${data.groupId}`,
        });
      }
      
      toast.success('Invite accepted');
    },
    onError: (error: any) => {
      logger.error('Error accepting invite', error as Error);
      toast.error(error.message || 'Failed to accept invite');
    },
  });
}


export function useDeclineGroupInvite() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (inviteId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedInviteId = validateUUID(inviteId, 'Invite ID');

      await supabase.rpc('decline_group_invite', {
        p_invite_id: validatedInviteId,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['groupInvites'] });
      toast.success('Invite declined');
    },
    onError: (error: any) => {
      logger.error('Error declining invite', error as Error);
      toast.error(error.message || 'Failed to decline invite');
    },
  });
}


export function useRemoveGroupMember() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const { notify } = useNotifications();

  return useMutation({
    mutationFn: async (data: { groupId: string; userId: string }) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedGroupId = validateUUID(data.groupId, 'Group ID');
      const validatedTargetUserId = validateUUID(data.userId, 'User ID');

      await supabase.rpc('remove_group_member', {
        p_group_id: validatedGroupId,
        p_user_id: validatedTargetUserId,
      });
    },
    onSuccess: async (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['group', variables.groupId] });
      queryClient.invalidateQueries({ queryKey: ['groups'] });
      
      
      const { data: groupData } = await supabase
        .from('groups')
        .select('name')
        .eq('id', variables.groupId)
        .single();
      
      const groupName = groupData?.name || 'the group';
      notify('groupUpdates', 'Group Member Removed', `A member was removed from ${groupName}`, {
        tag: `group-member-removed-${variables.groupId}`,
      });
      
      toast.success('Member removed from group');
    },
    onError: (error: any) => {
      logger.error('Error removing member', error as Error);
      toast.error(error.message || 'Failed to remove member');
    },
  });
}


export function useLeaveGroup() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (groupId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);
      const validatedGroupId = validateUUID(groupId, 'Group ID');

      
      await supabase.rpc('remove_group_member', {
        p_group_id: validatedGroupId,
        p_user_id: validatedUserId,
      });
    },
    onSuccess: (_, groupId) => {
      queryClient.invalidateQueries({ queryKey: ['group', groupId] });
      queryClient.invalidateQueries({ queryKey: ['groups'] });
      toast.success('Left group successfully');
    },
    onError: (error: any) => {
      logger.error('Error leaving group', error as Error);
      toast.error(error.message || 'Failed to leave group');
    },
  });
}

