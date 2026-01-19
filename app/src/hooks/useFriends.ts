import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId, validateUUID } from '@/lib/security';
import { toast } from 'sonner';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { useRef, useEffect } from 'react';
import { logger } from '@/lib/logger';

export interface Friendship {
  id: string;
  requester_id: string;
  addressee_id: string;
  status: 'pending' | 'accepted' | 'declined' | 'blocked';
  created_at: string;
  updated_at: string;
  
  requester_profile?: UserProfile;
  addressee_profile?: UserProfile;
}

export interface UserProfile {
  user_id: string;
  username: string;
  display_name: string | null;
  profile_picture_url: string | null;
  banner_url: string | null;
  about_me: string | null;
  created_at: string;
  friend_code?: string | null;
}


export function useFriends(userId?: string) {
  const { user } = useAuth();
  const targetUserId = userId || user?.id;
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousFriendIdsRef = useRef<Set<string>>(new Set());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined' && targetUserId === user?.id) {
      const stored = localStorage.getItem(`courtvision-notified-friends-${targetUserId}`);
      if (stored) {
        try {
          previousFriendIdsRef.current = new Set(JSON.parse(stored));
        } catch (e) {
          logger.warn('Error loading notified friends from localStorage', { error: e });
        }
      }
      hasInitializedRef.current = true;
    }
  }, [targetUserId, user?.id]);

  const query = useQuery<Friendship[], Error>({
    queryKey: ['friends', targetUserId],
    queryFn: async () => {
      if (!targetUserId) return [];

      
      const validatedUserId = validateUserId(targetUserId);
      
      
      
      const { data: requesterData, error: requesterError } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('requester_id', validatedUserId)
        .eq('status', 'accepted');
      
      const { data: addresseeData, error: addresseeError } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('addressee_id', validatedUserId)
        .eq('status', 'accepted');
      
      if (requesterError || addresseeError) {
        throw requesterError || addresseeError;
      }

      const friendshipsData = [...(requesterData || []), ...(addresseeData || [])];

      if (!friendshipsData || friendshipsData.length === 0) return [];

      
      const friendIds = friendshipsData.map((f: any) => 
        f.requester_id === targetUserId ? f.addressee_id : f.requester_id
      );

      
      const { data: profiles, error: profilesError } = await supabase
        .from('user_profiles')
        .select('user_id, username, display_name, profile_picture_url, banner_url, about_me, created_at, friend_code')
        .in('user_id', friendIds);

      if (profilesError) throw profilesError;

      
      const friendships = friendshipsData.map((friendship: any) => {
        const friendId = friendship.requester_id === targetUserId 
          ? friendship.addressee_id 
          : friendship.requester_id;
        const friendProfile = profiles?.find((p: any) => p.user_id === friendId);

        return {
          ...friendship,
          friend_profile: friendProfile,
        };
      }) as Friendship[];

      return friendships;
    },
    enabled: !!targetUserId && !(targetUserId === user?.id && doNotDisturb), 
    refetchInterval: targetUserId === user?.id ? 300000 : false, 
    refetchIntervalInBackground: targetUserId === user?.id ? true : false, 
    staleTime: 30000, 
    refetchOnWindowFocus: false, 
  });

  
  
  useEffect(() => {
    if (!query.data || targetUserId !== user?.id) return;

    const currentFriendIds = new Set(query.data.map(f => f.id));
    
    
    
    if (previousFriendIdsRef.current.size === 0 && currentFriendIds.size > 0) {
      
      currentFriendIds.forEach(id => previousFriendIdsRef.current.add(id));
      
      if (typeof window !== 'undefined') {
        localStorage.setItem(
          `courtvision-notified-friends-${targetUserId}`,
          JSON.stringify(Array.from(previousFriendIdsRef.current))
        );
      }
      
      return;
    }

    query.data.forEach(friendship => {
      if (!previousFriendIdsRef.current.has(friendship.id)) {
        
        const friendshipWithProfile = friendship as any;
        const friendName = friendshipWithProfile.friend_profile?.display_name || 
                          friendshipWithProfile.friend_profile?.username || 
                          'Someone';
        notify('friendRequestAccepted', 'Friend Request Accepted', `${friendName} accepted your friend request`, {
          tag: `friend-accepted-${friendship.id}`,
        });
        
        previousFriendIdsRef.current.add(friendship.id);
      }
    });
    
    
    if (typeof window !== 'undefined') {
      localStorage.setItem(
        `courtvision-notified-friends-${targetUserId}`,
        JSON.stringify(Array.from(previousFriendIdsRef.current))
      );
    }
  }, [query.data, targetUserId, user?.id, notify]);
  
  return query;
}


export function useFriendCount(userId?: string) {
  const { data: friends } = useFriends(userId);
  return friends?.length || 0;
}


export function useFriendRequests() {
  const { user } = useAuth();
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousReceivedIdsRef = useRef<Set<string>>(new Set());
  const previousSentIdsRef = useRef<Set<string>>(new Set());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined' && user?.id) {
      try {
        const stored = localStorage.getItem(`courtvision-notified-friend-requests-${user.id}`);
        if (stored) {
          previousReceivedIdsRef.current = new Set(JSON.parse(stored));
        }
      } catch (e) {
        logger.warn('Error loading friend request notification state from localStorage', { error: e });
      }
      hasInitializedRef.current = true;
    }
  }, [user?.id]);

  const query = useQuery<{
    sent: Friendship[];
    received: Friendship[];
  }, Error>({
    queryKey: ['friend-requests', user?.id],
    queryFn: async () => {
      if (!user?.id) return { sent: [], received: [] };

      
      const validatedUserId = validateUserId(user.id);

      
      
      const { data: requesterData, error: requesterError } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('requester_id', validatedUserId)
        .eq('status', 'pending');
      
      const { data: addresseeData, error: addresseeError } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('addressee_id', validatedUserId)
        .eq('status', 'pending');
      
      if (requesterError || addresseeError) {
        throw requesterError || addresseeError;
      }

      const simpleData = [...(requesterData || []), ...(addresseeData || [])];

      
        const sentIds = (simpleData || []).filter((f: any) => f.requester_id === user.id).map((f: any) => f.addressee_id);
        const receivedIds = (simpleData || []).filter((f: any) => f.addressee_id === user.id).map((f: any) => f.requester_id);

        const { data: sentProfiles } = await supabase
          .from('user_profiles')
          .select('user_id, username, display_name, profile_picture_url, banner_url, about_me, created_at, friend_code')
          .in('user_id', sentIds);

        const { data: receivedProfiles } = await supabase
          .from('user_profiles')
          .select('user_id, username, display_name, profile_picture_url, banner_url, about_me, created_at, friend_code')
          .in('user_id', receivedIds);

        const sent = (simpleData || [])
          .filter((f: any) => f.requester_id === user.id)
          .map((f: any) => ({
            ...f,
            addressee_profile: sentProfiles?.find((p: any) => p.user_id === f.addressee_id),
          }));

        const received = (simpleData || [])
          .filter((f: any) => f.addressee_id === user.id)
          .map((f: any) => ({
            ...f,
            requester_profile: receivedProfiles?.find((p: any) => p.user_id === f.requester_id),
          }));

      
      const currentReceivedIds = new Set(received.map(r => r.id));
      
      
      
      if (previousReceivedIdsRef.current.size === 0 && currentReceivedIds.size > 0) {
        
        previousReceivedIdsRef.current = new Set(currentReceivedIds);
        
        if (typeof window !== 'undefined' && user?.id) {
          localStorage.setItem(
            `courtvision-notified-friend-requests-${user.id}`,
            JSON.stringify(Array.from(currentReceivedIds))
          );
        }
      } else {
        received.forEach(request => {
          if (!previousReceivedIdsRef.current.has(request.id)) {
            const requesterName = request.requester_profile?.display_name || 
                                  request.requester_profile?.username || 
                                  'Someone';
            notify('invites', 'New Friend Request', `${requesterName} sent you a friend request`, {
              tag: `friend-request-${request.id}`,
            });
          }
        });
        previousReceivedIdsRef.current = new Set(currentReceivedIds);
      }

      
      
      const currentSentIds = new Set(sent.map(s => s.id));
      previousSentIdsRef.current.forEach(previousSentId => {
        if (!currentSentIds.has(previousSentId)) {
          
          
          
          
        }
      });
      previousSentIdsRef.current = currentSentIds;

      return { sent, received };
    },
    enabled: !!user?.id && !doNotDisturb, 
    refetchInterval: 300000, 
    refetchIntervalInBackground: true, 
    staleTime: 10000, 
    refetchOnWindowFocus: false,
  });
  
  return query;
}


export function useSendFriendRequest() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (addresseeId: string) => {
      if (!user?.id) throw new Error('User not authenticated');
      if (user.id === addresseeId) throw new Error('Cannot send friend request to yourself');

      
      const validatedUserId = validateUserId(user.id);
      const validatedAddresseeId = validateUserId(addresseeId);

      
      const { data: existing1 } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('requester_id', validatedUserId)
        .eq('addressee_id', validatedAddresseeId)
        .maybeSingle();
      
      const { data: existing2 } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('requester_id', validatedAddresseeId)
        .eq('addressee_id', validatedUserId)
        .maybeSingle();
      
      const existing = existing1 || existing2;

      
      if (existing) {
        if (existing.status === 'accepted') {
          throw new Error('Already friends');
        } else if (existing.status === 'pending') {
          throw new Error('Friend request already pending');
        } else if (existing.status === 'declined') {
          
          
          const { data, error } = await supabase
            .from('user_friendships')
            .update({
              status: 'pending',
              updated_at: new Date().toISOString(),
            })
            .eq('id', existing.id)
            .select()
            .single();
          
          if (error) throw error;
          return data;
        }
      }

      const { data, error } = await supabase
        .from('user_friendships')
        .insert({
          requester_id: validatedUserId,
          addressee_id: validatedAddresseeId,
          status: 'pending',
        })
        .select()
        .single();

      if (error) throw error;
      return data;
    },
    onMutate: async (addresseeId) => {
      
      await queryClient.cancelQueries({ queryKey: ['friend-requests'] });
      await queryClient.cancelQueries({ queryKey: ['friendship-status', user?.id, addresseeId] });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['friend-requests'] });
      queryClient.invalidateQueries({ queryKey: ['friends'] });
      queryClient.invalidateQueries({ queryKey: ['friendship-status'] });
      toast.success('Friend request sent');
    },
    onError: (error: any) => {
      logger.error('Error sending friend request', error as Error);
      toast.error(error.message || 'Failed to send friend request');
    },
  });
}


export function useAcceptFriendRequest() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const { notify } = useNotifications();

  return useMutation({
    mutationFn: async (friendshipId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      const { data, error } = await supabase
        .from('user_friendships')
        .update({ status: 'accepted' })
        .eq('id', friendshipId)
        .eq('addressee_id', user.id) 
        .select()
        .single();

      if (error) throw error;
      return data;
    },
    onMutate: async (friendshipId) => {
      
      await queryClient.cancelQueries({ queryKey: ['friend-requests'] });
      await queryClient.cancelQueries({ queryKey: ['friends'] });

      
      const previousRequests = queryClient.getQueryData(['friend-requests', user?.id]);
      const previousFriends = queryClient.getQueryData(['friends', user?.id]);

      
      queryClient.setQueryData(['friend-requests', user?.id], (old: any) => {
        if (!old) return old;
        return {
          ...old,
          received: old.received?.filter((r: any) => r.id !== friendshipId) || [],
        };
      });

      return { previousRequests, previousFriends };
    },
    onError: (err, friendshipId, context) => {
      
      if (context?.previousRequests) {
        queryClient.setQueryData(['friend-requests', user?.id], context.previousRequests);
      }
      if (context?.previousFriends) {
        queryClient.setQueryData(['friends', user?.id], context.previousFriends);
      }
      logger.error('Error accepting friend request', err as Error);
      toast.error('Failed to accept friend request');
    },
    onSuccess: async (data) => {
      
      queryClient.invalidateQueries({ queryKey: ['friend-requests'] });
      queryClient.invalidateQueries({ queryKey: ['friends'] });
      queryClient.invalidateQueries({ queryKey: ['friendship-status'] });
      
      
      
      
      
      
      
      toast.success('Friend request accepted');
    },
  });
}


export function useDeclineFriendRequest() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (friendshipId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      const { data, error } = await supabase
        .from('user_friendships')
        .update({ status: 'declined' })
        .eq('id', friendshipId)
        .eq('addressee_id', user.id) 
        .select()
        .single();

      if (error) throw error;
      return data;
    },
    onMutate: async (friendshipId) => {
      
      await queryClient.cancelQueries({ queryKey: ['friend-requests'] });

      
      const previousRequests = queryClient.getQueryData(['friend-requests', user?.id]);

      
      queryClient.setQueryData(['friend-requests', user?.id], (old: any) => {
        if (!old) return old;
        return {
          ...old,
          received: old.received?.filter((r: any) => r.id !== friendshipId) || [],
        };
      });

      return { previousRequests };
    },
    onError: (err, friendshipId, context) => {
      
      if (context?.previousRequests) {
        queryClient.setQueryData(['friend-requests', user?.id], context.previousRequests);
      }
      logger.error('Error declining friend request', err as Error);
      toast.error('Failed to decline friend request');
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['friend-requests'] });
      toast.success('Friend request declined');
    },
  });
}


export function useRemoveFriend() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (friendshipId: string) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);

      
      
      const { data: friendship, error: checkError } = await supabase
        .from('user_friendships')
        .select('requester_id, addressee_id')
        .eq('id', friendshipId)
        .maybeSingle();
      
      if (checkError) throw checkError;
      if (!friendship) throw new Error('Friendship not found');
      
      
      if (friendship.requester_id !== validatedUserId && friendship.addressee_id !== validatedUserId) {
        throw new Error('Unauthorized: You can only delete your own friendships');
      }
      
      const { error } = await supabase
        .from('user_friendships')
        .delete()
        .eq('id', friendshipId);

      if (error) throw error;
    },
    onMutate: async (friendshipId) => {
      
      await queryClient.cancelQueries({ queryKey: ['friends'] });
      await queryClient.cancelQueries({ queryKey: ['friendship-status'] });

      
      const previousFriends = queryClient.getQueryData(['friends', user?.id]);

      
      queryClient.setQueryData(['friends', user?.id], (old: any) => {
        if (!old) return old;
        return old.filter((f: any) => f.id !== friendshipId);
      });

      return { previousFriends };
    },
    onError: (err, friendshipId, context) => {
      
      if (context?.previousFriends) {
        queryClient.setQueryData(['friends', user?.id], context.previousFriends);
      }
      logger.error('Error removing friend', err as Error);
      toast.error('Failed to remove friend');
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['friends'] });
      queryClient.invalidateQueries({ queryKey: ['friendship-status'] });
      toast.success('Friend removed');
    },
  });
}


export function useSearchUsers() {
  const { user } = useAuth();

  return useMutation({
    mutationFn: async (searchQuery: string): Promise<UserProfile[]> => {
      if (!user?.id) throw new Error('User not authenticated');
      if (!searchQuery.trim()) return [];
      
      
      const trimmedQuery = searchQuery.trim().substring(0, 50);
      if (trimmedQuery.length === 0) return [];

      const { data, error } = await supabase
        .from('user_profiles')
        .select('user_id, username, display_name, profile_picture_url, banner_url, about_me, created_at, friend_code')
        .ilike('username', `%${trimmedQuery}%`)
        .neq('user_id', user.id) 
        .limit(20);

      if (error) {
        logger.error('Search users error', error as Error);
        
        if (error.code === '42501' || error.message.includes('permission') || error.message.includes('policy')) {
          throw new Error('Unable to search users. Please check your database permissions.');
        }
        throw error;
      }
      
      return (data || []) as UserProfile[];
    },
  });
}


export function useSearchUsersByFriendCode() {
  const { user } = useAuth();

  return useMutation({
    mutationFn: async (friendCode: string): Promise<UserProfile | null> => {
      if (!user?.id) throw new Error('User not authenticated');
      if (!friendCode.trim()) return null;

      const trimmedCode = friendCode.trim();
      
      if (!/^[A-Za-z0-9_-]{4,20}$/.test(trimmedCode)) {
        throw new Error('Invalid friend code format');
      }

      
      const { data, error } = await supabase
        .from('user_profiles')
        .select('user_id, username, display_name, profile_picture_url, banner_url, about_me, created_at, friend_code')
        .ilike('friend_code', trimmedCode)
        .neq('user_id', user.id) 
        .maybeSingle();

      if (error) {
        logger.error('Search by friend code error', error as Error);
        throw error;
      }
      
      return data as UserProfile | null;
    },
  });
}


export function useFriendshipStatus(otherUserId: string) {
  const { user } = useAuth();

  return useQuery<{
    status: 'none' | 'pending' | 'accepted' | 'declined' | 'blocked';
    isRequester: boolean;
    friendshipId?: string;
  }, Error>({
    queryKey: ['friendship-status', user?.id, otherUserId],
    queryFn: async () => {
      if (!user?.id || !otherUserId) return { status: 'none', isRequester: false };

      
      const validatedUserId = validateUserId(user.id);
      const validatedOtherUserId = validateUserId(otherUserId);

      
      const { data: friendship1 } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('requester_id', validatedUserId)
        .eq('addressee_id', validatedOtherUserId)
        .maybeSingle();
      
      const { data: friendship2 } = await supabase
        .from('user_friendships')
        .select('*')
        .eq('requester_id', validatedOtherUserId)
        .eq('addressee_id', validatedUserId)
        .maybeSingle();
      
      const data = friendship1 || friendship2;

      if (error) {
        throw error;
      }

      if (!data) {
        
        return { status: 'none', isRequester: false };
      }

      return {
        status: data.status,
        isRequester: data.requester_id === validatedUserId,
        friendshipId: data.id,
      };
    },
    enabled: !!user?.id && !!otherUserId,
    staleTime: 10000, 
    refetchOnWindowFocus: false,
  });
}
