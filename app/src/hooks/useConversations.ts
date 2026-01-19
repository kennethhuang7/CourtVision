import { useQuery, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId } from '@/lib/security';
import { useEffect, useRef } from 'react';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { logger } from '@/lib/logger';

export interface Conversation {
  id: string;
  conversation_type: 'dm' | 'group';
  conversation_id: string;
  other_user_id?: string;
  group_id?: string;
  last_message_at: string | null;
  last_message_preview: string | null;
  unread_count: number;
  is_archived: boolean;
  other_user_profile?: {
    user_id: string;
    username: string;
    display_name: string | null;
    profile_picture_url: string | null;
  };
  group_info?: {
    id: string;
    name: string;
    description: string | null;
    profile_picture_url: string | null;
  };
}

export function useConversations() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousUnreadCountsRef = useRef<Map<string, number>>(new Map());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined' && user?.id) {
      try {
        const stored = localStorage.getItem(`courtvision-notified-conversations-${user.id}`);
        if (stored) {
          previousUnreadCountsRef.current = new Map(JSON.parse(stored));
        }
      } catch (e) {
        logger.warn('Error loading conversation notification state from localStorage', { error: e });
      }
      hasInitializedRef.current = true;
    }
  }, [user?.id]);

  const query = useQuery<Conversation[], Error>({
    queryKey: ['conversations', user?.id],
    queryFn: async () => {
      if (!user) {
        throw new Error('You must be logged in to view conversations.');
      }

      
      const validatedUserId = validateUserId(user.id);

      const { data, error } = await supabase.rpc('get_user_conversations');

      if (error) {
        logger.error('Error fetching conversations', error as Error);
        throw error;
      }

      const conversations = (data || []) as Conversation[];
      
      
      const currentUnreadCounts = new Map<string, number>();
      conversations.forEach(conv => {
        const key = `${conv.conversation_type}:${conv.conversation_id}`;
        currentUnreadCounts.set(key, conv.unread_count);
      });
      
      
      
      if (previousUnreadCountsRef.current.size === 0 && currentUnreadCounts.size > 0) {
        
        previousUnreadCountsRef.current = new Map(currentUnreadCounts);
        
        if (typeof window !== 'undefined' && user?.id) {
          localStorage.setItem(
            `courtvision-notified-conversations-${user.id}`,
            JSON.stringify(Array.from(currentUnreadCounts.entries()))
          );
        }
        
        return conversations;
      }
      
      conversations.forEach(conv => {
        const key = `${conv.conversation_type}:${conv.conversation_id}`;
        const previousCount = previousUnreadCountsRef.current.get(key) || 0;
        const currentCount = conv.unread_count;
        
        if (currentCount > previousCount && conv.last_message_preview) {
          
          const senderName = conv.conversation_type === 'dm' 
            ? (conv.other_user_profile?.display_name || conv.other_user_profile?.username || 'Someone')
            : (conv.group_info?.name || 'Group');
          
          notify('messages', 'New Message', `${senderName}: ${conv.last_message_preview}`, {
            tag: `message-${key}`, 
          });
        }
      });
      
      previousUnreadCountsRef.current = currentUnreadCounts;
      
      
      if (typeof window !== 'undefined' && user?.id) {
        localStorage.setItem(
          `courtvision-notified-conversations-${user.id}`,
          JSON.stringify(Array.from(currentUnreadCounts.entries()))
        );
      }
      
      return conversations;
    },
    enabled: !!user && !doNotDisturb, 
    refetchInterval: 180000, 
    refetchIntervalInBackground: true, 
  });

  
  useEffect(() => {
    if (!user) return;

    
    let validatedUserId: string;
    try {
      validatedUserId = validateUserId(user.id);
    } catch {
      
      return;
    }

    const channel = supabase
      .channel(`conversations:${validatedUserId}`)
      .on(
        'postgres_changes',
        {
          event: '*',
          schema: 'public',
          table: 'user_conversations',
          filter: `user_id=eq.${validatedUserId}`,
        },
        () => {
          
          queryClient.invalidateQueries({ queryKey: ['conversations', validatedUserId] });
        }
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, [user, queryClient]);

  return query;
}

