import { useQuery, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUUID, validateUserId } from '@/lib/security';
import { useEffect, useState, useRef } from 'react';
import { useNotifications } from '@/contexts/NotificationContext';
import { logger } from '@/lib/logger';

export interface Message {
  id: string;
  sender_id: string;
  content: string;
  is_deleted: boolean;
  created_at: string;
  message_type?: 'text' | 'pick_share';
  metadata?: {
    pick_id?: string;
    player_id?: number;
    game_id?: string;
    stat_name?: string;
    line_value?: number;
    over_under?: 'over' | 'under';
  };
  sender_profile?: {
    user_id: string;
    username: string;
    display_name: string | null;
    profile_picture_url: string | null;
  };
}

export function useMessages(
  conversationType: 'dm' | 'group',
  conversationId: string,
  enabled: boolean = true
) {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [hasMore, setHasMore] = useState(true);
  const [offset, setOffset] = useState(0);
  const prevConversationRef = useRef<string>('');

  
  useEffect(() => {
    const currentKey = `${conversationType}:${conversationId}`;
    if (prevConversationRef.current !== currentKey && prevConversationRef.current !== '') {
      setOffset(0);
      setHasMore(true);
      
      queryClient.invalidateQueries({ 
        queryKey: ['messages', conversationType, conversationId] 
      });
    }
    prevConversationRef.current = currentKey;
  }, [conversationType, conversationId, queryClient]);

  const query = useQuery<Message[], Error>({
    queryKey: ['messages', conversationType, conversationId, offset],
    queryFn: async () => {
      if (!user) {
        throw new Error('You must be logged in to view messages.');
      }

      
      if (conversationType !== 'dm' && conversationType !== 'group') {
        throw new Error('Invalid conversation type');
      }

      
      if (!conversationId || conversationId.trim() === '') {
        throw new Error('Conversation ID is required');
      }

      let validatedConversationId: string;
      try {
        validatedConversationId = validateUUID(conversationId, 'Conversation ID');
      } catch (error) {
        logger.error('Invalid conversation ID', error as Error, { conversationId });
        throw error;
      }

      const { data, error } = await supabase.rpc('get_conversation_messages', {
        p_conversation_type: conversationType,
        p_conversation_id: validatedConversationId,
        p_limit: 50,
        p_offset: offset,
      });

      if (error) {
        logger.error('Error fetching messages', error as Error, { conversationType, conversationId: validatedConversationId });
        throw error;
      }

      const messages = (data || []) as Message[];
      
      
      if (messages.length < 50) {
        setHasMore(false);
      } else {
        setHasMore(true);
      }

      return messages.reverse(); 
    },
    enabled: enabled && !!user && !!conversationId && conversationId.trim() !== '',
  });

  
  useEffect(() => {
    if (!user || !conversationId || !enabled) return;

    
    let channel: ReturnType<typeof supabase.channel> | null = null;
    let isMounted = true;

    
    let validatedConversationId: string;
    let validatedUserId: string;
    try {
      validatedConversationId = validateUUID(conversationId, 'Conversation ID');
      validatedUserId = validateUserId(user.id);
    } catch {
      
      return;
    }

    
    if (conversationType !== 'dm' && conversationType !== 'group') {
      return;
    }

    
    if (!isMounted) return;

    channel = supabase
      .channel(`messages:${conversationType}:${validatedConversationId}`)
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'user_messages',
          filter: `conversation_type=eq.${conversationType}`,
        },
        (payload) => {
          
          if (payload.new.conversation_id === validatedConversationId) {
            
            queryClient.invalidateQueries({
              queryKey: ['messages', conversationType, validatedConversationId]
            });
            
            queryClient.invalidateQueries({ queryKey: ['conversations', validatedUserId] });
          }
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'user_messages',
          filter: `conversation_type=eq.${conversationType}`,
        },
        (payload) => {
          
          if (payload.new.conversation_id === validatedConversationId) {
            
            queryClient.invalidateQueries({
              queryKey: ['messages', conversationType, validatedConversationId]
            });
          }
        }
      )
      .subscribe();

    return () => {
      isMounted = false;
      if (channel) {
        supabase.removeChannel(channel);
      }
    };
  }, [user, conversationType, conversationId, enabled, queryClient]);

  const loadMore = () => {
    if (hasMore && !query.isFetching) {
      setOffset(prev => prev + 50);
    }
  };

  return {
    ...query,
    hasMore,
    loadMore,
    reset: () => {
      setOffset(0);
      setHasMore(true);
    },
  };
}

