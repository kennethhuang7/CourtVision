import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { logger } from '@/lib/logger';

export interface MessageReaction {
  id: string;
  message_id: string;
  user_id: string;
  emoji: string;
  created_at: string;
}

export interface ReactionCount {
  emoji: string;
  count: number;
  user_ids: string[];
  users?: Array<{
    user_id: string;
    username: string;
    display_name: string | null;
  }>;
}


export function useMessageReactions(messageId: string, enabled: boolean = true) {
  const { user } = useAuth();

  return useQuery<ReactionCount[], Error>({
    queryKey: ['messageReactions', messageId],
    queryFn: async () => {
      if (!messageId) {
        throw new Error('Message ID is required');
      }

      
      const { data, error } = await supabase.rpc('get_message_reaction_counts', {
        p_message_id: messageId,
      });

      if (error) {
        logger.error('Failed to fetch message reactions', error as Error, { messageId });
        throw error;
      }

      
      const allUserIds = Array.from(new Set((data || []).flatMap((r: any) => r.user_ids)));

      if (allUserIds.length > 0) {
        const { data: profiles, error: profilesError } = await supabase
          .from('user_profiles')
          .select('user_id, username, display_name')
          .in('user_id', allUserIds);

        if (profilesError) {
          logger.error('Failed to fetch reactor profiles', profilesError as Error);
        } else {
          
          const profilesMap = new Map(
            (profiles || []).map(p => [p.user_id, p])
          );

          return (data || []).map((reaction: any) => ({
            ...reaction,
            users: reaction.user_ids.map((id: string) => profilesMap.get(id)).filter(Boolean),
          }));
        }
      }

      return data || [];
    },
    enabled: enabled && !!user && !!messageId,
  });
}


export function useBatchMessageReactions(messageIds: string[], enabled: boolean = true) {
  const { user } = useAuth();

  
  const queryKey = ['batchMessageReactions', [...messageIds].sort().join(',')];

  return useQuery<Map<string, ReactionCount[]>, Error>({
    queryKey,
    placeholderData: (previousData) => previousData, 
    queryFn: async () => {
      if (!messageIds || messageIds.length === 0) {
        return new Map();
      }

      
      const { data: reactions, error } = await supabase
        .from('message_reactions')
        .select('id, message_id, user_id, emoji, created_at')
        .in('message_id', messageIds);

      if (error) {
        logger.error('Failed to fetch batch reactions', error as Error);
        throw error;
      }

      
      const reactionMap = new Map<string, ReactionCount[]>();

      messageIds.forEach(messageId => {
        const messageReactions = (reactions || []).filter(r => r.message_id === messageId);

        
        const emojiGroups = new Map<string, string[]>();
        messageReactions.forEach(r => {
          const existing = emojiGroups.get(r.emoji) || [];
          existing.push(r.user_id);
          emojiGroups.set(r.emoji, existing);
        });

        
        const counts: ReactionCount[] = Array.from(emojiGroups.entries()).map(([emoji, userIds]) => ({
          emoji,
          count: userIds.length,
          user_ids: userIds,
        }));

        if (counts.length > 0) {
          reactionMap.set(messageId, counts);
        }
      });

      
      const allUserIds = Array.from(new Set((reactions || []).map(r => r.user_id)));
      if (allUserIds.length > 0) {
        const { data: profiles } = await supabase
          .from('user_profiles')
          .select('user_id, username, display_name')
          .in('user_id', allUserIds);

        if (profiles) {
          const profilesMap = new Map(profiles.map(p => [p.user_id, p]));

          
          reactionMap.forEach((counts, messageId) => {
            counts.forEach(count => {
              count.users = count.user_ids.map(id => profilesMap.get(id)).filter(Boolean) as any;
            });
          });
        }
      }

      return reactionMap;
    },
    enabled: enabled && !!user && messageIds.length > 0,
    staleTime: 30000, 
    gcTime: 5 * 60 * 1000, 
  });
}
