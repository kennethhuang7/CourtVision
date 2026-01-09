import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { logger } from '@/lib/logger';


export function useReactionSubscription(messageIds: string[], enabled: boolean = true) {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!enabled || messageIds.length === 0) {
      return;
    }

    
    const channel = supabase
      .channel('message-reactions-changes')
      .on(
        'postgres_changes',
        {
          event: '*', 
          schema: 'public',
          table: 'message_reactions',
          filter: `message_id=in.(${messageIds.join(',')})`,
        },
        (payload) => {
          logger.info('Reaction change detected', { payload });

          
          if (payload.new && 'message_id' in payload.new) {
            const messageId = payload.new.message_id as string;
            queryClient.invalidateQueries({
              queryKey: ['messageReactions', messageId],
            });
          } else if (payload.old && 'message_id' in payload.old) {
            const messageId = payload.old.message_id as string;
            queryClient.invalidateQueries({
              queryKey: ['messageReactions', messageId],
            });
          }

          
          queryClient.invalidateQueries({
            queryKey: ['batchMessageReactions'],
          });
        }
      )
      .subscribe();

    
    return () => {
      supabase.removeChannel(channel);
    };
  }, [messageIds.join(','), enabled, queryClient]);
}
