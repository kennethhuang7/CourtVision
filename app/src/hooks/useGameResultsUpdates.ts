import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { useRef, useEffect } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import { format } from 'date-fns';
import { logger } from '@/lib/logger';


export function useGameResultsUpdates() {
  const { user } = useAuth();
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousUpdatedGamesRef = useRef<Set<string>>(new Set());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined' && user?.id) {
      try {
        const stored = localStorage.getItem(`courtvision-notified-game-results-${user.id}`);
        if (stored) {
          previousUpdatedGamesRef.current = new Set(JSON.parse(stored));
        }
      } catch (e) {
        logger.warn('Error loading game results notification state from localStorage', { error: e });
      }
      hasInitializedRef.current = true;
    }
  }, [user?.id]);

  const query = useQuery({
    queryKey: ['game-results-updates', user?.id],
    queryFn: async () => {
      if (!user) return [];

      
      const sevenDaysAgo = new Date();
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
      const dateStr = format(sevenDaysAgo, 'yyyy-MM-dd');

      
      
      
      const { data: predictions, error } = await supabase
        .from('predictions')
        .select('game_id') 
        .gte('prediction_date', `${dateStr}T00:00:00`)
        .not('actual_points', 'is', null)
        .limit(500); 

      if (error) {
        logger.error('Error fetching game results updates', error as Error);
        return [];
      }

      
      const gameIdsWithStats = Array.from(new Set((predictions || []).map(p => p.game_id)));
      return gameIdsWithStats;
    },
    enabled: !!user && !doNotDisturb, 
    refetchInterval: 600000, 
    refetchIntervalInBackground: true, 
    staleTime: 60000, 
  });

  
  useEffect(() => {
    if (!query.data) return;

    const currentGameIds = new Set(query.data);
    
    
    
    if (previousUpdatedGamesRef.current.size === 0 && currentGameIds.size > 0) {
      
      previousUpdatedGamesRef.current = new Set(currentGameIds);
      
      if (typeof window !== 'undefined' && user?.id) {
        localStorage.setItem(
          `courtvision-notified-game-results-${user.id}`,
          JSON.stringify(Array.from(currentGameIds))
        );
      }
      
      return;
    }

    currentGameIds.forEach(gameId => {
      if (!previousUpdatedGamesRef.current.has(gameId)) {
        
        notify(
          'gameResults',
          'Game Results Updated',
          'Game results have been updated',
          {
            tag: `game-results-${gameId}`,
          }
        );
      }
    });

    
    previousUpdatedGamesRef.current = new Set(currentGameIds);

    
    if (typeof window !== 'undefined' && user?.id) {
      localStorage.setItem(
        `courtvision-notified-game-results-${user.id}`,
        JSON.stringify(Array.from(currentGameIds))
      );
    }
  }, [query.data, notify, user?.id]);

  return query;
}

