import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { useRef, useEffect } from 'react';
import { format as dateFnsFormat } from 'date-fns';
import { logger } from '@/lib/logger';


export function useNewPredictions() {
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousPredictionCountsRef = useRef<Map<string, number>>(new Map());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined') {
      try {
        const stored = localStorage.getItem('courtvision-notified-prediction-counts');
        if (stored) {
          previousPredictionCountsRef.current = new Map(JSON.parse(stored));
        }
      } catch (e) {
        logger.warn('Error loading prediction counts from localStorage', { error: e });
      }
      hasInitializedRef.current = true;
    }
  }, []);

  
  const today = new Date();
  const tomorrow = new Date(today);
  tomorrow.setDate(tomorrow.getDate() + 1);
  
  const datesToCheck = [
    dateFnsFormat(today, 'yyyy-MM-dd'),
    dateFnsFormat(tomorrow, 'yyyy-MM-dd'),
  ];

  const query = useQuery({
    queryKey: ['new-predictions-check', datesToCheck],
    queryFn: async () => {
      const counts: Map<string, number> = new Map();

      for (const dateStr of datesToCheck) {
        const { count, error } = await supabase
          .from('predictions')
          .select('*', { count: 'exact', head: true })
          .gte('prediction_date', `${dateStr}T00:00:00`)
          .lt('prediction_date', `${dateStr}T23:59:59`);

        if (!error && count !== null) {
          counts.set(dateStr, count);
        }
      }

      return counts;
    },
    enabled: !doNotDisturb, 
    refetchInterval: 600000, 
    refetchIntervalInBackground: true, 
    staleTime: 300000, 
  });

  
  useEffect(() => {
    if (!query.data) return;

    query.data.forEach((count, dateStr) => {
      const previousCount = previousPredictionCountsRef.current.get(dateStr) || 0;
      
      if (count > previousCount && previousCount > 0) {
        
        const date = new Date(dateStr + 'T00:00:00');
        const isToday = dateFnsFormat(date, 'yyyy-MM-dd') === dateFnsFormat(today, 'yyyy-MM-dd');
        const dateLabel = isToday ? 'today' : 'tomorrow';
        
        const newCount = count - previousCount;
        notify(
          'newPredictions',
          'New Predictions Available',
          `${newCount} new prediction${newCount > 1 ? 's' : ''} available for ${dateLabel}`,
          {
            tag: `new-predictions-${dateStr}`,
          }
        );
      }

      
      previousPredictionCountsRef.current.set(dateStr, count);
    });

    
    if (typeof window !== 'undefined') {
      localStorage.setItem(
        'courtvision-notified-prediction-counts',
        JSON.stringify(Array.from(previousPredictionCountsRef.current.entries()))
      );
    }
  }, [query.data, notify, today]);

  return query;
}

