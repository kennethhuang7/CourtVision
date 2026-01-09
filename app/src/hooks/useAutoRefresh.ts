import { useState, useEffect, useCallback } from 'react';
import { useUserProfile } from './useUserProfile';
import { useUpdateUserProfile } from './useUserProfile';
import { logger } from '@/lib/logger';

const AUTO_REFRESH_STORAGE_KEY = 'courtvision-auto-refresh-interval';
const DEFAULT_INTERVAL = 'never';

type AutoRefreshInterval = 'never' | '5' | '15' | '30' | '60';


export function getRefreshIntervalMs(interval: string | null | undefined): number | false {
  if (!interval || interval === 'never') {
    return false; 
  }

  const minutes = parseInt(interval, 10);
  if (isNaN(minutes) || minutes <= 0) {
    return false;
  }

  return minutes * 60 * 1000; 
}


export function useAutoRefresh() {
  const { data: profile } = useUserProfile();
  const updateProfile = useUpdateUserProfile();

  
  const [interval, setInterval] = useState<AutoRefreshInterval>(() => {
    if (typeof window === 'undefined') return DEFAULT_INTERVAL as AutoRefreshInterval;
    
    const stored = localStorage.getItem(AUTO_REFRESH_STORAGE_KEY);
    if (stored && (stored === 'never' || stored === '5' || stored === '15' || stored === '30' || stored === '60')) {
      return stored as AutoRefreshInterval;
    }
    return DEFAULT_INTERVAL as AutoRefreshInterval;
  });

  
  useEffect(() => {
    if (profile?.auto_refresh_interval) {
      const dbInterval = profile.auto_refresh_interval;
      if (dbInterval === 'never' || dbInterval === '5' || dbInterval === '15' || dbInterval === '30' || dbInterval === '60') {
        setInterval(dbInterval as AutoRefreshInterval);
        
        localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, dbInterval);
      }
    } else if (profile && !profile.auto_refresh_interval) {
      
      const defaultInterval = DEFAULT_INTERVAL;
      setInterval(defaultInterval as AutoRefreshInterval);
      localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, defaultInterval);
      
      updateProfile.mutate({ auto_refresh_interval: defaultInterval } as any, { onError: () => {} });
    }
  }, [profile, updateProfile]);

  const updateInterval = useCallback(async (newInterval: AutoRefreshInterval) => {
    setInterval(newInterval);
    localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, newInterval);
    
    
    try {
      await updateProfile.mutateAsync({ auto_refresh_interval: newInterval } as any);
    } catch (err) {
      logger.error('Error saving auto-refresh interval to database', err as Error);
      
    }
  }, [updateProfile]);

  
  const refreshIntervalMs = getRefreshIntervalMs(interval);

  return {
    interval,
    refreshIntervalMs,
    updateInterval,
  };
}

