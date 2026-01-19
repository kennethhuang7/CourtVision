import { useEffect } from 'react';
import { useUserProfile } from './useUserProfile';

const AUTO_REFRESH_STORAGE_KEY = 'courtvision-auto-refresh-interval';


export function useAutoRefreshInit() {
  const { data: profile } = useUserProfile();

  useEffect(() => {
    if (profile?.auto_refresh_interval) {
      
      localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, profile.auto_refresh_interval);
    } else if (profile && !profile.auto_refresh_interval) {
      
      localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, 'never');
    }
  }, [profile]);
}

