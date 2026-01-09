import { createContext, useContext, useState, useEffect, useMemo, useCallback, ReactNode } from 'react';
import { cacheManager, CacheRetentionDays } from '../lib/cache';
import { logger } from '../lib/logger';
import { supabase } from '../lib/supabase';

const CACHE_RETENTION_KEY = 'courtvision-cache-retention';
const DEFAULT_RETENTION: CacheRetentionDays = 30;

interface CacheContextType {
  retentionDays: CacheRetentionDays;
  setRetentionDays: (days: CacheRetentionDays) => void;
  storageUsage: { totalBytes: number; itemCount: number; formattedSize: string };
  cacheCounts: { predictions: number };
  isOnline: boolean;
  clearCache: () => Promise<void>;
  refreshStats: () => Promise<void>;
  isInitialized: boolean;
  getAllCacheEntries: () => Promise<Array<{
    date: string;
    type: 'prediction' | 'gameResult';
    size: number;
    cachedAt: number;
    models?: string;
  }>>;
  deleteCacheEntries: (keys: string[]) => Promise<void>;
}

const CacheContext = createContext<CacheContextType | undefined>(undefined);

export function CacheProvider({ children }: { children: ReactNode }) {
  const [retentionDays, setRetentionDaysState] = useState<CacheRetentionDays>(() => {
    if (typeof window === 'undefined') return DEFAULT_RETENTION;
    const stored = localStorage.getItem(CACHE_RETENTION_KEY);
    if (stored === 'all') return 'all';
    const parsed = parseInt(stored || '', 10);
    if ([7, 14, 30, 60, 90].includes(parsed)) {
      return parsed as CacheRetentionDays;
    }
    return DEFAULT_RETENTION;
  });

  const [storageUsage, setStorageUsage] = useState({ totalBytes: 0, itemCount: 0, formattedSize: '0 B' });
  const [cacheCounts, setCacheCounts] = useState({ predictions: 0 });
  const [isOnline, setIsOnline] = useState(() => typeof navigator !== 'undefined' ? navigator.onLine : true);
  const [isInitialized, setIsInitialized] = useState(false);
  const [lastHealthCheck, setLastHealthCheck] = useState<number>(Date.now());

  
  useEffect(() => {
    const initCache = async () => {
      try {
        await cacheManager.init();
        setIsInitialized(true);

        
        await cacheManager.cleanup(retentionDays);

        
        await refreshStats();
      } catch (error) {
        logger.error('Failed to initialize cache', error as Error);
      }
    };

    initCache();
  }, []);

  
  useEffect(() => {
    let isMounted = true;
    let isChecking = false;

    const performHealthCheck = async () => {
      
      if (isChecking || !isMounted) return;
      isChecking = true;

      
      if (!navigator.onLine) {
        if (isMounted) setIsOnline(false);
        isChecking = false;
        return;
      }

      try {

        const timeoutPromise = new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Health check timeout')), 5000)
        );


        const queryPromise = supabase
          .from('teams')
          .select('team_id', { head: true })
          .limit(1);

        const { error } = await Promise.race([queryPromise, timeoutPromise]) as any;

        if (isMounted) {
          if (error) {
            logger.info('Health check returned error - marking as offline', error);
            setIsOnline(false);
          } else {
            setIsOnline(true);
          }
          setLastHealthCheck(Date.now());
        }
      } catch (error) {
        
        if (isMounted) {
          logger.info('Health check failed - marking as offline');
          setIsOnline(false);
        }
      } finally {
        isChecking = false;
      }
    };


    performHealthCheck();


    const interval = setInterval(performHealthCheck, 300000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  
  const refreshStats = useCallback(async () => {
    try {
      const [usage, counts] = await Promise.all([
        cacheManager.getStorageUsage(),
        cacheManager.getCacheCounts(),
      ]);

      setStorageUsage(usage);
      setCacheCounts(counts);
    } catch (error) {
      logger.error('Failed to refresh cache stats', error as Error);
    }
  }, []);

  
  const setRetentionDays = useCallback(async (days: CacheRetentionDays) => {
    try {
      setRetentionDaysState(days);

      if (typeof window !== 'undefined') {
        localStorage.setItem(CACHE_RETENTION_KEY, String(days));
      }

      
      await cacheManager.cleanup(days);

      
      await refreshStats();

      logger.info(`Cache retention updated to: ${days} days`);
    } catch (error) {
      logger.error('Failed to update retention days', error as Error);
    }
  }, [refreshStats]);

  
  const clearCache = useCallback(async () => {
    const result = await cacheManager.clearAll();
    await refreshStats();
    return result;
  }, [refreshStats]);

  
  const getAllCacheEntries = useCallback(async () => {
    try {
      return await cacheManager.getAllCacheEntries();
    } catch (error) {
      logger.error('Failed to get cache entries', error as Error);
      return [];
    }
  }, []);

  
  const deleteCacheEntries = useCallback(async (keys: string[]) => {
    try {
      await cacheManager.deleteEntries(keys);
      await refreshStats();
      logger.info(`Deleted ${keys.length} cache entries`);
    } catch (error) {
      logger.error('Failed to delete cache entries', error as Error);
      throw error;
    }
  }, [refreshStats]);

  
  useEffect(() => {
    if (typeof window === 'undefined') return;

    const handleOnline = () => {
      setIsOnline(true);
      logger.info('Connection restored');
    };

    const handleOffline = () => {
      setIsOnline(false);
      logger.warn('Connection lost - using cached data');
    };

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);


  useEffect(() => {
    const interval = setInterval(() => {
      refreshStats();
    }, 180000);

    return () => clearInterval(interval);
  }, [refreshStats]);

  
  const value = useMemo(
    () => ({
      retentionDays,
      setRetentionDays,
      storageUsage,
      cacheCounts,
      isOnline,
      clearCache,
      refreshStats,
      isInitialized,
      getAllCacheEntries,
      deleteCacheEntries,
    }),
    [retentionDays, setRetentionDays, storageUsage, cacheCounts, isOnline, clearCache, refreshStats, isInitialized, getAllCacheEntries, deleteCacheEntries]
  );

  return <CacheContext.Provider value={value}>{children}</CacheContext.Provider>;
}

export function useCache() {
  const context = useContext(CacheContext);
  if (context === undefined) {
    throw new Error('useCache must be used within a CacheProvider');
  }
  return context;
}
