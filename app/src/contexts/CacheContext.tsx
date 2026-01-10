import { createContext, useContext, useState, useEffect, useMemo, useCallback, ReactNode } from 'react';
import { cacheManager, CacheRetentionDays } from '../lib/cache';
import { logger } from '../lib/logger';
import { supabase } from '../lib/supabase';

const CACHE_RETENTION_KEY = 'courtvision-cache-retention';
const MODEL_PERF_RETENTION_KEY = 'courtvision-model-perf-retention';
const DEFAULT_RETENTION: CacheRetentionDays = 30;
const DEFAULT_MODEL_PERF_RETENTION: CacheRetentionDays = 30;

interface CacheContextType {
  retentionDays: CacheRetentionDays;
  setRetentionDays: (days: CacheRetentionDays) => void;
  modelPerfRetentionDays: CacheRetentionDays;
  setModelPerfRetentionDays: (days: CacheRetentionDays) => void;
  storageUsage: { totalBytes: number; itemCount: number; formattedSize: string };
  cacheCounts: { predictions: number; modelPerformance: number };
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
  getAllModelPerformanceEntries: () => Promise<Array<{
    cacheKey: string;
    timePeriod: string;
    stat: string;
    models: string[];
    size: number;
    cachedAt: number;
  }>>;
  deleteModelPerformanceEntries: (cacheKeys: string[]) => Promise<void>;
}

const CacheContext = createContext<CacheContextType | undefined>(undefined);

export function CacheProvider({ children }: { children: ReactNode }) {
  const [retentionDays, setRetentionDaysState] = useState<CacheRetentionDays>(() => {
    if (typeof window === 'undefined') return DEFAULT_RETENTION;
    const stored = localStorage.getItem(CACHE_RETENTION_KEY);
    if (stored === 'all' || stored === 'off') return stored;
    const parsed = parseInt(stored || '', 10);
    if ([7, 14, 30, 60, 90, 180].includes(parsed)) {
      return parsed as CacheRetentionDays;
    }
    return DEFAULT_RETENTION;
  });

  const [modelPerfRetentionDays, setModelPerfRetentionDaysState] = useState<CacheRetentionDays>(() => {
    if (typeof window === 'undefined') return DEFAULT_MODEL_PERF_RETENTION;
    const stored = localStorage.getItem(MODEL_PERF_RETENTION_KEY);
    if (stored === 'all' || stored === 'off') return stored;
    const parsed = parseInt(stored || '', 10);
    if ([7, 14, 30, 60, 90, 180].includes(parsed)) {
      return parsed as CacheRetentionDays;
    }
    return DEFAULT_MODEL_PERF_RETENTION;
  });

  const [storageUsage, setStorageUsage] = useState({ totalBytes: 0, itemCount: 0, formattedSize: '0 B' });
  const [cacheCounts, setCacheCounts] = useState({ predictions: 0, modelPerformance: 0 });
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

  const setModelPerfRetentionDays = useCallback(async (days: CacheRetentionDays) => {
    try {
      setModelPerfRetentionDaysState(days);

      if (typeof window !== 'undefined') {
        localStorage.setItem(MODEL_PERF_RETENTION_KEY, String(days));
      }

      await refreshStats();

      logger.info(`Model performance cache retention updated to: ${days}`);
    } catch (error) {
      logger.error('Failed to update model performance retention days', error as Error);
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


  const getAllModelPerformanceEntries = useCallback(async () => {
    try {
      return await cacheManager.getAllModelPerformanceEntries();
    } catch (error) {
      logger.error('Failed to get model performance cache entries', error as Error);
      return [];
    }
  }, []);


  const deleteModelPerformanceEntries = useCallback(async (cacheKeys: string[]) => {
    try {
      await cacheManager.deleteModelPerformanceEntries(cacheKeys);
      await refreshStats();
      logger.info(`Deleted ${cacheKeys.length} model performance cache entries`);
    } catch (error) {
      logger.error('Failed to delete model performance cache entries', error as Error);
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
      modelPerfRetentionDays,
      setModelPerfRetentionDays,
      storageUsage,
      cacheCounts,
      isOnline,
      clearCache,
      refreshStats,
      isInitialized,
      getAllCacheEntries,
      deleteCacheEntries,
      getAllModelPerformanceEntries,
      deleteModelPerformanceEntries,
    }),
    [retentionDays, setRetentionDays, modelPerfRetentionDays, setModelPerfRetentionDays, storageUsage, cacheCounts, isOnline, clearCache, refreshStats, isInitialized, getAllCacheEntries, deleteCacheEntries, getAllModelPerformanceEntries, deleteModelPerformanceEntries]
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
