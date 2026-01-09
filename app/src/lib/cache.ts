

import { openDB, DBSchema, IDBPDatabase } from 'idb';
import { logger } from './logger';

const DB_NAME = 'courtvision-cache';
const DB_VERSION = 1;


export type CacheRetentionDays = 7 | 14 | 30 | 60 | 90 | 'all';

interface CourtVisionDB extends DBSchema {
  predictions: {
    key: string; 
    value: {
      date: string;
      data: any; 
      cachedAt: number; 
      size: number; 
    };
  };
  metadata: {
    key: string;
    value: any;
  };
}

class CacheManager {
  private db: IDBPDatabase<CourtVisionDB> | null = null;

  
  async init(): Promise<void> {
    try {
      this.db = await openDB<CourtVisionDB>(DB_NAME, DB_VERSION, {
        upgrade(db) {
          
          if (!db.objectStoreNames.contains('predictions')) {
            db.createObjectStore('predictions', { keyPath: 'date' });
          }
          if (!db.objectStoreNames.contains('metadata')) {
            db.createObjectStore('metadata');
          }
        },
      });

      logger.info('IndexedDB cache initialized');
    } catch (error) {
      logger.error('Failed to initialize IndexedDB cache', error as Error);
      throw error;
    }
  }

  
  private async ensureInit(): Promise<IDBPDatabase<CourtVisionDB>> {
    if (!this.db) {
      await this.init();
    }
    return this.db!;
  }

  
  isCacheableDate(dateString: string): boolean {
    const date = new Date(dateString);
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const twoDaysAgo = new Date(today);
    twoDaysAgo.setDate(today.getDate() - 2);

    return date < twoDaysAgo;
  }

  
  async getPredictions(date: string, models?: string[]): Promise<any | null> {
    try {
      const db = await this.ensureInit();
      
      const cacheKey = models && models.length > 0
        ? `${date}|models:${models.join('|')}`
        : date;

      const cached = await db.get('predictions', cacheKey);

      if (!cached) {
        return null;
      }

      logger.info(`Cache hit for predictions: ${cacheKey}`);
      return cached.data;
    } catch (error) {
      logger.error('Failed to get cached predictions', error as Error);
      return null;
    }
  }

  
  async savePredictions(date: string, data: any, models?: string[]): Promise<void> {
    try {
      const db = await this.ensureInit();
      
      const cacheKey = models && models.length > 0
        ? `${date}|models:${models.join('|')}`
        : date;

      const dataStr = JSON.stringify(data);
      const size = new Blob([dataStr]).size;

      await db.put('predictions', {
        date: cacheKey, 
        data,
        cachedAt: Date.now(),
        size,
      });

      logger.info(`Cached predictions for ${cacheKey} (${(size / 1024).toFixed(1)}KB)`);
    } catch (error) {
      logger.error('Failed to cache predictions', error as Error);
    }
  }

  
  async getStorageUsage(): Promise<{ totalBytes: number; itemCount: number; formattedSize: string }> {
    try {
      const db = await this.ensureInit();
      let totalBytes = 0;
      let itemCount = 0;

      
      const predictions = await db.getAll('predictions');
      predictions.forEach((p) => {
        totalBytes += p.size;
        itemCount++;
      });

      const formattedSize = this.formatBytes(totalBytes);

      return { totalBytes, itemCount, formattedSize };
    } catch (error) {
      logger.error('Failed to calculate storage usage', error as Error);
      return { totalBytes: 0, itemCount: 0, formattedSize: '0 B' };
    }
  }

  
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
  }

  
  async cleanup(retentionDays: CacheRetentionDays): Promise<void> {
    if (retentionDays === 'all') {
      logger.info('Retention set to "all" - skipping cleanup');
      return;
    }

    try {
      const db = await this.ensureInit();
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);
      const cutoffDateStr = cutoffDate.toISOString().split('T')[0];

      let deletedCount = 0;

      
      const predictions = await db.getAll('predictions');
      for (const pred of predictions) {
        
        const dateOnly = pred.date.split('|')[0];
        if (dateOnly < cutoffDateStr) {
          await db.delete('predictions', pred.date);
          deletedCount++;
        }
      }

      if (deletedCount > 0) {
        logger.info(`Cleaned up ${deletedCount} old cache entries (retention: ${retentionDays} days)`);
      }
    } catch (error) {
      logger.error('Failed to cleanup cache', error as Error);
    }
  }

  
  async clearAll(): Promise<{ wasEmpty: boolean; deletedCount: number }> {
    try {
      const db = await this.ensureInit();

      
      const predictionCount = await db.count('predictions');
      const wasEmpty = predictionCount === 0;

      
      await db.clear('predictions');
      await db.clear('metadata');

      if (wasEmpty) {
        logger.debug('Cache was already empty');
      } else {
        logger.info(`Cache cleared: ${predictionCount} items deleted`);
      }

      return { wasEmpty, deletedCount: predictionCount };
    } catch (error) {
      logger.error('Failed to clear cache', error as Error);
      
      return { wasEmpty: true, deletedCount: 0 };
    }
  }

  
  async getCacheCounts(): Promise<{ predictions: number }> {
    try {
      const db = await this.ensureInit();
      const predictions = await db.count('predictions');

      return { predictions };
    } catch (error) {
      logger.error('Failed to get cache counts', error as Error);
      return { predictions: 0 };
    }
  }

  
  async getAllCacheEntries(): Promise<Array<{
    date: string;
    type: 'prediction';
    size: number;
    cachedAt: number;
    models?: string;
  }>> {
    try {
      const db = await this.ensureInit();
      const entries: Array<{
        date: string;
        type: 'prediction';
        size: number;
        cachedAt: number;
        models?: string;
      }> = [];

      
      const predictions = await db.getAll('predictions');
      predictions.forEach((p) => {
        
        const parts = p.date.split('|models:');
        const models = parts.length > 1 ? parts[1] : undefined;

        entries.push({
          date: p.date,
          type: 'prediction',
          size: p.size,
          cachedAt: p.cachedAt,
          models,
        });
      });

      return entries;
    } catch (error) {
      logger.error('Failed to get all cache entries', error as Error);
      return [];
    }
  }

  
  async deleteEntries(keys: string[]): Promise<void> {
    try {
      const db = await this.ensureInit();

      for (const key of keys) {
        try {
          await db.delete('predictions', key);
          logger.info(`Deleted prediction cache entry: ${key}`);
        } catch {
          logger.warn(`Cache entry not found: ${key}`);
        }
      }

      logger.info(`Deleted ${keys.length} cache entries`);
    } catch (error) {
      logger.error('Failed to delete cache entries', error as Error);
      throw error;
    }
  }
}


export const cacheManager = new CacheManager();
