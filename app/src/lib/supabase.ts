import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { logger } from './logger';
import { handleError } from './errorHandler';
import { rateLimiter } from './rateLimiter';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL as string;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY as string;


const QUERY_TIMEOUT_MS = 30000;


if (!supabaseUrl || !supabaseAnonKey) {
  const errorMessage = 
    'Missing required Supabase environment variables. Please ensure VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY are set in your .env.local file.';
  
  if (import.meta.env.PROD) {
    throw new Error(errorMessage);
  } else {
    logger.error(errorMessage);
  }
}


let supabaseInstance: SupabaseClient | null = null;


const rateLimitedFetch: typeof fetch = async (input, init) => {
  const method = (init?.method ?? 'GET').toUpperCase();
  const key = method === 'GET' ? 'supabase-query' : 'supabase-mutation';

  if (!rateLimiter.isAllowed(key)) {
    const retryMs = rateLimiter.getTimeUntilReset(key);
    const error = new Error(`Rate limited: ${key}. Try again in ${Math.ceil(retryMs / 1000)}s`);

    
    
    handleError(error, {
      severity: 'medium',
      context: { action: 'rate-limit-check', key, retryAfterSeconds: Math.ceil(retryMs / 1000) },
      showToast: false, 
    });

    
    throw error;
  }

  return fetch(input, init);
};

export const supabase = (() => {
  if (!supabaseInstance) {
    supabaseInstance = createClient(supabaseUrl, supabaseAnonKey, {
      auth: {
        persistSession: true,
        detectSessionInUrl: true,
        autoRefreshToken: true,
        persistSessionStorageKey: 'courtvision-auth',
      },
      db: {
        schema: 'public',
      },
      global: {
        
        fetch: rateLimitedFetch,
        headers: {
          'X-Client-Info': 'courtvision-web',
        },
      },
    });
  }
  return supabaseInstance;
})();


export async function queryWithTimeout<T>(
  queryPromise: Promise<{ data: T | null; error: unknown }>,
  timeoutMs: number = QUERY_TIMEOUT_MS
): Promise<{ data: T | null; error: unknown }> {
  const timeoutPromise = new Promise<{ data: null; error: Error }>((resolve) => {
    setTimeout(() => {
      resolve({
        data: null,
        error: new Error(`Query timeout after ${timeoutMs}ms`),
      });
    }, timeoutMs);
  });

  try {
    return await Promise.race([queryPromise, timeoutPromise]);
  } catch (error) {
    handleError(error, {
      severity: 'high',
      context: { action: 'queryWithTimeout' },
    });
    return { data: null, error };
  }
}


export async function ensureConnection(): Promise<boolean> {
  try {
    const { error } = await queryWithTimeout(
      supabase.auth.getSession(),
      5000 
    );
    
    if (error) {
      logger.warn('Database connection check failed', { error });
      return false;
    }
    
    return true;
  } catch (error) {
    logger.error('Database connection check error', error as Error);
    return false;
  }
}


