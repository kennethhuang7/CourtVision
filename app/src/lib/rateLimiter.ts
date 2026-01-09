

interface RateLimitConfig {
  maxRequests: number;
  windowMs: number;
}

interface RequestRecord {
  count: number;
  resetAt: number;
}

const MAX_KEYS = 1000; 

class RateLimiter {
  private requests: Map<string, RequestRecord> = new Map();
  private configs: Map<string, RateLimitConfig> = new Map();

  
  private pruneExpired(now: number) {
    if (this.requests.size === 0) return;

    for (const [key, record] of this.requests.entries()) {
      if (now > record.resetAt) {
        this.requests.delete(key);
      }
    }

    
    if (this.requests.size > MAX_KEYS) {
      const excess = this.requests.size - MAX_KEYS;
      const keys = Array.from(this.requests.keys());
      for (let i = 0; i < excess; i++) {
        this.requests.delete(keys[i]);
      }
    }
  }

  
  configure(key: string, config: RateLimitConfig) {
    this.configs.set(key, config);
  }

  
  isAllowed(key: string): boolean {
    const config = this.configs.get(key);
    if (!config) return true; 

    const now = Date.now();
    this.pruneExpired(now);
    const record = this.requests.get(key);

    
    if (!record || now > record.resetAt) {
      this.requests.set(key, {
        count: 1,
        resetAt: now + config.windowMs,
      });
      return true;
    }

    
    if (record.count >= config.maxRequests) {
      return false;
    }

    
    record.count++;
    return true;
  }

  
  getTimeUntilReset(key: string): number {
    const record = this.requests.get(key);
    if (!record) return 0;
    
    const remaining = record.resetAt - Date.now();
    return Math.max(0, remaining);
  }

  
  clear(key: string) {
    this.requests.delete(key);
  }

  
  clearAll() {
    this.requests.clear();
  }
}

export const rateLimiter = new RateLimiter();




rateLimiter.configure('supabase-query', { maxRequests: 200, windowMs: 60000 }); 
rateLimiter.configure('supabase-mutation', { maxRequests: 50, windowMs: 60000 }); 
rateLimiter.configure('filter-change', { maxRequests: 10, windowMs: 1000 }); 
rateLimiter.configure('date-change', { maxRequests: 5, windowMs: 1000 }); 


export function debounce<T extends (...args: unknown[]) => unknown>(
  func: T,
  waitMs: number
): ((...args: Parameters<T>) => void) & { cancel: () => void } {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  const debounced = function debounced(...args: Parameters<T>) {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
      timeoutId = null;
      func(...args);
    }, waitMs);
  } as ((...args: Parameters<T>) => void) & { cancel: () => void };

  
  debounced.cancel = () => {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }
  };

  return debounced;
}


export function throttle<T extends (...args: unknown[]) => unknown>(
  func: T,
  waitMs: number
): (...args: Parameters<T>) => void {
  let lastCall = 0;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  return function throttled(...args: Parameters<T>) {
    const now = Date.now();
    const timeSinceLastCall = now - lastCall;

    if (timeSinceLastCall >= waitMs) {
      lastCall = now;
      func(...args);
    } else {
      if (timeoutId !== null) {
        clearTimeout(timeoutId);
      }
      timeoutId = setTimeout(() => {
        lastCall = Date.now();
        func(...args);
      }, waitMs - timeSinceLastCall);
    }
  };
}

