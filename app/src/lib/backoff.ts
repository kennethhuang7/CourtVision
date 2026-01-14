export interface BackoffOptions {
  initialDelay?: number;
  maxDelay?: number;
  maxRetries?: number;
  factor?: number;
  jitter?: boolean;
}

export class ExponentialBackoff {
  private attempt: number = 0;
  private initialDelay: number;
  private maxDelay: number;
  private maxRetries: number;
  private factor: number;
  private jitter: boolean;

  constructor(options: BackoffOptions = {}) {
    this.initialDelay = options.initialDelay ?? 1000;
    this.maxDelay = options.maxDelay ?? 30000;
    this.maxRetries = options.maxRetries ?? 5;
    this.factor = options.factor ?? 2;
    this.jitter = options.jitter ?? true;
  }

  async execute<T>(
    fn: () => Promise<T>,
    onRetry?: (attempt: number, delay: number, error: Error) => void
  ): Promise<T> {
    this.attempt = 0;

    while (this.attempt <= this.maxRetries) {
      try {
        return await fn();
      } catch (error) {
        const err = error as Error;
        const isRateLimit = err.message?.includes('Rate limited') || 
                           err.message?.includes('429') ||
                           (err as any).code === 429;

        if (!isRateLimit || this.attempt >= this.maxRetries) {
          throw error;
        }

        this.attempt++;
        const delay = this.calculateDelay();

        if (onRetry) {
          onRetry(this.attempt, delay, err);
        }

        await this.sleep(delay);
      }
    }

    throw new Error('Max retries exceeded');
  }

  private calculateDelay(): number {
    const exponentialDelay = this.initialDelay * Math.pow(this.factor, this.attempt - 1);
    const delay = Math.min(exponentialDelay, this.maxDelay);

    if (this.jitter) {
      const jitterAmount = delay * 0.1;
      return delay + (Math.random() * jitterAmount * 2 - jitterAmount);
    }

    return delay;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  reset(): void {
    this.attempt = 0;
  }
}

export function withBackoff<T>(
  fn: () => Promise<T>,
  options?: BackoffOptions,
  onRetry?: (attempt: number, delay: number, error: Error) => void
): Promise<T> {
  const backoff = new ExponentialBackoff(options);
  return backoff.execute(fn, onRetry);
}
