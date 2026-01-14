import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Loader2, AlertCircle } from 'lucide-react';

interface RateLimitErrorProps {
  error: Error | null;
  onRetry?: () => void;
  showRetry?: boolean;
}

export function RateLimitError({ error, onRetry, showRetry = true }: RateLimitErrorProps) {
  const [retryCountdown, setRetryCountdown] = useState<number | null>(null);
  const isRateLimitError = error?.message?.includes('Rate limited') || error?.message?.includes('429');

  useEffect(() => {
    if (!isRateLimitError || !error) {
      setRetryCountdown(null);
      return;
    }

    const match = error.message.match(/Try again in (\d+)s/);
    const retrySeconds = match ? parseInt(match[1], 10) : 5;

    setRetryCountdown(retrySeconds);

    const countdownInterval = setInterval(() => {
      setRetryCountdown(prev => {
        if (prev === null || prev <= 1) {
          clearInterval(countdownInterval);
          if (onRetry) {
            onRetry();
          }
          return null;
        }
        return prev - 1;
      });
    }, 1000);

    return () => clearInterval(countdownInterval);
  }, [isRateLimitError, error, onRetry]);

  if (!isRateLimitError) return null;

  return (
    <div className="rounded-xl border border-border bg-card p-12 text-center">
      <div className="max-w-xl mx-auto">
        <div className="rounded-2xl bg-muted/30 border border-border/50 p-10 space-y-10">
          <div className="text-center space-y-5">
            <div className="relative w-20 h-20 mx-auto">
              <div className="absolute inset-0 rounded-full bg-primary/10 animate-pulse" />
              <div className="absolute inset-0 flex items-center justify-center">
                <Loader2 className="h-10 w-10 text-primary animate-spin shrink-0" />
              </div>
            </div>
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-foreground tracking-tight">
                Rate Limited
              </h3>
              <p className="text-muted-foreground">
                Too many requests. Please wait a moment before trying again.
              </p>
            </div>
          </div>

          <div className="flex items-center justify-center">
            <div className="relative">
              <div className="absolute inset-0 rounded-2xl bg-primary/5 blur-xl" />
              <div className="relative rounded-2xl bg-gradient-to-br from-primary/10 to-primary/5 border border-primary/20 px-10 py-6">
                <div className="flex items-baseline justify-center gap-3">
                  <span className="text-6xl font-bold text-primary tabular-nums leading-none tracking-tight">
                    {retryCountdown !== null ? retryCountdown : '...'}
                  </span>
                  <span className="text-lg text-primary/60 font-medium pb-1">sec</span>
                </div>
              </div>
            </div>
          </div>

          {showRetry && onRetry && (
            <div className="text-center">
              <Button
                onClick={onRetry}
                variant="ghost"
                size="sm"
                disabled={retryCountdown !== null && retryCountdown > 0}
              >
                Retry Now
              </Button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
