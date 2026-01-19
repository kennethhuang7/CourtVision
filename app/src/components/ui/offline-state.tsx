import { WifiOff, RefreshCw } from 'lucide-react';
import { Button } from './button';

interface OfflineStateProps {
  
  context?: string;
  
  availableOffline?: string[];
  
  onRetry?: () => void;
  
  showRetry?: boolean;
}

export function OfflineState({
  context = 'this page',
  availableOffline = [],
  onRetry,
  showRetry = true,
}: OfflineStateProps) {
  return (
    <div className="rounded-xl border border-border bg-card p-12 text-center">
      <div className="max-w-xl mx-auto">
        <div className="rounded-2xl bg-muted/30 border border-border/50 p-10 space-y-8">
          <div className="text-center space-y-5">
            <div className="relative w-20 h-20 mx-auto">
              <div className="absolute inset-0 rounded-full bg-muted-foreground/10" />
              <div className="absolute inset-0 flex items-center justify-center">
                <WifiOff className="h-10 w-10 text-muted-foreground" />
              </div>
            </div>
            <div className="space-y-2">
              <h3 className="text-2xl font-semibold text-foreground tracking-tight">
                No Internet Connection
              </h3>
              <p className="text-muted-foreground">
                Unable to load {context}
              </p>
            </div>
          </div>

          {availableOffline.length > 0 && (
            <div className="rounded-lg bg-muted/50 border border-border p-4 text-left space-y-2">
              <p className="text-sm font-medium text-foreground">Available offline:</p>
              <ul className="text-sm text-muted-foreground space-y-1">
                {availableOffline.map((item, index) => (
                  <li key={index} className="flex items-start gap-2">
                    <span className="text-primary mt-0.5">•</span>
                    <span>{item}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          <div className="text-center space-y-4">
            <p className="text-xs text-muted-foreground/80 max-w-sm mx-auto leading-relaxed">
              This feature requires an active internet connection. Data will load automatically when you reconnect.
            </p>
            {showRetry && onRetry && (
              <Button
                onClick={onRetry}
                variant="outline"
                size="sm"
                className="gap-2"
              >
                <RefreshCw className="h-3.5 w-3.5" />
                Try Again
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
