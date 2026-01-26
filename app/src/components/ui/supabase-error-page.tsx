import { AlertCircle, RefreshCw, WifiOff } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { supabase } from '@/lib/supabase';
import { queryWithTimeout } from '@/lib/supabase';
import { useState, useEffect } from 'react';
import { AnimatePresence, motion } from 'framer-motion';

interface SupabaseErrorPageProps {
  onRetry: () => void;
}

export function SupabaseErrorPage({ onRetry }: SupabaseErrorPageProps) {
  const [isRetrying, setIsRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);
  const [retrySuccess, setRetrySuccess] = useState(false);

  const handleRetry = async () => {
    setIsRetrying(true);
    setRetryError(null);
    setRetrySuccess(false);
    
    const startTime = Date.now();
    const minDisplayTime = 1000;
    let timeoutId: NodeJS.Timeout | null = null;
    let resolved = false;
    
    try {
      const timeoutPromise = new Promise<{ data: null; error: Error }>((resolve) => {
        timeoutId = setTimeout(() => {
          if (!resolved) {
            resolved = true;
            resolve({
              data: null,
              error: new Error('Connection timeout after 5 seconds'),
            });
          }
        }, 5000);
      });

      const sessionPromise = supabase.auth.getSession().then((result) => {
        if (!resolved && timeoutId) {
          resolved = true;
          clearTimeout(timeoutId);
        }
        return result;
      }).catch((err) => {
        if (!resolved && timeoutId) {
          resolved = true;
          clearTimeout(timeoutId);
        }
        throw err;
      });

      const result = await Promise.race([sessionPromise, timeoutPromise]) as any;
      
      const elapsed = Date.now() - startTime;
      if (elapsed < minDisplayTime) {
        await new Promise(resolve => setTimeout(resolve, minDisplayTime - elapsed));
      }
      
      const { error } = result || { error: new Error('Connection timeout') };

      if (!error) {
        setIsRetrying(false);
        setRetrySuccess(true);
        
        await new Promise(resolve => setTimeout(resolve, 1500));
        
        onRetry();
        
        setTimeout(() => {
          setRetrySuccess(false);
        }, 2000);
        return;
      } else {
        const errorMessage = error?.message || String(error);
        if (errorMessage.includes('timeout') || errorMessage.includes('Query timeout')) {
          setRetryError('Connection timed out. The database may still be unavailable.');
        } else {
          setRetryError('Connection failed. Please try again in a moment.');
        }
        setIsRetrying(false);
      }
    } catch (err: any) {
      const errorMessage = err?.message || String(err);
      if (errorMessage.includes('timeout') || errorMessage.includes('Query timeout')) {
        setRetryError('Connection timed out. The database may still be unavailable.');
      } else {
        setRetryError('Connection failed. Please try again in a moment.');
      }
      setIsRetrying(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-background density-padding">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-destructive/10">
            <WifiOff className="h-8 w-8 text-destructive" />
          </div>
          <CardTitle className="text-2xl">Database Connection Issue</CardTitle>
          <CardDescription className="mt-2">
            We're having trouble connecting to our database service.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="rounded-lg border border-destructive/20 bg-destructive/5 density-padding">
            <div className="flex items-start gap-3">
              <AlertCircle className="h-5 w-5 shrink-0 text-destructive mt-0.5" />
              <div className="space-y-1 text-sm">
                <p className="font-medium text-foreground">What's happening?</p>
                <p className="text-muted-foreground">
                  The database service (Supabase) may be experiencing temporary downtime or slow response times. 
                  This is usually resolved within a few minutes.
                </p>
              </div>
            </div>
          </div>

          <div className="space-y-2 text-sm text-muted-foreground">
            <p className="font-medium text-foreground">You can:</p>
            <ul className="list-disc list-inside space-y-1 ml-2">
              <li>Wait a moment and click "Retry Connection"</li>
              <li>Check your internet connection</li>
              <li>Try refreshing the page in a few minutes</li>
            </ul>
          </div>

          <AnimatePresence mode="wait">
            {retryError && (
              <motion.div
                key="error"
                initial={{ opacity: 0, y: -20, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: -10, scale: 0.95 }}
                transition={{ duration: 0.3, ease: "easeOut" }}
                className="rounded-lg border border-destructive/20 bg-destructive/5 p-3"
              >
                <p className="text-sm text-destructive font-medium">{retryError}</p>
              </motion.div>
            )}

            {retrySuccess && !retryError && (
              <motion.div
                key="success"
                initial={{ opacity: 0, y: -20, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: -10, scale: 0.95 }}
                transition={{ duration: 0.3, ease: "easeOut" }}
                className="rounded-lg border border-green-500/20 bg-green-500/5 p-3"
              >
                <p className="text-sm text-green-600 dark:text-green-400 font-medium">
                  Connection successful! Retrying authentication...
                </p>
              </motion.div>
            )}
          </AnimatePresence>

          <Button
            onClick={handleRetry}
            disabled={isRetrying}
            className="w-full"
            size="lg"
          >
            {isRetrying ? (
              <>
                <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                Checking Connection...
              </>
            ) : (
              <>
                <RefreshCw className="mr-2 h-4 w-4" />
                Retry Connection
              </>
            )}
          </Button>

          <p className="text-xs text-center text-muted-foreground">
            If this persists, the service may be temporarily unavailable. Please try again later.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}

