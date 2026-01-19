import { useEffect, useState, useCallback } from 'react';
import { AlertCircle, Wifi, WifiOff } from 'lucide-react';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import { supabase } from '@/lib/supabase';

interface ConnectionStatus {
  isConnected: boolean;
  isChecking: boolean;
  lastError: string | null;
}

export function SupabaseConnectionStatus() {
  const [status, setStatus] = useState<ConnectionStatus>({
    isConnected: true,
    isChecking: false,
    lastError: null,
  });

  const checkConnection = useCallback(async () => {
    setStatus(prev => ({ ...prev, isChecking: true }));
    try {
      
      const startTime = Date.now();
      const { error } = await Promise.race([
        supabase.auth.getSession(),
        new Promise<{ error: any }>((_, reject) =>
          setTimeout(() => reject(new Error('Connection timeout')), 10000) 
        ),
      ]) as { error: any };

      const responseTime = Date.now() - startTime;

      
      if (error) {
        const errorMessage = error.message || String(error);
        if (
          errorMessage.includes('timeout') ||
          errorMessage.includes('Failed to fetch') ||
          errorMessage.includes('NetworkError') ||
          errorMessage.includes('522') ||
          errorMessage.includes('504') ||
          errorMessage.includes('503')
        ) {
          setStatus({
            isConnected: false,
            isChecking: false,
            lastError: 'Unable to connect to the database server. Please try again in a moment.',
          });
          return;
        }
      }

      
      if (responseTime > 5000) {
        setStatus({
          isConnected: false,
          isChecking: false,
          lastError: 'Database connection is slow. Please wait a moment and try again.',
        });
        return;
      }

      
      setStatus({
        isConnected: true,
        isChecking: false,
        lastError: null,
      });
    } catch (err: any) {
      const errorMessage = err?.message || String(err);
      if (
        errorMessage.includes('timeout') ||
        errorMessage.includes('Connection timeout') ||
        errorMessage.includes('Failed to fetch')
      ) {
        setStatus({
          isConnected: false,
          isChecking: false,
          lastError: 'Unable to connect to the database server. The service may be temporarily unavailable.',
        });
      } else {
        setStatus({
          isConnected: false,
          isChecking: false,
          lastError: 'Database connection error. Please try again.',
        });
      }
    }
  }, []);

  
  useEffect(() => {
    checkConnection();
  }, [checkConnection]);

  
  useEffect(() => {
    if (!status.isConnected) {
      const interval = setInterval(() => {
        checkConnection();
      }, 30000);

      return () => clearInterval(interval);
    }
  }, [status.isConnected, checkConnection]);

  
  if (status.isConnected && !status.lastError) {
    return null;
  }

  return (
    <Alert variant="destructive" className="mb-4 border-destructive/50 bg-destructive/10">
      <AlertCircle className="h-4 w-4" />
      <AlertTitle className="flex items-center gap-2">
        {status.isChecking ? (
          <>
            <Wifi className="h-4 w-4 animate-pulse" />
            Checking connection...
          </>
        ) : status.isConnected ? (
          <>
            <Wifi className="h-4 w-4" />
            Connection Restored
          </>
        ) : (
          <>
            <WifiOff className="h-4 w-4" />
            Database Connection Issue
          </>
        )}
      </AlertTitle>
      <AlertDescription className="flex items-center justify-between">
        <span>
          {status.lastError ||
            'We are experiencing issues connecting to our database. Some features may not work properly. Please try again in a moment.'}
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={checkConnection}
          disabled={status.isChecking}
          className="ml-4"
        >
          {status.isChecking ? 'Checking...' : 'Retry'}
        </Button>
      </AlertDescription>
    </Alert>
  );
}
