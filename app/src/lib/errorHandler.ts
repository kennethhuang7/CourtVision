

import { toast } from 'sonner';
import { logger } from './logger';

export type ErrorSeverity = 'low' | 'medium' | 'high' | 'critical';

export interface ErrorContext {
  component?: string;
  action?: string;
  userId?: string;
  [key: string]: unknown;
}

export interface ErrorHandlingOptions {
  showToast?: boolean;
  toastMessage?: string;
  logError?: boolean;
  severity?: ErrorSeverity;
  context?: ErrorContext;
  onError?: (error: Error) => void;
}

const DEFAULT_OPTIONS: Required<Omit<ErrorHandlingOptions, 'toastMessage' | 'context' | 'onError'>> = {
  showToast: true,
  logError: true,
  severity: 'medium',
};


export function handleError(
  error: Error | unknown,
  options: ErrorHandlingOptions = {}
): void {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  
  
  const errorObj = error instanceof Error 
    ? error 
    : new Error(String(error));

  
  if (opts.logError) {
    const logMessage = opts.toastMessage || errorObj.message || 'An error occurred';
    logger.error(logMessage, errorObj, opts.context);
  }

  
  if (opts.showToast) {
    const message = opts.toastMessage || getDefaultErrorMessage(errorObj, opts.severity);
    
    if (opts.severity === 'critical') {
      toast.error(message, {
        duration: 10000,
      });
    } else if (opts.severity === 'high') {
      toast.error(message, {
        duration: 5000,
      });
    } else {
      toast.error(message);
    }
  }

  
  if (opts.onError) {
    opts.onError(errorObj);
  }
}


function getDefaultErrorMessage(error: Error, severity: ErrorSeverity): string {
  
  if (error.message.includes('fetch') || error.message.includes('network') || error.message.includes('timeout')) {
    return 'Network error. Please check your connection and try again.';
  }

  
  if (error.message.includes('auth') || error.message.includes('unauthorized') || error.message.includes('401')) {
    return 'Authentication error. Please log in again.';
  }

  
  if (error.message.includes('permission') || error.message.includes('403') || error.message.includes('forbidden')) {
    return 'You do not have permission to perform this action.';
  }

  
  if (error.message.includes('database') || error.message.includes('supabase') || error.message.includes('PGRST')) {
    return severity === 'critical' 
      ? 'Database connection error. Please try again in a moment.'
      : 'Failed to load data. Please try again.';
  }

  
  if (error.message.includes('invalid') || error.message.includes('validation')) {
    return error.message || 'Invalid input. Please check your data and try again.';
  }

  
  switch (severity) {
    case 'critical':
      return 'A critical error occurred. Please reload the page or contact support.';
    case 'high':
      return 'An error occurred. Please try again.';
    case 'medium':
      return error.message || 'Something went wrong. Please try again.';
    case 'low':
      return error.message || 'An issue occurred, but you can continue.';
    default:
      return 'An error occurred. Please try again.';
  }
}


export async function handleAsyncError<T>(
  asyncFn: () => Promise<T>,
  options: ErrorHandlingOptions = {}
): Promise<T | null> {
  try {
    return await asyncFn();
  } catch (error) {
    handleError(error, options);
    return null;
  }
}


export function withErrorHandling<T extends (...args: unknown[]) => Promise<unknown>>(
  fn: T,
  options: ErrorHandlingOptions = {}
): T {
  return (async (...args: Parameters<T>) => {
    try {
      return await fn(...args);
    } catch (error) {
      handleError(error, options);
      throw error; 
    }
  }) as T;
}

