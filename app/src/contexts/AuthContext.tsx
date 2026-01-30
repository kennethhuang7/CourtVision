import React, { createContext, useContext, useState, useEffect, useMemo, useCallback, ReactNode } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { supabase, queryWithTimeout } from '@/lib/supabase';
import { logger } from '@/lib/logger';
import { cleanupLocalStorage } from '@/lib/localStorageCleanup';

interface User {
  id: string;
  email: string;
  username: string;
  emailConfirmed: boolean;
}

interface AuthContextType {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  isSupabaseError: boolean;
  retryAuth: () => void;
  login: (email: string, password: string, rememberMe?: boolean) => Promise<void>;
  register: (email: string, username: string, displayName: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSupabaseError, setIsSupabaseError] = useState(false);
  const queryClient = useQueryClient();

  
  const mapUser = (supabaseUser: any, profile?: { username?: string | null }) => {
    if (!supabaseUser) return null;
    return {
      id: supabaseUser.id as string,
      email: supabaseUser.email as string,
      username: profile?.username || supabaseUser.user_metadata?.username || supabaseUser.email?.split('@')[0] || '',
      emailConfirmed: !!supabaseUser.email_confirmed_at,
    };
  };

  
  const initAuth = useCallback(async (isMountedRef: { current: boolean }) => {
    setIsLoading(true);
    setIsSupabaseError(false);

    try {
      const rememberMe = localStorage.getItem('courtvision-remember-me');
      if (rememberMe === 'false') {
        const sessionKeys = Object.keys(sessionStorage).filter(key =>
          key.startsWith('sb-') && key.includes('-auth-token')
        );

        
        sessionKeys.forEach(key => {
          const value = sessionStorage.getItem(key);
          if (value) {
            localStorage.setItem(key, value);
          }
        });
      }

      const startTime = Date.now();
      const {
        data: { session },
        error,
      } = await queryWithTimeout(
        supabase.auth.getSession(),
        15000
      );

      const responseTime = Date.now() - startTime;

      if (error) {
        const errorMessage = error.message || String(error);
        const isSupabaseIssue = 
          errorMessage.includes('timeout') ||
          errorMessage.includes('Failed to fetch') ||
          errorMessage.includes('NetworkError') ||
          errorMessage.includes('522') ||
          errorMessage.includes('504') ||
          errorMessage.includes('503') ||
          responseTime > 10000;

        if (isSupabaseIssue) {
          logger.warn('Supabase connection issue detected during auth init', { error, responseTime });
          if (isMountedRef.current) {
            setIsSupabaseError(true);
            setIsLoading(false);
          }
          return;
        }
        
        logger.error('Error getting Supabase session', error as Error);
      }

      if (session?.user && isMountedRef.current) {
        setUser(mapUser(session.user));

        cleanupLocalStorage(session.user.id);

        if (rememberMe === 'false') {
          const sessionKeys = Object.keys(localStorage).filter(key =>
            key.startsWith('sb-') && key.includes('-auth-token')
          );

          sessionKeys.forEach(key => {
            const value = localStorage.getItem(key);
            if (value) {
              sessionStorage.setItem(key, value);
              localStorage.removeItem(key);
            }
          });
        }
      }
    } catch (err: any) {
      const errorMessage = err?.message || String(err);
      const isSupabaseIssue = 
        errorMessage.includes('timeout') ||
        errorMessage.includes('Query timeout') ||
        errorMessage.includes('Failed to fetch') ||
        errorMessage.includes('NetworkError');

      if (isSupabaseIssue) {
        logger.warn('Supabase connection issue detected during auth init (catch)', { error: err });
        if (isMountedRef.current) {
          setIsSupabaseError(true);
          setIsLoading(false);
        }
        return;
      }

      logger.error('Unexpected error during auth init', err as Error);
    } finally {
      if (isMountedRef.current) {
        setIsLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    const isMountedRef = { current: true };
    
    initAuth(isMountedRef);

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange(async (_event, session) => {
      if (!isMountedRef.current) return;

      if (session?.user) {
        setUser(mapUser(session.user));
      } else {
        setUser(null);
      }
    });

    return () => {
      isMountedRef.current = false;
      subscription.unsubscribe();
    };
  }, [initAuth]);

  const retryAuth = useCallback(() => {
    const isMountedRef = { current: true };
    initAuth(isMountedRef);
  }, [initAuth]);

  const login = useCallback(async (email: string, password: string, rememberMe = true) => {
    logger.debug('Login attempt started', { email: email.substring(0, 3) + '***', rememberMe });
    setIsLoading(true);
    try {
      const { data, error } = await supabase.auth.signInWithPassword({
        email,
        password,
      });

      if (error) {
        logger.error('Login failed', error as Error, { email: email.substring(0, 3) + '***' });
        throw error;
      }

      if (data.session && data.user) {

        cleanupLocalStorage(data.user.id);


        if (rememberMe) {

          localStorage.setItem('courtvision-remember-me', 'true');
        } else {

          localStorage.setItem('courtvision-remember-me', 'false');


          const sessionKeys = Object.keys(localStorage).filter(key =>
            key.startsWith('sb-') && key.includes('-auth-token')
          );

          sessionKeys.forEach(key => {
            const value = localStorage.getItem(key);
            if (value) {
              sessionStorage.setItem(key, value);
              localStorage.removeItem(key);
            }
          });
        }

        setUser(mapUser(data.user));
      }
      logger.info('Login successful', { userId: data.user?.id, rememberMe });
    } finally {
    setIsLoading(false);
    }
  }, []);

  const register = useCallback(async (email: string, username: string, displayName: string, password: string) => {
    setIsLoading(true);
    try {
      const { error } = await supabase.auth.signUp({
        email,
        password,
        options: {
          data: {
            username,
            display_name: displayName
          },
        },
      });

      if (error) {
        logger.error('Registration failed', error as Error);
        throw error;
      }

      logger.info('Registration successful', { email: email.substring(0, 3) + '***' });
    } finally {
    setIsLoading(false);
    }
  }, []);

  const logout = useCallback(async () => {
    try {
      
      await supabase.auth.signOut();

      
      queryClient.clear();

      
      const keysToRemove = Object.keys(localStorage).filter(key =>
        key.startsWith('courtvision-notified-') ||
        key.startsWith('courtvision-auto-refresh-') ||
        key.startsWith('courtvision-remember-me') ||
        key.includes('ensemble-')
      );
      keysToRemove.forEach(key => localStorage.removeItem(key));

      
      const sessionKeysToRemove = Object.keys(sessionStorage).filter(key =>
        key.startsWith('sb-') && key.includes('-auth-token')
      );
      sessionKeysToRemove.forEach(key => sessionStorage.removeItem(key));

      
      setUser(null);
    } catch (error) {
      logger.error('Logout failed', error as Error);
      throw error;
    }
  }, [queryClient]);

  
  const value = useMemo(
    () => ({
      user,
      isAuthenticated: !!user,
      isLoading,
      isSupabaseError,
      retryAuth,
      login,
      register,
      logout,
    }),
    [user, isLoading, isSupabaseError, retryAuth, login, register, logout]
  );

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
