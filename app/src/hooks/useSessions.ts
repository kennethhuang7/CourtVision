import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { logger } from '@/lib/logger';
import { useEffect } from 'react';

export interface UserSession {
  id: string;
  user_id: string;
  session_id: string | null;
  device_type: string;
  client_type: string;
  os_version: string | null;
  ip_address: string | null;
  city: string | null;
  region: string | null;
  country: string | null;
  country_code: string | null;
  created_at: string;
  last_active: string;
  is_current: boolean;
  user_agent: string | null;
}


export function useUserSessions() {
  const { user } = useAuth();

  return useQuery({
    queryKey: ['user-sessions', user?.id],
    queryFn: async () => {
      if (!user?.id) return [];

      const { data, error } = await supabase
        .from('user_sessions')
        .select('*')
        .eq('user_id', user.id)
        .order('last_active', { ascending: false });

      if (error) {
        logger.error('Error fetching user sessions', error);
        throw error;
      }

      return data as UserSession[];
    },
    enabled: !!user?.id,
    staleTime: 30000,
    refetchInterval: 60000, 
  });
}


export function useDeleteSession() {
  const queryClient = useQueryClient();
  const { user } = useAuth();

  return useMutation({
    mutationFn: async (sessionId: string) => {
      const { error } = await supabase
        .from('user_sessions')
        .delete()
        .eq('id', sessionId);

      if (error) throw error;

      return sessionId;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['user-sessions', user?.id] });
    },
    onError: (error) => {
      logger.error('Error deleting session', error);
    },
  });
}


export function useTrackCurrentSession() {
  const { user } = useAuth();

  useEffect(() => {
    if (!user?.id) return;

    const trackSession = async () => {
      try {
        
        const userAgent = navigator.userAgent;
        const platform = navigator.platform;

        
        let deviceType = 'Web';
        let clientType = 'Unknown';

        if (window.electron) {
          deviceType = platform.includes('Win') ? 'Windows' : platform.includes('Mac') ? 'macOS' : 'Linux';
          clientType = 'Electron';
        } else {
          
          if (userAgent.includes('Chrome')) clientType = 'Chrome';
          else if (userAgent.includes('Firefox')) clientType = 'Firefox';
          else if (userAgent.includes('Safari')) clientType = 'Safari';
          else if (userAgent.includes('Edge')) clientType = 'Edge';
        }

        
        let locationData = null;
        const isDevelopment = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
        
        if (!isDevelopment) {
          try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000);
            
            const response = await fetch('https://ipapi.co/json/', {
              signal: controller.signal,
              mode: 'cors',
            });
            
            clearTimeout(timeoutId);
            
            if (response.ok) {
              locationData = await response.json();
            } else if (response.status === 429) {
              // Rate limited - don't log, just skip location data
              logger.debug('IP API rate limited, skipping location data');
            }
          } catch (e: any) {
            // Silently fail - location data is optional
            // Don't log CORS errors or rate limits as they're expected
            if (e?.name !== 'AbortError' && !e?.message?.includes('CORS') && !e?.message?.includes('429')) {
              logger.debug('Failed to get geolocation data', e);
            }
          }
        }

        
        const { data: existingSessions } = await supabase
          .from('user_sessions')
          .select('*')
          .eq('user_id', user.id)
          .eq('is_current', true);

        if (existingSessions && existingSessions.length > 0) {
          
          await supabase
            .from('user_sessions')
            .update({
              last_active: new Date().toISOString(),
              ip_address: locationData?.ip || null,
              city: locationData?.city || null,
              region: locationData?.region || null,
              country: locationData?.country_name || null,
              country_code: locationData?.country_code || null,
            })
            .eq('id', existingSessions[0].id);
        } else {
          
          await supabase
            .from('user_sessions')
            .insert({
              user_id: user.id,
              device_type: deviceType,
              client_type: clientType,
              os_version: platform,
              ip_address: locationData?.ip || null,
              city: locationData?.city || null,
              region: locationData?.region || null,
              country: locationData?.country_name || null,
              country_code: locationData?.country_code || null,
              is_current: true,
              user_agent: userAgent,
              last_active: new Date().toISOString(),
            });
        }
      } catch (error) {
        logger.error('Error tracking session', error);
      }
    };

    
    trackSession();

    
    const interval = setInterval(() => {
      trackSession();
    }, 5 * 60 * 1000);

    return () => clearInterval(interval);
  }, [user?.id]);
}
