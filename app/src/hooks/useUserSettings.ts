import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { logger } from '@/lib/logger';
import { useCallback } from 'react';

interface UserSettings {
  theme_mode?: string;
  ui_density?: string;
  font_scale?: number;
  zoom_level?: number;
  date_format?: string;
  time_format?: string;
  notification_sound_type?: string;
  notification_sound_volume?: number;
  sound_effects_enabled?: boolean;
  skin_tone_preference?: string;
  discord_rich_presence_enabled?: boolean;
  [key: string]: any;
}

const SETTINGS_STORAGE_KEY = 'courtvision-user-settings';

function getLocalSettings(): UserSettings {
  try {
    const stored = localStorage.getItem(SETTINGS_STORAGE_KEY);
    return stored ? JSON.parse(stored) : {};
  } catch (error) {
    logger.error('Error loading local settings', error as Error);
    return {};
  }
}

function setLocalSettings(settings: UserSettings) {
  try {
    localStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify(settings));
  } catch (error) {
    logger.error('Error saving local settings', error as Error);
  }
}

export function useUserSettings() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  const { data: settings = {} } = useQuery({
    queryKey: ['user-settings', user?.id],
    queryFn: async () => {
      if (!user?.id) return {};

      const localSettings = getLocalSettings();

      const { data, error } = await supabase
        .from('user_profiles')
        .select('theme_mode, ui_density, font_scale, zoom_level, date_format, time_format, notification_sound_type, notification_sound_volume, sound_effects_enabled, skin_tone_preference, discord_rich_presence_enabled')
        .eq('user_id', user.id)
        .single();

      if (error) {
        logger.error('Error fetching user settings', error);
        return localSettings;
      }

      const merged = { ...data, ...localSettings };
      setLocalSettings(merged);
      return merged;
    },
    enabled: !!user?.id,
    staleTime: 60000,
  });

  const updateSettings = useMutation({
    mutationFn: async (newSettings: Partial<UserSettings>) => {
      if (!user?.id) throw new Error('User not authenticated');

      const merged = { ...settings, ...newSettings };
      setLocalSettings(merged);

      const { error } = await supabase
        .from('user_profiles')
        .update(newSettings)
        .eq('user_id', user.id);

      if (error) throw error;

      return merged;
    },
    onSuccess: (updatedSettings) => {
      queryClient.setQueryData(['user-settings', user?.id], updatedSettings);
    },
    onError: (error) => {
      logger.error('Error updating user settings', error);
    },
  });

  const updateSetting = useCallback(
    (key: string, value: any) => {
      updateSettings.mutate({ [key]: value });
    },
    [updateSettings]
  );

  return {
    settings,
    updateSettings: updateSettings.mutate,
    updateSetting,
    isLoading: updateSettings.isPending,
  };
}
