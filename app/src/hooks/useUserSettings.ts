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
  notifications_enabled?: boolean;
  desktop_notifications?: boolean;
  notify_new_predictions?: boolean;
  notify_game_results?: boolean;
  notify_messages?: boolean;
  notify_pick_status?: boolean;
  notify_pick_tailed?: boolean;
  notify_invites?: boolean;
  notify_friend_request_accepted?: boolean;
  notify_group_updates?: boolean;
  [key: string]: any;
}

const SETTINGS_STORAGE_KEY = 'courtvision-user-settings';
const EMPTY_SETTINGS: UserSettings = {};

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

  // Important: keep a stable reference while loading to avoid effect loops in consumers.
  const { data: settings = EMPTY_SETTINGS } = useQuery({
    queryKey: ['user-settings', user?.id],
    queryFn: async () => {
      if (!user?.id) return {};

      const localSettings = getLocalSettings();

      const { data, error } = await supabase
        .from('user_profiles')
        .select('*')
        .eq('user_id', user.id)
        .single();

      if (error) {
        if (error.code !== 'PGRST116') {
          logger.error('Error fetching user settings', error);
        }
        return localSettings;
      }

      const extractedSettings: UserSettings = {};
      if (data) {
        if ('theme_mode' in data) extractedSettings.theme_mode = data.theme_mode;
        if ('ui_density' in data) extractedSettings.ui_density = data.ui_density;
        if ('font_scale' in data) extractedSettings.font_scale = data.font_scale;
        if ('zoom_level' in data) extractedSettings.zoom_level = data.zoom_level;
        if ('date_format' in data) extractedSettings.date_format = data.date_format;
        if ('time_format' in data) extractedSettings.time_format = data.time_format;
        if ('notification_sound_type' in data) extractedSettings.notification_sound_type = data.notification_sound_type;
        if ('notification_sound_volume' in data) extractedSettings.notification_sound_volume = data.notification_sound_volume;
        if ('sound_effects_enabled' in data) extractedSettings.sound_effects_enabled = data.sound_effects_enabled;
        if ('skin_tone_preference' in data) extractedSettings.skin_tone_preference = data.skin_tone_preference;
        if ('discord_rich_presence_enabled' in data) extractedSettings.discord_rich_presence_enabled = data.discord_rich_presence_enabled;
        if ('notifications_enabled' in data) extractedSettings.notifications_enabled = data.notifications_enabled;
        if ('desktop_notifications' in data) extractedSettings.desktop_notifications = data.desktop_notifications;
        if ('notify_new_predictions' in data) extractedSettings.notify_new_predictions = data.notify_new_predictions;
        if ('notify_game_results' in data) extractedSettings.notify_game_results = data.notify_game_results;
        if ('notify_messages' in data) extractedSettings.notify_messages = data.notify_messages;
        if ('notify_pick_status' in data) extractedSettings.notify_pick_status = data.notify_pick_status;
        if ('notify_pick_tailed' in data) extractedSettings.notify_pick_tailed = data.notify_pick_tailed;
        if ('notify_invites' in data) extractedSettings.notify_invites = data.notify_invites;
        if ('notify_friend_request_accepted' in data) extractedSettings.notify_friend_request_accepted = data.notify_friend_request_accepted;
        if ('notify_group_updates' in data) extractedSettings.notify_group_updates = data.notify_group_updates;
      }

      const merged = { ...localSettings, ...extractedSettings };
      setLocalSettings(merged);
      return merged;
    },
    enabled: !!user?.id,
    initialData: typeof window !== 'undefined' ? getLocalSettings() : {},
    staleTime: 60000,
    retry: false,
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
