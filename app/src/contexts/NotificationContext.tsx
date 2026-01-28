import { createContext, useContext, useState, useEffect, useMemo, ReactNode, useCallback } from 'react';
import { toast } from 'sonner';
import { playNotificationSound, type NotificationSoundType } from '@/lib/notificationSounds';
import { logger } from '@/lib/logger';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { useUserSettings } from '@/hooks/useUserSettings';

export type NotificationType =
  | 'newPredictions'
  | 'gameResults'
  | 'messages'
  | 'pickStatus'
  | 'pickTailed'
  | 'invites'
  | 'friendRequestAccepted'
  | 'groupUpdates';

interface NotificationSettings {
  enabled: boolean;
  desktop: boolean;
  inApp: boolean;
  sound: boolean;
  soundType: NotificationSoundType;
  soundVolume: number; 
  taskbarFlashing: boolean; 
  types: Record<NotificationType, boolean>;
}

const DEFAULT_SETTINGS: NotificationSettings = {
  enabled: true,
  desktop: false,
  inApp: true,
  sound: true,
  soundType: 'chime',
  soundVolume: 70, 
  taskbarFlashing: true, 
  types: {
    newPredictions: true,
    gameResults: true,
    messages: true,
    pickStatus: true,
    pickTailed: true,
    invites: true,
    friendRequestAccepted: true,
    groupUpdates: true,
  },
};

const STORAGE_KEY = 'courtvision-notification-settings';

interface NotificationContextType {
  settings: NotificationSettings;
  updateSettings: (settings: Partial<NotificationSettings>) => void;
  notify: (type: NotificationType, title: string, message: string, options?: { sound?: boolean; icon?: string; tag?: string }) => void;
  requestDesktopPermission: () => Promise<boolean>;
  hasDesktopPermission: boolean | null;
  previewSound: (soundType?: NotificationSoundType) => void;
}

const NotificationContext = createContext<NotificationContextType | undefined>(undefined);

export function NotificationProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth();
  const { settings: userSettings, updateSetting } = useUserSettings();

  const [settings, setSettings] = useState<NotificationSettings>(() => {
    if (typeof window === 'undefined') return DEFAULT_SETTINGS;
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored);

        return {
          ...DEFAULT_SETTINGS,
          ...parsed,
          soundType: parsed.soundType || DEFAULT_SETTINGS.soundType,
          soundVolume: parsed.soundVolume !== undefined ? parsed.soundVolume : DEFAULT_SETTINGS.soundVolume,
          taskbarFlashing: parsed.taskbarFlashing !== undefined ? parsed.taskbarFlashing : DEFAULT_SETTINGS.taskbarFlashing,
          types: {
            ...DEFAULT_SETTINGS.types,
            ...(parsed.types || {}),
          },
        };
      }
    } catch (error) {
      logger.error('Error loading notification settings', error as Error);
    }
    return DEFAULT_SETTINGS;
  });

  useEffect(() => {
    if (!user) return;

    const getValue = <T,>(value: T | null | undefined): T | undefined => {
      return value === null ? undefined : value;
    };

    const nextSoundType = getValue(
      userSettings?.notification_sound_type as NotificationSoundType | undefined
    );
    const nextSoundVolume = getValue(userSettings?.notification_sound_volume);
    const nextSoundEnabled = getValue(userSettings?.sound_effects_enabled);
    const nextNotificationsEnabled = getValue(userSettings?.notifications_enabled);
    const nextDesktopNotifications = getValue(userSettings?.desktop_notifications);
    const nextNotifyNewPredictions = getValue(userSettings?.notify_new_predictions);
    const nextNotifyGameResults = getValue(userSettings?.notify_game_results);
    const nextNotifyMessages = getValue(userSettings?.notify_messages);
    const nextNotifyPickStatus = getValue(userSettings?.notify_pick_status);
    const nextNotifyPickTailed = getValue(userSettings?.notify_pick_tailed);
    const nextNotifyInvites = getValue(userSettings?.notify_invites);
    const nextNotifyFriendRequestAccepted = getValue(userSettings?.notify_friend_request_accepted);
    const nextNotifyGroupUpdates = getValue(userSettings?.notify_group_updates);

    setSettings((prev) => {
      const updated = {
        ...prev,
        soundType: nextSoundType !== undefined ? nextSoundType : prev.soundType,
        soundVolume: nextSoundVolume !== undefined ? nextSoundVolume : prev.soundVolume,
        sound: nextSoundEnabled !== undefined ? nextSoundEnabled : prev.sound,
        enabled: nextNotificationsEnabled !== undefined ? nextNotificationsEnabled : prev.enabled,
        desktop: nextDesktopNotifications !== undefined ? nextDesktopNotifications : prev.desktop,
        types: {
          ...prev.types,
          ...(nextNotifyNewPredictions !== undefined && { newPredictions: nextNotifyNewPredictions }),
          ...(nextNotifyGameResults !== undefined && { gameResults: nextNotifyGameResults }),
          ...(nextNotifyMessages !== undefined && { messages: nextNotifyMessages }),
          ...(nextNotifyPickStatus !== undefined && { pickStatus: nextNotifyPickStatus }),
          ...(nextNotifyPickTailed !== undefined && { pickTailed: nextNotifyPickTailed }),
          ...(nextNotifyInvites !== undefined && { invites: nextNotifyInvites }),
          ...(nextNotifyFriendRequestAccepted !== undefined && { friendRequestAccepted: nextNotifyFriendRequestAccepted }),
          ...(nextNotifyGroupUpdates !== undefined && { groupUpdates: nextNotifyGroupUpdates }),
        },
      };

      if (
        updated.soundType === prev.soundType &&
        updated.soundVolume === prev.soundVolume &&
        updated.sound === prev.sound &&
        updated.enabled === prev.enabled &&
        updated.desktop === prev.desktop &&
        JSON.stringify(updated.types) === JSON.stringify(prev.types)
      ) {
        return prev;
      }
      return updated;
    });
  }, [
    user?.id,
    userSettings,
    userSettings?.notification_sound_type,
    userSettings?.notification_sound_volume,
    userSettings?.sound_effects_enabled,
    userSettings?.notifications_enabled,
    userSettings?.desktop_notifications,
    userSettings?.notify_new_predictions,
    userSettings?.notify_game_results,
    userSettings?.notify_messages,
    userSettings?.notify_pick_status,
    userSettings?.notify_pick_tailed,
    userSettings?.notify_invites,
    userSettings?.notify_friend_request_accepted,
    userSettings?.notify_group_updates,
  ]);

  const [hasDesktopPermission, setHasDesktopPermission] = useState<boolean | null>(() => {
    if (typeof window === 'undefined' || !('Notification' in window)) return null;
    return Notification.permission === 'granted';
  });

  
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
    } catch (error) {
      logger.error('Error saving notification settings', error as Error);
    }
  }, [settings]);

  
  const requestDesktopPermission = useCallback(async (): Promise<boolean> => {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return false;
    }

    if (Notification.permission === 'granted') {
      setHasDesktopPermission(true);
      return true;
    }

    if (Notification.permission === 'denied') {
      setHasDesktopPermission(false);
      return false;
    }

    try {
      const permission = await Notification.requestPermission();
      const granted = permission === 'granted';
      setHasDesktopPermission(granted);
      return granted;
    } catch (error) {
      logger.error('Error requesting notification permission', error as Error);
      setHasDesktopPermission(false);
      return false;
    }
  }, []);

  
  const updateSettings = useCallback((newSettings: Partial<NotificationSettings>) => {
    setSettings((prev) => ({
      ...prev,
      ...newSettings,
      types: {
        ...prev.types,
        ...(newSettings.types || {}),
      },
    }));

    if (user) {
      if (newSettings.soundType !== undefined) {
        updateSetting('notification_sound_type', newSettings.soundType);
      }
      if (newSettings.soundVolume !== undefined) {
        updateSetting('notification_sound_volume', newSettings.soundVolume);
      }
      if (newSettings.sound !== undefined) {
        updateSetting('sound_effects_enabled', newSettings.sound);
      }
      if (newSettings.enabled !== undefined) {
        updateSetting('notifications_enabled', newSettings.enabled);
      }
      if (newSettings.desktop !== undefined) {
        updateSetting('desktop_notifications', newSettings.desktop);
      }
      if (newSettings.types) {
        if (newSettings.types.newPredictions !== undefined) {
          updateSetting('notify_new_predictions', newSettings.types.newPredictions);
        }
        if (newSettings.types.gameResults !== undefined) {
          updateSetting('notify_game_results', newSettings.types.gameResults);
        }
        if (newSettings.types.messages !== undefined) {
          updateSetting('notify_messages', newSettings.types.messages);
        }
        if (newSettings.types.pickStatus !== undefined) {
          updateSetting('notify_pick_status', newSettings.types.pickStatus);
        }
        if (newSettings.types.pickTailed !== undefined) {
          updateSetting('notify_pick_tailed', newSettings.types.pickTailed);
        }
        if (newSettings.types.invites !== undefined) {
          updateSetting('notify_invites', newSettings.types.invites);
        }
        if (newSettings.types.friendRequestAccepted !== undefined) {
          updateSetting('notify_friend_request_accepted', newSettings.types.friendRequestAccepted);
        }
        if (newSettings.types.groupUpdates !== undefined) {
          updateSetting('notify_group_updates', newSettings.types.groupUpdates);
        }
      }
    }
  }, [user, updateSetting]);

  
  const playSound = useCallback(() => {
    if (!settings.sound) return;
    
    const volumeMultiplier = settings.soundVolume / 100;
    playNotificationSound(settings.soundType, volumeMultiplier);
  }, [settings.sound, settings.soundType, settings.soundVolume]);

  
  const showDesktopNotification = useCallback(
    (title: string, message: string, options?: { icon?: string; tag?: string }) => {
      if (!settings.enabled || !settings.desktop) return;
      if (!hasDesktopPermission) return;

      try {
        
        
        const notification = new Notification(title, {
          body: message,
          icon: options?.icon || '/favicon.ico',
          tag: options?.tag, 
          badge: '/favicon.ico',
          requireInteraction: false,
          silent: false, 
        });

        
        setTimeout(() => {
          notification.close();
        }, 5000);

        
        notification.onclick = () => {
          if (window.electron) {
            
            window.focus();
          } else {
            
            window.focus();
          }
          notification.close();
        };
      } catch (error) {
        logger.error('Error showing desktop notification', error as Error);
      }
    },
    [settings.enabled, settings.desktop, hasDesktopPermission]
  );

  
  const showInAppNotification = useCallback((title: string, message: string) => {
    if (!settings.enabled || !settings.inApp) return;

    toast.info(message, {
      title,
      duration: 4000,
    });
  }, [settings.enabled, settings.inApp]);

  
  const saveNotificationToDatabase = useCallback(async (
    type: NotificationType,
    title: string,
    message: string,
    options?: { relatedId?: string; relatedType?: string; actionUrl?: string }
  ) => {
    if (!user?.id) return;

    try {
      const { error } = await supabase
        .from('user_notifications')
        .insert({
          user_id: user.id,
          type,
          title,
          message,
          read: false,
          related_id: options?.relatedId || null,
          related_type: options?.relatedType || null,
          action_url: options?.actionUrl || null,
        });

      if (error) {
        logger.error('Error saving notification to database', error);
      }

      
      if (window.electron?.ipcRenderer?.send) {
        
        const { count, error: countError } = await supabase
          .from('user_notifications')
          .select('*', { count: 'exact', head: true })
          .eq('user_id', user.id)
          .eq('read', false);

        if (!countError && count !== null) {
          window.electron.ipcRenderer.send('update-badge-count', count);
        }
      }
    } catch (error) {
      logger.error('Error saving notification', error as Error);
    }
  }, [user]);

  
  const notify = useCallback(
    (
      type: NotificationType,
      title: string,
      message: string,
      options?: { sound?: boolean; icon?: string; tag?: string; checkDoNotDisturb?: boolean; relatedId?: string; relatedType?: string; actionUrl?: string }
    ) => {
      
      if (options?.checkDoNotDisturb !== false) {
        try {
          const dndValue = localStorage.getItem('courtvision-do-not-disturb');
          if (dndValue === 'true') {
            
            saveNotificationToDatabase(type, title, message, {
              relatedId: options?.relatedId,
              relatedType: options?.relatedType,
              actionUrl: options?.actionUrl,
            });
            return; 
          }
        } catch (e) {
          
        }
      }

      
      if (!settings.enabled || !settings.types[type]) {
        
        saveNotificationToDatabase(type, title, message, {
          relatedId: options?.relatedId,
          relatedType: options?.relatedType,
          actionUrl: options?.actionUrl,
        });
        return;
      }

      
      saveNotificationToDatabase(type, title, message, {
        relatedId: options?.relatedId,
        relatedType: options?.relatedType,
        actionUrl: options?.actionUrl,
      });

      
      if (options?.sound !== false) {
        playSound();
      }

      
      if (settings.desktop) {
        showDesktopNotification(title, message, options);
      }

      
      if (settings.inApp) {
        showInAppNotification(title, message);
      }

      
      if (window.electron?.ipcRenderer?.send && settings.taskbarFlashing) {
        window.electron.ipcRenderer.send('flash-window');
      }
    },
    [settings, playSound, showDesktopNotification, showInAppNotification, saveNotificationToDatabase]
  );

  
  useEffect(() => {
    if (settings.enabled && settings.desktop && hasDesktopPermission === false) {
      
      return;
    }
    
    if (settings.enabled && settings.desktop && hasDesktopPermission === null) {
      
      requestDesktopPermission();
    }
  }, [settings.enabled, settings.desktop, hasDesktopPermission, requestDesktopPermission]);

  
  const previewSound = useCallback((soundType?: NotificationSoundType) => {
    
    const volumeMultiplier = settings.soundVolume / 100;
    playNotificationSound(soundType || settings.soundType, volumeMultiplier);
  }, [settings.soundType, settings.soundVolume]);

  
  const value = useMemo(
    () => ({
      settings,
      updateSettings,
      notify,
      requestDesktopPermission,
      hasDesktopPermission,
      previewSound,
    }),
    [settings, updateSettings, notify, requestDesktopPermission, hasDesktopPermission, previewSound]
  );

  return (
    <NotificationContext.Provider value={value}>
      {children}
    </NotificationContext.Provider>
  );
}

export function useNotifications() {
  const context = useContext(NotificationContext);
  if (context === undefined) {
    throw new Error('useNotifications must be used within a NotificationProvider');
  }
  return context;
}

