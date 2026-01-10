import { useEffect, useCallback } from 'react';
import type { DiscordActivity } from '@/types/electron';

export function useDiscordPresence() {
  const isElectron = typeof window !== 'undefined' && window.electron;

  const setActivity = useCallback((activity: DiscordActivity) => {
    if (!isElectron || !window.electron) return;

    window.electron.discordSetActivity(activity).catch((error) => {
      console.error('Failed to set Discord activity:', error);
    });
  }, [isElectron]);

  const clearActivity = useCallback(() => {
    if (!isElectron || !window.electron) return;

    window.electron.discordClearActivity().catch((error) => {
      console.error('Failed to clear Discord activity:', error);
    });
  }, [isElectron]);

  const updateActivity = useCallback((details: string, state?: string, additionalInfo?: Partial<DiscordActivity>) => {
    if (!isElectron) return;

    const activity: DiscordActivity = {
      details,
      state,
      startTimestamp: Date.now(),
      largeImageKey: 'courtvision',
      largeImageText: 'CourtVision',
      ...additionalInfo,
    };

    setActivity(activity);
  }, [isElectron, setActivity]);

  useEffect(() => {
    return () => {
      if (isElectron && window.electron) {
        window.electron.discordClearActivity().catch(() => {});
      }
    };
  }, [isElectron]);

  return {
    updateActivity,
    setActivity,
    clearActivity,
    isAvailable: isElectron,
  };
}
