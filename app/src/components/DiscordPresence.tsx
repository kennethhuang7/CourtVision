import { useEffect, useState, useCallback, useRef } from 'react';
import { useLocation } from 'react-router-dom';
import { useDiscordPresence } from '@/hooks/useDiscordPresence';

const ROUTE_ACTIVITIES: Record<string, { details: string; state?: string }> = {
  '/predictions': { details: 'Viewing Predictions', state: 'Analyzing player stats' },
  '/player-analysis': { details: 'Analyzing Players', state: 'Studying performance data' },
  '/model-performance': { details: 'Checking Model Performance', state: 'Reviewing accuracy metrics' },
  '/trends': { details: 'Exploring Trends', state: 'Discovering patterns' },
  '/analytics': { details: 'Viewing Analytics', state: 'Deep diving into data' },
  '/pick-finder': { details: 'Finding Picks', state: 'Searching for value' },
  '/community': { details: 'Browsing Community', state: 'Exploring posts' },
};

const IDLE_TIMEOUT = 5 * 60 * 1000;

export function DiscordPresence() {
  const location = useLocation();
  const { updateActivity, clearActivity, isAvailable } = useDiscordPresence();
  const [isIdle, setIsIdle] = useState(false);
  const currentActivityRef = useRef<{ details: string; state?: string; startTimestamp: number } | null>(null);
  const idleTimerRef = useRef<NodeJS.Timeout | null>(null);

  const resetIdleTimer = useCallback(() => {
    if (isIdle) {
      setIsIdle(false);
    }

    if (idleTimerRef.current) {
      clearTimeout(idleTimerRef.current);
    }

    idleTimerRef.current = setTimeout(() => {
      setIsIdle(true);
    }, IDLE_TIMEOUT);
  }, [isIdle]);

  useEffect(() => {
    if (!isAvailable) return;

    const events = ['mousedown', 'mousemove', 'keydown', 'scroll', 'touchstart'];

    events.forEach(event => {
      document.addEventListener(event, resetIdleTimer, { passive: true });
    });

    resetIdleTimer();

    return () => {
      events.forEach(event => {
        document.removeEventListener(event, resetIdleTimer);
      });

      if (idleTimerRef.current) {
        clearTimeout(idleTimerRef.current);
      }
    };
  }, [isAvailable, resetIdleTimer]);

  useEffect(() => {
    if (!isAvailable) return;

    const activity = ROUTE_ACTIVITIES[location.pathname];
    const now = Date.now();

    if (activity) {
      if (!currentActivityRef.current || currentActivityRef.current.details !== activity.details) {
        currentActivityRef.current = {
          details: activity.details,
          state: activity.state,
          startTimestamp: now,
        };
      }
    } else {
      if (!currentActivityRef.current || currentActivityRef.current.details !== 'Using CourtVision') {
        currentActivityRef.current = {
          details: 'Using CourtVision',
          state: 'NBA analytics platform',
          startTimestamp: now,
        };
      }
    }

    if (isIdle && currentActivityRef.current) {
      updateActivity(
        currentActivityRef.current.details,
        'Away',
        { startTimestamp: currentActivityRef.current.startTimestamp }
      );
    } else if (currentActivityRef.current) {
      updateActivity(
        currentActivityRef.current.details,
        currentActivityRef.current.state,
        { startTimestamp: currentActivityRef.current.startTimestamp }
      );
    }

    return () => {
      clearActivity();
    };
  }, [location.pathname, isIdle, updateActivity, clearActivity, isAvailable]);

  return null;
}
