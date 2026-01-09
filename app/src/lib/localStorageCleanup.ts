import { logger } from './logger';

/**
 * Cleanup old localStorage entries for CourtVision app
 * Removes notification state for users who are no longer logged in
 * and keeps only the current user's data
 */
export function cleanupLocalStorage(currentUserId?: string): void {
  if (typeof window === 'undefined') return;

  try {
    const allKeys = Object.keys(localStorage);
    const courtVisionKeys = allKeys.filter(key =>
      key.startsWith('courtvision-notified-') && key.includes('-')
    );

    let removedCount = 0;

    courtVisionKeys.forEach(key => {
      // Extract user ID from key pattern: courtvision-notified-{type}-{userId}
      const parts = key.split('-');

      // Keys should be in format: courtvision-notified-{type}-{userId}
      // Examples:
      // - courtvision-notified-pick-results-{userId}
      // - courtvision-notified-tailed-picks-{userId}
      // - courtvision-notified-game-results-{userId}
      // - courtvision-notified-prediction-counts-{userId}

      if (parts.length >= 4) {
        const userId = parts[parts.length - 1]; // Last part is user ID

        // Remove if not current user
        if (currentUserId && userId !== currentUserId) {
          localStorage.removeItem(key);
          removedCount++;
        }
      }
    });

    if (removedCount > 0) {
      logger.info(`Cleaned up ${removedCount} old localStorage entries`);
    }
  } catch (error) {
    logger.warn('Error during localStorage cleanup', { error });
  }
}

/**
 * Remove all CourtVision notification data (used on logout)
 */
export function clearNotificationStorage(): void {
  if (typeof window === 'undefined') return;

  try {
    const allKeys = Object.keys(localStorage);
    const notificationKeys = allKeys.filter(key =>
      key.startsWith('courtvision-notified-')
    );

    notificationKeys.forEach(key => {
      localStorage.removeItem(key);
    });

    if (notificationKeys.length > 0) {
      logger.info(`Cleared ${notificationKeys.length} notification storage entries`);
    }
  } catch (error) {
    logger.warn('Error clearing notification storage', { error });
  }
}
