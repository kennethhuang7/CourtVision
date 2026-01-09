import { useEffect } from 'react';
import { useUserProfile } from './useUserProfile';
import { logger } from '@/lib/logger';

const LOGGER_CONFIG_STORAGE_KEY = 'courtvision-logger-config';


export function useErrorLoggingInit() {
  const { data: profile } = useUserProfile();

  
  useEffect(() => {
    const initializeFolders = async () => {
      if (window.electron) {
        try {
          await window.electron.ensureCourtVisionFolders();
        } catch (e) {
          
        }
      }
    };

    initializeFolders();
  }, []);

  
  useEffect(() => {
    if (profile) {
      const initializeLogConfig = async () => {
        try {
          
          const existingConfigStr = localStorage.getItem(LOGGER_CONFIG_STORAGE_KEY);
          const existingConfig = existingConfigStr ? JSON.parse(existingConfigStr) : {};

          
          let logFolder = existingConfig.logFolder || '';
          if (window.electron && !logFolder) {
            try {
              const folders = await window.electron.getDefaultCourtVisionFolders();
              if (folders) {
                logFolder = folders.logs;
              }
            } catch (e) {
              
            }
          }

          
          
          const loggerConfig = {
            enabled: (profile as any).error_logging_enabled !== null ? (profile as any).error_logging_enabled : true,
            logFolder: logFolder,
            logLevel: existingConfig.logLevel || 'error',
          };

          
          localStorage.setItem(LOGGER_CONFIG_STORAGE_KEY, JSON.stringify(loggerConfig));

          
          logger.reloadConfig();
        } catch (e) {
          
        }
      };

      initializeLogConfig();
    }
  }, [profile]);
}

