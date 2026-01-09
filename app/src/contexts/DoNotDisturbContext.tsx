import { createContext, useContext, useState, useEffect, useMemo, useCallback, ReactNode, useRef } from 'react';

const DO_NOT_DISTURB_KEY = 'courtvision-do-not-disturb';
const SUPPRESSION_END_TIME_KEY = 'courtvision-dnd-suppression-end';
const SUPPRESSION_DURATION_KEY = 'courtvision-dnd-suppression-duration';

export type SuppressionDuration = 5 | 10 | 15 | 30 | 60 | 120 | 240 | 480 | 1440; 

interface DoNotDisturbContextType {
  isEnabled: boolean;
  isPermanent: boolean;
  suppressionEndTime: number | null; 
  activeSuppressionDuration: SuppressionDuration | null; 
  remainingMinutes: number | null; 
  enable: () => void;
  disable: () => void;
  suppressFor: (minutes: SuppressionDuration) => void;
  cancelSuppression: () => void;
}

const DoNotDisturbContext = createContext<DoNotDisturbContextType | undefined>(undefined);

export function DoNotDisturbProvider({ children }: { children: ReactNode }) {
  const timerRef = useRef<NodeJS.Timeout | null>(null);
  
  
  const [state, setState] = useState(() => {
    if (typeof window === 'undefined') {
      return { isEnabled: false, isPermanent: false, suppressionEndTime: null, activeSuppressionDuration: null };
    }

    
    const suppressionEndTimeStr = localStorage.getItem(SUPPRESSION_END_TIME_KEY);
    const suppressionDurationStr = localStorage.getItem(SUPPRESSION_DURATION_KEY);
    if (suppressionEndTimeStr) {
      const suppressionEndTime = parseInt(suppressionEndTimeStr, 10);
      const now = Date.now();
      
      if (suppressionEndTime > now) {
        
        const activeSuppressionDuration = suppressionDurationStr
          ? (parseInt(suppressionDurationStr, 10) as SuppressionDuration)
          : null;
        return {
          isEnabled: true,
          isPermanent: false,
          suppressionEndTime,
          activeSuppressionDuration,
        };
      } else {
        
        localStorage.removeItem(SUPPRESSION_END_TIME_KEY);
        localStorage.removeItem(SUPPRESSION_DURATION_KEY);
      }
    }

    
    const stored = localStorage.getItem(DO_NOT_DISTURB_KEY);
    const isPermanent = stored === 'true';
    
    return {
      isEnabled: isPermanent,
      isPermanent,
      suppressionEndTime: null,
      activeSuppressionDuration: null,
    };
  });

  
  const remainingMinutes = state.suppressionEndTime
    ? Math.max(0, Math.ceil((state.suppressionEndTime - Date.now()) / 60000))
    : null;

  
  useEffect(() => {
    
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }

    
    if (state.isEnabled && state.suppressionEndTime && !state.isPermanent) {
      const now = Date.now();
      const timeUntilExpiry = state.suppressionEndTime - now;

      if (timeUntilExpiry > 0) {
        timerRef.current = setTimeout(() => {
          
          setState((prev) => ({
            ...prev,
            isEnabled: false,
            isPermanent: false,
            suppressionEndTime: null,
            activeSuppressionDuration: null,
          }));
          localStorage.removeItem(SUPPRESSION_END_TIME_KEY);
          localStorage.removeItem(SUPPRESSION_DURATION_KEY);
        }, timeUntilExpiry);
      } else {
        
        setState((prev) => ({
          ...prev,
          isEnabled: false,
          isPermanent: false,
          suppressionEndTime: null,
          activeSuppressionDuration: null,
        }));
        localStorage.removeItem(SUPPRESSION_END_TIME_KEY);
        localStorage.removeItem(SUPPRESSION_DURATION_KEY);
      }
    }

    
    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [state.isEnabled, state.suppressionEndTime, state.isPermanent]);

  
  useEffect(() => {
    if (!state.isEnabled || state.isPermanent || !state.suppressionEndTime) {
      return;
    }

    const interval = setInterval(() => {
      const now = Date.now();
      if (state.suppressionEndTime && state.suppressionEndTime <= now) {
        
        setState((prev) => ({
          ...prev,
          isEnabled: false,
          isPermanent: false,
          suppressionEndTime: null,
          activeSuppressionDuration: null,
        }));
        localStorage.removeItem(SUPPRESSION_END_TIME_KEY);
        localStorage.removeItem(SUPPRESSION_DURATION_KEY);
      }
    }, 60000); 

    return () => clearInterval(interval);
  }, [state.isEnabled, state.isPermanent, state.suppressionEndTime]);

  const enable = useCallback(() => {
    
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }

    setState({
      isEnabled: true,
      isPermanent: true,
      suppressionEndTime: null,
      activeSuppressionDuration: null,
    });

    if (typeof window !== 'undefined') {
      localStorage.setItem(DO_NOT_DISTURB_KEY, 'true');
      localStorage.removeItem(SUPPRESSION_END_TIME_KEY);
      localStorage.removeItem(SUPPRESSION_DURATION_KEY);
    }
  }, []);

  const disable = useCallback(() => {
    
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }

    setState({
      isEnabled: false,
      isPermanent: false,
      suppressionEndTime: null,
      activeSuppressionDuration: null,
    });

    if (typeof window !== 'undefined') {
      localStorage.setItem(DO_NOT_DISTURB_KEY, 'false');
      localStorage.removeItem(SUPPRESSION_END_TIME_KEY);
      localStorage.removeItem(SUPPRESSION_DURATION_KEY);
    }
  }, []);

  const suppressFor = useCallback((minutes: SuppressionDuration) => {
    
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }

    const suppressionEndTime = Date.now() + minutes * 60000; 

    setState({
      isEnabled: true,
      isPermanent: false,
      suppressionEndTime,
      activeSuppressionDuration: minutes,
    });

    if (typeof window !== 'undefined') {
      localStorage.setItem(DO_NOT_DISTURB_KEY, 'true'); 
      localStorage.setItem(SUPPRESSION_END_TIME_KEY, String(suppressionEndTime));
      localStorage.setItem(SUPPRESSION_DURATION_KEY, String(minutes));
    }
  }, []);

  const cancelSuppression = useCallback(() => {
    
    if (state.isEnabled && !state.isPermanent) {
      disable();
    } else if (state.isEnabled && state.isPermanent) {
      disable();
    }
  }, [state.isEnabled, state.isPermanent, disable]);

  
  const value = useMemo(
    () => ({
      isEnabled: state.isEnabled,
      isPermanent: state.isPermanent,
      suppressionEndTime: state.suppressionEndTime,
      activeSuppressionDuration: state.activeSuppressionDuration,
      remainingMinutes,
      enable,
      disable,
      suppressFor,
      cancelSuppression,
    }),
    [state.isEnabled, state.isPermanent, state.suppressionEndTime, state.activeSuppressionDuration, remainingMinutes, enable, disable, suppressFor, cancelSuppression]
  );

  return (
    <DoNotDisturbContext.Provider value={value}>
      {children}
    </DoNotDisturbContext.Provider>
  );
}

export function useDoNotDisturb() {
  const context = useContext(DoNotDisturbContext);
  if (context === undefined) {
    throw new Error('useDoNotDisturb must be used within a DoNotDisturbProvider');
  }
  return context;
}
