import React, { createContext, useContext, useState, useEffect, useMemo, ReactNode } from 'react';
import { logger } from '@/lib/logger';

export type ThemeMode = 'light' | 'dark' | 'midnight' | 'amoled' | 'sync';
export type UIDensity = 'compact' | 'default' | 'spacious';
export type DateFormat = 'MM/DD/YYYY' | 'DD/MM/YYYY' | 'YYYY-MM-DD';
export type TimeFormat = '12h' | '24h';

interface ThemeContextType {
  theme: ThemeMode;
  setTheme: (theme: ThemeMode) => void;
  density: UIDensity;
  setDensity: (density: UIDensity) => void;
  fontScale: number;
  setFontScale: (scale: number) => void;
  zoomLevel: number;
  setZoomLevel: (zoom: number) => void;
  dateFormat: DateFormat;
  setDateFormat: (format: DateFormat) => void;
  timeFormat: TimeFormat;
  setTimeFormat: (format: TimeFormat) => void;
  resolvedTheme: 'light' | 'dark' | 'midnight' | 'amoled';
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<ThemeMode>(() => {
    try {
      const stored = localStorage.getItem('app-theme');
      return (stored as ThemeMode) || 'dark';
    } catch (e) {
      logger.warn('Error loading theme from localStorage', { error: e });
      return 'dark';
    }
  });

  const [density, setDensity] = useState<UIDensity>(() => {
    try {
      const stored = localStorage.getItem('app-density');
      return (stored as UIDensity) || 'default';
    } catch (e) {
      logger.warn('Error loading density from localStorage', { error: e });
      return 'default';
    }
  });

  const [fontScale, setFontScale] = useState<number>(() => {
    try {
      const stored = localStorage.getItem('app-font-scale');
      return stored ? parseInt(stored) : 16;
    } catch (e) {
      logger.warn('Error loading fontScale from localStorage', { error: e });
      return 16;
    }
  });

  const [zoomLevel, setZoomLevel] = useState<number>(() => {
    try {
      const stored = localStorage.getItem('app-zoom-level');
      return stored ? parseInt(stored) : 100;
    } catch (e) {
      logger.warn('Error loading zoomLevel from localStorage', { error: e });
      return 100;
    }
  });

  const [dateFormat, setDateFormat] = useState<DateFormat>(() => {
    try {
      const stored = localStorage.getItem('app-date-format');
      return (stored as DateFormat) || 'MM/DD/YYYY';
    } catch (e) {
      logger.warn('Error loading dateFormat from localStorage', { error: e });
      return 'MM/DD/YYYY';
    }
  });

  const [timeFormat, setTimeFormat] = useState<TimeFormat>(() => {
    try {
      const stored = localStorage.getItem('app-time-format');
      return (stored as TimeFormat) || '12h';
    } catch (e) {
      logger.warn('Error loading timeFormat from localStorage', { error: e });
      return '12h';
    }
  });

  const [systemTheme, setSystemTheme] = useState<'light' | 'dark'>(() => {
    if (typeof window !== 'undefined') {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    return 'dark';
  });

  
  const resolvedTheme = theme === 'sync' ? systemTheme : theme;

  
  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    const handleChange = (e: MediaQueryListEvent) => {
      setSystemTheme(e.matches ? 'dark' : 'light');
    };
    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, []);

  
  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove('light', 'dark', 'midnight', 'amoled');
    root.classList.add(resolvedTheme);
    try {
      localStorage.setItem('app-theme', theme);
    } catch (e) {
      logger.warn('Error saving theme to localStorage', { error: e });
    }
  }, [theme, resolvedTheme]);

  
  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove('density-compact', 'density-default', 'density-spacious');
    root.classList.add(`density-${density}`);
    try {
      localStorage.setItem('app-density', density);
    } catch (e) {
      logger.warn('Error saving density to localStorage', { error: e });
    }
  }, [density]);

  
  useEffect(() => {
    const scale = fontScale / 16;
    document.documentElement.style.setProperty('--font-scale', `${scale}`);
    try {
      localStorage.setItem('app-font-scale', fontScale.toString());
    } catch (e) {
      logger.warn('Error saving fontScale to localStorage', { error: e });
    }
  }, [fontScale]);

  
  useEffect(() => {
    const zoomValue = `${zoomLevel}%`;
    document.documentElement.style.setProperty('--zoom-level', zoomValue);
    try {
      localStorage.setItem('app-zoom-level', zoomLevel.toString());
    } catch (e) {
      logger.warn('Error saving zoomLevel to localStorage', { error: e });
    }
  }, [zoomLevel]);

  
  useEffect(() => {
    try {
      localStorage.setItem('app-date-format', dateFormat);
    } catch (e) {
      logger.warn('Error saving dateFormat to localStorage', { error: e });
    }
  }, [dateFormat]);

  
  useEffect(() => {
    try {
      localStorage.setItem('app-time-format', timeFormat);
    } catch (e) {
      logger.warn('Error saving timeFormat to localStorage', { error: e });
    }
  }, [timeFormat]);

  
  const value = useMemo(
    () => ({
      theme,
      setTheme,
      density,
      setDensity,
      fontScale,
      setFontScale,
      zoomLevel,
      setZoomLevel,
      dateFormat,
      setDateFormat,
      timeFormat,
      setTimeFormat,
      resolvedTheme,
    }),
    [theme, density, fontScale, zoomLevel, dateFormat, timeFormat, resolvedTheme]
  );

  return (
    <ThemeContext.Provider value={value}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}
