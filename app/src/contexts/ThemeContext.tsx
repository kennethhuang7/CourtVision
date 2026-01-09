import React, { createContext, useContext, useState, useEffect, useMemo, ReactNode } from 'react';

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
    const stored = localStorage.getItem('app-theme');
    return (stored as ThemeMode) || 'dark';
  });

  const [density, setDensity] = useState<UIDensity>(() => {
    const stored = localStorage.getItem('app-density');
    return (stored as UIDensity) || 'default';
  });

  const [fontScale, setFontScale] = useState<number>(() => {
    const stored = localStorage.getItem('app-font-scale');
    return stored ? parseInt(stored) : 16;
  });

  const [zoomLevel, setZoomLevel] = useState<number>(() => {
    const stored = localStorage.getItem('app-zoom-level');
    return stored ? parseInt(stored) : 100;
  });

  const [dateFormat, setDateFormat] = useState<DateFormat>(() => {
    const stored = localStorage.getItem('app-date-format');
    return (stored as DateFormat) || 'MM/DD/YYYY';
  });

  const [timeFormat, setTimeFormat] = useState<TimeFormat>(() => {
    const stored = localStorage.getItem('app-time-format');
    return (stored as TimeFormat) || '12h';
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
    localStorage.setItem('app-theme', theme);
  }, [theme, resolvedTheme]);

  
  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove('density-compact', 'density-default', 'density-spacious');
    root.classList.add(`density-${density}`);
    localStorage.setItem('app-density', density);
  }, [density]);

  
  useEffect(() => {
    const scale = fontScale / 16;
    document.documentElement.style.setProperty('--font-scale', `${scale}`);
    localStorage.setItem('app-font-scale', fontScale.toString());
  }, [fontScale]);

  
  useEffect(() => {
    const zoomValue = `${zoomLevel}%`;
    document.documentElement.style.setProperty('--zoom-level', zoomValue);
    localStorage.setItem('app-zoom-level', zoomLevel.toString());
  }, [zoomLevel]);

  
  useEffect(() => {
    localStorage.setItem('app-date-format', dateFormat);
  }, [dateFormat]);

  
  useEffect(() => {
    localStorage.setItem('app-time-format', timeFormat);
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
