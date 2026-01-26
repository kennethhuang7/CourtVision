import { format } from 'date-fns';
import { supabase } from './supabase';
import { logger } from './logger';

function parseDateStringToLocalDate(dateStr: string): Date {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateStr);
  if (m) {
    const year = Number(m[1]);
    const month = Number(m[2]);
    const day = Number(m[3]);
    return new Date(year, month - 1, day);
  }
  return new Date(dateStr);
}

export function parseStoredDate(stored: string): Date | null {
  if (!stored) return null;
  const dateOnly = /^\d{4}-\d{2}-\d{2}$/.test(stored);
  const parsed = dateOnly ? parseDateStringToLocalDate(stored) : new Date(stored);
  return isNaN(parsed.getTime()) ? null : parsed;
}

export function toDateOnlyString(date: Date): string {
  return format(date, 'yyyy-MM-dd');
}

export function formatUserDate(date: Date, formatStr: string, includeYear?: boolean): string {
  if (formatStr === 'MM/DD/YYYY') {
    return format(date, includeYear ? 'MM/dd/yyyy' : 'MM/dd');
  } else if (formatStr === 'DD/MM/YYYY') {
    return format(date, includeYear ? 'dd/MM/yyyy' : 'dd/MM');
  } else {
    return format(date, includeYear ? 'yyyy-MM-dd' : 'MM-dd');
  }
}

export function formatTableDate(dateStr: string, formatStr: string): string {
  const date = parseDateStringToLocalDate(dateStr);
  if (isNaN(date.getTime())) {
    return dateStr;
  }
  return formatUserDate(date, formatStr, true);
}

export function formatUserTime(date: Date, timeFormat: '12h' | '24h'): string {
  if (timeFormat === '24h') {
    return format(date, 'HH:mm');
  } else {
    return format(date, 'h:mm a');
  }
}

export function formatUserDateTime(date: Date, dateFormat: string, timeFormat: '12h' | '24h'): string {
  const dateStr = formatUserDate(date, dateFormat, true);
  const timeStr = formatUserTime(date, timeFormat);
  return `${dateStr} ${timeStr}`;
}

export async function getMostRecentDateWithPredictions(): Promise<Date | null> {
  try {
    const { data, error } = await supabase
      .from('predictions')
      .select('prediction_date')
      .order('prediction_date', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) {
      logger.warn('Error fetching most recent prediction date', error);
      return null;
    }

    if (!data || !data.prediction_date) {
      return null;
    }

    const dateStr = typeof data.prediction_date === 'string'
      ? data.prediction_date
      : data.prediction_date.toISOString().split('T')[0];

    const date = parseDateStringToLocalDate(dateStr);
    if (isNaN(date.getTime())) {
      return null;
    }

    return date;
  } catch (error) {
    logger.error('Error in getMostRecentDateWithPredictions', error as Error);
    return null;
  }
}

/**
 * Gets the best date to show on first load:
 * 1. If today has predictions, return today
 * 2. Otherwise, return the most recent date with predictions
 */
export async function getBestInitialDate(): Promise<Date> {
  try {
    const today = new Date();
    const todayStr = toDateOnlyString(today);

    const { data: todayData, error: todayError } = await supabase
      .from('predictions')
      .select('prediction_date')
      .eq('prediction_date', todayStr)
      .limit(1)
      .maybeSingle();

    if (!todayError && todayData) {
      return today;
    }

    const mostRecent = await getMostRecentDateWithPredictions();
    if (mostRecent) {
      return mostRecent;
    }

    return today;
  } catch (error) {
    logger.error('Error in getBestInitialDate', error as Error);
    return new Date();
  }
}

export async function getInitialDate(): Promise<Date> {
  const stored = sessionStorage.getItem('shared-selected-date');
  if (stored) {
    const parsed = parseStoredDate(stored);
    if (parsed) {
      return parsed;
    }
  }

  const mostRecent = await getMostRecentDateWithPredictions();
  if (mostRecent) {
    return mostRecent;
  }

  return new Date();
}
