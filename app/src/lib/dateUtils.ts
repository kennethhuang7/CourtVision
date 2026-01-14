import { format } from 'date-fns';
import { supabase } from './supabase';
import { logger } from './logger';

export function formatUserDate(date: Date, formatStr: string, includeYear?: boolean): string {
  if (formatStr === 'MM/DD/YYYY') {
    return format(date, includeYear ? 'MM/dd/yyyy' : 'MM/dd');
  } else if (formatStr === 'DD/MM/YYYY') {
    return format(date, includeYear ? 'dd/MM/yyyy' : 'dd/MM');
  } else {
    return format(date, includeYear ? 'yyyy-MM-dd' : 'MM-dd');
  }
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
    
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) {
      return null;
    }

    return date;
  } catch (error) {
    logger.error('Error in getMostRecentDateWithPredictions', error as Error);
    return null;
  }
}

export async function getInitialDate(): Promise<Date> {
  const stored = sessionStorage.getItem('shared-selected-date');
  if (stored) {
    const parsed = new Date(stored);
    if (!isNaN(parsed.getTime())) {
      return parsed;
    }
  }

  const mostRecent = await getMostRecentDateWithPredictions();
  if (mostRecent) {
    return mostRecent;
  }

  return new Date();
}
