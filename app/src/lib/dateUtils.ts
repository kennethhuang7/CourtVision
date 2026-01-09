import { format } from 'date-fns';
import type { DateFormat, TimeFormat } from '@/contexts/ThemeContext';


export function parseDateString(dateStr: string | Date | null | undefined): Date {
  
  if (!dateStr) {
    return new Date();
  }

  if (dateStr instanceof Date) {
    return dateStr;
  }

  
  if (typeof dateStr !== 'string') {
    return new Date();
  }

  
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    const [year, month, day] = dateStr.split('-').map(Number);
    
    if (isNaN(year) || isNaN(month) || isNaN(day)) {
      return new Date();
    }
    return new Date(year, month - 1, day); 
  }

  
  const parsed = new Date(dateStr);
  
  return isNaN(parsed.getTime()) ? new Date() : parsed;
}


export function formatUserDate(
  date: Date | string,
  formatType: DateFormat,
  longFormat = false
): string {
  const dateObj = typeof date === 'string' ? parseDateString(date) : date;

  if (longFormat) {
    
    return format(dateObj, 'MMMM d, yyyy');
  }

  
  const formatMap: Record<DateFormat, string> = {
    'MM/DD/YYYY': 'MM/dd/yyyy',
    'DD/MM/YYYY': 'dd/MM/yyyy',
    'YYYY-MM-DD': 'yyyy-MM-dd',
  };

  return format(dateObj, formatMap[formatType]);
}


export function formatTableDate(date: Date | string, formatType: DateFormat): string {
  return formatUserDate(date, formatType, false);
}


export function formatUserTime(
  date: Date | string,
  timeFormatType: TimeFormat
): string {
  const dateObj = typeof date === 'string' ? parseDateString(date) : date;

  
  if (timeFormatType === '12h') {
    return format(dateObj, 'h:mm a'); 
  } else {
    return format(dateObj, 'HH:mm'); 
  }
}


export function formatUserDateTime(
  date: Date | string,
  dateFormatType: DateFormat,
  timeFormatType: TimeFormat
): string {
  const dateObj = typeof date === 'string' ? parseDateString(date) : date;

  
  const dateFormatMap: Record<DateFormat, string> = {
    'MM/DD/YYYY': 'MMM d, yyyy',
    'DD/MM/YYYY': 'd MMM yyyy',
    'YYYY-MM-DD': 'yyyy-MM-dd',
  };

  const timeFormatString = timeFormatType === '12h' ? 'h:mm a' : 'HH:mm';

  return format(dateObj, `${dateFormatMap[dateFormatType]} ${timeFormatString}`);
}

