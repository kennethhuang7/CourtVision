

interface LogEntry {
  timestamp: string;
  level: 'info' | 'warn' | 'error' | 'debug';
  message: string;
  context?: Record<string, unknown>;
  error?: Error;
  stack?: string;
}

interface LoggerConfig {
  enabled: boolean;
  logFolder?: string;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  maxFileSize: number; 
  maxFiles: number;
  
  consoleFallback?: boolean;
}

class Logger {
  private config: LoggerConfig = {
    enabled: true,
    logLevel: 'error',
    maxFileSize: 5 * 1024 * 1024, 
    maxFiles: 10,
    consoleFallback: false,
  };

  private logBuffer: LogEntry[] = [];
  private flushInterval: number | null = null;
  private isElectron = typeof window !== 'undefined' && window.electron;

  constructor() {
    this.loadConfig();
    this.startFlushInterval();
  }

  private loadConfig() {
    if (typeof window === 'undefined') return;
    
    try {
      const stored = localStorage.getItem('courtvision-logger-config');
      if (stored) {
        this.config = { ...this.config, ...JSON.parse(stored) };
      }
    } catch (e) {
      
    }
  }

  private saveConfig() {
    if (typeof window === 'undefined') return;
    
    try {
      localStorage.setItem('courtvision-logger-config', JSON.stringify(this.config));
    } catch (e) {
      
    }
  }

  updateConfig(updates: Partial<LoggerConfig>) {
    this.config = { ...this.config, ...updates };
    this.saveConfig();
    
    if (this.config.enabled) {
      this.startFlushInterval();
    } else {
      this.stopFlushInterval();
    }
  }

  
  reloadConfig() {
    this.loadConfig();
    if (this.config.enabled) {
      this.startFlushInterval();
    } else {
      this.stopFlushInterval();
    }
  }

  getConfig(): LoggerConfig {
    return { ...this.config };
  }

  private shouldLog(level: LogEntry['level']): boolean {
    if (!this.config.enabled) return false;
    
    const levels: LogEntry['level'][] = ['debug', 'info', 'warn', 'error'];
    const configLevelIndex = levels.indexOf(this.config.logLevel);
    const entryLevelIndex = levels.indexOf(level);
    
    return entryLevelIndex >= configLevelIndex;
  }

  private createLogEntry(
    level: LogEntry['level'],
    message: string,
    context?: Record<string, unknown>,
    error?: Error
  ): LogEntry {
    return {
      timestamp: new Date().toISOString(),
      level,
      message,
      context,
      error: error ? {
        name: error.name,
        message: error.message,
      } : undefined,
      stack: error?.stack,
    };
  }

  private addToBuffer(entry: LogEntry) {
    if (!this.shouldLog(entry.level)) return;
    
    this.logBuffer.push(entry);
    
    
    if (entry.level === 'error' && this.logBuffer.length > 0) {
      this.flush();
    }
    
    
    const shouldMirrorToConsole =
      this.config.consoleFallback &&
      (!this.isElectron || !this.config.logFolder);

    if (shouldMirrorToConsole) {
      const consoleMethod = entry.level === 'error' ? 'error' : 
                           entry.level === 'warn' ? 'warn' : 
                           entry.level === 'debug' ? 'debug' : 'log';
      
      console[consoleMethod](`[${entry.level.toUpperCase()}]`, entry.message, entry.context || '', entry.error || '');
    }
  }

  private async writeToFile(entries: LogEntry[]) {
    
    if (!this.isElectron || !this.config.logFolder || entries.length === 0) {
      
      if (!this.isElectron && this.config.enabled && this.config.consoleFallback) {
        entries.forEach(entry => {
          const consoleMethod = entry.level === 'error' ? 'error' : 
                               entry.level === 'warn' ? 'warn' : 
                               entry.level === 'debug' ? 'debug' : 'log';
          
          console[consoleMethod](`[${entry.level.toUpperCase()}]`, entry.message, entry.context || '', entry.error || '');
        });
      }
      return;
    }
    
    
    

    try {
      const logContent = entries.map(entry => {
        const parts = [
          `[${entry.timestamp}]`,
          `[${entry.level.toUpperCase()}]`,
          entry.message,
        ];
        
        if (entry.context && Object.keys(entry.context).length > 0) {
          parts.push(`Context: ${JSON.stringify(entry.context)}`);
        }
        
        if (entry.error) {
          parts.push(`Error: ${entry.error.name} - ${entry.error.message}`);
        }
        
        if (entry.stack) {
          parts.push(`Stack: ${entry.stack}`);
        }
        
        return parts.join(' | ');
      }).join('\n') + '\n';

      await window.electron.writeLogFile(this.config.logFolder, logContent);
    } catch (error) {
      
      
      
    }
  }

  private async flush() {
    if (this.logBuffer.length === 0) return;
    
    const entries = [...this.logBuffer];
    this.logBuffer = [];
    
    await this.writeToFile(entries);
  }

  private startFlushInterval() {
    this.stopFlushInterval();
    
    if (!this.config.enabled) return;
    
    
    this.flushInterval = window.setInterval(() => {
      if (this.logBuffer.length >= 50) {
        this.flush();
      }
    }, 5000);
  }

  private stopFlushInterval() {
    if (this.flushInterval !== null) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
  }

  
  info(message: string, context?: Record<string, unknown>) {
    this.addToBuffer(this.createLogEntry('info', message, context));
  }

  warn(message: string, context?: Record<string, unknown>) {
    this.addToBuffer(this.createLogEntry('warn', message, context));
  }

  error(message: string, error?: Error, context?: Record<string, unknown>) {
    this.addToBuffer(this.createLogEntry('error', message, context, error));
  }

  debug(message: string, context?: Record<string, unknown>) {
    this.addToBuffer(this.createLogEntry('debug', message, context));
  }

  
  async cleanup() {
    await this.flush();
    this.stopFlushInterval();
  }
}

export const logger = new Logger();

