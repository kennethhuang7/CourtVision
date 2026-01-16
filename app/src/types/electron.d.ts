export interface AppSettings {
  hardwareAcceleration: boolean;
  minimizeToTray: boolean;
  startWithSystem: boolean;
  startMinimized: boolean;
  alwaysOnTop: boolean;
  discordRichPresence: boolean;
}

export interface CourtVisionFolders {
  storagePath?: string;
  base: string;
  logs: string;
  exports: string;
}

export interface DiscordActivity {
  details?: string;
  state?: string;
  startTimestamp?: number;
  largeImageKey?: string;
  largeImageText?: string;
  smallImageKey?: string;
  smallImageText?: string;
}

export interface ElectronAPI {
  platform: string;
  minimize: () => Promise<void>;
  maximize: () => Promise<void>;
  restore: () => Promise<void>;
  close: () => Promise<void>;
  isMaximized: () => Promise<boolean>;
  onMaximize: (callback: () => void) => () => void;
  onRestore: (callback: () => void) => () => void;
  getAppSettings: () => Promise<AppSettings>;
  setAppSettings: (settings: Partial<AppSettings>) => Promise<{ success: boolean }>;
  writeLogFile: (content: string) => Promise<void>;
  selectFolder: () => Promise<string | null>;
  getDefaultCourtVisionFolders: () => Promise<CourtVisionFolders | null>;
  ensureCourtVisionFolders: () => Promise<CourtVisionFolders | null>;
  getStoragePath: () => Promise<string | null>;
  selectStoragePath: () => Promise<string | null>;
  getCourtVisionFolders: () => Promise<CourtVisionFolders | null>;
  openCourtVisionFolder: (folderType: 'base' | 'logs' | 'exports') => Promise<{ success: boolean }>;
  saveImageFile: (fileName: string, dataUrl: string) => Promise<{ success: boolean; filePath?: string; error?: string }>;
  discordSetActivity: (activity: DiscordActivity) => Promise<void>;
  discordClearActivity: () => Promise<void>;
  discordIsConnected: () => Promise<boolean>;
}

declare global {
  interface Window {
    electron?: ElectronAPI;
  }
}

