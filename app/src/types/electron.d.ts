export interface AppSettings {
  hardwareAcceleration: boolean;
  minimizeToTray: boolean;
  startWithSystem: boolean;
  startMinimized: boolean;
  alwaysOnTop: boolean;
  discordRichPresence: boolean;
}

export interface CourtVisionFolders {
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
  writeLogFile: (folderPath: string, content: string) => Promise<void>;
  selectFolder: () => Promise<string | null>;
  getDefaultCourtVisionFolders: () => Promise<CourtVisionFolders | null>;
  ensureCourtVisionFolders: () => Promise<CourtVisionFolders | null>;
  saveImageFile: (fileName: string, dataUrl: string, customFolder?: string) => Promise<{ success: boolean; filePath?: string; error?: string }>;
  discordSetActivity: (activity: DiscordActivity) => Promise<void>;
  discordClearActivity: () => Promise<void>;
  discordIsConnected: () => Promise<boolean>;
}

declare global {
  interface Window {
    electron?: ElectronAPI;
  }
}

