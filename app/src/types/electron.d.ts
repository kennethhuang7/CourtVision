export interface AppSettings {
  hardwareAcceleration: boolean;
  minimizeToTray: boolean;
  startWithSystem: boolean;
  startMinimized: boolean;
  alwaysOnTop: boolean;
  discordRichPresence: boolean;
  checkForUpdatesOnStartup: boolean;
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

export interface OAuthTokens {
  access_token: string;
  refresh_token?: string;
}

export interface UpdateStatus {
  status: 'checking' | 'available' | 'not-available' | 'downloading' | 'downloaded' | 'error';
  version?: string;
  releaseNotes?: string;
  releaseDate?: string;
  percent?: number;
  bytesPerSecond?: number;
  transferred?: number;
  total?: number;
  error?: string;
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
  // OAuth methods for production Electron
  getOAuthRedirectUrl: () => Promise<string>;
  isElectronProduction: () => Promise<boolean>;
  onOAuthCallback: (callback: (tokens: OAuthTokens) => void) => () => void;
  // Auto-updater methods
  checkForUpdates: () => Promise<void>;
  downloadUpdate: () => Promise<void>;
  installUpdate: () => Promise<void>;
  getAppVersion: () => Promise<string>;
  onUpdateStatus: (callback: (status: UpdateStatus) => void) => () => void;
}

declare global {
  interface Window {
    electron?: ElectronAPI;
  }
}

