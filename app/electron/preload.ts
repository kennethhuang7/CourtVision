const { contextBridge, ipcRenderer } = require('electron');


function validateSettings(settings: Record<string, any>): Record<string, boolean> {
  const validated: Record<string, boolean> = {};


  const allowedSettings = [
    'hardwareAcceleration',
    'minimizeToTray',
    'startWithSystem',
    'startMinimized',
    'alwaysOnTop',
    'discordRichPresence',
    'checkForUpdatesOnStartup'
  ];

  for (const key of allowedSettings) {
    if (key in settings && typeof settings[key] === 'boolean') {
      validated[key] = settings[key];
    }
  }

  return validated;
}



contextBridge.exposeInMainWorld('electron', {
  platform: process.platform,

  minimize: () => ipcRenderer.invoke('window-minimize'),
  maximize: () => ipcRenderer.invoke('window-maximize'),
  restore: () => ipcRenderer.invoke('window-restore'),
  close: () => ipcRenderer.invoke('window-close'),
  isMaximized: () => ipcRenderer.invoke('window-is-maximized'),

  onMaximize: (callback: () => void) => {
    ipcRenderer.on('window-maximized', callback);
    return () => ipcRenderer.removeListener('window-maximized', callback);
  },
  onRestore: (callback: () => void) => {
    ipcRenderer.on('window-restored', callback);
    return () => ipcRenderer.removeListener('window-restored', callback);
  },

  getAppSettings: () => ipcRenderer.invoke('get-app-settings'),
  setAppSettings: (settings: Record<string, any>) => {

    const validated = validateSettings(settings);
    return ipcRenderer.invoke('set-app-settings', validated);
  },

  writeLogFile: (content: string) => {
    if (typeof content !== 'string') {
      throw new Error('Invalid writeLogFile parameters');
    }
    return ipcRenderer.invoke('write-log-file', content);
  },
  selectFolder: () => ipcRenderer.invoke('select-folder'),

  getDefaultCourtVisionFolders: () => ipcRenderer.invoke('get-default-courtvision-folders'),
  ensureCourtVisionFolders: () => ipcRenderer.invoke('ensure-courtvision-folders'),
  getStoragePath: () => ipcRenderer.invoke('get-storage-path'),
  selectStoragePath: () => ipcRenderer.invoke('select-storage-path'),
  getCourtVisionFolders: () => ipcRenderer.invoke('get-courtvision-folders'),
  openCourtVisionFolder: (folderType: 'base' | 'logs' | 'exports') => ipcRenderer.invoke('open-courtvision-folder', folderType),
  saveImageFile: (fileName: string, dataUrl: string) => {

    if (typeof fileName !== 'string' || typeof dataUrl !== 'string') {
      throw new Error('Invalid saveImageFile parameters');
    }

    if (fileName.includes('..') || fileName.includes('/') || fileName.includes('\\')) {
      throw new Error('Invalid file name - path traversal detected');
    }
    return ipcRenderer.invoke('save-image-file', fileName, dataUrl);
  },

  discordSetActivity: (activity: any) => ipcRenderer.invoke('discord-set-activity', activity),
  discordClearActivity: () => ipcRenderer.invoke('discord-clear-activity'),
  discordIsConnected: () => ipcRenderer.invoke('discord-is-connected'),

  // OAuth methods for production Electron
  getOAuthRedirectUrl: () => ipcRenderer.invoke('get-oauth-redirect-url'),
  isElectronProduction: () => ipcRenderer.invoke('is-electron-production'),
  onOAuthCallback: (callback: (tokens: { access_token: string; refresh_token?: string }) => void) => {
    const handler = (_event: any, tokens: { access_token: string; refresh_token?: string }) => callback(tokens);
    ipcRenderer.on('oauth-callback', handler);
    return () => ipcRenderer.removeListener('oauth-callback', handler);
  },

  // Auto-updater methods
  checkForUpdates: () => ipcRenderer.invoke('check-for-updates'),
  downloadUpdate: () => ipcRenderer.invoke('download-update'),
  installUpdate: () => ipcRenderer.invoke('install-update'),
  getAppVersion: () => ipcRenderer.invoke('get-app-version'),
  onUpdateStatus: (callback: (status: any) => void) => {
    const handler = (_event: any, status: any) => callback(status);
    ipcRenderer.on('update-status', handler);
    return () => ipcRenderer.removeListener('update-status', handler);
  },
});
