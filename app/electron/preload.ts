const { contextBridge, ipcRenderer } = require('electron');


function validateSettings(settings: Record<string, any>): Record<string, boolean> {
  const validated: Record<string, boolean> = {};


  const allowedSettings = [
    'hardwareAcceleration',
    'minimizeToTray',
    'startWithSystem',
    'startMinimized',
    'alwaysOnTop',
    'chatWindowAlwaysOnTop',
    'discordRichPresence',
    'checkForUpdatesOnStartup',
    'showSplashScreen'
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

  getOAuthRedirectUrl: () => ipcRenderer.invoke('get-oauth-redirect-url'),
  isElectronProduction: () => ipcRenderer.invoke('is-electron-production'),
  onOAuthCallback: (callback: (tokens: { access_token: string; refresh_token?: string }) => void) => {
    const handler = (_event: any, tokens: { access_token: string; refresh_token?: string }) => callback(tokens);
    ipcRenderer.on('oauth-callback', handler);
    return () => ipcRenderer.removeListener('oauth-callback', handler);
  },

  checkForUpdates: () => ipcRenderer.invoke('check-for-updates'),
  downloadUpdate: () => ipcRenderer.invoke('download-update'),
  installUpdate: () => ipcRenderer.invoke('install-update'),
  getAppVersion: () => ipcRenderer.invoke('get-app-version'),
  onUpdateStatus: (callback: (status: any) => void) => {
    const handler = (_event: any, status: any) => callback(status);
    ipcRenderer.on('update-status', handler);
    return () => ipcRenderer.removeListener('update-status', handler);
  },
  openExternal: (url: string) => ipcRenderer.invoke('open-external', url),
  chatWindowShow: () => ipcRenderer.invoke('chat-window-show'),
  chatWindowHide: () => ipcRenderer.invoke('chat-window-hide'),
  chatWindowClose: () => ipcRenderer.invoke('chat-window-close'),
  chatWindowToggle: () => ipcRenderer.invoke('chat-window-toggle'),
  chatWindowIsVisible: () => ipcRenderer.invoke('chat-window-is-visible'),
  chatWindowMinimize: () => ipcRenderer.invoke('chat-window-minimize'),
  chatWindowMaximize: () => ipcRenderer.invoke('chat-window-maximize'),
  chatWindowIsMaximized: () => ipcRenderer.invoke('chat-window-is-maximized'),
  onChatWindowClosed: (callback: () => void) => {
    const handler = () => callback();
    ipcRenderer.on('chat-window-closed', handler);
    return () => ipcRenderer.removeListener('chat-window-closed', handler);
  },
  onChatWindowMaximize: (callback: () => void) => {
    const handler = () => callback();
    ipcRenderer.on('chat-window-maximized', handler);
    return () => ipcRenderer.removeListener('chat-window-maximized', handler);
  },
  onChatWindowUnmaximize: (callback: () => void) => {
    const handler = () => callback();
    ipcRenderer.on('chat-window-unmaximized', handler);
    return () => ipcRenderer.removeListener('chat-window-unmaximized', handler);
  },
  onChatWindowVisibilityChanged: (callback: (visible: boolean) => void) => {
    const handler = (_event: any, visible: boolean) => callback(visible);
    ipcRenderer.on('chat-window-visibility-changed', handler);
    return () => ipcRenderer.removeListener('chat-window-visibility-changed', handler);
  },
  onNavigateToRoute: (callback: (route: string) => void) => {
    const handler = (_event: any, route: string) => callback(route);
    ipcRenderer.on('navigate-to-route', handler);
    return () => ipcRenderer.removeListener('navigate-to-route', handler);
  },
  
  splashReady: () => ipcRenderer.invoke('splash-ready'),
  getUserInfo: () => ipcRenderer.invoke('get-user-info'),
  fetchPipelineStatus: () => ipcRenderer.invoke('fetch-pipeline-status'),
});
