const { app, BrowserWindow, shell, Menu, ipcMain, session, globalShortcut, Tray, nativeImage } = require('electron');
const path = require('path');
const { join } = path;
const StoreModule = require('electron-store');
const RPC = require('discord-rpc');

const Store = StoreModule.default || StoreModule;

const DISCORD_CLIENT_ID = '1459638871594635356';

let discordClient: any = null;
let isDiscordConnected = false;
let discordReconnectTimeout: NodeJS.Timeout | null = null;
let lastDiscordActivity: any = null;

function initDiscordRPC() {
  if (discordClient) {
    return;
  }

  discordClient = new RPC.Client({ transport: 'ipc' });

  discordClient.on('ready', () => {
    isDiscordConnected = true;
    if (lastDiscordActivity) {
      setActivity(lastDiscordActivity);
    }
  });

  discordClient.on('disconnected', () => {
    isDiscordConnected = false;
    scheduleDiscordReconnect();
  });

  tryDiscordConnect();
}

function tryDiscordConnect() {
  if (!discordClient) return;

  discordClient.login({ clientId: DISCORD_CLIENT_ID }).catch((error: Error) => {
    isDiscordConnected = false;
    scheduleDiscordReconnect();
  });
}

function scheduleDiscordReconnect() {
  if (discordReconnectTimeout) {
    clearTimeout(discordReconnectTimeout);
  }

  discordReconnectTimeout = setTimeout(() => {
    if (!store.get('discordRichPresence', false)) {
      return;
    }
    tryDiscordConnect();
  }, 15000);
}

function setActivity(activity: any) {
  lastDiscordActivity = activity;

  if (!discordClient || !isDiscordConnected) {
    return;
  }

  const formattedActivity = {
    details: activity.details || 'Viewing Dashboard',
    state: activity.state,
    startTimestamp: activity.startTimestamp || Date.now(),
    largeImageKey: activity.largeImageKey || 'courtvision',
    largeImageText: activity.largeImageText || 'CourtVision',
    smallImageKey: activity.smallImageKey,
    smallImageText: activity.smallImageText,
    instance: false,
  };

  const cleanActivity: any = {};
  for (const [key, value] of Object.entries(formattedActivity)) {
    if (value !== undefined && value !== null && value !== '') {
      cleanActivity[key] = value;
    }
  }

  discordClient.setActivity(cleanActivity).catch((error: Error) => {});
}

function clearActivity() {
  lastDiscordActivity = null;

  if (!discordClient || !isDiscordConnected) {
    return;
  }

  discordClient.clearActivity().catch((error: Error) => {});
}

function destroyDiscordRPC() {
  if (discordReconnectTimeout) {
    clearTimeout(discordReconnectTimeout);
    discordReconnectTimeout = null;
  }

  if (discordClient) {
    try {
      discordClient.clearActivity();
      discordClient.destroy();
    } catch (error) {
      console.error('Error destroying Discord RPC:', error);
    }
    discordClient = null;
    isDiscordConnected = false;
  }
}

function isDiscordRPCConnected(): boolean {
  return isDiscordConnected;
}


app.commandLine.appendSwitch('disable-http2');


const store = new Store({
  defaults: {
    hardwareAcceleration: true,
    minimizeToTray: false,
    startWithSystem: false,
    startMinimized: false,
    alwaysOnTop: false,
    discordRichPresence: false,
  },
});


if (!store.get('hardwareAcceleration')) {
  app.disableHardwareAcceleration();
}



const isDev = process.env.NODE_ENV === 'development' || !app.isPackaged;

if (isDev) {
  process.env.DIST = join(__dirname, '..');
  process.env.VITE_DEV_SERVER_URL = process.env.VITE_DEV_SERVER_URL || 'http://localhost:5173';
} else {
  process.env.DIST = join(__dirname, '../dist');
}

process.env.PUBLIC = app.isPackaged
  ? process.env.DIST
  : join(process.env.DIST, '../public');

let win = null;
let tray = null;


const preload = join(__dirname, 'preload.cjs');

const url = process.env.VITE_DEV_SERVER_URL;
const indexHtml = join(process.env.DIST, 'index.html');

async function createWindow() {
  
  session.defaultSession.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );

  const startMinimized = store.get('startMinimized', false);
  const alwaysOnTop = store.get('alwaysOnTop', false);

  
  const iconFile = process.platform === 'win32' ? 'courtvision.ico' : 'courtvision.png';

  win = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1200,
    minHeight: 700,
    icon: join(process.env.PUBLIC, iconFile),
    frame: false, 
    show: false, 
    alwaysOnTop: alwaysOnTop,
    webPreferences: {
      preload,
      nodeIntegration: false,
      contextIsolation: true,
      webSecurity: true, 
    },
  });

  
  Menu.setApplicationMenu(null);

  
  const minimizeToTray = store.get('minimizeToTray', false);
  
  win.on('close', (event) => {
    if (minimizeToTray && !app.isQuitting) {
      event.preventDefault();
      win.hide();
      
      
      if (process.platform !== 'darwin' && tray) {
        tray.displayBalloon({
          title: 'CourtVision',
          content: 'The app is still running in the system tray. Click the icon to show the window.',
        });
      }
    }
  });

  
  win.on('minimize', () => {
    if (minimizeToTray) {
      win.hide();
    }
  });

  
  if (startMinimized) {
    if (minimizeToTray) {
      
      
    } else {
      
      win.minimize();
    }
  } else {
    win.maximize();
    win.show();
  }

  
  win.webContents.on('did-finish-load', () => {
    win?.webContents.send('main-process-message', new Date().toLocaleString());
  });

  if (isDev && url) {
    win.loadURL(url);
    
    win.webContents.openDevTools();
  } else {
    win.loadFile(indexHtml);
  }

  
  win.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith('https://') || url.startsWith('http://')) {
      try {
        const parsedUrl = new URL(url);
        if (parsedUrl.hostname) {
          shell.openExternal(url);
        }
      } catch (error) {
        console.error('Invalid URL blocked:', url);
      }
    }
    return { action: 'deny' };
  });

  
  win.on('maximize', () => {
    win?.webContents.send('window-maximized');
  });

  win.on('unmaximize', () => {
    win?.webContents.send('window-restored');
  });

  
  win.on('focus', () => {
    if (win) {
      win.flashFrame(false);
    }
  });
}


function registerDevShortcuts() {
  if (!isDev || !win) return;

  
  const hardRefreshAccelerator = process.platform === 'darwin' ? 'Command+Shift+R' : 'Ctrl+Shift+R';
  try {
    globalShortcut.register(hardRefreshAccelerator, () => {
      if (win) {
        win.webContents.reloadIgnoringCache();
      }
    });
  } catch (err) {
    
    console.error('Failed to register hard refresh shortcut:', err);
  }

  
  const devToolsAccelerator = process.platform === 'darwin' ? 'Command+Shift+I' : 'Ctrl+Shift+I';
  try {
    globalShortcut.register(devToolsAccelerator, () => {
      if (win) {
        win.webContents.toggleDevTools();
      }
    });
  } catch (err) {
    console.error('Failed to register dev tools shortcut:', err);
  }

  
  const devToolsAltAccelerator = process.platform === 'darwin' ? 'Command+Shift+C' : 'Ctrl+Shift+C';
  try {
    globalShortcut.register(devToolsAltAccelerator, () => {
      if (win) {
        win.webContents.toggleDevTools();
      }
    });
  } catch (err) {
    console.error('Failed to register alternative dev tools shortcut:', err);
  }
}

function updateTrayContextMenu() {
  if (!tray || !win) return;
  
  const isVisible = win.isVisible();
  const contextMenu = Menu.buildFromTemplate([
    {
      label: 'Show CourtVision',
      click: () => {
        if (win) {
          win.show();
          win.focus();
        }
      },
      enabled: !isVisible,
    },
    {
      label: 'Hide',
      click: () => {
        if (win) {
          win.hide();
        }
      },
      enabled: isVisible,
    },
    { type: 'separator' },
    {
      label: 'Quit',
      click: () => {
        app.isQuitting = true;
        app.quit();
      },
    },
  ]);
  
  tray.setContextMenu(contextMenu);
}

function createTray() {
  
  const iconFile = process.platform === 'win32' ? 'courtvision.ico' : 'courtvision.png';
  const iconPath = join(process.env.PUBLIC, iconFile);
  let trayIcon = nativeImage.createFromPath(iconPath);
  
  
  if (process.platform === 'darwin') {
    
    trayIcon.setTemplateImage(true);
  } else {
    
    const sizes = trayIcon.getSize();
    const targetSize = process.platform === 'win32' ? 16 : 22;
    if (sizes.width !== targetSize || sizes.height !== targetSize) {
      trayIcon = trayIcon.resize({ width: targetSize, height: targetSize });
    }
  }
  
  tray = new Tray(trayIcon);
  tray.setToolTip('CourtVision');
  
  
  updateTrayContextMenu();
  
  
  tray.on('click', () => {
    if (process.platform === 'darwin') {
      
      const contextMenu = Menu.buildFromTemplate([
        {
          label: win?.isVisible() ? 'Hide' : 'Show CourtVision',
          click: () => {
            if (win) {
              if (win.isVisible()) {
                win.hide();
              } else {
                win.show();
                win.focus();
              }
            }
          },
        },
        { type: 'separator' },
        {
          label: 'Quit',
          click: () => {
            app.isQuitting = true;
            app.quit();
          },
        },
      ]);
      tray.popUpContextMenu(contextMenu);
    } else {
      
      if (win) {
        if (win.isVisible()) {
          win.hide();
        } else {
          win.show();
          win.focus();
        }
      }
    }
  });
  
  
  if (win) {
    win.on('show', updateTrayContextMenu);
    win.on('hide', updateTrayContextMenu);
  }
}

function setupAutoStart() {
  const startWithSystem = store.get('startWithSystem', false);
  const startMinimized = store.get('startMinimized', false);
  
  app.setLoginItemSettings({
    openAtLogin: startWithSystem,
    openAsHidden: startMinimized && process.platform === 'darwin', 
  });
}


const gotTheLock = app.requestSingleInstanceLock();

if (!gotTheLock) {
  
  app.quit();
} else {
  
  app.on('second-instance', () => {
    
    if (win) {
      
      if (win.isMinimized()) {
        win.restore();
      }
      if (!win.isVisible()) {
        win.show();
      }
      win.focus();
    }
  });

  app.whenReady().then(() => {
    createWindow();


    if (store.get('minimizeToTray', false)) {
      createTray();
    }


    setupAutoStart();


    registerDevShortcuts();


    if (store.get('discordRichPresence', false)) {
      initDiscordRPC();
    }
  });
}


ipcMain.handle('window-minimize', () => {
  if (win) {
    win.minimize();
  }
});

ipcMain.handle('window-maximize', () => {
  if (win) {
    win.maximize();
  }
});

ipcMain.handle('window-restore', () => {
  if (win) {
    win.restore();
  }
});

ipcMain.handle('window-close', () => {
  if (win) {
    win.close();
  }
});

ipcMain.handle('window-is-maximized', () => {
  return win ? win.isMaximized() : false;
});


ipcMain.on('flash-window', () => {
  if (win && !win.isFocused()) {
    win.flashFrame(true);
  }
});

ipcMain.on('update-badge-count', (event, count) => {
  if (process.platform === 'darwin') {
    
    app.dock.setBadge(count > 0 ? count.toString() : '');
  } else if (process.platform === 'win32') {
    
    app.setBadgeCount(count);
  }
});


ipcMain.handle('get-app-settings', () => {
  return {
    hardwareAcceleration: store.get('hardwareAcceleration', true),
    minimizeToTray: store.get('minimizeToTray', false),
    startWithSystem: store.get('startWithSystem', false),
    startMinimized: store.get('startMinimized', false),
    alwaysOnTop: store.get('alwaysOnTop', false),
    discordRichPresence: store.get('discordRichPresence', false),
  };
});

ipcMain.handle('set-app-settings', (event, settings) => {
  if (settings.hasOwnProperty('hardwareAcceleration')) {
    store.set('hardwareAcceleration', settings.hardwareAcceleration);
  }
  
  if (settings.hasOwnProperty('minimizeToTray')) {
    store.set('minimizeToTray', settings.minimizeToTray);
    
    
    if (!settings.minimizeToTray && store.get('startMinimized', false)) {
      store.set('startMinimized', false);
    }
    
    
    if (settings.minimizeToTray && !tray) {
      createTray();
    } else if (!settings.minimizeToTray && tray) {
      tray.destroy();
      tray = null;
    }
  }
  
  if (settings.hasOwnProperty('startWithSystem') || settings.hasOwnProperty('startMinimized')) {
    if (settings.hasOwnProperty('startWithSystem')) {
      store.set('startWithSystem', settings.startWithSystem);
    }
    if (settings.hasOwnProperty('startMinimized')) {
      
      const minimizeToTray = store.get('minimizeToTray', false);
      if (settings.startMinimized && !minimizeToTray) {
        store.set('startMinimized', false);
      } else {
        store.set('startMinimized', settings.startMinimized);
      }
    }
    setupAutoStart();
  }
  
  if (settings.hasOwnProperty('alwaysOnTop')) {
    store.set('alwaysOnTop', settings.alwaysOnTop);
    if (win) {
      win.setAlwaysOnTop(settings.alwaysOnTop);
    }
  }

  if (settings.hasOwnProperty('discordRichPresence')) {
    store.set('discordRichPresence', settings.discordRichPresence);
    if (settings.discordRichPresence) {
      initDiscordRPC();
    } else {
      destroyDiscordRPC();
    }
  }

  return { success: true };
});


app.isQuitting = false;

app.on('window-all-closed', () => {
  const minimizeToTray = store.get('minimizeToTray', false);
  
  if (process.platform !== 'darwin' && !minimizeToTray) {
    app.quit();
  } else if (!minimizeToTray) {
    win = null;
  }
});


app.on('activate', () => {
  const allWindows = BrowserWindow.getAllWindows();
  if (allWindows.length) {
    allWindows[0].focus();
  } else {
    createWindow();
    
    registerDevShortcuts();
  }
});


app.on('will-quit', () => {
  if (isDev) {
    globalShortcut.unregisterAll();
  }
  destroyDiscordRPC();
});


const fs = require('fs');
const { dialog } = require('electron');
const os = require('os');

ipcMain.handle('write-log-file', async (event, folderPath, content) => {
  try {
    if (!folderPath) return;

    
    const documentsPath = app.getPath('documents');
    const courtVisionPath = path.join(documentsPath, 'CourtVision', 'Logs');
    const resolvedPath = path.resolve(folderPath);

    
    if (!resolvedPath.startsWith(courtVisionPath)) {
      console.error('Invalid log folder path - security violation attempt:', resolvedPath);
      throw new Error('Invalid log folder path');
    }

    const logFileName = `courtvision-error-${new Date().toISOString().split('T')[0]}.txt`;
    const logFilePath = path.join(folderPath, logFileName);

    
    fs.appendFileSync(logFilePath, content, 'utf8');

    
    const stats = fs.statSync(logFilePath);
    if (stats.size > 5 * 1024 * 1024) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const rotatedName = `courtvision-error-${timestamp}.txt`;
      const rotatedPath = path.join(folderPath, rotatedName);
      fs.renameSync(logFilePath, rotatedPath);
    }
  } catch (error) {
    console.error('Failed to write log file:', error);
    throw error;
  }
});

ipcMain.handle('select-folder', async () => {
  try {
    const result = await dialog.showOpenDialog(win, {
      properties: ['openDirectory'],
      title: 'Select folder for error logs',
    });

    if (result.canceled || result.filePaths.length === 0) {
      return null;
    }

    return result.filePaths[0];
  } catch (error) {
    console.error('Failed to select folder:', error);
    return null;
  }
});


ipcMain.handle('get-default-courtvision-folders', () => {
  try {
    const documentsPath = app.getPath('documents');
    const courtVisionPath = path.join(documentsPath, 'CourtVision');

    return {
      base: courtVisionPath,
      logs: path.join(courtVisionPath, 'Logs'),
      exports: path.join(courtVisionPath, 'Exports'),
    };
  } catch (error) {
    console.error('Failed to get default CourtVision folders:', error);
    return null;
  }
});


ipcMain.handle('ensure-courtvision-folders', () => {
  try {
    const documentsPath = app.getPath('documents');
    const courtVisionPath = path.join(documentsPath, 'CourtVision');
    const logsPath = path.join(courtVisionPath, 'Logs');
    const exportsPath = path.join(courtVisionPath, 'Exports');

    
    if (!fs.existsSync(courtVisionPath)) {
      fs.mkdirSync(courtVisionPath, { recursive: true });
    }
    if (!fs.existsSync(logsPath)) {
      fs.mkdirSync(logsPath, { recursive: true });
    }
    if (!fs.existsSync(exportsPath)) {
      fs.mkdirSync(exportsPath, { recursive: true });
    }

    return {
      base: courtVisionPath,
      logs: logsPath,
      exports: exportsPath,
    };
  } catch (error) {
    console.error('Failed to ensure CourtVision folders:', error);
    return null;
  }
});


ipcMain.handle('save-image-file', async (event, fileName, dataUrl, customFolder) => {
  try {

    let exportsPath;
    if (customFolder) {
      exportsPath = customFolder;
    } else {
      const documentsPath = app.getPath('documents');
      exportsPath = path.join(documentsPath, 'CourtVision', 'Exports');
    }


    if (!fs.existsSync(exportsPath)) {
      fs.mkdirSync(exportsPath, { recursive: true });
    }


    const base64Data = dataUrl.replace(/^data:image\/\w+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');


    const filePath = path.join(exportsPath, fileName);


    fs.writeFileSync(filePath, buffer);

    return { success: true, filePath };
  } catch (error) {
    console.error('Failed to save image file:', error);
    return { success: false, error: error.message };
  }
});

ipcMain.handle('discord-set-activity', (event, activity) => {
  if (store.get('discordRichPresence', false)) {
    setActivity(activity);
  }
});

ipcMain.handle('discord-clear-activity', () => {
  if (store.get('discordRichPresence', false)) {
    clearActivity();
  }
});

ipcMain.handle('discord-is-connected', () => {
  return isDiscordRPCConnected();
});