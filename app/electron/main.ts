const { app, BrowserWindow, shell, Menu, ipcMain, session, globalShortcut, Tray, nativeImage } = require('electron');
const path = require('path');
const { join } = path;
const StoreModule = require('electron-store');
const RPC = require('discord-rpc');
const { autoUpdater } = require('electron-updater');

const Store = StoreModule.default || StoreModule;

const DISCORD_CLIENT_ID = '1459638871594635356';
const OAUTH_PROTOCOL = 'courtvision';

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
    buttons: [
      {
        label: 'Download CourtVision',
        url: 'https://github.com/kennethhuang7/CourtVision/releases'
      }
    ]
  };

  const cleanActivity: any = {};
  for (const [key, value] of Object.entries(formattedActivity)) {
    if (value !== undefined && value !== null && value !== '') {
      cleanActivity[key] = value;
    }
  }

  discordClient.setActivity(cleanActivity).catch((error: Error) => {
    logMainError('Failed to set Discord activity', error);
  });
}

function clearActivity() {
  lastDiscordActivity = null;

  if (!discordClient || !isDiscordConnected) {
    return;
  }

  discordClient.clearActivity().catch((error: Error) => {
    logMainError('Failed to clear Discord activity', error);
  });
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
      logMainError('Error destroying Discord RPC', error);
    }
    discordClient = null;
    isDiscordConnected = false;
  }
}

function isDiscordRPCConnected(): boolean {
  return isDiscordConnected;
}

let updateAvailable = false;
let updateDownloaded = false;
let updateInfo: any = null;

function initAutoUpdater() {
  autoUpdater.autoDownload = false;
  autoUpdater.autoInstallOnAppQuit = false;

  autoUpdater.on('checking-for-update', () => {
    if (win) {
      win.webContents.send('update-status', { status: 'checking' });
    }
  });

  autoUpdater.on('update-available', (info: any) => {
    updateAvailable = true;
    updateInfo = info;
    if (win) {
      win.webContents.send('update-status', {
        status: 'available',
        version: info.version,
        releaseNotes: info.releaseNotes,
        releaseDate: info.releaseDate,
      });
    }
  });

  autoUpdater.on('update-not-available', (info: any) => {
    updateAvailable = false;
    updateInfo = info;
    if (win) {
      win.webContents.send('update-status', {
        status: 'not-available',
        version: app.getVersion(),
      });
    }
  });

  autoUpdater.on('download-progress', (progress: any) => {
    if (win) {
      win.webContents.send('update-status', {
        status: 'downloading',
        percent: progress.percent,
        bytesPerSecond: progress.bytesPerSecond,
        transferred: progress.transferred,
        total: progress.total,
      });
    }
  });

  autoUpdater.on('update-downloaded', (info: any) => {
    updateDownloaded = true;
    updateInfo = info;
    if (win) {
      win.webContents.send('update-status', {
        status: 'downloaded',
        version: info.version,
      });
    }
  });

  autoUpdater.on('error', (err: Error) => {
    logMainError('Auto-updater error', err);
    if (win) {
      win.webContents.send('update-status', {
        status: 'error',
        error: err.message,
      });
    }
  });
}

function checkForUpdates() {
  if (isDev) {
    if (win) {
      win.webContents.send('update-status', {
        status: 'not-available',
        version: app.getVersion(),
      });
    }
    return;
  }
  autoUpdater.checkForUpdates().catch((err: Error) => {
    logMainError('Failed to check for updates', err);
  });
}

function downloadUpdate() {
  if (isDev) {
    if (win) {
      win.webContents.send('update-status', {
        status: 'error',
        error: 'Updates are not available in development mode',
      });
    }
    return;
  }
  if (!updateAvailable) {
    if (win) {
      win.webContents.send('update-status', {
        status: 'error',
        error: 'No update available to download',
      });
    }
    return;
  }
  autoUpdater.downloadUpdate().catch((err: Error) => {
    logMainError('Failed to download update', err);
    if (win) {
      win.webContents.send('update-status', {
        status: 'error',
        error: err.message || 'Failed to download update',
      });
    }
  });
}

function installUpdate() {
  if (!updateDownloaded || isDev) {
    if (win) {
      win.webContents.send('update-status', {
        status: 'error',
        error: isDev ? 'Updates are not available in development mode' : 'Update not downloaded yet',
      });
    }
    return;
  }
  autoUpdater.quitAndInstall(false, true);
}

app.commandLine.appendSwitch('disable-http2');


const store = new Store({
  defaults: {
    hardwareAcceleration: true,
    minimizeToTray: false,
    startWithSystem: false,
    startMinimized: false,
    alwaysOnTop: false,
    chatWindowAlwaysOnTop: false,
    discordRichPresence: false,
    checkForUpdatesOnStartup: true,
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

function handleOAuthCallback(url: string) {
  if (!url || !win) return;

  try {
    const parsedUrl = new URL(url);

    const hashParams = new URLSearchParams(parsedUrl.hash.slice(1));
    const queryParams = new URLSearchParams(parsedUrl.search);

    const error = queryParams.get('error') || hashParams.get('error');
    const errorDescription = queryParams.get('error_description') || hashParams.get('error_description');

    if (error) {
      win.webContents.send('oauth-callback', {
        error,
        errorDescription: errorDescription || 'Authentication failed'
      });
      return;
    }

    const accessToken = hashParams.get('access_token');
    const refreshToken = hashParams.get('refresh_token');
    const expiresIn = hashParams.get('expires_in');
    const tokenType = hashParams.get('token_type');

    if (accessToken) {
      win.webContents.send('oauth-callback', {
        access_token: accessToken,
        refresh_token: refreshToken,
        expires_in: expiresIn ? parseInt(expiresIn) : null,
        token_type: tokenType,
      });
    } else {
      win.webContents.send('oauth-callback', {
        error: 'no_tokens',
        errorDescription: 'No authentication tokens received'
      });
    }
  } catch (err) {
    logMainError('Failed to handle OAuth callback', err);
    if (win) {
      win.webContents.send('oauth-callback', {
        error: 'parse_error',
        errorDescription: 'Failed to process authentication response'
      });
    }
  }

  if (win) {
    if (win.isMinimized()) win.restore();
    if (!win.isVisible()) win.show();
    win.focus();
  }
}

process.env.PUBLIC = app.isPackaged
  ? process.env.DIST
  : join(process.env.DIST, 'public');

let win = null;
let chatWin = null;
let splashWin = null;
let tray = null;


const preload = join(__dirname, 'preload.cjs');

const url = process.env.VITE_DEV_SERVER_URL;
const indexHtml = join(process.env.DIST, 'index.html');
const splashHtml = join(process.env.PUBLIC, 'splash.html');

function createSplashWindow() {
  if (splashWin) {
    splashWin.show();
    splashWin.focus();
    return;
  }

  const iconFile = process.platform === 'win32' ? 'courtvision.ico' : 'courtvision.png';
  
  const { screen } = require('electron');
  const primaryDisplay = screen.getPrimaryDisplay();
  const { width, height } = primaryDisplay.workAreaSize;
  
  const splashWidth = 600;
  const splashHeight = 400;
  const x = Math.floor((width - splashWidth) / 2);
  const y = Math.floor((height - splashHeight) / 2);

  splashWin = new BrowserWindow({
    width: splashWidth,
    height: splashHeight,
    x: x,
    y: y,
    frame: false,
    transparent: true,
    alwaysOnTop: true,
    resizable: false,
    skipTaskbar: true,
    icon: join(process.env.PUBLIC, iconFile),
    webPreferences: {
      preload,
      nodeIntegration: false,
      contextIsolation: true,
      webSecurity: true,
    },
  });

  Menu.setApplicationMenu(null);

  splashWin.loadFile(splashHtml);

  splashWin.on('closed', () => {
    splashWin = null;
  });

  if (process.platform !== 'darwin') {
    splashWin.setSkipTaskbar(true);
  }
}

async function createWindow() {
  session.defaultSession.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );

  const startMinimized = store.get('startMinimized', false);
  const minimizeToTray = store.get('minimizeToTray', false);
  const alwaysOnTop = store.get('alwaysOnTop', false);

  const iconFile = process.platform === 'win32' ? 'courtvision.ico' : 'courtvision.png';

  const { screen } = require('electron');
  const primaryDisplay = screen.getPrimaryDisplay();
  const { width: screenWidth, height: screenHeight } = primaryDisplay.workAreaSize;

  win = new BrowserWindow({
    width: screenWidth,
    height: screenHeight,
    x: 0,
    y: 0,
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

  win.on('close', (event) => {
    const minimizeToTray = store.get('minimizeToTray', false);
    if (minimizeToTray && !app.isQuitting) {
      event.preventDefault();
      win.hide();
      
      if (process.platform !== 'darwin' && tray) {
        tray.displayBalloon({
          title: 'CourtVision',
          content: 'The app is still running in the system tray. Click the icon to show the window.',
        });
      }
    } else if (!minimizeToTray && !app.isQuitting) {
      if (chatWin) {
        chatWin.close();
      }
    }
  });

  
  win.on('minimize', () => {
    if (win && !win.isDestroyed()) {
      win.webContents.send('window-restored');
    }
  });

  
  // Track if main window is ready
  let mainWindowReady = false;
  let splashAnimationComplete = false;

  const showSplashScreen = store.get('showSplashScreen', true);
  
  const closeSplashAndShowMain = () => {
    if (mainWindowReady && (splashAnimationComplete || !showSplashScreen || !splashWin)) {
      if (splashWin && !splashWin.isDestroyed()) {
        splashWin.close();
      }
      
      if (startMinimized) {
        if (minimizeToTray) {
        } else {
          win.minimize();
        }
      } else {
        if (win && !win.isDestroyed()) {
          const { screen } = require('electron');
          const primaryDisplay = screen.getPrimaryDisplay();
          const { width: screenWidth, height: screenHeight } = primaryDisplay.workAreaSize;
          win.setBounds({
            x: 0,
            y: 0,
            width: screenWidth,
            height: screenHeight
          });
          win.show();
        }
      }
    }
  };

  if (showSplashScreen) {
    setTimeout(() => {
      splashAnimationComplete = true;
      closeSplashAndShowMain();
    }, 4750);
  } else {
    splashAnimationComplete = true;
  }

  win.webContents.once('did-finish-load', () => {
    mainWindowReady = true;
    closeSplashAndShowMain();
  });

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
        logMainWarn('Invalid URL blocked', { url });
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

  win.on('restore', () => {
    if (win && !win.isDestroyed()) {
      win.webContents.send('window-restored');
    }
  });

  win.on('show', () => {
    if (win && !win.isDestroyed()) {
      const isMaximized = win.isMaximized();
      win.webContents.send(isMaximized ? 'window-maximized' : 'window-restored');
    }
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
    
    logMainWarn('Failed to register hard refresh shortcut', err);
  }

  
  const devToolsAccelerator = process.platform === 'darwin' ? 'Command+Shift+I' : 'Ctrl+Shift+I';
  try {
    globalShortcut.register(devToolsAccelerator, () => {
      if (win) {
        win.webContents.toggleDevTools();
      }
    });
  } catch (err) {
    logMainWarn('Failed to register dev tools shortcut', err);
  }

  
  const devToolsAltAccelerator = process.platform === 'darwin' ? 'Command+Shift+C' : 'Ctrl+Shift+C';
  try {
    globalShortcut.register(devToolsAltAccelerator, () => {
      if (win) {
        win.webContents.toggleDevTools();
      }
    });
  } catch (err) {
    logMainWarn('Failed to register alternative dev tools shortcut', err);
  }
}

function updateTrayContextMenu() {
  if (!tray || !win) return;
  
  const isVisible = win.isVisible();
  const chatWindowVisible = chatWin ? chatWin.isVisible() : false;
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
      label: chatWindowVisible ? 'Hide Chat Window' : 'Show Chat Window',
      click: async () => {
        if (chatWin) {
          if (chatWindowVisible) {
            chatWin.hide();
          } else {
            chatWin.show();
            chatWin.focus();
          }
        } else {
          await createChatWindow();
        }
        setTimeout(() => {
          updateTrayContextMenu();
          if (win && !win.isDestroyed()) {
            win.webContents.send('chat-window-visibility-changed', chatWin ? chatWin.isVisible() : false);
          }
        }, 100);
      },
    },
    { type: 'separator' },
    {
      label: 'Close',
      click: () => {
        app.isQuitting = true;
        if (chatWin) {
          chatWin.close();
        }
        app.quit();
      },
    },
  ]);
  
  tray.setContextMenu(contextMenu);
}

function createTray() {
  const iconFile = process.platform === 'win32' ? 'courtvision.ico' : 'courtvision.png';
  let iconPath = join(process.env.PUBLIC, iconFile);
  
  const fs = require('fs');
  if (!fs.existsSync(iconPath)) {
    const altPath = join(process.env.DIST, iconFile);
    if (fs.existsSync(altPath)) {
      iconPath = altPath;
    } else {
      return;
    }
  }
  
  let trayIcon;
  try {
    trayIcon = nativeImage.createFromPath(iconPath);
    
    if (trayIcon.isEmpty()) {
      return;
    }
  } catch (error) {
    return;
  }
  
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
          label: 'Close',
          click: () => {
            app.isQuitting = true;
            if (chatWin) {
              chatWin.close();
            }
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
  
  app.on('second-instance', (event, commandLine) => {
    const oauthUrl = commandLine.find(arg => arg.startsWith(`${OAUTH_PROTOCOL}://`));
    if (oauthUrl) {
      handleOAuthCallback(oauthUrl);
      return;
    }

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

  app.on('open-url', (event, url) => {
    event.preventDefault();
    if (url.startsWith(`${OAUTH_PROTOCOL}://`)) {
      handleOAuthCallback(url);
    }
  });

function setupDockMenu() {
  if (process.platform !== 'darwin') return;

  const iconFile = join(process.env.PUBLIC, 'courtvision.png');
  let appIcon = null;
  try {
    if (require('fs').existsSync(iconFile)) {
      appIcon = nativeImage.createFromPath(iconFile);
    }
  } catch (e) {
  }

  const updateDockMenu = () => {
    const chatWindowVisible = chatWin ? chatWin.isVisible() : false;
    const dockMenu = Menu.buildFromTemplate([
      {
        label: 'Main',
        submenu: [
          { label: 'Predictions', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard'); } } },
          { label: 'Player Analysis', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/player-analysis'); } } },
          { label: 'Pick Finder', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/pick-finder'); } } },
          { label: 'Trends', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/trends'); } } },
          { label: 'My Picks', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/saved-picks'); } } },
        ],
      },
      {
        label: 'Social',
        submenu: [
          { label: 'Community', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/community'); } } },
          { label: 'Messages', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/messages'); } } },
          { label: 'My Friends', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/friends'); } } },
          { label: 'My Groups', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/groups'); } } },
        ],
      },
      {
        label: 'Insights',
        submenu: [
          { label: 'Analytics', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/analytics'); } } },
          { label: 'Model Performance', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/model-performance'); } } },
          { label: 'How It Works', icon: appIcon, click: () => { if (win) { win.show(); win.focus(); win.webContents.send('navigate-to-route', '/dashboard/how-it-works'); } } },
        ],
      },
      { type: 'separator' },
      {
        label: chatWindowVisible ? 'Hide Chat Window' : 'Show Chat Window',
        icon: appIcon,
        click: async () => {
          if (chatWin) {
            if (chatWin.isVisible()) {
              chatWin.hide();
            } else {
              chatWin.show();
              chatWin.focus();
            }
          } else {
            await createChatWindow();
          }
          if (tray) {
            updateTrayContextMenu();
          }
          if (win && !win.isDestroyed()) {
            win.webContents.send('chat-window-visibility-changed', chatWin ? chatWin.isVisible() : false);
          }
          setTimeout(updateDockMenu, 100);
        },
      },
    ]);
    app.dock.setMenu(dockMenu);
  };

  updateDockMenu();

  if (chatWin) {
    chatWin.on('show', updateDockMenu);
    chatWin.on('hide', updateDockMenu);
  }
}

function setupJumpList() {
  if (process.platform !== 'win32') return;

  app.setJumpList([
    {
      type: 'custom',
      name: 'Main',
      items: [
        { type: 'task', title: 'Predictions', description: 'View predictions', program: process.execPath, args: '--route=/dashboard' },
        { type: 'task', title: 'Player Analysis', description: 'Analyze players', program: process.execPath, args: '--route=/dashboard/player-analysis' },
        { type: 'task', title: 'Pick Finder', description: 'Find picks', program: process.execPath, args: '--route=/dashboard/pick-finder' },
        { type: 'task', title: 'Trends', description: 'View trends', program: process.execPath, args: '--route=/dashboard/trends' },
        { type: 'task', title: 'My Picks', description: 'View saved picks', program: process.execPath, args: '--route=/dashboard/saved-picks' },
      ],
    },
    {
      type: 'custom',
      name: 'Social',
      items: [
        { type: 'task', title: 'Community', description: 'View community', program: process.execPath, args: '--route=/dashboard/community' },
        { type: 'task', title: 'Messages', description: 'View messages', program: process.execPath, args: '--route=/dashboard/messages' },
        { type: 'task', title: 'My Friends', description: 'View friends', program: process.execPath, args: '--route=/dashboard/friends' },
        { type: 'task', title: 'My Groups', description: 'View groups', program: process.execPath, args: '--route=/dashboard/groups' },
      ],
    },
    {
      type: 'custom',
      name: 'Insights',
      items: [
        { type: 'task', title: 'Analytics', description: 'View analytics', program: process.execPath, args: '--route=/dashboard/analytics' },
        { type: 'task', title: 'Model Performance', description: 'View model performance', program: process.execPath, args: '--route=/dashboard/model-performance' },
        { type: 'task', title: 'How It Works', description: 'Learn how it works', program: process.execPath, args: '--route=/dashboard/how-it-works' },
      ],
    },
  ]);
}

app.whenReady().then(() => {
  if (process.platform === 'win32') {
    app.setAppUserModelId('com.courtvision.app');
  }
  
  if (process.defaultApp || isDev) {
    if (process.platform === 'win32') {
      const exePath = process.execPath;
        app.setAsDefaultProtocolClient(OAUTH_PROTOCOL, exePath);
      } else {
        app.setAsDefaultProtocolClient(OAUTH_PROTOCOL);
      }
    } else {
      app.setAsDefaultProtocolClient(OAUTH_PROTOCOL);
    }
    
    applyInstallerStoragePath();
    
    if (process.platform === 'win32' || process.platform === 'linux') {
      const oauthUrl = process.argv.find(arg => arg.startsWith(`${OAUTH_PROTOCOL}://`));
      const routeArg = process.argv.find(arg => arg.startsWith('--route='));
      if (oauthUrl) {
        setTimeout(() => {
          handleOAuthCallback(oauthUrl);
        }, 1000);
      } else if (routeArg && win) {
        const route = routeArg.split('=')[1];
        setTimeout(() => {
          if (win && win.webContents) {
            win.webContents.send('navigate-to-route', route);
          }
        }, 1000);
      }
    }
    
    const showSplashScreen = store.get('showSplashScreen', true);
    if (showSplashScreen) {
      createSplashWindow();
    }
    createWindow();

    setupDockMenu();
    setupJumpList();

    if (store.get('minimizeToTray', false)) {
      createTray();
    }


    setupAutoStart();


    registerDevShortcuts();


    if (store.get('discordRichPresence', false)) {
      initDiscordRPC();
    }

    initAutoUpdater();

    if (store.get('checkForUpdatesOnStartup', true)) {
      setTimeout(() => {
        checkForUpdates();
      }, 3000);
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

async function createChatWindow() {
  if (chatWin) {
    chatWin.show();
    chatWin.focus();
    if (tray) {
      updateTrayContextMenu();
    }
    return;
  }

  const chatWindowAlwaysOnTop = store.get('chatWindowAlwaysOnTop', false);
  const savedBounds = store.get('chatWindowBounds', null);

  const iconFile = process.platform === 'win32' ? 'courtvision.ico' : 'courtvision.png';

  const defaultBounds = {
    width: 800,
    height: 600,
    x: savedBounds?.x ?? (win ? win.getBounds().x + 100 : 100),
    y: savedBounds?.y ?? (win ? win.getBounds().y + 100 : 100),
  };

  chatWin = new BrowserWindow({
    width: savedBounds?.width ?? defaultBounds.width,
    height: savedBounds?.height ?? defaultBounds.height,
    x: defaultBounds.x,
    y: defaultBounds.y,
    minWidth: 400,
    minHeight: 300,
    icon: join(process.env.PUBLIC, iconFile),
    frame: false,
    show: false,
    alwaysOnTop: chatWindowAlwaysOnTop,
    webPreferences: {
      preload,
      nodeIntegration: false,
      contextIsolation: true,
      webSecurity: true,
    },
  });

  Menu.setApplicationMenu(null);

  chatWin.on('close', (event) => {
    const minimizeToTray = store.get('minimizeToTray', false);
    if (minimizeToTray && !app.isQuitting) {
      event.preventDefault();
      if (chatWin) {
        chatWin.hide();
        const bounds = chatWin.getBounds();
        store.set('chatWindowBounds', bounds);
        if (win && !win.isDestroyed()) {
          win.webContents.send('chat-window-visibility-changed', false);
        }
      }
      if (tray) {
        updateTrayContextMenu();
      }
    } else {
      if (chatWin) {
        const bounds = chatWin.getBounds();
        store.set('chatWindowBounds', bounds);
        if (win && !win.isDestroyed()) {
          win.webContents.send('chat-window-closed');
        }
      }
      chatWin = null;
      if (tray) {
        updateTrayContextMenu();
      }
    }
  });

  chatWin.on('show', () => {
    if (tray) {
      updateTrayContextMenu();
    }
    if (process.platform === 'darwin') {
      setupDockMenu();
    }
    if (win && !win.isDestroyed()) {
      win.webContents.send('chat-window-visibility-changed', true);
    }
    if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
      const isMaximized = chatWin.isMaximized();
      chatWin.webContents.send(isMaximized ? 'chat-window-maximized' : 'chat-window-unmaximized');
    }
  });

  chatWin.on('restore', () => {
    if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
      chatWin.webContents.send('chat-window-unmaximized');
    }
  });

  chatWin.on('hide', () => {
    if (tray) {
      updateTrayContextMenu();
    }
    if (process.platform === 'darwin') {
      setupDockMenu();
    }
    if (win && !win.isDestroyed()) {
      win.webContents.send('chat-window-visibility-changed', false);
    }
  });

  chatWin.on('moved', () => {
    if (chatWin) {
      const bounds = chatWin.getBounds();
      store.set('chatWindowBounds', bounds);
    }
  });

  chatWin.on('resized', () => {
    if (chatWin) {
      const bounds = chatWin.getBounds();
      store.set('chatWindowBounds', bounds);
    }
  });

  chatWin.on('maximize', () => {
    if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
      chatWin.webContents.send('chat-window-maximized');
    }
  });

  chatWin.on('unmaximize', () => {
    if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
      chatWin.webContents.send('chat-window-unmaximized');
    }
  });

  chatWin.on('restore', () => {
    if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
      chatWin.webContents.send('chat-window-unmaximized');
    }
  });

  chatWin.on('resize', () => {
    if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
      setTimeout(() => {
        if (chatWin && chatWin.webContents && !chatWin.webContents.isDestroyed()) {
          const isMaximized = chatWin.isMaximized();
          chatWin.webContents.send(isMaximized ? 'chat-window-maximized' : 'chat-window-unmaximized');
        }
      }, 50);
    }
  });

  const chatUrl = isDev && url ? `${url}#/chat-window` : `file://${indexHtml}#/chat-window`;

  if (isDev && url) {
    chatWin.loadURL(chatUrl);
    chatWin.webContents.openDevTools();
  } else {
    chatWin.loadFile(indexHtml).then(() => {
      if (chatWin) {
        chatWin.webContents.executeJavaScript(`window.location.hash = '#/chat-window'`);
      }
    });
  }

  chatWin.once('ready-to-show', () => {
    if (chatWin) {
      chatWin.show();
      chatWin.focus();
    }
  });

  chatWin.webContents.on('did-finish-load', () => {
    if (chatWin && !chatWin.isVisible()) {
      chatWin.show();
      chatWin.focus();
    }
  });

  chatWin.webContents.on('did-fail-load', (event, errorCode, errorDescription) => {
    console.error('Chat window failed to load:', { errorCode, errorDescription });
    if (chatWin) {
      chatWin.close();
    }
  });

  setTimeout(() => {
    if (chatWin && !chatWin.isVisible()) {
      chatWin.show();
      chatWin.focus();
    }
  }, 2000);
}

ipcMain.handle('chat-window-show', async () => {
  try {
    await createChatWindow();
    if (tray) {
      updateTrayContextMenu();
    }
    return { success: true };
  } catch (error) {
    console.error('Error showing chat window:', error);
    return { success: false, error: String(error) };
  }
});

ipcMain.handle('chat-window-hide', () => {
  if (chatWin) {
    chatWin.hide();
  }
  if (tray) {
    updateTrayContextMenu();
  }
  return { success: true };
});

ipcMain.handle('chat-window-close', () => {
  if (chatWin) {
    const minimizeToTray = store.get('minimizeToTray', false);
    if (minimizeToTray && !app.isQuitting) {
      chatWin.hide();
      if (win && !win.isDestroyed()) {
        win.webContents.send('chat-window-visibility-changed', false);
      }
    } else {
      chatWin.close();
    }
  }
  if (tray) {
    updateTrayContextMenu();
  }
  return { success: true };
});

ipcMain.handle('chat-window-toggle', async () => {
  try {
    if (chatWin) {
      if (chatWin.isVisible()) {
        chatWin.hide();
      } else {
        chatWin.show();
        chatWin.focus();
      }
    } else {
      await createChatWindow();
    }
    if (tray) {
      updateTrayContextMenu();
    }
    return { success: true };
  } catch (error) {
    console.error('Error toggling chat window:', error);
    return { success: false, error: String(error) };
  }
});

ipcMain.handle('chat-window-is-visible', () => {
  return chatWin ? chatWin.isVisible() : false;
});

ipcMain.handle('chat-window-minimize', () => {
  if (chatWin && !chatWin.isMinimized()) {
    chatWin.minimize();
  }
  return { success: true };
});

ipcMain.handle('chat-window-maximize', () => {
  if (chatWin) {
    const wasMaximized = chatWin.isMaximized();
    if (wasMaximized) {
      chatWin.unmaximize();
    } else {
      chatWin.maximize();
    }
  }
  return { success: true };
});

ipcMain.handle('chat-window-is-maximized', () => {
  return chatWin ? chatWin.isMaximized() : false;
});

ipcMain.handle('splash-ready', () => {
  return { success: true };
});

ipcMain.handle('get-user-info', async () => {
  try {
    return null;
  } catch (error) {
    return null;
  }
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
    chatWindowAlwaysOnTop: store.get('chatWindowAlwaysOnTop', false),
    discordRichPresence: store.get('discordRichPresence', false),
    checkForUpdatesOnStartup: store.get('checkForUpdatesOnStartup', true),
    showSplashScreen: store.get('showSplashScreen', true),
  };
});

ipcMain.handle('set-app-settings', (event, settings) => {
  if (Object.prototype.hasOwnProperty.call(settings, 'hardwareAcceleration')) {
    store.set('hardwareAcceleration', settings.hardwareAcceleration);
  }
  
  if (Object.prototype.hasOwnProperty.call(settings, 'minimizeToTray')) {
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
  
  if (Object.prototype.hasOwnProperty.call(settings, 'startWithSystem') || Object.prototype.hasOwnProperty.call(settings, 'startMinimized')) {
    if (Object.prototype.hasOwnProperty.call(settings, 'startWithSystem')) {
      store.set('startWithSystem', settings.startWithSystem);
    }
    if (Object.prototype.hasOwnProperty.call(settings, 'startMinimized')) {
      
      const minimizeToTray = store.get('minimizeToTray', false);
      if (settings.startMinimized && !minimizeToTray) {
        store.set('startMinimized', false);
      } else {
        store.set('startMinimized', settings.startMinimized);
      }
    }
    setupAutoStart();
  }
  
  if (Object.prototype.hasOwnProperty.call(settings, 'alwaysOnTop')) {
    store.set('alwaysOnTop', settings.alwaysOnTop);
    if (win) {
      win.setAlwaysOnTop(settings.alwaysOnTop);
    }
  }

  if (Object.prototype.hasOwnProperty.call(settings, 'chatWindowAlwaysOnTop')) {
    store.set('chatWindowAlwaysOnTop', settings.chatWindowAlwaysOnTop);
    if (chatWin) {
      chatWin.setAlwaysOnTop(settings.chatWindowAlwaysOnTop);
    }
  }

  if (Object.prototype.hasOwnProperty.call(settings, 'discordRichPresence')) {
    store.set('discordRichPresence', settings.discordRichPresence);
    if (settings.discordRichPresence) {
      initDiscordRPC();
    } else {
      destroyDiscordRPC();
    }
  }

  if (Object.prototype.hasOwnProperty.call(settings, 'checkForUpdatesOnStartup')) {
    store.set('checkForUpdatesOnStartup', settings.checkForUpdatesOnStartup);
  }

  if (Object.prototype.hasOwnProperty.call(settings, 'showSplashScreen')) {
    store.set('showSplashScreen', settings.showSplashScreen);
  }

  return { success: true };
});


app.isQuitting = false;

app.on('window-all-closed', () => {
  const minimizeToTray = store.get('minimizeToTray', false);
  
  if (process.platform !== 'darwin' && !minimizeToTray) {
    if (chatWin) {
      chatWin.close();
    }
    app.quit();
  } else if (!minimizeToTray) {
    if (chatWin) {
      chatWin.close();
    }
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

function getStoragePath() {
  try {
    const stored = store.get('storagePath', '');
    if (stored && typeof stored === 'string') return stored;
  } catch {
  }
  return app.getPath('documents');
}

function getCourtVisionFolders(storagePath) {
  const base = path.join(storagePath, 'CourtVision');
  return {
    storagePath,
    base,
    logs: path.join(base, 'Logs'),
    exports: path.join(base, 'Exports'),
  };
}

function ensureFolders(folders) {
  if (!fs.existsSync(folders.base)) fs.mkdirSync(folders.base, { recursive: true });
  if (!fs.existsSync(folders.logs)) fs.mkdirSync(folders.logs, { recursive: true });
  if (!fs.existsSync(folders.exports)) fs.mkdirSync(folders.exports, { recursive: true });
  return folders;
}

function applyInstallerStoragePath() {
  try {
    const markerCandidates = [
      path.join(app.getPath('userData'), 'installer-storage-path.txt'),
      path.join(app.getPath('appData'), 'CourtVision', 'installer-storage-path.txt'),
    ];
    const markerPath = markerCandidates.find((p) => {
      try {
        return fs.existsSync(p);
      } catch {
        return false;
      }
    });
    if (!markerPath) return;

    const raw = fs.readFileSync(markerPath, 'utf8');
    const chosen = typeof raw === 'string' ? raw.trim() : '';
    if (chosen) {
      store.set('storagePath', chosen);
      ensureFolders(getCourtVisionFolders(getStoragePath()));
    }
    fs.unlinkSync(markerPath);
  } catch (e) {
    logMainError('Failed to apply installer storage path', e);
  }
}

function writeMainLog(level, message, error) {
  try {
    const logsPath = getCourtVisionFolders(getStoragePath()).logs;
    if (!fs.existsSync(logsPath)) {
      fs.mkdirSync(logsPath, { recursive: true });
    }
    const logFileName = `courtvision-main-${new Date().toISOString().split('T')[0]}.txt`;
    const logFilePath = path.join(logsPath, logFileName);
    const err = error instanceof Error ? error : error ? new Error(String(error)) : null;
    const line = [
      `[${new Date().toISOString()}]`,
      `[${level}]`,
      message,
      err ? `${err.name}: ${err.message}` : '',
      err && err.stack ? err.stack : '',
    ].filter(Boolean).join(' | ') + os.EOL;
    fs.appendFileSync(logFilePath, line, 'utf8');
  } catch {
  }
}

function logMainError(message, error) {
  writeMainLog('ERROR', message, error);
}

function logMainWarn(message, error) {
  writeMainLog('WARN', message, error);
}

ipcMain.handle('write-log-file', async (event, content) => {
  try {
    const actualContent = typeof content === 'string' ? content : '';
    if (!actualContent) return;

    const folders = ensureFolders(getCourtVisionFolders(getStoragePath()));
    const resolvedPath = path.resolve(folders.logs);

    const logFileName = `courtvision-error-${new Date().toISOString().split('T')[0]}.txt`;
    const logFilePath = path.join(resolvedPath, logFileName);

    fs.appendFileSync(logFilePath, actualContent, 'utf8');

    if (fs.existsSync(logFilePath)) {
      const stats = fs.statSync(logFilePath);
      if (stats.size > 5 * 1024 * 1024) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const rotatedName = `courtvision-error-${timestamp}.txt`;
        const rotatedPath = path.join(resolvedPath, rotatedName);
        if (fs.existsSync(logFilePath)) {
          fs.renameSync(logFilePath, rotatedPath);
        }
      }
    }
  } catch (error) {
    logMainError('Failed to write log file', error);
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
    logMainError('Failed to select folder', error);
    return null;
  }
});

ipcMain.handle('get-storage-path', () => {
  try {
    return getStoragePath();
  } catch (error) {
    logMainError('Failed to get storage path', error);
    return null;
  }
});

ipcMain.handle('set-storage-path', async (event, storagePath) => {
  try {
    if (typeof storagePath !== 'string' || !storagePath) return { success: false };
    store.set('storagePath', storagePath);
    ensureFolders(getCourtVisionFolders(getStoragePath()));
    return { success: true };
  } catch (error) {
    logMainError('Failed to set storage path', error);
    return { success: false };
  }
});

ipcMain.handle('select-storage-path', async () => {
  try {
    const result = await dialog.showOpenDialog(win, {
      properties: ['openDirectory'],
      title: 'Select data storage location',
    });
    if (result.canceled || result.filePaths.length === 0) {
      return null;
    }
    const selected = result.filePaths[0];
    store.set('storagePath', selected);
    ensureFolders(getCourtVisionFolders(getStoragePath()));
    return selected;
  } catch (error) {
    logMainError('Failed to select storage path', error);
    return null;
  }
});

ipcMain.handle('get-courtvision-folders', () => {
  try {
    return ensureFolders(getCourtVisionFolders(getStoragePath()));
  } catch (error) {
    logMainError('Failed to get CourtVision folders', error);
    return null;
  }
});

ipcMain.handle('open-courtvision-folder', async (event, folderType) => {
  try {
    const folders = ensureFolders(getCourtVisionFolders(getStoragePath()));
    const type = typeof folderType === 'string' ? folderType : 'base';
    const target =
      type === 'logs' ? folders.logs :
      type === 'exports' ? folders.exports :
      folders.base;
    await shell.openPath(target);
    return { success: true };
  } catch (error) {
    logMainError('Failed to open CourtVision folder', error);
    return { success: false };
  }
});


ipcMain.handle('get-default-courtvision-folders', () => {
  try {
    const folders = getCourtVisionFolders(getStoragePath());
    return {
      base: folders.base,
      logs: folders.logs,
      exports: folders.exports,
    };
  } catch (error) {
    logMainError('Failed to get default CourtVision folders', error);
    return null;
  }
});


ipcMain.handle('ensure-courtvision-folders', () => {
  try {
    const folders = ensureFolders(getCourtVisionFolders(getStoragePath()));
    return {
      base: folders.base,
      logs: folders.logs,
      exports: folders.exports,
    };
  } catch (error) {
    logMainError('Failed to ensure CourtVision folders', error);
    return null;
  }
});


ipcMain.handle('save-image-file', async (event, fileName, dataUrl) => {
  try {

    const exportsPath = ensureFolders(getCourtVisionFolders(getStoragePath())).exports;


    if (!fs.existsSync(exportsPath)) {
      fs.mkdirSync(exportsPath, { recursive: true });
    }


    const base64Data = dataUrl.replace(/^data:image\/\w+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');


    const filePath = path.join(exportsPath, fileName);


    fs.writeFileSync(filePath, buffer);

    return { success: true, filePath };
  } catch (error) {
    logMainError('Failed to save image file', error);
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

// Update handlers
ipcMain.handle('check-for-updates', () => {
  checkForUpdates();
});

ipcMain.handle('download-update', () => {
  downloadUpdate();
});

ipcMain.handle('install-update', () => {
  installUpdate();
});

ipcMain.handle('get-app-version', () => {
  return app.getVersion();
});

ipcMain.handle('get-oauth-redirect-url', () => {
  return `${OAUTH_PROTOCOL}://auth/callback`;
});

ipcMain.handle('is-electron-production', () => {
  return !isDev;
});

ipcMain.handle('open-external', async (event, url) => {
  try {
    if (typeof url !== 'string') {
      return { success: false, error: 'Invalid URL' };
    }
    
    if (!url.startsWith('https://') && !url.startsWith('http://')) {
      return { success: false, error: 'Only HTTP/HTTPS URLs are allowed' };
    }
    
    await shell.openExternal(url);
    return { success: true };
  } catch (error) {
    logMainError('Failed to open external URL', error);
    return { success: false, error: error.message };
  }
});

ipcMain.on('navigate-to-route', (event, route: string) => {
  if (win && win.webContents) {
    win.webContents.send('navigate-to-route', route);
  }
});

ipcMain.on('toggle-chat-window-from-menu', async () => {
  if (chatWin) {
    if (chatWin.isVisible()) {
      chatWin.hide();
    } else {
      chatWin.show();
      chatWin.focus();
    }
  } else {
    await createChatWindow();
  }
  if (tray) {
    updateTrayContextMenu();
  }
  if (win && !win.isDestroyed()) {
    win.webContents.send('chat-window-visibility-changed', chatWin ? chatWin.isVisible() : false);
  }
});