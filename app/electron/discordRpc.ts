const RPC = require('discord-rpc');

const CLIENT_ID = '1459638871594635356';

let client: any = null;
let isConnected = false;
let reconnectTimeout: NodeJS.Timeout | null = null;
let lastActivity: any = null;

export function initDiscordRPC() {
  if (client) return;

  client = new RPC.Client({ transport: 'ipc' });

  client.on('ready', () => {
    console.log('Discord RPC connected');
    isConnected = true;
    if (lastActivity) {
      setActivity(lastActivity);
    }
  });

  client.on('disconnected', () => {
    console.log('Discord RPC disconnected');
    isConnected = false;
    scheduleReconnect();
  });

  tryConnect();
}

function tryConnect() {
  if (!client) return;

  client.login({ clientId: CLIENT_ID }).catch((error: Error) => {
    console.error('Failed to connect to Discord RPC:', error.message);
    isConnected = false;
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }

  reconnectTimeout = setTimeout(() => {
    console.log('Attempting to reconnect to Discord RPC...');
    tryConnect();
  }, 15000);
}

export function setActivity(activity: any) {
  lastActivity = activity;

  if (!client || !isConnected) {
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

  client.setActivity(cleanActivity).catch((error: Error) => {
    console.error('Failed to set Discord activity:', error.message);
  });
}

export function clearActivity() {
  lastActivity = null;

  if (!client || !isConnected) {
    return;
  }

  client.clearActivity().catch((error: Error) => {
    console.error('Failed to clear Discord activity:', error.message);
  });
}

export function destroyDiscordRPC() {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }

  if (client) {
    try {
      client.clearActivity();
      client.destroy();
    } catch (error) {
      console.error('Error destroying Discord RPC:', error);
    }
    client = null;
    isConnected = false;
  }
}

export function isDiscordRPCConnected(): boolean {
  return isConnected;
}
