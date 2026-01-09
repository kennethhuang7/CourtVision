

const APP_VERSION = '1.0.0';
const VERSION_KEY = 'courtvision-app-version';


const VERSIONED_KEYS = [
  'app-theme',
  'app-density',
  'app-font-scale',
  'app-zoom-level',
  'app-date-format',
  'app-ensemble-models',
  'courtvision-chat-window-visible',
  'courtvision-notification-settings',
  'courtvision-do-not-disturb',
  'courtvision-dnd-suppression-end',
  'courtvision-dnd-suppression-duration',
];


export function initializeStorage() {
  if (typeof window === 'undefined') return;

  const storedVersion = localStorage.getItem(VERSION_KEY);

  if (!storedVersion || storedVersion !== APP_VERSION) {
    
    handleVersionMigration(storedVersion, APP_VERSION);
    localStorage.setItem(VERSION_KEY, APP_VERSION);
  }
}


function handleVersionMigration(fromVersion: string | null, toVersion: string) {
  

  
  

  
  
  
  
  
  
  
  
  
}


export function getAppVersion(): string {
  return APP_VERSION;
}


export function getStoredVersion(): string | null {
  if (typeof window === 'undefined') return null;
  return localStorage.getItem(VERSION_KEY);
}


export function clearVersionedStorage() {
  if (typeof window === 'undefined') return;

  VERSIONED_KEYS.forEach((key) => {
    localStorage.removeItem(key);
  });

  localStorage.removeItem(VERSION_KEY);
}


export function getVersionedStorageData(): Record<string, string | null> {
  if (typeof window === 'undefined') return {};

  const data: Record<string, string | null> = {
    [VERSION_KEY]: localStorage.getItem(VERSION_KEY),
  };

  VERSIONED_KEYS.forEach((key) => {
    data[key] = localStorage.getItem(key);
  });

  return data;
}
