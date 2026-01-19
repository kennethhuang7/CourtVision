import { useCache } from '@/contexts/CacheContext';
import { WifiOff, Wifi } from 'lucide-react';
import { useEffect, useState } from 'react';

export function OfflineIndicator() {
  const { isOnline } = useCache();
  const [show, setShow] = useState(!isOnline);
  const [hasBeenOffline, setHasBeenOffline] = useState(false);

  useEffect(() => {
    if (!isOnline) {
      setShow(true);
      setHasBeenOffline(true);
    } else if (hasBeenOffline) {
      
      setShow(true);
      const timer = setTimeout(() => setShow(false), 3000);
      return () => clearTimeout(timer);
    }
  }, [isOnline, hasBeenOffline]);

  if (!show) return null;

  return (
    <div
      className={`fixed top-4 right-4 z-50 flex items-center gap-2 px-4 py-2 rounded-lg shadow-xl shadow-black/20 backdrop-blur-md transition-all border animate-in slide-in-from-top-2 fade-in duration-300 ${
        isOnline
          ? 'bg-success/90 text-success-foreground border-success/50'
          : 'bg-warning/90 text-warning-foreground border-warning/50'
      }`}
    >
      {isOnline ? (
        <>
          <Wifi className="h-4 w-4" />
          <span className="text-sm font-medium">Back online</span>
        </>
      ) : (
        <>
          <WifiOff className="h-4 w-4" />
          <span className="text-sm font-medium">Offline - Using cached data</span>
        </>
      )}
    </div>
  );
}
