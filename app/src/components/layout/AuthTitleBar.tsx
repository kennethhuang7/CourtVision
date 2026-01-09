import { useState, useEffect } from 'react';
import { Minus, Square, X } from 'lucide-react';

export function AuthTitleBar() {
  const [isMaximized, setIsMaximized] = useState(false);

  useEffect(() => {
    
    if (window.electron?.isMaximized) {
      window.electron.isMaximized().then(setIsMaximized);
    }

    
    const cleanupMaximize = window.electron?.onMaximize?.(() => setIsMaximized(true));
    const cleanupRestore = window.electron?.onRestore?.(() => setIsMaximized(false));

    return () => {
      cleanupMaximize?.();
      cleanupRestore?.();
    };
  }, []);

  const handleMinimize = () => {
    if (window.electron?.minimize) {
      window.electron.minimize();
    }
  };

  const handleMaximize = () => {
    if (isMaximized) {
      if (window.electron?.restore) {
        window.electron.restore();
      }
    } else {
      if (window.electron?.maximize) {
        window.electron.maximize();
      }
    }
  };

  const handleClose = () => {
    if (window.electron?.close) {
      window.electron.close();
    }
  };

  return (
    <div className="fixed top-0 left-0 right-0 z-50 flex h-10 items-center justify-between bg-background/95 backdrop-blur-sm border-b border-border/30 px-2 select-none" style={{ WebkitAppRegion: 'drag' } as React.CSSProperties}>
      <div className="flex items-center gap-2 pl-2">
        <span className="text-sm font-medium text-foreground/80">CourtVision</span>
      </div>

      <div className="flex items-center gap-1" style={{ WebkitAppRegion: 'no-drag' } as React.CSSProperties}>
        <button
          onClick={handleMinimize}
          className="flex h-8 w-10 items-center justify-center text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
          title="Minimize"
        >
          <Minus className="h-4 w-4" />
        </button>

        <button
          onClick={handleMaximize}
          className="flex h-8 w-10 items-center justify-center text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
          title={isMaximized ? "Restore" : "Maximize"}
        >
          {isMaximized ? (
            <div className="relative h-3.5 w-3.5">
              <div className="absolute bottom-0 left-0 h-2.5 w-2.5 border border-current" />
              <div className="absolute top-0 right-0 h-2.5 w-2.5 border border-current bg-card" />
            </div>
          ) : (
            <Square className="h-3.5 w-3.5" />
          )}
        </button>

        <button
          onClick={handleClose}
          className="flex h-8 w-10 items-center justify-center text-muted-foreground hover:text-white hover:bg-destructive transition-colors"
          title="Close"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}

