import { useState, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Minus, Square, Pin } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ChatWindow } from './ChatWindow';
import { cn } from '@/lib/utils';

export type ChatWindowState = 'hidden' | 'open' | 'minimized';

interface DraggableChatWindowProps {
  isVisible: boolean;
  onClose: () => void;
}

export function DraggableChatWindow({ isVisible, onClose }: DraggableChatWindowProps) {
  const isElectron = typeof window !== 'undefined' && window.electron;
  const [isMinimized, setIsMinimized] = useState(false);
  const [isMaximized, setIsMaximized] = useState(false);
  const [alwaysOnTop, setAlwaysOnTop] = useState(false);
  const [position, setPosition] = useState(() => {
    if (typeof window !== 'undefined' && !isElectron) {
      const windowWidth = 800;
      const windowHeight = 600;
      const margin = 20;
      return {
        x: Math.max(0, window.innerWidth - windowWidth - margin),
        y: Math.max(0, window.innerHeight - windowHeight - margin)
      };
    }
    return { x: 0, y: 0 };
  });
  const [isDragging, setIsDragging] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const windowRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (isElectron && window.electron?.chatWindowIsMaximized) {
      const checkMaximized = async () => {
        try {
          const maximized = await window.electron!.chatWindowIsMaximized();
          setIsMaximized(maximized);
        } catch (error) {
          console.error('Error checking chat window maximized state:', error);
        }
      };

      checkMaximized();
      const interval = setInterval(checkMaximized, 500);

      const cleanupMaximize = window.electron?.onChatWindowMaximize?.(() => setIsMaximized(true));
      const cleanupUnmaximize = window.electron?.onChatWindowUnmaximize?.(() => setIsMaximized(false));

      return () => {
        clearInterval(interval);
        cleanupMaximize?.();
        cleanupUnmaximize?.();
      };
    }
  }, [isElectron]);

  useEffect(() => {
    if (!isVisible) {
      setIsMinimized(false);
    }
  }, [isVisible]);

  useEffect(() => {
    if (isElectron && window.electron?.getAppSettings) {
      const loadSettings = () => {
        window.electron!.getAppSettings().then((settings) => {
          setAlwaysOnTop(settings.chatWindowAlwaysOnTop || false);
        }).catch((error) => {
          console.error('Error loading app settings:', error);
        });
      };

      loadSettings();
      const interval = setInterval(loadSettings, 1000);

      return () => {
        clearInterval(interval);
      };
    }
  }, [isElectron]);

  const handleMouseDown = (e: React.MouseEvent) => {
    const target = e.target as HTMLElement;
    const titleBar = target.closest('[data-title-bar]');
    const isButton = target.closest('button');

    if (!titleBar || isButton) return;

    setIsDragging(true);
    const rect = windowRef.current?.getBoundingClientRect();
    if (rect) {
      setDragOffset({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
      });
    }
  };

  useEffect(() => {
    if (!isDragging) return;

    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging) return;

      const newX = e.clientX - dragOffset.x;
      const newY = e.clientY - dragOffset.y;

      const width = windowRef.current?.offsetWidth || 800;
      const height = windowRef.current?.offsetHeight || 600;
      const maxX = window.innerWidth - width;
      const maxY = window.innerHeight - height;

      setPosition({
        x: Math.max(0, Math.min(newX, maxX)),
        y: Math.max(0, Math.min(newY, maxY)),
      });
    };

    const handleMouseUp = () => {
      setIsDragging(false);
    };

    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);

    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isDragging, dragOffset]);

  const handleMinimize = () => {
    if (isElectron && window.electron?.chatWindowMinimize) {
      window.electron.chatWindowMinimize();
    } else {
      setIsMinimized(true);
    }
  };

  const handleMaximize = async () => {
    if (isElectron && window.electron?.chatWindowMaximize) {
      await window.electron.chatWindowMaximize();
      setTimeout(async () => {
        try {
          const maximized = await window.electron!.chatWindowIsMaximized();
          setIsMaximized(maximized);
        } catch (error) {
          console.error('Error checking maximized state after toggle:', error);
        }
      }, 150);
    } else {
      setIsMinimized(false);
    }
  };

  const handleClose = () => {
    if (isElectron && window.electron?.chatWindowClose) {
      window.electron.chatWindowClose();
    } else {
      setIsMinimized(false);
      onClose();
    }
  };

  const handleToggleAlwaysOnTop = async () => {
    if (isElectron && window.electron?.setAppSettings) {
      const newValue = !alwaysOnTop;
      setAlwaysOnTop(newValue);
      try {
        await window.electron.setAppSettings({ chatWindowAlwaysOnTop: newValue });
      } catch (error) {
        console.error('Error setting always on top:', error);
        setAlwaysOnTop(!newValue);
      }
    }
  };

  if (isElectron) {
    return (
      <div className="h-screen w-screen overflow-hidden bg-background flex flex-col">
        <div
          data-title-bar
          className="flex items-center justify-between h-10 bg-background/90 backdrop-blur-md border-b border-border/20 select-none shrink-0 px-2"
          style={{ WebkitAppRegion: 'drag' } as React.CSSProperties}
        >
          <div className="flex items-center gap-2" style={{ WebkitAppRegion: 'no-drag' } as React.CSSProperties}>
            <button
              onClick={handleToggleAlwaysOnTop}
              className={cn(
                "flex h-7 w-7 shrink-0 items-center justify-center rounded transition-colors outline-none focus:outline-none focus-visible:outline-none",
                alwaysOnTop 
                  ? "text-primary hover:text-primary/80 hover:bg-primary/10" 
                  : "text-muted-foreground hover:text-foreground hover:bg-secondary/50"
              )}
              title={alwaysOnTop ? "Unpin from top" : "Pin to top"}
            >
              <Pin className={cn("h-4 w-4 shrink-0", alwaysOnTop && "fill-current")} />
            </button>
          </div>
          <div className="flex items-center gap-2 min-w-0 flex-1 pl-2 overflow-hidden">
            <span className="text-sm font-medium text-foreground/70 whitespace-nowrap truncate">Messages</span>
          </div>
          <div className="flex items-center gap-1" style={{ WebkitAppRegion: 'no-drag' } as React.CSSProperties}>
            <button
              onClick={handleMinimize}
              className="flex h-8 w-10 shrink-0 items-center justify-center text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors outline-none focus:outline-none focus-visible:outline-none"
              title="Minimize"
            >
              <Minus className="h-4 w-4 shrink-0" />
            </button>
            <button
              onClick={handleMaximize}
              className="flex h-8 w-10 shrink-0 items-center justify-center text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors outline-none focus:outline-none focus-visible:outline-none"
              title={isMaximized ? "Restore" : "Maximize"}
            >
              {isMaximized ? (
                <div className="relative h-3.5 w-3.5 shrink-0">
                  <div className="absolute bottom-0 left-0 h-2.5 w-2.5 border border-current" />
                  <div className="absolute top-0 right-0 h-2.5 w-2.5 border border-current bg-background" />
                </div>
              ) : (
                <Square className="h-3.5 w-3.5 shrink-0" />
              )}
            </button>
            <button
              onClick={handleClose}
              className="flex h-8 w-10 shrink-0 items-center justify-center text-muted-foreground hover:text-white hover:bg-destructive transition-colors outline-none focus:outline-none focus-visible:outline-none"
              title="Close"
            >
              <X className="h-4 w-4 shrink-0" />
            </button>
          </div>
        </div>
        <div className="flex-1 overflow-hidden">
          <ChatWindow />
        </div>
      </div>
    );
  }

  return (
    <AnimatePresence>
      {isVisible && (
        <motion.div
          ref={windowRef}
          initial={{ opacity: 0, scale: 0.9, y: 20 }}
          animate={{
            opacity: 1,
            scale: 1,
            y: 0,
            height: isMinimized ? 'auto' : 600,
            width: isMinimized ? 300 : 800
          }}
          exit={{ opacity: 0, scale: 0.9, y: 20 }}
          transition={{ duration: 0.25, ease: [0.4, 0, 0.2, 1] }}
          className={cn(
            'fixed z-[100] bg-background border border-border rounded-lg shadow-2xl flex flex-col overflow-hidden',
            isDragging && 'cursor-grabbing select-none'
          )}
          style={{
            left: `${position.x}px`,
            top: `${position.y}px`,
          }}
        >
          <div
            data-title-bar
            className={cn(
              'flex items-center justify-between px-3 py-2 bg-muted/30 select-none shrink-0',
              isMinimized ? 'rounded-lg cursor-grab active:cursor-grabbing' : 'rounded-t-lg border-b border-border cursor-grab active:cursor-grabbing'
            )}
            onMouseDown={handleMouseDown}
          >
            <div className="flex items-center gap-2 min-w-0 flex-1">
              <span className="text-sm font-medium text-foreground/80">Messages</span>
            </div>
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="sm"
                className="h-6 w-6 p-0 hover:bg-secondary"
                onClick={isMinimized ? handleMaximize : handleMinimize}
                title={isMinimized ? 'Maximize' : 'Minimize'}
              >
                <motion.div
                  animate={{ rotate: isMinimized ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                >
                  {isMinimized ? (
                    <Square className="h-3.5 w-3.5" />
                  ) : (
                    <Minus className="h-3.5 w-3.5" />
                  )}
                </motion.div>
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 w-6 p-0 hover:bg-destructive/20 hover:text-destructive"
                onClick={handleClose}
                title="Close"
              >
                <X className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>

          <AnimatePresence initial={false} mode="wait">
            {!isMinimized && (
              <motion.div
                key="chat-content"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.15 }}
                className="flex-1 overflow-hidden rounded-b-lg"
              >
                <ChatWindow />
              </motion.div>
            )}
          </AnimatePresence>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
