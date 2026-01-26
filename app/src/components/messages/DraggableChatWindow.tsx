import { useState, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Minus, Maximize2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ChatWindow } from './ChatWindow';
import { cn } from '@/lib/utils';

export type ChatWindowState = 'hidden' | 'open' | 'minimized';

interface DraggableChatWindowProps {
  isVisible: boolean;
  onClose: () => void;
}

export function DraggableChatWindow({ isVisible, onClose }: DraggableChatWindowProps) {
  const [isMinimized, setIsMinimized] = useState(false);
  const [position, setPosition] = useState(() => {
    if (typeof window !== 'undefined') {
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
    if (!isVisible) {
      setIsMinimized(false);
    }
  }, [isVisible]);

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
    setIsMinimized(true);
  };

  const handleMaximize = () => {
    setIsMinimized(false);
  };

  const handleClose = () => {
    setIsMinimized(false);
    onClose();
  };

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
                    <Maximize2 className="h-3.5 w-3.5" />
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
