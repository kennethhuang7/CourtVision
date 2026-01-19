import { useState, useRef, useEffect } from 'react';
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

  if (!isVisible) {
    return null;
  }

  return (
    <div
      ref={windowRef}
      className={cn(
        'fixed z-[100] bg-background border border-border rounded-lg shadow-2xl flex flex-col',
        'transition-all duration-200 ease-out',
        !isDragging && 'transition-transform',
        isDragging && 'cursor-grabbing select-none',
        isMinimized ? 'h-auto' : 'h-[600px] w-[800px]'
      )}
      style={{
        left: `${position.x}px`,
        top: `${position.y}px`,
      }}
    >
      <div
        data-title-bar
        className={cn(
          'flex items-center justify-between px-3 py-2 bg-muted/30 rounded-t-lg select-none',
          isMinimized ? 'rounded-b-lg cursor-grab active:cursor-grabbing' : 'border-b border-border cursor-grab active:cursor-grabbing'
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
            {isMinimized ? (
              <Maximize2 className="h-3.5 w-3.5" />
            ) : (
              <Minus className="h-3.5 w-3.5" />
            )}
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

      {!isMinimized && (
        <div className="flex-1 overflow-hidden rounded-b-lg">
          <ChatWindow />
        </div>
      )}
    </div>
  );
}

