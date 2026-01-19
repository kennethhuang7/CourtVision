import { createContext, useContext, useState, useEffect, useMemo, useCallback, ReactNode } from 'react';

const CHAT_WINDOW_VISIBILITY_KEY = 'courtvision-chat-window-visible';

interface ChatWindowContextType {
  isVisible: boolean;
  toggle: () => void;
  open: () => void;
  close: () => void;
}

const ChatWindowContext = createContext<ChatWindowContextType | undefined>(undefined);

export function ChatWindowProvider({ children }: { children: ReactNode }) {
  const [isVisible, setIsVisible] = useState(() => {
    
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem(CHAT_WINDOW_VISIBILITY_KEY);
      return stored === 'true';
    }
    return false;
  });

  useEffect(() => {
    
    if (typeof window !== 'undefined') {
      localStorage.setItem(CHAT_WINDOW_VISIBILITY_KEY, String(isVisible));
    }
  }, [isVisible]);

  const toggle = useCallback(() => {
    setIsVisible(prev => !prev);
  }, []);

  const open = useCallback(() => {
    setIsVisible(true);
  }, []);

  const close = useCallback(() => {
    setIsVisible(false);
  }, []);

  
  const value = useMemo(
    () => ({ isVisible, toggle, open, close }),
    [isVisible, toggle, open, close]
  );

  return (
    <ChatWindowContext.Provider value={value}>
      {children}
    </ChatWindowContext.Provider>
  );
}

export function useChatWindow() {
  const context = useContext(ChatWindowContext);
  if (context === undefined) {
    throw new Error('useChatWindow must be used within a ChatWindowProvider');
  }
  return context;
}

