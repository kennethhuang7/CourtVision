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
      if (window.electron) {
        return false;
      }
      const stored = localStorage.getItem(CHAT_WINDOW_VISIBILITY_KEY);
      return stored === 'true';
    }
    return false;
  });

  useEffect(() => {
    if (typeof window === 'undefined' || !window.electron) return;

    const checkVisibility = async () => {
      try {
        const visible = await window.electron!.chatWindowIsVisible();
        setIsVisible(visible);
      } catch (error) {
        console.error('Error checking chat window visibility:', error);
      }
    };

    checkVisibility();
    const interval = setInterval(checkVisibility, 500);

    const unsubscribe = window.electron.onChatWindowClosed(() => {
      setIsVisible(false);
    });

    const unsubscribeVisibilityChanged = window.electron.onChatWindowVisibilityChanged?.((visible: boolean) => {
      setIsVisible(visible);
    });

    return () => {
      clearInterval(interval);
      unsubscribe();
      unsubscribeVisibilityChanged?.();
    };
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined' || !window.electron) return;

    const handleVisibilityChange = async () => {
      try {
        const visible = await window.electron!.chatWindowIsVisible();
        setIsVisible(visible);
      } catch (error) {
        console.error('Error checking chat window visibility:', error);
      }
    };

    const unsubscribeMaximize = window.electron.onChatWindowMaximize?.(handleVisibilityChange);
    const unsubscribeUnmaximize = window.electron.onChatWindowUnmaximize?.(handleVisibilityChange);

    return () => {
      unsubscribeMaximize?.();
      unsubscribeUnmaximize?.();
    };
  }, []);

  useEffect(() => {
    if (typeof window !== 'undefined' && !window.electron) {
      localStorage.setItem(CHAT_WINDOW_VISIBILITY_KEY, String(isVisible));
    }
  }, [isVisible]);

  const toggle = useCallback(async () => {
    if (typeof window !== 'undefined' && window.electron?.chatWindowToggle) {
      try {
        await window.electron.chatWindowToggle();
        setTimeout(async () => {
          try {
            const visible = await window.electron!.chatWindowIsVisible();
            setIsVisible(visible);
          } catch (err) {
            console.error('Error checking visibility after toggle:', err);
          }
        }, 200);
      } catch (error) {
        console.error('Error toggling chat window:', error);
      }
    } else {
      setIsVisible(prev => !prev);
    }
  }, []);

  const open = useCallback(async () => {
    if (typeof window !== 'undefined' && window.electron?.chatWindowShow) {
      try {
        await window.electron.chatWindowShow();
        setTimeout(async () => {
          try {
            const visible = await window.electron!.chatWindowIsVisible();
            setIsVisible(visible);
          } catch (err) {
            console.error('Error checking visibility after open:', err);
            setIsVisible(true);
          }
        }, 200);
      } catch (error) {
        console.error('Error opening chat window:', error);
      }
    } else {
      setIsVisible(true);
    }
  }, []);

  const close = useCallback(async () => {
    if (typeof window !== 'undefined' && window.electron?.chatWindowClose) {
      try {
        await window.electron.chatWindowClose();
        setIsVisible(false);
      } catch (error) {
        console.error('Error closing chat window:', error);
      }
    } else {
      setIsVisible(false);
    }
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

