import { useState, useEffect, useRef, useMemo, useImperativeHandle, forwardRef } from 'react';
import { ALL_EMOJIS } from '@/lib/emojiData';
import { applyDefaultSkinTone, loadCustomEmojis } from '@/lib/emojiUtils';
import { cn } from '@/lib/utils';

interface EmojiAutocompleteProps {
  text: string;
  cursorPosition: number;
  onSelect: (emoji: string, startPos: number, endPos: number) => void;
  onClose: () => void;
}

export interface EmojiAutocompleteHandle {
  handleKeyDown: (e: React.KeyboardEvent) => boolean;
}

export const EmojiAutocomplete = forwardRef<EmojiAutocompleteHandle, EmojiAutocompleteProps>(
  ({ text, cursorPosition, onSelect, onClose }, ref) => {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [customEmojis, setCustomEmojis] = useState<Array<{ name: string; url: string; emoji: string }>>([]);
  const containerRef = useRef<HTMLDivElement>(null);
  const itemRefs = useRef<(HTMLButtonElement | null)[]>([]);

  useEffect(() => {
    loadCustomEmojis().then(setCustomEmojis);
  }, []);

  const match = useMemo(() => {
    const beforeCursor = text.substring(0, cursorPosition);
    const colonIndex = beforeCursor.lastIndexOf(':');
    
    if (colonIndex === -1) return null;
    
    const afterColon = beforeCursor.substring(colonIndex + 1);
    if (afterColon.includes(' ') || afterColon.includes('\n') || afterColon.includes(':')) return null;
    
    const completePattern = /:([a-z0-9_+-]+):$/;
    if (completePattern.test(beforeCursor)) return null;
    
    const query = afterColon.toLowerCase();
    
    const unicodeMatches = ALL_EMOJIS
      .filter(item => item.name.toLowerCase().startsWith(query))
      .map(item => ({
        emoji: item.emoji,
        displayEmoji: applyDefaultSkinTone(item.emoji, item.supportsSkinTone || false),
        name: item.name,
        startPos: colonIndex,
        endPos: cursorPosition,
        isCustom: false,
      }));
    
    const customMatches = customEmojis
      .filter(item => item.name.toLowerCase().startsWith(query))
      .map(item => ({
        emoji: `:${item.name}:`,
        displayEmoji: item.url,
        name: item.name,
        startPos: colonIndex,
        endPos: cursorPosition,
        isCustom: true,
      }));
    
    const matches = [...unicodeMatches, ...customMatches].slice(0, 20);
    
    if (matches.length === 0) return null;
    
    return {
      query,
      matches,
      startPos: colonIndex,
      endPos: cursorPosition,
    };
  }, [text, cursorPosition, customEmojis]);

  useEffect(() => {
    if (!match) {
      onClose();
      return;
    }
    setSelectedIndex(0);
  }, [match, onClose]);

  useEffect(() => {
    if (match && itemRefs.current[selectedIndex] && containerRef.current) {
      const selectedElement = itemRefs.current[selectedIndex];
      const scrollContainer = containerRef.current.querySelector('.emoji-list-container') as HTMLElement;
      
      if (scrollContainer && selectedElement) {
        const containerRect = scrollContainer.getBoundingClientRect();
        const elementRect = selectedElement.getBoundingClientRect();
        
        if (elementRect.top < containerRect.top) {
          scrollContainer.scrollTop -= (containerRect.top - elementRect.top);
        } else if (elementRect.bottom > containerRect.bottom) {
          scrollContainer.scrollTop += (elementRect.bottom - containerRect.bottom);
        }
      }
    }
  }, [selectedIndex, match]);

  const handleSelect = (index: number) => {
    const selected = match.matches[index];
    onSelect(selected.emoji, selected.startPos, selected.endPos);
  };

  const handleKeyDown = (e: React.KeyboardEvent): boolean => {
    if (!match) return false;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIndex(prev => Math.min(prev + 1, match.matches.length - 1));
      return true;
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIndex(prev => Math.max(prev - 1, 0));
      return true;
    } else if (e.key === 'Tab' || (e.key === 'Enter' && !e.shiftKey)) {
      e.preventDefault();
      handleSelect(selectedIndex);
      return true;
    } else if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
      return true;
    }
    return false;
  };

  useImperativeHandle(ref, () => ({
    handleKeyDown,
  }));

  if (!match || match.matches.length === 0) return null;

  return (
    <div
      ref={containerRef}
      className="absolute bottom-full left-0 right-0 mb-2 bg-popover border border-border rounded-lg shadow-lg overflow-hidden z-50"
    >
      <div className="p-2 border-b border-border text-xs text-muted-foreground">
        Emoji matching :{match.query}
      </div>
      <div className="h-56 overflow-hidden emoji-list-container">
        {match.matches.map((item, index) => (
          <button
            key={`${item.emoji}-${index}`}
            ref={el => itemRefs.current[index] = el}
            onClick={() => handleSelect(index)}
            className={cn(
              "w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-secondary/50 transition-colors",
              selectedIndex === index && "bg-secondary"
            )}
          >
            {item.isCustom ? (
              <img src={item.displayEmoji} alt={item.name} className="w-6 h-6 object-contain" />
            ) : (
              <span className="text-xl">{item.displayEmoji}</span>
            )}
            <span className="text-sm text-foreground">:{item.name}:</span>
          </button>
        ))}
      </div>
    </div>
  );
});
