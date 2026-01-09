import { useState, useMemo, useRef, useEffect } from 'react';
import { X, Search, Smile, Users, TreePine, Coffee, Dumbbell, Plane, Lightbulb, Hash, Clock } from 'lucide-react';
import { cn } from '@/lib/utils';
import { EMOJI_CATEGORIES, DEFAULT_REACTION_EMOJIS } from '@/lib/emojiData';
import { searchEmojis, getRecentlyUsedEmojis, addToRecentlyUsed } from '@/lib/emojiUtils';

interface EmojiPickerProps {
  onEmojiSelect: (emoji: string) => void;
  onClose: () => void;
  mode?: 'insert' | 'react'; 
  className?: string;
}

const CATEGORY_ICONS: Record<string, React.ComponentType<{ className?: string }>> = {
  smileys: Smile,
  gestures: Users,
  nature: TreePine,
  food: Coffee,
  activities: Dumbbell,
  travel: Plane,
  objects: Lightbulb,
  symbols: Hash,
};

export function EmojiPicker({ onEmojiSelect, onClose, mode = 'insert', className }: EmojiPickerProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<string>('recent');
  const [recentEmojis, setRecentEmojis] = useState<string[]>([]);
  const [showFullPicker, setShowFullPicker] = useState(false);
  const searchInputRef = useRef<HTMLInputElement>(null);

  
  useEffect(() => {
    setRecentEmojis(getRecentlyUsedEmojis());
  }, []);

  
  useEffect(() => {
    if (mode === 'insert' || showFullPicker) {
      searchInputRef.current?.focus();
    }
  }, [mode, showFullPicker]);

  
  const handleEmojiClick = (emoji: string) => {
    addToRecentlyUsed(emoji);
    setRecentEmojis(getRecentlyUsedEmojis());
    onEmojiSelect(emoji);

    
    if (mode === 'react') {
      onClose();
    }
  };

  
  const displayEmojis = useMemo(() => {
    
    if (searchQuery.trim()) {
      return searchEmojis(searchQuery);
    }

    
    if (selectedCategory === 'recent') {
      return recentEmojis.length > 0 ? recentEmojis : DEFAULT_REACTION_EMOJIS;
    }

    
    const category = EMOJI_CATEGORIES.find(cat => cat.id === selectedCategory);
    return category ? category.emojis.map(e => e.emoji) : [];
  }, [searchQuery, selectedCategory, recentEmojis]);

  
  if (mode === 'react' && !showFullPicker) {
    return (
      <div className={cn('flex items-center gap-1', className)}>
        {DEFAULT_REACTION_EMOJIS.map(emoji => (
          <button
            key={emoji}
            onClick={() => handleEmojiClick(emoji)}
            className="text-2xl hover:bg-accent rounded p-1 transition-colors"
            aria-label={`React with ${emoji}`}
          >
            {emoji}
          </button>
        ))}
        <div className="w-px h-6 bg-border mx-1" />
        <button
          onClick={() => setShowFullPicker(true)}
          className="text-xs text-muted-foreground hover:text-foreground px-2 py-1 hover:bg-accent rounded transition-colors"
        >
          View More
        </button>
      </div>
    );
  }

  return (
    <div className={cn('bg-popover border border-border rounded-lg shadow-lg w-[352px]', className)}>
      <div className="p-3 border-b border-border">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            ref={searchInputRef}
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search emojis..."
            className="w-full pl-9 pr-9 py-2 bg-background border border-border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-primary"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery('')}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>

      {!searchQuery && (
        <div className="flex items-center gap-1 px-2 py-2 border-b border-border overflow-x-auto">
          <button
            onClick={() => setSelectedCategory('recent')}
            className={cn(
              'p-2 rounded hover:bg-accent transition-colors flex-shrink-0',
              selectedCategory === 'recent' && 'bg-accent'
            )}
            aria-label="Recently Used"
          >
            <Clock className="h-4 w-4" />
          </button>

          {EMOJI_CATEGORIES.map(category => {
            const Icon = CATEGORY_ICONS[category.id] || Smile;
            return (
              <button
                key={category.id}
                onClick={() => setSelectedCategory(category.id)}
                className={cn(
                  'p-2 rounded hover:bg-accent transition-colors flex-shrink-0',
                  selectedCategory === category.id && 'bg-accent'
                )}
                aria-label={category.name}
              >
                <Icon className="h-4 w-4" />
              </button>
            );
          })}
        </div>
      )}

      <div className="p-2 max-h-[280px] overflow-y-auto">
        {displayEmojis.length === 0 ? (
          <div className="text-center text-muted-foreground py-8 text-sm">
            {searchQuery ? 'No emojis found' : 'No recently used emojis'}
          </div>
        ) : (
          <div className="grid grid-cols-8 gap-1">
            {displayEmojis.map((emoji, index) => (
              <button
                key={`${emoji}-${index}`}
                onClick={() => handleEmojiClick(emoji)}
                className="text-2xl hover:bg-accent rounded p-1 transition-colors aspect-square flex items-center justify-center"
                aria-label={`Select ${emoji}`}
              >
                {emoji}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="p-2 border-t border-border flex items-center justify-between text-xs text-muted-foreground">
        <span>Click to select emoji</span>
        <button
          onClick={onClose}
          className="hover:text-foreground transition-colors"
        >
          Close
        </button>
      </div>
    </div>
  );
}
