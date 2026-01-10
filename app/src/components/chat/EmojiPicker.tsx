import { useState, useMemo, useRef, useEffect } from 'react';
import { X, Search, Smile, Users, TreePine, Coffee, Dumbbell, Plane, Lightbulb, Hash, Clock } from 'lucide-react';
import { cn } from '@/lib/utils';
import { EMOJI_CATEGORIES, DEFAULT_REACTION_EMOJIS, ALL_EMOJIS, getSkinToneVariants, type SkinTone, SKIN_TONE_LABELS } from '@/lib/emojiData';
import { searchEmojis, getRecentlyUsedEmojis, addToRecentlyUsed, applyDefaultSkinTone, setSkinTonePreference } from '@/lib/emojiUtils';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';

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
  const [skinTonePopoverOpen, setSkinTonePopoverOpen] = useState<string | null>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  
  useEffect(() => {
    setRecentEmojis(getRecentlyUsedEmojis());
  }, []);

  
  useEffect(() => {
    if (mode === 'insert' || showFullPicker) {
      searchInputRef.current?.focus();
    }
  }, [mode, showFullPicker]);



  const handleEmojiClick = (emoji: string, skinTone?: SkinTone) => {
    // Just insert the emoji - don't change the default preference
    // User must change default in Settings
    addToRecentlyUsed(emoji);
    setRecentEmojis(getRecentlyUsedEmojis());
    onEmojiSelect(emoji);

    // Close popover
    setSkinTonePopoverOpen(null);


    if (mode === 'react') {
      onClose();
    }
  };



  const displayEmojis = useMemo(() => {
    let emojiList: Array<{ emoji: string; supportsSkinTone: boolean }> = [];


    if (searchQuery.trim()) {
      const searchResults = searchEmojis(searchQuery);
      emojiList = searchResults.map(emoji => {
        const emojiData = ALL_EMOJIS.find(e => e.emoji === emoji);
        return {
          emoji,
          supportsSkinTone: emojiData?.supportsSkinTone || false,
        };
      });
    }

    else if (selectedCategory === 'recent') {
      const recent = recentEmojis.length > 0 ? recentEmojis : DEFAULT_REACTION_EMOJIS;
      emojiList = recent.map(emoji => {
        const emojiData = ALL_EMOJIS.find(e => e.emoji === emoji);
        return {
          emoji,
          supportsSkinTone: emojiData?.supportsSkinTone || false,
        };
      });
    }

    else {
      const category = EMOJI_CATEGORIES.find(cat => cat.id === selectedCategory);
      emojiList = category ? category.emojis.map(e => ({
        emoji: e.emoji,
        supportsSkinTone: e.supportsSkinTone || false,
      })) : [];
    }

    // Apply default skin tone to emojis that support it
    return emojiList.map(item => ({
      ...item,
      displayEmoji: applyDefaultSkinTone(item.emoji, item.supportsSkinTone),
    }));
  }, [searchQuery, selectedCategory, recentEmojis]);



  if (mode === 'react' && !showFullPicker) {
    const quickReactions = DEFAULT_REACTION_EMOJIS.map(emoji => {
      const emojiData = ALL_EMOJIS.find(e => e.emoji === emoji);
      const supportsSkinTone = emojiData?.supportsSkinTone || false;
      return {
        emoji,
        displayEmoji: applyDefaultSkinTone(emoji, supportsSkinTone),
      };
    });

    return (
      <div className={cn('flex items-center gap-1', className)}>
        {quickReactions.map((item) => (
          <button
            key={item.emoji}
            onClick={() => handleEmojiClick(item.displayEmoji)}
            className="text-2xl hover:bg-accent rounded p-1 transition-colors"
            aria-label={`React with ${item.displayEmoji}`}
          >
            {item.displayEmoji}
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
            {displayEmojis.map((item, index) => {
              if (!item.supportsSkinTone) {
                // Regular emoji without skin tone support
                return (
                  <button
                    key={`${item.emoji}-${index}`}
                    onClick={() => handleEmojiClick(item.displayEmoji)}
                    className="text-2xl hover:bg-accent rounded p-1 transition-colors aspect-square flex items-center justify-center"
                    aria-label={`Select ${item.displayEmoji}`}
                  >
                    {item.displayEmoji}
                  </button>
                );
              }

              // Emoji with skin tone support - show popover
              const variants = getSkinToneVariants(item.emoji);
              const skinToneKeys: SkinTone[] = ['default', 'light', 'mediumLight', 'medium', 'mediumDark', 'dark'];

              return (
                <Popover
                  key={`${item.emoji}-${index}`}
                  open={skinTonePopoverOpen === `${item.emoji}-${index}`}
                  onOpenChange={(open) => setSkinTonePopoverOpen(open ? `${item.emoji}-${index}` : null)}
                >
                  <PopoverTrigger asChild>
                    <button
                      className="text-2xl hover:bg-accent rounded p-1 transition-colors aspect-square flex items-center justify-center relative group"
                      aria-label={`Select ${item.displayEmoji} or choose skin tone`}
                    >
                      {item.displayEmoji}
                      <div className="absolute bottom-0 right-0 w-1.5 h-1.5 bg-primary/60 rounded-full opacity-0 group-hover:opacity-100 transition-opacity" />
                    </button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-2" align="center">
                    <div className="flex gap-1">
                      {variants.map((variant, variantIndex) => (
                        <button
                          key={variantIndex}
                          onClick={() => handleEmojiClick(variant, skinToneKeys[variantIndex])}
                          className="text-2xl hover:bg-accent rounded p-1 transition-colors w-10 h-10 flex items-center justify-center"
                          aria-label={`Select ${SKIN_TONE_LABELS[skinToneKeys[variantIndex]]}`}
                          title={SKIN_TONE_LABELS[skinToneKeys[variantIndex]]}
                        >
                          {variant}
                        </button>
                      ))}
                    </div>
                  </PopoverContent>
                </Popover>
              );
            })}
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
