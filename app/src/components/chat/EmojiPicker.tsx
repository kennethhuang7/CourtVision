import { useState, useMemo, useRef, useEffect } from 'react';
import { X, Search, Smile, Users, TreePine, Coffee, Dumbbell, Plane, Lightbulb, Hash, Clock, Sparkles } from 'lucide-react';
import { cn } from '@/lib/utils';
import { EMOJI_CATEGORIES, DEFAULT_REACTION_EMOJIS, ALL_EMOJIS, getSkinToneVariants, type SkinTone, SKIN_TONE_LABELS } from '@/lib/emojiData';
import { searchEmojis, getRecentlyUsedEmojis, pruneRecentlyUsedEmojis, addToRecentlyUsed, applyDefaultSkinTone, setSkinTonePreference, loadCustomEmojis, getCustomEmojisMap } from '@/lib/emojiUtils';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';

interface EmojiPickerProps {
  onEmojiSelect: (emoji: string) => void;
  onClose: () => void;
  mode?: 'insert' | 'react'; 
  className?: string;
  userReactions?: string[];
  onEmojiToggle?: (emoji: string, isRemoving: boolean) => void;
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
  custom: Sparkles,
};

export function EmojiPicker({ onEmojiSelect, onClose, mode = 'insert', className, userReactions = [], onEmojiToggle }: EmojiPickerProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<string>('recent');
  const [recentEmojis, setRecentEmojis] = useState<string[]>([]);
  const [showFullPicker, setShowFullPicker] = useState(false);
  const [skinTonePopoverOpen, setSkinTonePopoverOpen] = useState<string | null>(null);
  const [customEmojis, setCustomEmojis] = useState<Array<{ name: string; url: string; emoji: string }>>([]);
  const searchInputRef = useRef<HTMLInputElement>(null);

  
  useEffect(() => {
    pruneRecentlyUsedEmojis().then(setRecentEmojis);
    loadCustomEmojis().then(setCustomEmojis);
  }, []);

  
  useEffect(() => {
    if (mode === 'insert' || showFullPicker) {
      searchInputRef.current?.focus();
    }
  }, [mode, showFullPicker]);



  const handleEmojiClick = (emoji: string, skinTone?: SkinTone) => {
    if (mode === 'react' && onEmojiToggle) {
      const isReacted = userReactions.includes(emoji);
      onEmojiToggle(emoji, isReacted);
      if (!isReacted) {
        addToRecentlyUsed(emoji);
        setRecentEmojis(getRecentlyUsedEmojis());
      }
      setSkinTonePopoverOpen(null);
      onClose();
      return;
    }

    addToRecentlyUsed(emoji);
    setRecentEmojis(getRecentlyUsedEmojis());
    onEmojiSelect(emoji);

    setSkinTonePopoverOpen(null);

    if (mode === 'react') {
      onClose();
    }
  };



  const isCustomEmoji = (emoji: string): boolean => {
    return emoji.startsWith(':') && emoji.endsWith(':') || emoji.startsWith('/custom-emojis/');
  };

  const displayEmojis = useMemo(() => {
    let emojiList: Array<{ emoji: string; supportsSkinTone: boolean; isCustom?: boolean; customUrl?: string; customName?: string }> = [];

    if (searchQuery.trim()) {
      const searchResults = searchEmojis(searchQuery);
      emojiList = searchResults.map(emoji => {
        const emojiData = ALL_EMOJIS.find(e => e.emoji === emoji);
        return {
          emoji,
          supportsSkinTone: emojiData?.supportsSkinTone || false,
        };
      });

      const queryLower = searchQuery.toLowerCase();
      const matchingCustom = customEmojis.filter(ce => 
        ce.name.toLowerCase().includes(queryLower)
      );
      matchingCustom.forEach(ce => {
        emojiList.push({
          emoji: `:${ce.name}:`,
          supportsSkinTone: false,
          isCustom: true,
          customUrl: ce.url,
          customName: ce.name,
        });
      });
    }

    else if (selectedCategory === 'recent') {
      const recent = recentEmojis.length > 0 ? recentEmojis : DEFAULT_REACTION_EMOJIS;
      emojiList = recent.map(emoji => {
        if (isCustomEmoji(emoji)) {
          const name = emoji.replace(/^:/, '').replace(/:$/, '').toLowerCase();
          const customEmoji = customEmojis.find(ce => ce.name.toLowerCase() === name);
          if (customEmoji) {
            return {
              emoji: `:${customEmoji.name}:`,
              supportsSkinTone: false,
              isCustom: true,
              customUrl: customEmoji.url,
              customName: customEmoji.name,
            };
          }
        }
        const emojiData = ALL_EMOJIS.find(e => e.emoji === emoji);
        return {
          emoji,
          supportsSkinTone: emojiData?.supportsSkinTone || false,
        };
      });
    }

    else if (selectedCategory === 'custom') {
      emojiList = customEmojis.map(ce => ({
        emoji: `:${ce.name}:`,
        supportsSkinTone: false,
        isCustom: true,
        customUrl: ce.url,
        customName: ce.name,
      }));
    }

    else {
      const category = EMOJI_CATEGORIES.find(cat => cat.id === selectedCategory);
      emojiList = category ? category.emojis.map(e => ({
        emoji: e.emoji,
        supportsSkinTone: e.supportsSkinTone || false,
      })) : [];
    }

    return emojiList.map(item => {
      if (item.isCustom) {
        return {
          ...item,
          displayEmoji: item.emoji,
        };
      }
      return {
        ...item,
        displayEmoji: applyDefaultSkinTone(item.emoji, item.supportsSkinTone),
      };
    });
  }, [searchQuery, selectedCategory, recentEmojis, customEmojis]);



  if (mode === 'react' && !showFullPicker) {
    const recentForQuick = recentEmojis.slice(0, 6).filter(e => !isCustomEmoji(e));
    const quickEmojis = recentForQuick.length > 0 ? recentForQuick : DEFAULT_REACTION_EMOJIS.slice(0, 6);
    
    type QuickReaction = {
      emoji: string;
      displayEmoji: string;
      isCustom: boolean;
      customUrl?: string;
      customName?: string;
    };

    const quickReactions: QuickReaction[] = quickEmojis.map(emoji => {
      const emojiData = ALL_EMOJIS.find(e => e.emoji === emoji);
      const supportsSkinTone = emojiData?.supportsSkinTone || false;
      return {
        emoji,
        displayEmoji: applyDefaultSkinTone(emoji, supportsSkinTone),
        isCustom: false,
      };
    });

    const recentCustom = recentEmojis.filter(e => isCustomEmoji(e)).slice(0, 2);
    recentCustom.forEach(emoji => {
      const name = emoji.replace(/^:/, '').replace(/:$/, '').toLowerCase();
      const customEmoji = customEmojis.find(ce => ce.name.toLowerCase() === name);
      if (customEmoji) {
        quickReactions.push({
          emoji: `:${customEmoji.name}:`,
          displayEmoji: `:${customEmoji.name}:`,
          isCustom: true,
          customUrl: customEmoji.url,
          customName: customEmoji.name,
        });
      }
    });

    return (
      <div className={cn('flex items-center gap-1', className)}>
        {quickReactions.map((item) => {
          const isReacted = userReactions.includes(item.emoji);
          if (item.isCustom && item.customUrl) {
            return (
              <button
                key={item.emoji}
                onClick={() => handleEmojiClick(item.displayEmoji)}
                className={cn(
                  "hover:bg-muted rounded p-1 transition-colors",
                  isReacted && "bg-muted/80 ring-2 ring-ring"
                )}
                aria-label={`React with ${item.customName || item.emoji}`}
              >
                <img
                  src={item.customUrl}
                  alt={item.customName || item.emoji}
                  className="w-8 h-8 object-contain"
                  style={{ imageRendering: 'crisp-edges' as any }}
                />
              </button>
            );
          }
          return (
            <button
              key={item.emoji}
              onClick={() => handleEmojiClick(item.displayEmoji)}
              className={cn(
                "text-2xl hover:bg-muted rounded p-1 transition-colors",
                isReacted && "bg-muted/80 ring-2 ring-ring"
              )}
              aria-label={`React with ${item.displayEmoji}`}
            >
              {item.displayEmoji}
            </button>
          );
        })}
        <div className="w-px h-6 bg-border mx-1" />
        <button
          onClick={() => setShowFullPicker(true)}
          className="text-xs text-muted-foreground hover:text-foreground px-2 py-1 hover:bg-muted rounded transition-colors"
        >
          View More
        </button>
      </div>
    );
  }

  return (
    <div className={cn('bg-popover border border-border rounded-lg shadow-lg w-[400px] max-w-[100vw]', className)}>
      <div className="p-3 border-b border-border">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            ref={searchInputRef}
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search emojis..."
            className="w-full pl-9 pr-9 py-2 bg-background border border-border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-ring"
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
        <div className="flex flex-wrap items-center gap-1 px-2 py-2 border-b border-border">
          <button
            onClick={() => setSelectedCategory('recent')}
            className={cn(
              'p-2 rounded hover:bg-muted transition-colors flex-shrink-0',
              selectedCategory === 'recent' && 'bg-muted'
            )}
            aria-label="Recently Used"
          >
            <Clock className="h-4 w-4" />
          </button>

          {customEmojis.length > 0 && (
            <button
              onClick={() => setSelectedCategory('custom')}
              className={cn(
                'p-2 rounded hover:bg-muted transition-colors flex-shrink-0',
                selectedCategory === 'custom' && 'bg-muted'
              )}
              aria-label="Custom Emojis"
            >
              <Sparkles className="h-4 w-4" />
            </button>
          )}

          {EMOJI_CATEGORIES.map(category => {
            const Icon = CATEGORY_ICONS[category.id] || Smile;
            return (
              <button
                key={category.id}
                onClick={() => setSelectedCategory(category.id)}
                className={cn(
                  'p-2 rounded hover:bg-muted transition-colors flex-shrink-0',
                  selectedCategory === category.id && 'bg-muted'
                )}
                aria-label={category.name}
              >
                <Icon className="h-4 w-4" />
              </button>
            );
          })}
        </div>
      )}

      <div className="p-2 max-h-[320px] overflow-y-auto">
        {displayEmojis.length === 0 ? (
          <div className="text-center text-muted-foreground py-8 text-sm">
            {searchQuery ? 'No emojis found' : 'No recently used emojis'}
          </div>
        ) : (
          <div className="grid grid-cols-8 gap-1">
            {displayEmojis.map((item, index) => {
              if (item.isCustom && item.customUrl) {
                const isReacted = userReactions.includes(item.emoji);
                return (
                  <button
                    key={`${item.emoji}-${index}`}
                    onClick={() => handleEmojiClick(item.displayEmoji)}
                    className={cn(
                      "hover:bg-muted rounded p-1 transition-colors aspect-square flex items-center justify-center",
                      isReacted && "bg-muted/80 ring-2 ring-ring"
                    )}
                    aria-label={`Select ${item.customName || item.emoji}`}
                  >
                    <img
                      src={item.customUrl}
                      alt={item.customName || item.emoji}
                      className="w-[2em] h-[2em] object-contain"
                      style={{ imageRendering: 'crisp-edges' as any }}
                    />
                  </button>
                );
              }

              if (!item.supportsSkinTone) {
                const isReacted = userReactions.includes(item.emoji);
                return (
                  <button
                    key={`${item.emoji}-${index}`}
                    onClick={() => handleEmojiClick(item.displayEmoji)}
                    className={cn(
                      "text-2xl hover:bg-muted rounded p-1 transition-colors aspect-square flex items-center justify-center",
                      isReacted && "bg-muted/80 ring-2 ring-ring"
                    )}
                    aria-label={`Select ${item.displayEmoji}`}
                  >
                    {item.displayEmoji}
                  </button>
                );
              }

              const variants = getSkinToneVariants(item.emoji);
              const skinToneKeys: SkinTone[] = ['default', 'light', 'mediumLight', 'medium', 'mediumDark', 'dark'];
              const isReacted = variants.some(v => userReactions.includes(v));

              return (
                <Popover
                  key={`${item.emoji}-${index}`}
                  open={skinTonePopoverOpen === `${item.emoji}-${index}`}
                  onOpenChange={(open) => setSkinTonePopoverOpen(open ? `${item.emoji}-${index}` : null)}
                >
                  <PopoverTrigger asChild>
                    <button
                    className={cn(
                      "text-2xl hover:bg-muted rounded p-1 transition-colors aspect-square flex items-center justify-center relative group",
                      isReacted && "bg-muted/80 ring-2 ring-ring"
                    )}
                      aria-label={`Select ${item.displayEmoji} or choose skin tone`}
                    >
                      {item.displayEmoji}
                      <div className="absolute bottom-0 right-0 w-1.5 h-1.5 bg-primary/60 rounded-full opacity-0 group-hover:opacity-100 transition-opacity" />
                    </button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-2" align="center">
                    <div className="flex gap-1">
                      {variants.map((variant, variantIndex) => {
                        const variantReacted = userReactions.includes(variant);
                        return (
                          <button
                            key={variantIndex}
                            onClick={() => handleEmojiClick(variant, skinToneKeys[variantIndex])}
                            className={cn(
                              "text-2xl hover:bg-muted rounded p-1 transition-colors w-10 h-10 flex items-center justify-center",
                              variantReacted && "bg-muted/80 ring-2 ring-ring"
                            )}
                            aria-label={`Select ${SKIN_TONE_LABELS[skinToneKeys[variantIndex]]}`}
                            title={SKIN_TONE_LABELS[skinToneKeys[variantIndex]]}
                          >
                            {variant}
                          </button>
                        );
                      })}
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
