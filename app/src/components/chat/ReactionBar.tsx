import { useState, useEffect } from 'react';
import { cn } from '@/lib/utils';
import { useAuth } from '@/contexts/AuthContext';
import { useAddReaction } from '@/hooks/useAddReaction';
import { useRemoveReaction } from '@/hooks/useRemoveReaction';
import type { ReactionCount } from '@/hooks/useMessageReactions';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { motion, AnimatePresence } from 'framer-motion';
import { getCustomEmojisMap } from '@/lib/emojiUtils';

interface ReactionBarProps {
  messageId: string;
  reactions: ReactionCount[];
  className?: string;
}

const MAX_VISIBLE_REACTIONS = 4; 

export function ReactionBar({ messageId, reactions, className }: ReactionBarProps) {
  const { user } = useAuth();
  const addReaction = useAddReaction();
  const removeReaction = useRemoveReaction();
  const [showAllReactions, setShowAllReactions] = useState(false);
  const [customEmojisMap, setCustomEmojisMap] = useState<Map<string, string>>(new Map());

  useEffect(() => {
    getCustomEmojisMap().then(setCustomEmojisMap);
  }, []);

  if (!reactions || reactions.length === 0) {
    return null;
  }

  
  const sortedReactions = [...reactions].sort((a, b) => b.count - a.count);

  
  const visibleReactions = showAllReactions
    ? sortedReactions
    : sortedReactions.slice(0, MAX_VISIBLE_REACTIONS);

  const hiddenCount = sortedReactions.length - MAX_VISIBLE_REACTIONS;

  const handleReactionClick = async (emoji: string, userIds: string[]) => {
    if (!user) return;

    const hasReacted = userIds.includes(user.id);

    if (hasReacted) {
      
      await removeReaction.mutateAsync({ messageId, emoji });
    } else {
      
      await addReaction.mutateAsync({ messageId, emoji });
    }
  };

  const formatUsernames = (users: ReactionCount['users']) => {
    if (!users || users.length === 0) return '';

    const names = users.map(u => u.display_name || u.username);

    if (names.length === 1) {
      return names[0];
    } else if (names.length === 2) {
      return `${names[0]} and ${names[1]}`;
    } else if (names.length === 3) {
      return `${names[0]}, ${names[1]}, and ${names[2]}`;
    } else {
      return `${names[0]}, ${names[1]}, and ${names.length - 2} other${names.length - 2 > 1 ? 's' : ''}`;
    }
  };

  const isCustomReactionEmoji = (emoji: string): boolean => {
    return /^:([a-z0-9_+-]+):$/i.test(emoji) || /\/custom-emojis\//i.test(emoji);
  };

  const renderReactionEmoji = (emoji: string, size: 'chip' | 'tooltip' = 'chip') => {
    const shortcodeMatch = emoji.match(/^:([a-z0-9_+-]+):$/i);
    let url: string | undefined;

    if (shortcodeMatch) {
      const name = shortcodeMatch[1].toLowerCase();
      url = customEmojisMap.get(name) || `/custom-emojis/${name}.png`;
    } else if (/\/custom-emojis\//i.test(emoji)) {
      url = emoji;
    }

    if (url) {
      const common = {
        src: url,
        alt: emoji,
        style: { imageRendering: 'crisp-edges' as any },
      };

      if (size === 'tooltip') {
        return (
          <img
            {...common}
            className="inline-block align-middle ml-0.5 w-4 h-4 object-contain"
          />
        );
      }

      return (
        <span className="inline-flex items-center justify-center w-4 h-4 shrink-0 align-middle">
          <img
            {...common}
            className="w-[1.2em] h-[1.2em] max-w-4 max-h-4 object-contain block"
          />
        </span>
      );
    }

    if (size === 'tooltip') {
      return <span className="text-sm">{emoji}</span>;
    }

    return (
      <span className="inline-flex items-center justify-center w-4 h-4 text-sm leading-none shrink-0 align-middle">
        {emoji}
      </span>
    );
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: -5 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className={cn('flex items-center gap-1 flex-wrap mt-0', className)}
    >
      <AnimatePresence mode="popLayout">
        {visibleReactions.map((reaction) => {
          const hasReacted = user && reaction.user_ids.includes(user.id);

          return (
            <motion.div
              key={reaction.emoji}
              layout
              initial={{ scale: 0, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0, opacity: 0 }}
              transition={{
                type: "spring",
                stiffness: 500,
                damping: 30,
                duration: 0.2
              }}
              className="inline-flex items-center"
            >
              <TooltipProvider>
                <Tooltip delayDuration={300}>
                  <TooltipTrigger asChild>
                    <motion.button
                      whileTap={{ scale: 0.9 }}
                      whileHover={{ scale: 1.05 }}
                      onClick={() => handleReactionClick(reaction.emoji, reaction.user_ids)}
                      className={cn(
                        'inline-flex items-center justify-center gap-1 px-2 py-0.5 h-6 min-h-6 rounded-full text-xs leading-none border transition-all duration-200 align-middle',
                        hasReacted
                          ? 'bg-primary/10 border-primary text-primary hover:bg-primary/20'
                          : 'bg-accent/50 border-border hover:bg-accent hover:border-border/80'
                      )}
                      disabled={addReaction.isPending || removeReaction.isPending}
                      style={{ verticalAlign: 'middle' }}
                    >
                      {renderReactionEmoji(reaction.emoji, 'chip')}
                      <motion.span
                        key={reaction.count}
                        initial={{ scale: 1.2 }}
                        animate={{ scale: 1 }}
                        className="font-medium leading-none"
                      >
                        {reaction.count}
                      </motion.span>
                    </motion.button>
                  </TooltipTrigger>
                  <TooltipContent side="top" className="max-w-xs">
                    <p className="text-xs flex items-center gap-1">
                      <span>{formatUsernames(reaction.users)} reacted with</span>
                      {renderReactionEmoji(reaction.emoji, 'tooltip')}
                    </p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </motion.div>
          );
        })}

        {!showAllReactions && hiddenCount > 0 && (
          <motion.button
            key="show-more"
            layout
            initial={{ scale: 0, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            exit={{ scale: 0, opacity: 0 }}
            transition={{ type: "spring", stiffness: 500, damping: 30 }}
            whileTap={{ scale: 0.9 }}
            whileHover={{ scale: 1.05 }}
            onClick={() => setShowAllReactions(true)}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium border border-border bg-secondary hover:bg-secondary/80 text-secondary-foreground transition-colors"
          >
            +{hiddenCount}
          </motion.button>
        )}

        {showAllReactions && sortedReactions.length > MAX_VISIBLE_REACTIONS && (
          <motion.button
            key="show-less"
            layout
            initial={{ scale: 0, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            exit={{ scale: 0, opacity: 0 }}
            transition={{ type: "spring", stiffness: 500, damping: 30 }}
            whileTap={{ scale: 0.9 }}
            whileHover={{ scale: 1.05 }}
            onClick={() => setShowAllReactions(false)}
            className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border border-border bg-secondary hover:bg-secondary/80 text-secondary-foreground transition-colors"
          >
            Show less
          </motion.button>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
