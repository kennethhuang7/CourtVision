import { useState } from 'react';
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

  return (
    <motion.div
      initial={{ opacity: 0, y: -5 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className={cn('flex items-center gap-1 flex-wrap mt-1.5', className)}
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
            >
              <TooltipProvider>
                <Tooltip delayDuration={300}>
                  <TooltipTrigger asChild>
                    <motion.button
                      whileTap={{ scale: 0.9 }}
                      whileHover={{ scale: 1.05 }}
                      onClick={() => handleReactionClick(reaction.emoji, reaction.user_ids)}
                      className={cn(
                        'inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border transition-all duration-200',
                        hasReacted
                          ? 'bg-primary/10 border-primary text-primary hover:bg-primary/20'
                          : 'bg-accent/50 border-border hover:bg-accent hover:border-border/80'
                      )}
                      disabled={addReaction.isPending || removeReaction.isPending}
                    >
                      <span className="text-sm">{reaction.emoji}</span>
                      <motion.span
                        key={reaction.count}
                        initial={{ scale: 1.2 }}
                        animate={{ scale: 1 }}
                        className="font-medium"
                      >
                        {reaction.count}
                      </motion.span>
                    </motion.button>
                  </TooltipTrigger>
                  <TooltipContent side="top" className="max-w-xs">
                    <p className="text-xs">
                      {formatUsernames(reaction.users)} reacted with {reaction.emoji}
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
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border border-border bg-accent/30 hover:bg-accent transition-colors text-muted-foreground hover:text-foreground"
          >
            <span className="font-medium">+{hiddenCount}</span>
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
            className="inline-flex items-center px-2 py-0.5 rounded-full text-xs border border-border bg-accent/30 hover:bg-accent transition-colors text-muted-foreground hover:text-foreground"
          >
            Show less
          </motion.button>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
