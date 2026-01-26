import { X, CheckCircle2, XCircle, Clock, ArrowUp, ArrowDown } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import type { UserPick } from '@/hooks/useUserPicks';

const statLabels: Record<string, string> = {
  points: 'Points',
  rebounds: 'Rebounds',
  assists: 'Assists',
  steals: 'Steals',
  blocks: 'Blocks',
  turnovers: 'Turnovers',
  threePointersMade: '3-Pointers',
};

function getPlayerPhotoUrl(playerId: number): string {
  return `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${playerId}.png`;
}

interface PicksModalProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  subtitle?: string;
  picks: UserPick[];
  accentColor?: 'green' | 'red' | 'purple' | 'blue' | 'yellow' | 'orange' | 'gray';
}

export function PicksModal({
  isOpen,
  onClose,
  title,
  subtitle,
  picks,
  accentColor = 'purple',
}: PicksModalProps) {
  const colorClasses = {
    green: {
      bg: 'from-green-500/20 via-green-500/10 to-transparent',
      border: 'border-green-500/30',
      text: 'text-green-500',
      glow: 'shadow-green-500/20',
    },
    red: {
      bg: 'from-red-500/20 via-red-500/10 to-transparent',
      border: 'border-red-500/30',
      text: 'text-red-500',
      glow: 'shadow-red-500/20',
    },
    purple: {
      bg: 'from-purple-500/20 via-purple-500/10 to-transparent',
      border: 'border-purple-500/30',
      text: 'text-purple-500',
      glow: 'shadow-purple-500/20',
    },
    blue: {
      bg: 'from-blue-500/20 via-blue-500/10 to-transparent',
      border: 'border-blue-500/30',
      text: 'text-blue-500',
      glow: 'shadow-blue-500/20',
    },
    yellow: {
      bg: 'from-yellow-500/20 via-yellow-500/10 to-transparent',
      border: 'border-yellow-500/30',
      text: 'text-yellow-500',
      glow: 'shadow-yellow-500/20',
    },
    orange: {
      bg: 'from-orange-500/20 via-orange-500/10 to-transparent',
      border: 'border-orange-500/30',
      text: 'text-orange-500',
      glow: 'shadow-orange-500/20',
    },
    gray: {
      bg: 'from-gray-400/20 via-gray-400/10 to-transparent',
      border: 'border-gray-400/30',
      text: 'text-gray-400',
      glow: 'shadow-gray-400/20',
    },
  };

  const colors = colorClasses[accentColor];

  const wonPicks = picks.filter(p => p.result === 'win').length;
  const lostPicks = picks.filter(p => p.result === 'loss').length;
  const pendingPicks = picks.filter(p => p.result === 'pending').length;
  const settledPicks = wonPicks + lostPicks;
  const winRate = settledPicks > 0 ? (wonPicks / settledPicks) * 100 : 0;

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={onClose}
            className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm"
          />

          {/* Modal Container - centers the modal */}
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none">
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              transition={{ type: 'spring', damping: 25, stiffness: 300 }}
              onClick={(e) => e.stopPropagation()}
              className={cn(
                "relative flex flex-col bg-card border rounded-2xl shadow-2xl overflow-hidden w-full max-w-2xl max-h-[85vh] pointer-events-auto",
                colors.border,
                colors.glow
              )}>
              {/* Header gradient */}
              <div className={cn(
                "absolute top-0 left-0 right-0 h-32 bg-gradient-to-b pointer-events-none",
                colors.bg
              )} />

              {/* Header */}
              <div className="relative density-padding pb-4">
                <div className="flex items-start justify-between">
                  <div>
                    <motion.h2
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: 0.1 }}
                      className="text-2xl font-bold text-foreground"
                    >
                      {title}
                    </motion.h2>
                    {subtitle && (
                      <motion.p
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: 0.15 }}
                        className="text-muted-foreground mt-1"
                      >
                        {subtitle}
                      </motion.p>
                    )}
                  </div>
                  <motion.button
                    initial={{ opacity: 0, scale: 0.8 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ delay: 0.1 }}
                    onClick={onClose}
                    className="p-2 rounded-xl hover:bg-muted/50 transition-colors"
                  >
                    <X className="h-5 w-5 text-muted-foreground" />
                  </motion.button>
                </div>

                {/* Stats summary */}
                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.2 }}
                  className="flex flex-wrap items-center gap-x-4 gap-y-2 mt-4 text-sm"
                >
                  <div className="flex items-center gap-1.5">
                    <span className="font-semibold text-foreground">{picks.length}</span>
                    <span className="text-muted-foreground">picks</span>
                  </div>
                  {settledPicks > 0 && (
                    <>
                      <div className="w-px h-4 bg-border" />
                      <div className="flex items-center gap-1.5">
                        <span className={cn(
                          "font-semibold",
                          winRate >= 50 ? "text-green-500" : "text-red-500"
                        )}>
                          {winRate.toFixed(0)}%
                        </span>
                        <span className="text-muted-foreground">win rate</span>
                      </div>
                      <div className="w-px h-4 bg-border" />
                      <div className="flex items-center gap-1.5">
                        <span className="text-green-500 font-medium">{wonPicks}W</span>
                        <span className="text-muted-foreground">-</span>
                        <span className="text-red-500 font-medium">{lostPicks}L</span>
                      </div>
                    </>
                  )}
                  {pendingPicks > 0 && (
                    <>
                      <div className="w-px h-4 bg-border" />
                      <div className="flex items-center gap-1.5 text-yellow-500">
                        <Clock className="h-3.5 w-3.5" />
                        <span className="font-medium">{pendingPicks}</span>
                        <span className="text-muted-foreground">pending</span>
                      </div>
                    </>
                  )}
                </motion.div>
              </div>

              {/* Picks list */}
              <div className="flex-1 overflow-y-auto density-padding pt-0">
                {picks.length === 0 ? (
                  <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="flex flex-col items-center justify-center py-12 text-center"
                  >
                    <div className="w-16 h-16 rounded-full bg-muted/50 flex items-center justify-center mb-4">
                      <Clock className="h-8 w-8 text-muted-foreground/50" />
                    </div>
                    <p className="text-muted-foreground">No picks found</p>
                  </motion.div>
                ) : (
                  <div className="space-y-2">
                    {picks.map((pick, index) => (
                      <motion.div
                        key={pick.id}
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: 0.1 + index * 0.03 }}
                        className={cn(
                          "group relative flex items-center gap-3 density-padding rounded-xl border bg-card/50 overflow-hidden transition-all duration-300",
                          pick.result === 'win' && "border-green-500/20 hover:border-green-500/40",
                          pick.result === 'loss' && "border-red-500/20 hover:border-red-500/40",
                          pick.result === 'pending' && "border-yellow-500/20 hover:border-yellow-500/40"
                        )}
                      >
                        {/* Subtle gradient background */}
                        <div className={cn(
                          "absolute inset-0 opacity-30",
                          pick.result === 'win' && "bg-gradient-to-r from-green-500/10 via-transparent to-transparent",
                          pick.result === 'loss' && "bg-gradient-to-r from-red-500/10 via-transparent to-transparent",
                          pick.result === 'pending' && "bg-gradient-to-r from-yellow-500/10 via-transparent to-transparent"
                        )} />

                        {/* Shine effect */}
                        <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none">
                          <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
                        </div>

                        {/* Player photo */}
                        <div className="relative flex-shrink-0">
                          <div className={cn(
                            "w-10 h-10 sm:w-12 sm:h-12 rounded-full overflow-hidden ring-2 transition-all duration-300",
                            pick.result === 'win' && "ring-green-500/40 group-hover:ring-green-500/60",
                            pick.result === 'loss' && "ring-red-500/40 group-hover:ring-red-500/60",
                            pick.result === 'pending' && "ring-yellow-500/40 group-hover:ring-yellow-500/60"
                          )}>
                            <img
                              src={getPlayerPhotoUrl(pick.player_id)}
                              alt={pick.player?.full_name || 'Player'}
                              className="w-full h-full object-cover object-top bg-muted"
                              onError={(e) => {
                                e.currentTarget.src = '/player-placeholder.png';
                              }}
                            />
                          </div>
                        </div>

                        {/* Pick info */}
                        <div className="relative flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <p className="font-semibold text-foreground truncate">
                              {pick.player?.full_name || 'Unknown Player'}
                            </p>
                            {pick.player?.team_abbr && (
                              <span className="text-xs text-muted-foreground px-1.5 py-0.5 rounded bg-muted/50">
                                {pick.player.team_abbr}
                              </span>
                            )}
                          </div>
                          <div className="flex items-center gap-2 text-sm">
                            <span className={cn(
                              "inline-flex items-center gap-1 font-medium",
                              pick.over_under === 'over' ? "text-green-500" : "text-red-500"
                            )}>
                              {pick.over_under === 'over' ? (
                                <ArrowUp className="h-3.5 w-3.5" />
                              ) : (
                                <ArrowDown className="h-3.5 w-3.5" />
                              )}
                              {pick.over_under.charAt(0).toUpperCase() + pick.over_under.slice(1)}
                            </span>
                            <span className="text-muted-foreground">
                              {pick.line_value} {statLabels[pick.stat_name] || pick.stat_name}
                            </span>
                          </div>
                          <p className="text-xs text-muted-foreground mt-1">
                            {format(new Date(pick.created_at), 'MMM d, yyyy')}
                            {pick.actual_stat !== null && pick.actual_stat !== undefined && (
                              <span className="ml-2">
                                · Actual: <span className="font-medium text-foreground">{pick.actual_stat}</span>
                              </span>
                            )}
                          </p>
                        </div>

                        {/* Result indicator */}
                        <div className="relative flex-shrink-0">
                          {pick.result === 'win' && (
                            <div className="flex items-center gap-1.5 px-2 sm:px-3 py-1.5 rounded-lg bg-green-500/20 text-green-500">
                              <CheckCircle2 className="h-4 w-4" />
                              <span className="font-semibold text-sm hidden sm:inline">Win</span>
                            </div>
                          )}
                          {pick.result === 'loss' && (
                            <div className="flex items-center gap-1.5 px-2 sm:px-3 py-1.5 rounded-lg bg-red-500/20 text-red-500">
                              <XCircle className="h-4 w-4" />
                              <span className="font-semibold text-sm hidden sm:inline">Loss</span>
                            </div>
                          )}
                          {pick.result === 'pending' && (
                            <div className="flex items-center gap-1.5 px-2 sm:px-3 py-1.5 rounded-lg bg-yellow-500/20 text-yellow-500">
                              <Clock className="h-4 w-4" />
                              <span className="font-semibold text-sm hidden sm:inline">Pending</span>
                            </div>
                          )}
                        </div>
                      </motion.div>
                    ))}
                  </div>
                )}
              </div>
            </motion.div>
          </div>
        </>
      )}
    </AnimatePresence>
  );
}
