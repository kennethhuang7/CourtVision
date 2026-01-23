import { useMemo, useState } from 'react';
import { BarChart3, TrendingUp, Calendar, Users, ArrowUp, ArrowDown, Trophy, ChevronDown } from 'lucide-react';
import { useUserPicks, type UserPick } from '@/hooks/useUserPicks';
import { subDays } from 'date-fns';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { PicksModal } from '@/components/analytics/PicksModal';

// Circular progress component with animation
function WinRateRing({
  value,
  size = 160,
  strokeWidth = 14,
  won,
  lost,
  hasData,
}: {
  value: number;
  size?: number;
  strokeWidth?: number;
  won: number;
  lost: number;
  hasData: boolean;
}) {
  const radius = (size - strokeWidth) / 2;
  const circumference = radius * 2 * Math.PI;
  const offset = hasData ? circumference - (value / 100) * circumference : circumference;
  const isGood = hasData && value >= 50;

  return (
    <div className="relative inline-flex items-center justify-center">
      {/* Glow effect */}
      {hasData && (
        <div className={cn(
          "absolute inset-0 rounded-full blur-xl opacity-30",
          isGood ? "bg-green-500" : "bg-red-500"
        )} />
      )}

      <svg width={size} height={size} className="-rotate-90 relative">
        {/* Background circle */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth={strokeWidth}
          className="text-muted/20"
        />
        {/* Progress circle */}
        {hasData && (
          <motion.circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke={isGood ? "url(#greenGradient)" : "url(#redGradient)"}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            initial={{ strokeDashoffset: circumference }}
            animate={{ strokeDashoffset: offset }}
            transition={{ duration: 1.2, ease: "easeOut", delay: 0.2 }}
          />
        )}
        <defs>
          <linearGradient id="greenGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#22c55e" />
            <stop offset="100%" stopColor="#4ade80" />
          </linearGradient>
          <linearGradient id="redGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#ef4444" />
            <stop offset="100%" stopColor="#f87171" />
          </linearGradient>
        </defs>
      </svg>

      {/* Center content */}
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <motion.span
          initial={{ opacity: 0, scale: 0.5 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.5, delay: 0.8 }}
          className={cn(
            "text-3xl font-bold tabular-nums",
            !hasData ? "text-muted-foreground" :
            isGood ? "text-green-500" : "text-red-500"
          )}
        >
          {hasData ? `${value.toFixed(0)}%` : '--%'}
        </motion.span>
        <motion.span
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.5, delay: 1 }}
          className="text-xs text-muted-foreground"
        >
          {hasData ? `${won}W - ${lost}L` : 'No picks'}
        </motion.span>
      </div>
    </div>
  );
}

const statOptions = [
  { value: 'overall', label: 'Overall' },
  { value: 'points', label: 'Points' },
  { value: 'rebounds', label: 'Rebounds' },
  { value: 'assists', label: 'Assists' },
  { value: 'steals', label: 'Steals' },
  { value: 'blocks', label: 'Blocks' },
  { value: 'turnovers', label: 'Turnovers' },
  { value: 'threePointersMade', label: '3-Pointers' },
];

export default function Analytics() {
  const { data: picks = [], isLoading } = useUserPicks();
  const [selectedStat, setSelectedStat] = useState('overall');

  // Modal state
  const [modalOpen, setModalOpen] = useState(false);
  const [modalTitle, setModalTitle] = useState('');
  const [modalSubtitle, setModalSubtitle] = useState<string | undefined>();
  const [modalPicks, setModalPicks] = useState<UserPick[]>([]);
  const [modalColor, setModalColor] = useState<'green' | 'red' | 'purple' | 'blue' | 'yellow' | 'orange' | 'gray'>('purple');

  const openPlayerModal = (playerName: string, rank: number) => {
    const playerPicks = picks.filter(p => p.player?.full_name === playerName);
    setModalTitle(playerName);
    setModalSubtitle(`All picks for ${playerName}`);
    setModalPicks(playerPicks);
    // Gold for #1, silver for #2, bronze for #3, purple for others
    const rankColor = rank === 0 ? 'yellow' : rank === 1 ? 'gray' : rank === 2 ? 'orange' : 'purple';
    setModalColor(rankColor as typeof modalColor);
    setModalOpen(true);
  };

  const openPeriodModal = (period: string, days: number) => {
    const cutoffDate = subDays(new Date(), days);
    const periodPicks = picks.filter(p => new Date(p.created_at) >= cutoffDate);
    setModalTitle(period);
    setModalSubtitle(`Picks from the ${period.toLowerCase()}`);
    setModalPicks(periodPicks);
    setModalColor('purple');
    setModalOpen(true);
  };

  const openOverUnderModal = (type: 'over' | 'under') => {
    const filteredPicks = picks.filter(p => p.over_under === type);
    setModalTitle(`${type.charAt(0).toUpperCase() + type.slice(1)} Picks`);
    setModalSubtitle(`All ${type} picks`);
    setModalPicks(filteredPicks);
    setModalColor(type === 'over' ? 'green' : 'red');
    setModalOpen(true);
  };

  const settledPicks = picks.filter(p => p.result === 'win' || p.result === 'loss');
  const hasSettledPicks = settledPicks.length > 0;

  const overallStats = useMemo(() => {
    if (!hasSettledPicks) {
      return {
        totalPicks: picks.length,
        wonPicks: 0,
        lostPicks: 0,
        pendingPicks: picks.length,
        winRate: 0,
        currentStreak: 0,
        streakType: null,
        bestStreak: 0,
      };
    }
    const totalPicks = picks.length;
    const wonPicks = picks.filter(p => p.result === 'win').length;
    const lostPicks = picks.filter(p => p.result === 'loss').length;
    const pendingPicks = picks.filter(p => p.result === 'pending').length;
    const winRate = totalPicks > 0 ? (wonPicks / (wonPicks + lostPicks)) * 100 : 0;

    const sortedPicks = [...picks]
      .filter(p => p.result !== 'pending')
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

    let currentStreak = 0;
    let streakType: 'win' | 'loss' | null = null;

    for (const pick of sortedPicks) {
      if (streakType === null) {
        streakType = pick.result as 'win' | 'loss';
        currentStreak = 1;
      } else if (pick.result === streakType) {
        currentStreak++;
      } else {
        break;
      }
    }

    let bestStreak = 0;
    let tempStreak = 0;

    for (const pick of sortedPicks.reverse()) {
      if (pick.result === 'win') {
        tempStreak++;
        bestStreak = Math.max(bestStreak, tempStreak);
      } else {
        tempStreak = 0;
      }
    }

    return {
      totalPicks,
      wonPicks,
      lostPicks,
      pendingPicks,
      winRate: isNaN(winRate) ? 0 : winRate,
      currentStreak,
      streakType,
      bestStreak,
    };
  }, [picks, hasSettledPicks]);

  // Filtered stats based on selected stat dropdown
  const filteredStats = useMemo(() => {
    if (selectedStat === 'overall') {
      return {
        won: overallStats.wonPicks,
        lost: overallStats.lostPicks,
        winRate: overallStats.winRate,
        hasData: overallStats.wonPicks + overallStats.lostPicks > 0,
      };
    }

    const filteredPicks = picks.filter(p => p.stat_name === selectedStat);
    const won = filteredPicks.filter(p => p.result === 'win').length;
    const lost = filteredPicks.filter(p => p.result === 'loss').length;
    const settled = won + lost;
    const winRate = settled > 0 ? (won / settled) * 100 : 0;

    return {
      won,
      lost,
      winRate: !isFinite(winRate) || isNaN(winRate) ? 0 : winRate,
      hasData: settled > 0,
    };
  }, [picks, selectedStat, overallStats]);

  const overUnderPerformance = useMemo(() => {
    if (!hasSettledPicks) {
      return [
        { type: 'Over', total: 0, won: 0, lost: 0, winRate: 0 },
        { type: 'Under', total: 0, won: 0, lost: 0, winRate: 0 },
      ];
    }

    const overPicks = picks.filter(p => p.over_under === 'over');
    const underPicks = picks.filter(p => p.over_under === 'under');

    const overWon = overPicks.filter(p => p.result === 'win').length;
    const overLost = overPicks.filter(p => p.result === 'loss').length;
    const underWon = underPicks.filter(p => p.result === 'win').length;
    const underLost = underPicks.filter(p => p.result === 'loss').length;

    const overSettled = overWon + overLost;
    const underSettled = underWon + underLost;
    const overWinRate = overSettled > 0 ? (overWon / overSettled) * 100 : 0;
    const underWinRate = underSettled > 0 ? (underWon / underSettled) * 100 : 0;

    return [
      {
        type: 'Over',
        total: overPicks.length,
        won: overWon,
        lost: overLost,
        winRate: !isFinite(overWinRate) || isNaN(overWinRate) ? 0 : overWinRate,
      },
      {
        type: 'Under',
        total: underPicks.length,
        won: underWon,
        lost: underLost,
        winRate: !isFinite(underWinRate) || isNaN(underWinRate) ? 0 : underWinRate,
      },
    ];
  }, [picks, hasSettledPicks]);

  const playerPerformance = useMemo(() => {
    if (!hasSettledPicks) return [];

    const playerGroups: Record<string, { name: string; playerId: number; total: number; won: number; lost: number }> = {};

    picks.forEach(pick => {
      const playerName = pick.player?.full_name || 'Unknown';
      const playerId = pick.player_id;
      if (!playerGroups[playerName]) {
        playerGroups[playerName] = { name: playerName, playerId, total: 0, won: 0, lost: 0 };
      }
      playerGroups[playerName].total++;
      if (pick.result === 'win') playerGroups[playerName].won++;
      if (pick.result === 'loss') playerGroups[playerName].lost++;
    });

    return Object.values(playerGroups)
      .map(data => {
        const settled = data.won + data.lost;
        const winRate = settled > 0 ? (data.won / settled) * 100 : 0;
        return {
          ...data,
          winRate: !isFinite(winRate) || isNaN(winRate) ? 0 : winRate,
        };
      })
      .filter(player => {
        const settled = player.won + player.lost;
        return settled > 0 && isFinite(player.winRate) && !isNaN(player.winRate);
      })
      .sort((a, b) => {
        if (Math.abs(b.winRate - a.winRate) > 0.1) {
          return b.winRate - a.winRate;
        }
        return b.total - a.total;
      })
      .slice(0, 10);
  }, [picks, hasSettledPicks]);

  const recentForm = useMemo(() => {
    if (!hasSettledPicks) {
      return [
        { period: 'Last 7 Days', total: 0, won: 0, lost: 0, winRate: 0 },
        { period: 'Last 30 Days', total: 0, won: 0, lost: 0, winRate: 0 },
        { period: 'Last 90 Days', total: 0, won: 0, lost: 0, winRate: 0 },
      ];
    }

    const now = new Date();
    const periods = [
      { label: 'Last 7 Days', days: 7 },
      { label: 'Last 30 Days', days: 30 },
      { label: 'Last 90 Days', days: 90 },
    ];

    return periods.map(period => {
      const cutoffDate = subDays(now, period.days);
      const recentPicks = picks.filter(p => new Date(p.created_at) >= cutoffDate);
      const won = recentPicks.filter(p => p.result === 'win').length;
      const lost = recentPicks.filter(p => p.result === 'loss').length;
      const total = recentPicks.length;
      const settled = won + lost;
      const winRate = settled > 0 ? (won / settled) * 100 : 0;

      return {
        period: period.label,
        total,
        won,
        lost,
        winRate: !isFinite(winRate) || isNaN(winRate) ? 0 : winRate,
      };
    });
  }, [picks, hasSettledPicks]);

  // Loading state
  if (isLoading) {
    return (
      <div className="space-y-6 animate-fade-in">
        <div className="min-w-0">
          <div className="h-9 w-48 bg-muted animate-pulse rounded-lg mb-2" />
          <div className="h-5 w-72 bg-muted/60 animate-pulse rounded" />
        </div>

        <div className="rounded-lg border border-border/50 bg-card/30 backdrop-blur-sm density-padding">
          <div className="flex flex-wrap items-center gap-3">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-10 w-36 bg-muted animate-pulse rounded-lg" />
            ))}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-12 density-gap">
          <div className="lg:col-span-4 rounded-xl border border-border bg-card/50 p-6 flex items-center justify-center min-h-[280px]">
            <div className="w-40 h-40 bg-muted/30 animate-pulse rounded-full" />
          </div>
          <div className="lg:col-span-4 rounded-xl border border-border bg-card/50 p-6">
            <div className="h-6 w-32 bg-muted animate-pulse rounded mb-4" />
            <div className="space-y-3">
              {[...Array(2)].map((_, i) => (
                <div key={i} className="h-16 bg-muted/30 animate-pulse rounded-lg" />
              ))}
            </div>
          </div>
          <div className="lg:col-span-4 rounded-xl border border-border bg-card/50 p-6">
            <div className="h-6 w-32 bg-muted animate-pulse rounded mb-4" />
            <div className="space-y-3">
              {[...Array(3)].map((_, i) => (
                <div key={i} className="h-16 bg-muted/30 animate-pulse rounded-lg" />
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Empty state - no picks
  if (picks.length === 0) {
    return (
      <div className="space-y-6 animate-fade-in">
        <div className="min-w-0">
          <h1 className="text-3xl font-bold text-foreground mb-2 leading-tight">Pick Analytics</h1>
          <p className="text-muted-foreground leading-tight">Track your performance and insights</p>
        </div>
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="rounded-xl border border-border bg-card/50 p-12"
        >
          <div className="flex flex-col items-center justify-center text-center max-w-md mx-auto">
            <div className="relative mb-6">
              <div className="absolute inset-0 rounded-full bg-primary/20 blur-2xl animate-pulse" />
              <div className="relative w-24 h-24 rounded-full bg-gradient-to-br from-primary/20 to-primary/5 border border-primary/20 flex items-center justify-center">
                <BarChart3 className="h-12 w-12 text-primary/60" />
              </div>
            </div>
            <h3 className="text-xl font-semibold text-foreground mb-2">No picks yet</h3>
            <p className="text-muted-foreground">
              Start making picks to see your analytics and performance trends
            </p>
          </div>
        </motion.div>
      </div>
    );
  }

  // Pending state - all picks pending
  if (!hasSettledPicks) {
    return (
      <div className="space-y-6 animate-fade-in">
        <div className="min-w-0">
          <h1 className="text-3xl font-bold text-foreground mb-2 leading-tight">Pick Analytics</h1>
          <p className="text-muted-foreground leading-tight">Track your performance and insights</p>
        </div>

        {/* Summary bar for pending */}
        <div className="rounded-lg border border-border/50 bg-card/30 backdrop-blur-sm density-padding">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-primary/10 border border-primary/20">
              <BarChart3 className="h-4 w-4 text-primary" />
              <span className="font-semibold text-foreground">{picks.length}</span>
              <span className="text-muted-foreground">Picks</span>
            </div>
            <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-yellow-500/10 border border-yellow-500/20">
              <Calendar className="h-4 w-4 text-yellow-500" />
              <span className="font-semibold text-yellow-500">{picks.length}</span>
              <span className="text-muted-foreground">Pending</span>
            </div>
          </div>
        </div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="rounded-xl border border-yellow-500/20 bg-card/50 p-12"
        >
          <div className="flex flex-col items-center justify-center text-center max-w-md mx-auto">
            <div className="relative mb-6">
              <div className="absolute inset-0 rounded-full bg-yellow-500/20 blur-2xl animate-pulse" />
              <div className="relative w-24 h-24 rounded-full bg-gradient-to-br from-yellow-500/20 to-yellow-500/5 border border-yellow-500/20 flex items-center justify-center">
                <Calendar className="h-12 w-12 text-yellow-500/60" />
              </div>
            </div>
            <h3 className="text-xl font-semibold text-foreground mb-2">All picks are pending</h3>
            <p className="text-muted-foreground">
              Analytics will appear once your picks are settled
            </p>
          </div>
        </motion.div>
      </div>
    );
  }

  // Main analytics view
  return (
    <div className="space-y-6 animate-fade-in">
      {/* Header */}
      <div className="min-w-0">
        <h1 className="text-3xl font-bold text-foreground mb-2 leading-tight truncate">Pick Analytics</h1>
        <p className="text-muted-foreground leading-tight truncate">Comprehensive insights into your pick performance</p>
      </div>

      {/* Compact Summary Bar */}
      <motion.div
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        className="rounded-lg border border-border/50 bg-card/30 backdrop-blur-sm density-padding"
      >
        <div className="flex flex-wrap items-center gap-x-6 gap-y-3 text-sm">
          {/* Picks Settled */}
          <motion.div
            whileHover={{ scale: 1.02 }}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-gradient-to-r from-primary/15 to-primary/5 border border-primary/20"
          >
            <span className="font-bold text-lg text-primary">{overallStats.wonPicks + overallStats.lostPicks}</span>
            <span className="text-muted-foreground">Picks Settled</span>
          </motion.div>

          <div className="hidden sm:block w-px h-6 bg-border/50" />

          {/* Win Rate */}
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">Win Rate</span>
            <span className={cn(
              "font-bold text-lg",
              overallStats.winRate >= 50 ? "text-green-500" : "text-red-500"
            )}>
              {overallStats.winRate.toFixed(1)}%
            </span>
            <span className="text-xs text-muted-foreground">({overallStats.wonPicks}W - {overallStats.lostPicks}L)</span>
          </div>

          <div className="hidden sm:block w-px h-6 bg-border/50" />

          {/* Current Streak */}
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">Current Streak</span>
            <span className={cn(
              "font-bold",
              overallStats.streakType === 'win' ? "text-green-500" :
              overallStats.streakType === 'loss' ? "text-red-500" : "text-muted-foreground"
            )}>
              {overallStats.currentStreak > 0
                ? `${overallStats.currentStreak} ${overallStats.streakType === 'win' ? 'Wins' : 'Losses'}`
                : '-'}
            </span>
          </div>

          <div className="hidden sm:block w-px h-6 bg-border/50" />

          {/* Best Streak */}
          <motion.div
            whileHover={{ scale: 1.02 }}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-gradient-to-r from-yellow-500/15 to-yellow-500/5 border border-yellow-500/20"
          >
            <Trophy className="h-4 w-4 text-yellow-500" />
            <span className="text-muted-foreground">Best Streak</span>
            <span className="font-bold text-yellow-500">{overallStats.bestStreak} Wins</span>
          </motion.div>

          {/* Pending */}
          {overallStats.pendingPicks > 0 && (
            <>
              <div className="hidden sm:block w-px h-6 bg-border/50" />
              <span className="text-muted-foreground">{overallStats.pendingPicks} pending</span>
            </>
          )}
        </div>
      </motion.div>

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-12 density-gap">
        {/* Win Rate Ring - Featured */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
          className="lg:col-span-4 rounded-xl border border-border bg-gradient-to-br from-card via-card to-primary/5 overflow-hidden"
        >
          <div className="density-padding border-b border-border/50">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <BarChart3 className="h-4 w-4 text-primary" />
                <h2 className="font-semibold text-foreground">Win Rate</h2>
              </div>
              <DropdownMenu>
                <DropdownMenuTrigger className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-muted/50 hover:bg-muted border border-border/50 text-sm font-medium transition-colors">
                  {statOptions.find(s => s.value === selectedStat)?.label || 'Overall'}
                  <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="min-w-[140px]">
                  {statOptions.map((option) => (
                    <DropdownMenuItem
                      key={option.value}
                      onClick={() => setSelectedStat(option.value)}
                      className={cn(
                        "cursor-pointer",
                        selectedStat === option.value && "bg-primary/10 text-primary"
                      )}
                    >
                      {option.label}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
          <div className="density-padding flex flex-col items-center justify-center min-h-[220px]">
            <WinRateRing
              value={filteredStats.winRate}
              won={filteredStats.won}
              lost={filteredStats.lost}
              hasData={filteredStats.hasData}
            />
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 1.2 }}
              className="text-sm text-muted-foreground mt-4 text-center"
            >
              {filteredStats.hasData
                ? `${filteredStats.won + filteredStats.lost} picks settled`
                : `No ${selectedStat === 'overall' ? '' : statOptions.find(s => s.value === selectedStat)?.label.toLowerCase() + ' '}picks yet`}
            </motion.p>
          </div>
        </motion.div>

        {/* Over vs Under */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15 }}
          className="lg:col-span-4 rounded-xl border border-border bg-card/50 overflow-hidden"
        >
          <div className="density-padding border-b border-border/50">
            <div className="flex items-center gap-2">
              <TrendingUp className="h-4 w-4 text-blue-500" />
              <h2 className="font-semibold text-foreground">Over vs Under</h2>
            </div>
          </div>
          <div className="density-padding space-y-3">
            {overUnderPerformance.map((data, index) => {
              const hasData = data.won + data.lost > 0;
              const isGood = hasData && data.winRate >= 50;

              return (
                <motion.div
                  key={data.type}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.2 + index * 0.1 }}
                  whileHover={{ scale: 1.01, x: 4 }}
                  onClick={() => openOverUnderModal(data.type.toLowerCase() as 'over' | 'under')}
                  className={cn(
                    "group relative flex items-center gap-3 density-padding rounded-xl border overflow-hidden transition-all duration-300 cursor-pointer",
                    data.type === 'Over'
                      ? "border-green-500/20 hover:border-green-500/40"
                      : "border-red-500/20 hover:border-red-500/40"
                  )}
                >
                  {/* Animated background gradient */}
                  <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className={cn(
                      "absolute inset-0 opacity-50",
                      data.type === 'Over'
                        ? "bg-gradient-to-r from-green-500/10 via-green-500/5 to-transparent"
                        : "bg-gradient-to-r from-red-500/10 via-red-500/5 to-transparent"
                    )}
                  />

                  {/* Shine effect on hover */}
                  <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none">
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
                  </div>

                  <div className={cn(
                    "relative w-10 h-10 rounded-xl flex items-center justify-center",
                    data.type === 'Over' ? "bg-green-500/20" : "bg-red-500/20"
                  )}>
                    {data.type === 'Over' ? (
                      <ArrowUp className="h-5 w-5 text-green-500" />
                    ) : (
                      <ArrowDown className="h-5 w-5 text-red-500" />
                    )}
                  </div>
                  <div className="relative flex-1 min-w-0">
                    <p className="font-semibold text-foreground">{data.type}</p>
                    <p className="text-xs text-muted-foreground">{data.won}W - {data.lost}L</p>
                  </div>
                  <span className={cn(
                    "relative text-2xl font-bold tabular-nums",
                    !hasData ? 'text-muted-foreground' :
                    isGood ? 'text-green-500' : 'text-red-500'
                  )}>
                    {!hasData ? '-' : `${data.winRate.toFixed(0)}%`}
                  </span>
                </motion.div>
              );
            })}

          </div>
        </motion.div>

        {/* Recent Form */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="lg:col-span-4 rounded-xl border border-border bg-card/50 overflow-hidden"
        >
          <div className="density-padding border-b border-border/50">
            <div className="flex items-center gap-2">
              <Calendar className="h-4 w-4 text-purple-500" />
              <h2 className="font-semibold text-foreground">Recent Form</h2>
            </div>
          </div>
          <div className="density-padding space-y-2">
            {recentForm.map((period, index) => {
              const hasData = period.won + period.lost > 0;
              const isGood = hasData && period.winRate >= 50;

              const daysMap: Record<string, number> = {
                'Last 7 Days': 7,
                'Last 30 Days': 30,
                'Last 90 Days': 90,
              };

              return (
                <motion.div
                  key={period.period}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.25 + index * 0.08 }}
                  whileHover={{ scale: 1.01, x: 4 }}
                  onClick={() => openPeriodModal(period.period, daysMap[period.period])}
                  className={cn(
                    "group relative flex items-center gap-3 density-padding rounded-xl border overflow-hidden transition-all duration-300 cursor-pointer",
                    hasData && isGood ? "border-green-500/20 hover:border-green-500/40" :
                    hasData && !isGood ? "border-red-500/20 hover:border-red-500/40" :
                    "border-border/50 hover:border-border"
                  )}
                >
                  {/* Background fill animation */}
                  {hasData && (
                    <motion.div
                      initial={{ width: 0 }}
                      animate={{ width: `${period.winRate}%` }}
                      transition={{ duration: 0.8, delay: 0.5 + index * 0.1 }}
                      className={cn(
                        "absolute inset-y-0 left-0 opacity-20 rounded-xl",
                        isGood ? "bg-gradient-to-r from-green-500 to-green-500/50" : "bg-gradient-to-r from-red-500 to-red-500/50"
                      )}
                    />
                  )}

                  {/* Shine effect on hover */}
                  <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none">
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
                  </div>

                  <div className="relative flex-1 min-w-0">
                    <p className="font-medium text-foreground">{period.period}</p>
                    <p className="text-xs text-muted-foreground">{period.won}W - {period.lost}L</p>
                  </div>
                  <span className={cn(
                    "relative text-xl font-bold tabular-nums",
                    !hasData ? 'text-muted-foreground' :
                    isGood ? 'text-green-500' : 'text-red-500'
                  )}>
                    {!hasData ? '-' : `${period.winRate.toFixed(0)}%`}
                  </span>
                </motion.div>
              );
            })}
          </div>
        </motion.div>
      </div>

      {/* Top Players Section */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.25 }}
        className="rounded-xl border border-border bg-card/50 overflow-hidden"
      >
        <div className="density-padding border-b border-border/50">
          <div className="flex items-center gap-2">
            <Users className="h-4 w-4 text-orange-500" />
            <h2 className="font-semibold text-foreground">Top Players by Win Rate</h2>
          </div>
        </div>
        {playerPerformance.length === 0 ? (
          <div className="density-padding">
            <div className="py-8 text-center">
              <Users className="h-10 w-10 mx-auto mb-2 text-muted-foreground/30" />
              <p className="text-sm text-muted-foreground">No settled picks yet</p>
            </div>
          </div>
        ) : (
          <div className="density-padding">
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 density-gap">
              <AnimatePresence>
                {playerPerformance.map((player, index) => (
                  <motion.div
                    key={player.name}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.3 + index * 0.04 }}
                    whileHover={{ scale: 1.02, y: -2 }}
                    onClick={() => openPlayerModal(player.name, index)}
                    className={cn(
                      "group relative flex items-center gap-3 density-padding rounded-xl border overflow-hidden transition-all duration-300 cursor-pointer",
                      index === 0 ? "border-yellow-500/30 hover:border-yellow-500/50" :
                      index === 1 ? "border-gray-400/30 hover:border-gray-400/50" :
                      index === 2 ? "border-orange-500/30 hover:border-orange-500/50" :
                      "border-border/50 hover:border-border"
                    )}
                  >
                    {/* Gradient background for top 3 */}
                    {index < 3 && (
                      <div className={cn(
                        "absolute inset-0 opacity-30",
                        index === 0 ? "bg-gradient-to-br from-yellow-500/30 via-transparent to-transparent" :
                        index === 1 ? "bg-gradient-to-br from-gray-400/30 via-transparent to-transparent" :
                        "bg-gradient-to-br from-orange-500/30 via-transparent to-transparent"
                      )} />
                    )}

                    {/* Shine effect on hover for top 3 */}
                    {index < 3 && (
                      <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none">
                        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
                      </div>
                    )}

                    {/* Player Photo with Rank */}
                    <div className="relative flex-shrink-0">
                      <div className={cn(
                        "w-10 h-10 rounded-full overflow-hidden ring-2 transition-all duration-300",
                        index === 0 ? "ring-yellow-500/50 group-hover:ring-yellow-500/70" :
                        index === 1 ? "ring-gray-400/50 group-hover:ring-gray-400/70" :
                        index === 2 ? "ring-orange-500/50 group-hover:ring-orange-500/70" :
                        "ring-border/50 group-hover:ring-border"
                      )}>
                        <img
                          src={`https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${player.playerId}.png`}
                          alt={player.name}
                          className="w-full h-full object-cover object-top bg-muted"
                          onError={(e) => {
                            e.currentTarget.src = '/player-placeholder.png';
                          }}
                        />
                      </div>
                      {/* Rank badge */}
                      <div className={cn(
                        "absolute -bottom-1 -right-1 w-5 h-5 rounded-full flex items-center justify-center text-xs font-bold border-2 border-card",
                        index === 0 ? "bg-yellow-500 text-yellow-950" :
                        index === 1 ? "bg-gray-400 text-gray-950" :
                        index === 2 ? "bg-orange-500 text-orange-950" :
                        "bg-muted text-muted-foreground"
                      )}>
                        {index + 1}
                      </div>
                    </div>

                    {/* Player Info */}
                    <div className="relative flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <p className="font-semibold text-foreground truncate">{player.name}</p>
                        {player.winRate === 100 && player.won >= 2 && (
                          <span className="text-xs px-1.5 py-0.5 rounded bg-green-500/20 text-green-500 font-medium">
                            Perfect
                          </span>
                        )}
                      </div>
                      <p className="text-xs text-muted-foreground">{player.total} picks · {player.won}W - {player.lost}L</p>
                    </div>

                    {/* Win Rate */}
                    <div className="relative flex flex-col items-end gap-1">
                      <span className={cn(
                        "font-bold tabular-nums text-lg",
                        player.winRate >= 50 ? 'text-green-500' : 'text-red-500'
                      )}>
                        {player.winRate.toFixed(0)}%
                      </span>
                      <div className="w-16 h-1.5 bg-muted/30 rounded-full overflow-hidden">
                        <motion.div
                          initial={{ width: 0 }}
                          animate={{ width: `${player.winRate}%` }}
                          transition={{ duration: 0.6, delay: 0.4 + index * 0.04 }}
                          className={cn(
                            "h-full rounded-full",
                            player.winRate >= 50 ? "bg-gradient-to-r from-green-500 to-green-400" : "bg-gradient-to-r from-red-500 to-red-400"
                          )}
                        />
                      </div>
                    </div>
                  </motion.div>
                ))}
              </AnimatePresence>
            </div>
          </div>
        )}
      </motion.div>

      {/* Picks Modal */}
      <PicksModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        title={modalTitle}
        subtitle={modalSubtitle}
        picks={modalPicks}
        accentColor={modalColor}
      />
    </div>
  );
}
