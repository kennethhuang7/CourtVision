import { useMemo } from 'react';
import { BarChart3, TrendingUp, TrendingDown, Target, Calendar, Users, ArrowUp, ArrowDown } from 'lucide-react';
import { useUserPicks } from '@/hooks/useUserPicks';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import { subDays } from 'date-fns';

const statLabels: Record<string, string> = {
  points: 'Points',
  rebounds: 'Rebounds',
  assists: 'Assists',
  steals: 'Steals',
  blocks: 'Blocks',
  turnovers: 'Turnovers',
  threePointersMade: '3-Pointers',
};

export default function Analytics() {
  const { data: picks = [], isLoading } = useUserPicks();

  
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

  
  const statTypePerformance = useMemo(() => {
    if (!hasSettledPicks) return [];

    const statGroups: Record<string, { total: number; won: number; lost: number }> = {};

    picks.forEach(pick => {
      if (!statGroups[pick.stat_name]) {
        statGroups[pick.stat_name] = { total: 0, won: 0, lost: 0 };
      }
      statGroups[pick.stat_name].total++;
      if (pick.result === 'win') statGroups[pick.stat_name].won++;
      if (pick.result === 'loss') statGroups[pick.stat_name].lost++;
    });

    return Object.entries(statGroups)
      .map(([stat, data]) => {
        const settled = data.won + data.lost;
        const winRate = settled > 0 ? (data.won / settled) * 100 : 0;
        return {
          stat: statLabels[stat] || stat,
          total: data.total,
          won: data.won,
          lost: data.lost,
          winRate: !isFinite(winRate) || isNaN(winRate) ? 0 : winRate,
        };
      })
      .filter(stat => {
        const settled = stat.won + stat.lost;
        return settled > 0 && isFinite(stat.winRate) && !isNaN(stat.winRate);
      })
      .sort((a, b) => b.winRate - a.winRate); 
  }, [picks, hasSettledPicks]);

  
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

    const playerGroups: Record<string, { name: string; total: number; won: number; lost: number }> = {};

    picks.forEach(pick => {
      const playerName = pick.player?.full_name || 'Unknown';
      if (!playerGroups[playerName]) {
        playerGroups[playerName] = { name: playerName, total: 0, won: 0, lost: 0 };
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
        { period: '7 Days', total: 0, won: 0, lost: 0, winRate: 0 },
        { period: '30 Days', total: 0, won: 0, lost: 0, winRate: 0 },
        { period: '90 Days', total: 0, won: 0, lost: 0, winRate: 0 },
      ];
    }

    const now = new Date();
    const periods = [
      { label: '7 Days', days: 7 },
      { label: '30 Days', days: 30 },
      { label: '90 Days', days: 90 },
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

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-muted-foreground">Loading analytics...</div>
      </div>
    );
  }

  if (picks.length === 0) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold text-foreground mb-2">Pick Analytics</h1>
          <p className="text-muted-foreground">Track your performance and insights</p>
        </div>
        <div className="stat-card flex flex-col items-center justify-center py-12">
          <BarChart3 className="h-16 w-16 text-muted-foreground mb-4" />
          <h3 className="text-xl font-semibold text-foreground mb-2">No picks yet</h3>
          <p className="text-muted-foreground text-center">
            Start making picks to see your analytics and performance trends
          </p>
        </div>
      </div>
    );
  }

  if (!hasSettledPicks) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold text-foreground mb-2">Pick Analytics</h1>
          <p className="text-muted-foreground">Track your performance and insights</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="stat-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="data-label">Total Picks</p>
                <p className="data-value">{picks.length}</p>
              </div>
              <Target className="h-8 w-8 text-primary opacity-50" />
            </div>
          </div>

          <div className="stat-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="data-label">Pending</p>
                <p className="data-value text-yellow-500">{picks.length}</p>
              </div>
              <Calendar className="h-8 w-8 text-yellow-500 opacity-50" />
            </div>
          </div>
        </div>

        <div className="stat-card flex flex-col items-center justify-center py-12">
          <Calendar className="h-16 w-16 text-yellow-500 mb-4" />
          <h3 className="text-xl font-semibold text-foreground mb-2">All picks are pending</h3>
          <p className="text-muted-foreground text-center">
            Analytics will appear once your picks are settled
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="min-w-0">
        <h1 className="text-3xl font-bold text-foreground mb-2 leading-tight truncate">Pick Analytics</h1>
        <p className="text-muted-foreground leading-tight truncate">Comprehensive insights into your pick performance</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="stat-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="data-label">Total Picks</p>
              <p className="data-value">{overallStats.totalPicks}</p>
            </div>
            <Target className="h-8 w-8 text-primary opacity-50" />
          </div>
        </div>

        <div className="stat-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="data-label">Win Rate</p>
              <p className="data-value">{overallStats.winRate.toFixed(1)}%</p>
              <p className="text-xs text-muted-foreground mt-1">
                {overallStats.wonPicks}W - {overallStats.lostPicks}L
              </p>
            </div>
            <TrendingUp className="h-8 w-8 text-green-500 opacity-50" />
          </div>
        </div>

        <div className="stat-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="data-label">Current Streak</p>
              <p className={`data-value ${overallStats.streakType === 'win' ? 'text-green-500' : overallStats.streakType === 'loss' ? 'text-red-500' : ''}`}>
                {overallStats.currentStreak > 0 ? `${overallStats.currentStreak}${overallStats.streakType === 'win' ? 'W' : 'L'}` : '-'}
              </p>
            </div>
            {overallStats.streakType === 'win' ? (
              <TrendingUp className="h-8 w-8 text-green-500 opacity-50" />
            ) : (
              <TrendingDown className="h-8 w-8 text-red-500 opacity-50" />
            )}
          </div>
        </div>

        <div className="stat-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="data-label">Best Streak</p>
              <p className="data-value text-green-500">{overallStats.bestStreak}W</p>
            </div>
            <TrendingUp className="h-8 w-8 text-green-500 opacity-50" />
          </div>
        </div>
      </div>

      <div className="stat-card">
        <h2 className="text-xl font-semibold text-foreground mb-4 flex items-center gap-2">
          <BarChart3 className="h-5 w-5" />
          Win Rate by Stat Type
        </h2>
        <div className="space-y-3">
          {statTypePerformance.length === 0 ? (
            <div className="py-12 text-center text-muted-foreground">
              <BarChart3 className="h-12 w-12 mx-auto mb-2 opacity-50" />
              <p>No settled picks yet</p>
              <p className="text-sm">Win rates will appear once your picks are settled</p>
            </div>
          ) : (
            statTypePerformance.map((stat) => (
              <div key={stat.stat} className="flex items-center justify-between p-3 rounded-lg border border-border hover:bg-accent/50 transition-colors">
                <div className="flex-1">
                  <p className="font-medium text-foreground">{stat.stat}</p>
                  <p className="text-xs text-muted-foreground">
                    {stat.won}W - {stat.lost}L of {stat.total} picks
                  </p>
                </div>
                <div className={`text-xl font-bold ${
                  stat.winRate >= 55 ? 'text-green-500' :
                  stat.winRate >= 45 ? 'text-yellow-500' :
                  'text-red-500'
                }`}>
                  {stat.winRate.toFixed(1)}%
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="stat-card">
          <h2 className="text-xl font-semibold text-foreground mb-4 flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            Over vs Under
          </h2>
          <div className="space-y-3">
            {overUnderPerformance.map((data) => (
              <div key={data.type} className="flex items-center justify-between p-3 rounded-lg border border-border hover:bg-accent/50 transition-colors">
                <div className="flex items-center gap-3">
                  {data.type === 'Over' ? (
                    <ArrowUp className="h-4 w-4 text-muted-foreground" />
                  ) : (
                    <ArrowDown className="h-4 w-4 text-muted-foreground" />
                  )}
                  <div>
                    <p className="font-medium text-foreground">{data.type}</p>
                    <p className="text-xs text-muted-foreground">
                      {data.total} picks • {data.won}W - {data.lost}L
                    </p>
                  </div>
                </div>
                <p className={`text-xl font-bold ${data.winRate >= 50 ? 'text-green-500' : 'text-red-500'}`}>
                  {data.winRate.toFixed(1)}%
                </p>
              </div>
            ))}
          </div>
        </div>

        <div className="stat-card">
          <h2 className="text-xl font-semibold text-foreground mb-4 flex items-center gap-2">
            <Calendar className="h-5 w-5" />
            Recent Form
          </h2>
          <div className="space-y-3">
            {recentForm.map(period => (
              <div key={period.period} className="flex items-center justify-between p-3 rounded-lg border border-border hover:bg-accent/50 transition-colors">
                <div>
                  <p className="font-medium text-foreground">{period.period}</p>
                  <p className="text-xs text-muted-foreground">
                    {period.total} picks • {period.won}W - {period.lost}L
                  </p>
                </div>
                <p className={`text-xl font-bold ${period.winRate >= 50 ? 'text-green-500' : 'text-red-500'}`}>
                  {period.winRate.toFixed(1)}%
                </p>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="stat-card">
        <h2 className="text-xl font-semibold text-foreground mb-4 flex items-center gap-2">
          <Users className="h-5 w-5" />
          Top 10 Players by Win Rate
        </h2>
        {playerPerformance.length === 0 ? (
          <div className="py-12 text-center text-muted-foreground">
            <Users className="h-12 w-12 mx-auto mb-2 opacity-50" />
            <p>No settled picks yet</p>
            <p className="text-sm">Player rankings will appear once picks are settled</p>
          </div>
        ) : (
          <div className="space-y-2">
            {playerPerformance.map((player, index) => (
              <div key={player.name} className="flex items-center gap-3 p-3 rounded-lg border border-border hover:bg-accent/50 transition-colors">
                <div className={`flex items-center justify-center w-7 h-7 rounded-full font-bold text-xs flex-shrink-0 ${
                  index === 0 ? 'bg-yellow-500/10 text-yellow-500 border border-yellow-500/20' :
                  index === 1 ? 'bg-gray-400/10 text-gray-400 border border-gray-400/20' :
                  index === 2 ? 'bg-orange-500/10 text-orange-500 border border-orange-500/20' :
                  'text-muted-foreground'
                }`}>
                  {index + 1}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-foreground truncate">{player.name}</p>
                  <p className="text-xs text-muted-foreground">
                    {player.total} picks • {player.won}W - {player.lost}L
                  </p>
                </div>
                <div className="text-right flex-shrink-0">
                  <p className={`text-lg font-bold ${player.winRate >= 50 ? 'text-green-500' : 'text-red-500'}`}>
                    {player.winRate.toFixed(1)}%
                  </p>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
