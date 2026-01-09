import { useState, useMemo } from 'react';
import { TrendingUp, Target, Home, History, Check, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { Trend } from '@/types/trends';
import { usePlayerHistoricalGames } from '@/hooks/usePlayerHistoricalGames';

interface TrendDetailProps {
  trend: Trend;
}

export function TrendDetail({ trend }: TrendDetailProps) {
  const [activeTab, setActiveTab] = useState<'last10' | 'h2h' | 'splits'>('last10');

  
  const { data: allGames = [], isLoading } = usePlayerHistoricalGames(trend.playerId);

  
  const filteredGames = useMemo(() => {
    if (!allGames.length) return [];

    switch (activeTab) {
      case 'last10':
        return allGames.slice(0, 10);

      case 'h2h':
        return allGames.filter(game => game.opponentAbbr === trend.opponentAbbr);

      case 'splits':
        return allGames.filter(game => game.isHome === trend.isHome);

      default:
        return allGames.slice(0, 10);
    }
  }, [allGames, activeTab, trend.opponentAbbr, trend.isHome]);

  const getStatLabel = (statType: string): string => {
    const labels: Record<string, string> = {
      points: 'Points',
      rebounds: 'Rebounds',
      assists: 'Assists',
      steals: 'Steals',
      blocks: 'Blocks',
      turnovers: 'Turnovers',
      threePointersMade: '3-Pointers',
    };
    return labels[statType] || statType;
  };

  
  const getStatValue = (stats: any, statType: string): number => {
    const statMap: Record<string, keyof typeof stats> = {
      points: 'points',
      rebounds: 'rebounds',
      assists: 'assists',
      steals: 'steals',
      blocks: 'blocks',
      turnovers: 'turnovers',
      threePointersMade: 'threePointersMade',
    };
    return stats[statMap[statType]] ?? 0;
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-start gap-4">
        <img
          src={trend.playerPhotoUrl}
          alt={trend.playerName}
          className="w-24 h-24 rounded-full bg-zinc-800 object-cover"
          onError={(e) => {
            e.currentTarget.src = '/player-placeholder.png';
          }}
        />
        <div className="flex-1">
          <h2 className="text-3xl font-bold text-white mb-2">
            {trend.playerName}
          </h2>
          <p className="text-zinc-400 mb-4">
            {trend.position} • {trend.team}
          </p>

          <div className="flex items-center gap-3">
            <span className={cn(
              'px-4 py-2 rounded-lg font-semibold text-lg',
              trend.overUnder === 'over'
                ? 'bg-green-500/20 text-green-400'
                : 'bg-red-500/20 text-red-400'
            )}>
              {trend.overUnder === 'over' ? 'Over' : 'Under'} {trend.line} {getStatLabel(trend.statType)}
            </span>
            <span className="text-zinc-500">vs</span>
            <span className="text-white font-medium">{trend.opponentAbbr}</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-4 gap-4">
        <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="h-4 w-4 text-blue-400" />
            <p className="text-xs text-zinc-500 uppercase tracking-wide">Recent Form</p>
          </div>
          <p className="text-3xl font-bold text-white mb-1">
            {trend.hitRate.toFixed(0)}%
          </p>
          <p className="text-sm text-zinc-400">
            {trend.hitCount} of {trend.totalGames} games
          </p>
        </div>

        <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Target className="h-4 w-4 text-orange-400" />
            <p className="text-xs text-zinc-500 uppercase tracking-wide">Streak</p>
          </div>
          <p className="text-3xl font-bold text-white mb-1">
            {trend.consecutiveHits}
          </p>
          <p className="text-sm text-zinc-400">
            {trend.consecutiveHits >= 2 ? 'games in a row' : 'current'}
          </p>
        </div>

        <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <History className="h-4 w-4 text-purple-400" />
            <p className="text-xs text-zinc-500 uppercase tracking-wide">Line</p>
          </div>
          <p className="text-3xl font-bold text-white mb-1">
            {trend.line}
          </p>
          <p className="text-sm text-zinc-400">
            {getStatLabel(trend.statType)}
          </p>
          <p className="text-xs text-zinc-500 mt-1">
            {trend.lineMethod === 'player-average' ? 'Player Avg' : 'AI Pred'} • {' '}
            {trend.lineAdjustment === 'standard' ? 'Standard' :
             trend.lineAdjustment === 'favorable' ? 'Favorable' : 'Custom'}
          </p>
        </div>

        {trend.aiPrediction !== undefined && (trend.confidence ?? 0) > 10 && (
          <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-2">
              <div className="h-4 w-4 rounded bg-gradient-to-br from-blue-500 to-purple-600" />
              <p className="text-xs text-zinc-500 uppercase tracking-wide">AI Predicts</p>
            </div>
            <p className="text-3xl font-bold text-white mb-1">
              {trend.aiPrediction.toFixed(1)}
            </p>
            <p className="text-sm text-zinc-400">
              {trend.confidence?.toFixed(0)}% confidence
            </p>
          </div>
        )}
      </div>

      <div className="bg-gradient-to-br from-orange-500/10 to-red-600/10 border border-orange-500/20 rounded-lg p-4">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-orange-500/20 rounded-lg">
            <TrendingUp className="h-5 w-5 text-orange-400" />
          </div>
          <div>
            <p className="font-semibold text-white mb-1">Why This Is Trending</p>
            <p className="text-sm text-zinc-300">{trend.trendLabel}</p>
          </div>
        </div>
      </div>

      <div className="flex gap-2 border-b border-zinc-800">
        <button
          onClick={() => setActiveTab('last10')}
          className={cn(
            'px-4 py-2 font-medium transition-colors',
            activeTab === 'last10'
              ? 'text-blue-500 border-b-2 border-blue-500'
              : 'text-zinc-400 hover:text-zinc-300'
          )}
        >
          Last 10 Games
        </button>
        <button
          onClick={() => setActiveTab('h2h')}
          className={cn(
            'px-4 py-2 font-medium transition-colors',
            activeTab === 'h2h'
              ? 'text-blue-500 border-b-2 border-blue-500'
              : 'text-zinc-400 hover:text-zinc-300'
          )}
        >
          Head to Head
        </button>
        <button
          onClick={() => setActiveTab('splits')}
          className={cn(
            'px-4 py-2 font-medium transition-colors',
            activeTab === 'splits'
              ? 'text-blue-500 border-b-2 border-blue-500'
              : 'text-zinc-400 hover:text-zinc-300'
          )}
        >
          {trend.isHome ? 'Home' : 'Away'} Splits
        </button>
      </div>

      <div className="bg-zinc-900/30 rounded-lg border border-zinc-800 overflow-hidden">
        {isLoading ? (
          <div className="p-8 text-center">
            <p className="text-zinc-400">Loading game log...</p>
          </div>
        ) : filteredGames.length === 0 ? (
          <div className="p-8 text-center">
            <p className="text-zinc-400">
              No games found for {activeTab === 'last10' ? 'last 10 games' : activeTab === 'h2h' ? 'head to head matchup' : trend.isHome ? 'home games' : 'away games'}.
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-800">
                  <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider whitespace-nowrap">Date</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider whitespace-nowrap">Matchup</th>
                  <th className="px-4 py-3 text-center text-xs font-medium text-zinc-400 uppercase tracking-wider whitespace-nowrap">Result</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-zinc-400 uppercase tracking-wider whitespace-nowrap">{getStatLabel(trend.statType)}</th>
                  <th className="px-4 py-3 text-center text-xs font-medium text-zinc-400 uppercase tracking-wider whitespace-nowrap">Hit</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-zinc-400 uppercase tracking-wider whitespace-nowrap">MIN</th>
                </tr>
              </thead>
              <tbody>
                {filteredGames.map((game, index) => {
                  const statValue = getStatValue(game.stats, trend.statType);
                  const didHit = trend.overUnder === 'over' ? statValue > trend.line : statValue < trend.line;

                  return (
                    <tr key={game.id} className={cn(
                      'border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors',
                      index % 2 === 0 ? 'bg-zinc-900/20' : 'bg-transparent'
                    )}>
                      <td className="px-4 py-3 text-sm text-zinc-300 whitespace-nowrap">
                        {new Date(game.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                      </td>
                      <td className="px-4 py-3 text-sm text-zinc-300 whitespace-nowrap">
                        <span className="flex items-center gap-1.5">
                          {game.isHome ? 'vs' : '@'} {game.opponentAbbr}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        <span className={cn(
                          'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium whitespace-nowrap',
                          game.result === 'W' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                        )}>
                          {game.result} {game.score}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span className={cn(
                          'text-sm font-semibold whitespace-nowrap',
                          didHit ? 'text-green-400' : 'text-red-400'
                        )}>
                          {statValue}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        {didHit ? (
                          <Check className="h-4 w-4 text-green-400 inline shrink-0" />
                        ) : (
                          <X className="h-4 w-4 text-red-400 inline shrink-0" />
                        )}
                      </td>
                      <td className="px-4 py-3 text-right text-sm text-zinc-400 whitespace-nowrap">
                        {game.minutesPlayed || '-'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
