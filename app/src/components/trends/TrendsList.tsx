import { TrendingUp, Flame } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { Trend } from '@/types/trends';

interface TrendsListProps {
  trends: Trend[];
  selectedTrend: Trend | null;
  onSelectTrend: (trend: Trend) => void;
}

export function TrendsList({ trends, selectedTrend, onSelectTrend }: TrendsListProps) {
  const getStatLabel = (statType: string): string => {
    const labels: Record<string, string> = {
      points: 'PTS',
      rebounds: 'REB',
      assists: 'AST',
      steals: 'STL',
      blocks: 'BLK',
      turnovers: 'TO',
      threePointersMade: '3PM',
    };
    return labels[statType] || statType.toUpperCase();
  };

  return (
    <div className="p-4 space-y-2">
      <div className="flex items-center gap-2 mb-4">
        <TrendingUp className="h-5 w-5 text-orange-500" />
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Trending Now ({trends.length})
        </h2>
      </div>

      {trends.map((trend, index) => {
        const isSelected = selectedTrend?.playerId === trend.playerId &&
          selectedTrend?.statType === trend.statType &&
          selectedTrend?.overUnder === trend.overUnder;

        
        const getFireIntensity = (score: number) => {
          if (score >= 90) return 'text-orange-500';
          if (score >= 80) return 'text-orange-400';
          return 'text-orange-300';
        };

        return (
          <button
            key={`${trend.playerId}-${trend.statType}-${trend.overUnder}`}
            onClick={() => onSelectTrend(trend)}
            className={cn(
              'group relative w-full p-4 rounded-lg text-left transition-all duration-200 hover:-translate-y-0.5 overflow-hidden',
              'border border-border/60 hover:border-border',
              'animate-in fade-in slide-in-from-left-2',
              isSelected ? 'bg-secondary border-primary' : 'bg-card/50 hover:bg-secondary/70'
            )}
            style={{ animationDelay: `${index * 50}ms` }}
          >
            {/* Shine effect */}
            <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none">
              <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
            </div>
            <div className="relative">
              <div className="flex items-start gap-3 mb-2">
                <img
                  src={trend.playerPhotoUrl}
                  alt={trend.playerName}
                  className="w-10 h-10 rounded-full bg-secondary object-cover"
                  onError={(e) => {
                    e.currentTarget.src = '/player-placeholder.png';
                  }}
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <h3 className="font-semibold text-foreground truncate">
                      {trend.playerName}
                    </h3>
                    <Flame className={cn('h-4 w-4 flex-shrink-0', getFireIntensity(trend.trendScore))} />
                  </div>
                  <p className="text-xs text-muted-foreground/80">{trend.teamAbbr} vs {trend.opponentAbbr}</p>
                </div>
              </div>

              <div className="mb-2">
                <span className={cn(
                  'inline-flex items-center px-2 py-1 rounded text-xs font-medium',
                  trend.overUnder === 'over'
                    ? 'bg-green-500/10 text-green-400'
                    : 'bg-red-500/10 text-red-400'
                )}>
                  {trend.overUnder === 'over' ? 'Over' : 'Under'} {trend.line} {getStatLabel(trend.statType)}
                </span>
              </div>

              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground/80">Recent Form</p>
                  <p className="text-sm font-semibold text-foreground">
                    {trend.hitRate.toFixed(0)}% ({trend.hitCount}/{trend.totalGames})
                  </p>
                </div>

                {trend.consecutiveHits >= 2 && (
                  <div className="text-right">
                    <p className="text-xs text-muted-foreground/80">Streak</p>
                    <p className="text-sm font-semibold text-orange-400">
                      {trend.consecutiveHits}
                    </p>
                  </div>
                )}
              </div>

              <div className="mt-2 pt-2 border-t border-border/60">
                <p className="text-xs text-muted-foreground">
                  {trend.trendLabel}
                </p>
              </div>
            </div>
          </button>
        );
      })}
    </div>
  );
}
