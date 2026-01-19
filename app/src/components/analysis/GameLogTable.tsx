import { useState } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { CheckCircle2, XCircle, ChevronDown, AlertCircle } from 'lucide-react';
import { HistoricalGame } from '@/types/nba';
import { formatTableDate } from '@/lib/dateUtils';
import { useTheme } from '@/contexts/ThemeContext';
import { getTeamLogoUrl } from '@/utils/teamLogos';

interface GameLogTableProps {
  games: HistoricalGame[];
  selectedStat: string;
  lineValue: number;
  overUnder: 'over' | 'under';
  isLoading?: boolean;
  isError?: boolean;
  error?: Error | null;
}

const statLabels: Record<string, string> = {
  points: 'PTS',
  rebounds: 'REB',
  assists: 'AST',
  steals: 'STL',
  blocks: 'BLK',
  turnovers: 'TO',
  threePointersMade: '3PM',
};

const INITIAL_DISPLAY_LIMIT = 20;

export function GameLogTable({ games, selectedStat, lineValue, overUnder, isLoading, isError, error }: GameLogTableProps) {
  const { dateFormat } = useTheme();
  const [showAll, setShowAll] = useState(false);

  const isHit = (value: number) => {
    return overUnder === 'over' ? value > lineValue : value < lineValue;
  };

  const displayedGames = showAll ? games : games.slice(0, INITIAL_DISPLAY_LIMIT);
  const hasMore = games.length > INITIAL_DISPLAY_LIMIT;

  return (
    <div className="stat-card overflow-hidden">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-4">
        <h3 className="font-semibold text-foreground">
          {isLoading ? 'Games Played (Loading...)' : showAll ? `Games Played (${games.length})` : hasMore ? `Games Played (Showing ${INITIAL_DISPLAY_LIMIT} of ${games.length})` : `Games Played (${games.length})`}
        </h3>
      </div>

      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow className="border-border hover:bg-transparent">
              <TableHead className="w-[60px] text-center text-muted-foreground">Hit</TableHead>
              <TableHead className="text-center text-muted-foreground">Date</TableHead>
              <TableHead className="text-center text-muted-foreground">Versus</TableHead>
              <TableHead className="text-center text-muted-foreground">Result</TableHead>
              <TableHead className="text-center text-muted-foreground">{statLabels[selectedStat] || 'PTS'}</TableHead>
              <TableHead className="text-center text-muted-foreground">MIN</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center py-8">
                  <div className="flex flex-col items-center gap-2">
                    <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary"></div>
                    <p className="text-sm text-muted-foreground">Loading games...</p>
                  </div>
                </TableCell>
              </TableRow>
            ) : isError ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center py-8">
                  <div className="flex flex-col items-center gap-2">
                    <AlertCircle className="h-8 w-8 text-muted-foreground mb-1" />
                    <p className="text-sm font-medium text-foreground">Failed to load games</p>
                    <p className="text-xs text-muted-foreground max-w-md">
                      {error?.message || 'There was a problem fetching historical data. Please wait a moment and refresh.'}
                    </p>
                  </div>
                </TableCell>
              </TableRow>
            ) : displayedGames.length > 0 ? (
              displayedGames.map((game) => {
                const statValue = game.stats[selectedStat as keyof typeof game.stats] || 0;
                const hit = isHit(statValue);

                return (
                  <TableRow
                    key={game.id}
                    className="border-border/50 hover:bg-secondary/30 transition-colors"
                  >
                    <TableCell className="text-center">
                      {hit ? (
                        <CheckCircle2 className="h-5 w-5 text-success mx-auto" />
                      ) : (
                        <XCircle className="h-5 w-5 text-destructive mx-auto" />
                      )}
                    </TableCell>
                    <TableCell className="text-center text-foreground font-medium">
                      {formatTableDate(game.date, dateFormat)}
                    </TableCell>
                    <TableCell className="text-center">
                      <div className="flex items-center justify-center gap-2">
                        <span className="text-xs text-muted-foreground">
                          {game.isHome ? 'vs' : '@'}
                        </span>
                        <span className="text-foreground font-medium">{game.opponentAbbr}</span>
                        {game.opponentTeamId && (
                          <img
                            src={getTeamLogoUrl(game.opponentAbbr, game.opponentTeamId).primary}
                            alt={game.opponentAbbr}
                            className="h-5 w-5 object-contain"
                            crossOrigin="anonymous"
                            onError={(e) => {
                              
                              const fallback = getTeamLogoUrl(game.opponentAbbr, game.opponentTeamId).fallback;
                              if (fallback && e.currentTarget.src !== fallback) {
                                e.currentTarget.src = fallback;
                              }
                            }}
                          />
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="text-center">
                      <span className={cn(
                        'font-medium',
                        game.result === 'W' ? 'text-success' : 'text-destructive'
                      )}>
                        {game.result} · {game.score}
                      </span>
                    </TableCell>
                    <TableCell className={cn(
                      'text-center font-bold',
                      hit ? 'text-success' : 'text-destructive'
                    )}>
                      {statValue}
                    </TableCell>
                    <TableCell className="text-center text-muted-foreground">
                      {game.minutesPlayed}
                    </TableCell>
                  </TableRow>
                );
              })
            ) : (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                  No games found for this filter
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {hasMore && (
        <div className="flex justify-center mt-4 pt-4 border-t border-border">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setShowAll(!showAll)}
            className="gap-2"
          >
            {showAll ? (
              <>Show Less</>
            ) : (
              <>
                Show All {games.length} Games
                <ChevronDown className="h-4 w-4" />
              </>
            )}
          </Button>
        </div>
      )}
    </div>
  );
}