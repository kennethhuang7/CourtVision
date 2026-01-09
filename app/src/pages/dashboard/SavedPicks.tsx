import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Trophy, CheckCircle2, XCircle, Clock, Brain, Filter, Trash2, Eye, Share2, ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';
import { Tooltip, TooltipTrigger, TooltipContent } from '@/components/ui/tooltip';
import { formatUserDate, formatUserDateTime } from '@/lib/dateUtils';
import { useTheme } from '@/contexts/ThemeContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useUserPicks } from '@/hooks/useUserPicks';
import { useDeletePick } from '@/hooks/useDeletePick';
import { SharePickModal } from '@/components/picks/SharePickModal';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import { logger } from '@/lib/logger';
import { toast } from 'sonner';
import { cleanNameForMatching } from '@/lib/nameUtils';
import { getTeamLogoUrl } from '@/utils/teamLogos';
import { TEAM_SEARCH_MAP } from '@/constants/teams';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';

const statLabels: Record<string, string> = {
  points: 'Points',
  rebounds: 'Rebounds',
  assists: 'Assists',
  steals: 'Steals',
  blocks: 'Blocks',
  turnovers: 'Turnovers',
  threePointersMade: '3-Pointers Made',
};


export default function SavedPicks() {
  const navigate = useNavigate();
  const { dateFormat, timeFormat } = useTheme();
  const [statusFilter, setStatusFilter] = useState<string>('pending');
  const [playerSearch, setPlayerSearch] = useState('');
  const [statFilter, setStatFilter] = useState<string>('all');
  const [sortField, setSortField] = useState<string>('created_at'); 
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc'); 
  const [deletePickId, setDeletePickId] = useState<string | null>(null);
  const [sharePickId, setSharePickId] = useState<string | null>(null);
  
  const deletePickMutation = useDeletePick();

  const confirmDelete = async () => {
    if (!deletePickId) return;
    
    try {
      await deletePickMutation.mutateAsync(deletePickId);
      toast.success('Pick deleted successfully');
      setDeletePickId(null);
    } catch (error) {
      toast.error('Failed to delete pick');
      logger.error('Error deleting pick', error as Error);
    }
  };

  const handlePickClick = (pick: any) => {
    
    if (pick.game?.game_date) {
      
      const dateStr = typeof pick.game.game_date === 'string' 
        ? pick.game.game_date 
        : pick.game.game_date.toISOString().split('T')[0];
      
      const [year, month, day] = dateStr.split('-').map(Number);
      
      const isoString = new Date(Date.UTC(year, month - 1, day)).toISOString();
      sessionStorage.setItem('shared-selected-date', isoString);
    }
    
    
    localStorage.setItem('player-analysis-selected-game', pick.game_id);
    localStorage.setItem('player-analysis-selected-player', pick.player_id.toString());
    localStorage.setItem('player-analysis-selected-stat', pick.stat_name);
    localStorage.setItem('player-analysis-line-value', pick.line_value.toString());
    
    
    navigate('/dashboard/player-analysis');
  };

  const {
    data: picks = [],
    isLoading,
    isError,
  } = useUserPicks();

  
  const filteredPicks = useMemo(() => {
    let filtered = [...picks];

    
    if (statusFilter !== 'all') {
      filtered = filtered.filter(p => p.result === statusFilter);
    }

    
    if (statFilter !== 'all') {
      filtered = filtered.filter(p => p.stat_name === statFilter);
    }

    
    if (playerSearch.trim()) {
      const normalizedSearch = cleanNameForMatching(playerSearch.toLowerCase().trim());
      filtered = filtered.filter(p => {
        
        const playerName = p.player?.full_name || '';
        const normalizedPlayerName = cleanNameForMatching(playerName.toLowerCase());
        if (normalizedPlayerName.includes(normalizedSearch)) {
          return true;
        }

        
        const teamAbbr = p.player?.team_abbr || '';
        if (teamAbbr) {
          const normalizedAbbr = cleanNameForMatching(teamAbbr.toLowerCase());
          if (normalizedAbbr.includes(normalizedSearch)) {
            return true;
          }
          
          
          const aliases = TEAM_SEARCH_MAP[teamAbbr] || [];
          if (aliases.some(alias => cleanNameForMatching(alias.toLowerCase()).includes(normalizedSearch))) {
            return true;
          }
        }

        return false;
      });
    }

    
    filtered.sort((a, b) => {
      const ascending = sortOrder === 'asc';
      const multiplier = ascending ? 1 : -1;

      switch (sortField) {
        case 'game':
          
          const parseDateSafe = (dateStr: string | Date | undefined): Date => {
            if (!dateStr) return new Date(0);
            if (dateStr instanceof Date) return dateStr;
            if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
              const [year, month, day] = dateStr.split('-').map(Number);
              return new Date(year, month - 1, day);
            }
            return new Date(dateStr);
          };
          const dateA = a.game?.game_date ? parseDateSafe(a.game.game_date).getTime() : 0;
          const dateB = b.game?.game_date ? parseDateSafe(b.game.game_date).getTime() : 0;
          return (dateA - dateB) * multiplier;

        case 'created_at':
          const createdA = new Date(a.created_at).getTime();
          const createdB = new Date(b.created_at).getTime();
          return (createdA - createdB) * multiplier;

        case 'player':
          const nameA = (a.player?.full_name || '').toLowerCase();
          const nameB = (b.player?.full_name || '').toLowerCase();
          return nameA.localeCompare(nameB) * multiplier;

        case 'stat':
          const statA = (statLabels[a.stat_name] || a.stat_name).toLowerCase();
          const statB = (statLabels[b.stat_name] || b.stat_name).toLowerCase();
          return statA.localeCompare(statB) * multiplier;

        case 'line':
          return (a.line_value - b.line_value) * multiplier;

        default:
          return 0;
      }
    });

    return filtered;
  }, [picks, statusFilter, statFilter, playerSearch, sortField, sortOrder]);

  
  const stats = useMemo(() => {
    const completed = picks.filter(p => p.result !== 'pending');
    const wins = completed.filter(p => p.result === 'win').length;
    const losses = completed.filter(p => p.result === 'loss').length;
    const winRate = completed.length > 0 ? (wins / completed.length) * 100 : 0;
    const pending = picks.filter(p => p.result === 'pending').length;

    return { total: picks.length, wins, losses, winRate, pending, completed: completed.length };
  }, [picks]);

  
  if (isLoading) {
    return (
      <div className="space-y-6">
        <div>
          <div className="h-8 w-48 bg-muted animate-pulse rounded mb-2" />
          <div className="h-4 w-64 bg-muted animate-pulse rounded" />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="stat-card">
              <div className="flex items-center justify-between">
                <div className="flex-1">
                  <div className="h-4 w-20 bg-muted animate-pulse rounded mb-2" />
                  <div className="h-8 w-16 bg-muted animate-pulse rounded" />
                </div>
                <div className="h-8 w-8 bg-muted animate-pulse rounded-full" />
              </div>
            </div>
          ))}
        </div>

        <div className="flex flex-wrap gap-3">
          <div className="h-10 w-32 bg-muted animate-pulse rounded" />
          <div className="h-10 w-48 bg-muted animate-pulse rounded" />
          <div className="h-10 w-32 bg-muted animate-pulse rounded" />
        </div>

        <div className="space-y-3">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="stat-card p-4">
              <div className="flex items-center gap-4">
                <div className="h-16 w-16 bg-muted animate-pulse rounded-lg" />
                <div className="flex-1 space-y-2">
                  <div className="h-5 w-40 bg-muted animate-pulse rounded" />
                  <div className="h-4 w-56 bg-muted animate-pulse rounded" />
                  <div className="h-3 w-24 bg-muted animate-pulse rounded" />
                </div>
                <div className="flex items-center gap-2">
                  <div className="h-8 w-20 bg-muted animate-pulse rounded-full" />
                  <div className="h-8 w-8 bg-muted animate-pulse rounded" />
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="flex h-[calc(100vh-10rem)] items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <Brain className="h-16 w-16 text-destructive" />
          <p className="text-destructive">Error loading picks. Please try again.</p>
        </div>
      </div>
    );
  }

  if (picks.length === 0) {
    return (
      <div className="flex h-[calc(100vh-10rem)] items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <Brain className="h-16 w-16 text-muted-foreground" />
          <p className="text-muted-foreground">No picks saved yet.</p>
          <p className="text-sm text-muted-foreground">Save picks from the Player Analysis page to see them here.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="min-w-0">
        <h1 className="text-3xl font-bold leading-tight truncate">My Picks</h1>
        <p className="text-muted-foreground mt-1 leading-tight truncate">Track your predictions and results</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        <div className="stat-card">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0 flex-1">
              <p className="text-sm text-muted-foreground whitespace-nowrap">Total Picks</p>
              <p className="text-2xl font-bold whitespace-nowrap">{stats.total}</p>
            </div>
            <Trophy className="h-8 w-8 text-muted-foreground shrink-0" />
          </div>
        </div>
        <div className="stat-card">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0 flex-1">
              <p className="text-sm text-muted-foreground whitespace-nowrap">Pending</p>
              <p className="text-2xl font-bold text-yellow-600 whitespace-nowrap">{stats.pending}</p>
            </div>
            <Clock className="h-8 w-8 text-yellow-600 shrink-0" />
          </div>
        </div>
        <div className="stat-card">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0 flex-1">
              <p className="text-sm text-muted-foreground whitespace-nowrap">Wins</p>
              <p className="text-2xl font-bold text-green-600 whitespace-nowrap">{stats.wins}</p>
            </div>
            <CheckCircle2 className="h-8 w-8 text-green-600 shrink-0" />
          </div>
        </div>
        <div className="stat-card">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0 flex-1">
              <p className="text-sm text-muted-foreground whitespace-nowrap">Losses</p>
              <p className="text-2xl font-bold text-red-600 whitespace-nowrap">{stats.losses}</p>
            </div>
            <XCircle className="h-8 w-8 text-red-600 shrink-0" />
          </div>
        </div>
        <div className="stat-card">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0 flex-1">
              <p className="text-sm text-muted-foreground whitespace-nowrap">Win Rate</p>
              <p className="text-2xl font-bold whitespace-nowrap">{stats.winRate.toFixed(1)}%</p>
            </div>
            <Trophy className="h-8 w-8 text-muted-foreground shrink-0" />
          </div>
        </div>
      </div>

      <div className="flex flex-wrap gap-4 items-center">
        <div className="flex items-center gap-2">
          <Filter className="h-4 w-4 text-muted-foreground shrink-0" />
          <span className="text-sm text-muted-foreground">Filters:</span>
        </div>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[150px]">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            <SelectItem value="win">Wins</SelectItem>
            <SelectItem value="loss">Losses</SelectItem>
            <SelectItem value="pending">Pending</SelectItem>
          </SelectContent>
        </Select>
        <Select value={statFilter} onValueChange={setStatFilter}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Stat Type" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Stats</SelectItem>
            {Object.entries(statLabels).map(([value, label]) => (
              <SelectItem key={value} value={value}>
                {label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Input
          placeholder="Search player or team..."
          value={playerSearch}
          onChange={(e) => setPlayerSearch(e.target.value)}
          className="w-[200px]"
        />
        <div className="flex items-center gap-2 ml-auto">
          <ArrowUpDown className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm text-muted-foreground">Sort:</span>
          <Select value={sortField} onValueChange={(value) => {
            setSortField(value);
            setSortOrder('desc'); 
          }}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Sort by" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="created_at">Pick Date</SelectItem>
              <SelectItem value="game">Game Date</SelectItem>
              <SelectItem value="player">Player Name</SelectItem>
              <SelectItem value="stat">Stat Type</SelectItem>
              <SelectItem value="line">Line Value</SelectItem>
            </SelectContent>
          </Select>
          <button
            onClick={() => setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')}
            className="flex items-center justify-center w-9 h-9 rounded-md border border-border bg-background hover:bg-secondary transition-colors"
            title={sortOrder === 'asc' ? 'Ascending' : 'Descending'}
          >
            {sortOrder === 'asc' ? (
              <ArrowUp className="h-4 w-4 text-muted-foreground" />
            ) : (
              <ArrowDown className="h-4 w-4 text-muted-foreground" />
            )}
          </button>
        </div>
      </div>

      <div className="space-y-3">
        {filteredPicks.length === 0 ? (
          <div className="text-center py-12 text-muted-foreground">
            No picks match your filters.
          </div>
        ) : (
          filteredPicks.map((pick) => {
            
            
            let gameDate = 'Unknown';
            if (pick.game?.game_date) {
              const dateStr = typeof pick.game.game_date === 'string' 
                ? pick.game.game_date 
                : pick.game.game_date.toISOString().split('T')[0];
              
              const [year, month, day] = dateStr.split('-').map(Number);
              const localDate = new Date(year, month - 1, day); 
              gameDate = format(localDate, dateFormat === 'US' ? 'MMM d, yyyy' : 'd MMM yyyy');
            }
            
            const isWin = pick.result === 'win';
            const isLoss = pick.result === 'loss';
            const isPending = pick.result === 'pending';

            const playerPhotoUrl = pick.player?.player_id
              ? `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${pick.player.player_id}.png`
              : null;
            const teamAbbr = pick.player?.team_abbr || '';
            const teamId = pick.player?.team_id;

            const handleDelete = (e: React.MouseEvent) => {
              e.stopPropagation(); 
              setDeletePickId(pick.id);
            };

            return (
              <div
                key={pick.id}
                className={cn(
                  'stat-card border-l-4 transition-all hover:shadow-lg hover:scale-[1.01]',
                  isWin && 'border-l-green-600',
                  isLoss && 'border-l-red-600',
                  isPending && 'border-l-yellow-600'
                )}
              >
                <div className="flex items-center gap-4">
                  <div className="relative flex-shrink-0">
                    <div className="w-16 h-16 md:w-20 md:h-20 rounded-full overflow-hidden ring-2 ring-primary/30 bg-gradient-to-br from-secondary via-muted to-secondary">
                      {playerPhotoUrl ? (
                        <img
                          src={playerPhotoUrl}
                          alt={pick.player?.full_name || 'Player'}
                          className="w-full h-full object-cover object-top"
                          onError={(e) => {
                            e.currentTarget.src = '/player-placeholder.png';
                          }}
                        />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center bg-muted text-muted-foreground">
                          <Brain className="h-8 w-8" />
                        </div>
                      )}
                    </div>
                    {teamAbbr && (
                      <div className="absolute -bottom-1 -right-1 w-8 h-8 rounded-full bg-background border-2 border-primary/30 overflow-hidden">
                        {teamId ? (
                          <img
                            src={getTeamLogoUrl(teamAbbr, teamId).primary}
                            alt={teamAbbr}
                            className="w-full h-full object-contain"
                            crossOrigin="anonymous"
                            onError={(e) => {
                              
                              const fallback = getTeamLogoUrl(teamAbbr, teamId).fallback;
                              if (fallback && e.currentTarget.src !== fallback) {
                                e.currentTarget.src = fallback;
                              }
                            }}
                          />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center text-xs font-bold text-foreground">
                            {teamAbbr}
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                      <h3 className="font-semibold text-lg text-foreground truncate">
                        {pick.player?.full_name || 'Unknown Player'}
                      </h3>
                      {isWin && (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 shrink-0">
                          Win
                        </span>
                      )}
                      {isLoss && (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200 shrink-0">
                          Loss
                        </span>
                      )}
                      {isPending && (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200 shrink-0">
                          Pending
                        </span>
                      )}
                    </div>
                    <div className="space-y-1">
                      <p className="text-base font-semibold text-foreground">
                        <span className="text-muted-foreground">{statLabels[pick.stat_name] || pick.stat_name}</span>{' '}
                        {pick.over_under === 'over' ? 'Over' : 'Under'}{' '}
                        <span className="text-primary">{pick.line_value.toFixed(1)}</span>
                      </p>
                      <div className="flex items-center gap-4 text-sm text-muted-foreground flex-wrap">
                        <span>Game: {gameDate}</span>
                        {pick.actual_stat !== null && pick.actual_stat !== undefined && (
                          <span className="text-foreground">
                            Actual: <span className="font-semibold">{pick.actual_stat.toFixed(1)}</span>
                          </span>
                        )}
                        <span className="text-xs">
                          Saved: {formatUserDateTime(new Date(pick.created_at), dateFormat, timeFormat)}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="flex-shrink-0 ml-2 flex items-center gap-2">
                    <button
                      onClick={() => handlePickClick(pick)}
                      className="text-muted-foreground hover:text-foreground hover:bg-primary/10 rounded-md p-1.5 flex items-center justify-center transition-colors"
                      title="View details"
                    >
                      <Eye className="h-4 w-4" />
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setSharePickId(pick.id);
                      }}
                      className={cn(
                        "rounded-md p-1.5 flex items-center justify-center transition-colors",
                        pick.visibility && pick.visibility !== 'private'
                          ? "text-primary hover:text-primary hover:bg-primary/20 bg-primary/10"
                          : "text-muted-foreground hover:text-foreground hover:bg-primary/10"
                      )}
                      title={pick.visibility && pick.visibility !== 'private' ? 'Edit sharing' : 'Share pick'}
                    >
                      <Share2 className={cn(
                        "h-4 w-4",
                        pick.visibility && pick.visibility !== 'private' && "fill-current"
                      )} />
                    </button>
                    {isPending ? (
                      <button
                        onClick={handleDelete}
                        className="text-destructive hover:text-destructive hover:bg-destructive/10 rounded-md p-1.5 flex items-center justify-center transition-colors"
                        title="Delete pick"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    ) : (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <button
                            disabled
                            className="text-destructive/50 cursor-not-allowed rounded-md p-1.5 flex items-center justify-center transition-colors"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>You can't delete a settled pick</p>
                        </TooltipContent>
                      </Tooltip>
                    )}
                    
                    <div className="ml-1">
                      {isWin && <CheckCircle2 className="h-5 w-5 text-green-600" />}
                      {isLoss && <XCircle className="h-5 w-5 text-red-600" />}
                      {isPending && <Clock className="h-5 w-5 text-yellow-600" />}
                    </div>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>

      
      <AlertDialog open={!!deletePickId} onOpenChange={(open) => !open && setDeletePickId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Pick?</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this pick? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deletePickMutation.isPending}
            >
              {deletePickMutation.isPending ? 'Deleting...' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      
      {sharePickId && (() => {
        const pick = picks.find(p => p.id === sharePickId);
        return (
          <SharePickModal
            open={!!sharePickId}
            onOpenChange={(open) => !open && setSharePickId(null)}
            pickId={sharePickId}
            currentVisibility={(pick?.visibility as any) || 'private'}
            currentGroupId={(pick as any)?.shared_group_id || null}
          />
        );
      })()}
    </div>
  );
}

