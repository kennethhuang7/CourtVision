import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Trophy, CheckCircle2, XCircle, Clock, Brain, Filter, Eye, ArrowUpDown, ArrowUp, ArrowDown, Users, UserPlus, Globe, Copy } from 'lucide-react';
import { Tooltip, TooltipTrigger, TooltipContent } from '@/components/ui/tooltip';
import { useSavePick } from '@/hooks/useSavePick';
import { useUserPicks } from '@/hooks/useUserPicks';
import { toast } from 'sonner';
import { formatUserDate } from '@/lib/dateUtils';
import { useTheme } from '@/contexts/ThemeContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useCommunityPicks, type CommunityFilter } from '@/hooks/useCommunityPicks';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import { cleanNameForMatching } from '@/lib/nameUtils';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials } from '@/lib/utils';
import { UserProfileCard } from '@/components/friends/UserProfileCard';
import { GroupDetailModal } from '@/components/groups/GroupDetailModal';
import { getTeamLogoUrl } from '@/utils/teamLogos';
import { TEAM_SEARCH_MAP } from '@/constants/teams';

const statLabels: Record<string, string> = {
  points: 'Points',
  rebounds: 'Rebounds',
  assists: 'Assists',
  steals: 'Steals',
  blocks: 'Blocks',
  turnovers: 'Turnovers',
  threePointersMade: '3-Pointers Made',
};


export default function Community() {
  const navigate = useNavigate();
  const { dateFormat } = useTheme();
  const [filter, setFilter] = useState<CommunityFilter>('friends');
  const [statusFilter, setStatusFilter] = useState<string>('pending');
  const [playerSearch, setPlayerSearch] = useState('');
  const [statFilter, setStatFilter] = useState<string>('all');
  const [sortField, setSortField] = useState<string>('created_at');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);

  const {
    data: picks = [],
    isLoading,
    isError,
  } = useCommunityPicks(filter);

  const { data: userPicks = [] } = useUserPicks();
  const savePickMutation = useSavePick();

  
  const isDuplicatePick = (pick: any) => {
    return userPicks.some(userPick => 
      userPick.player_id === pick.player_id &&
      userPick.game_id === pick.game_id &&
      userPick.stat_name === pick.stat_name &&
      userPick.line_value === pick.line_value &&
      userPick.over_under === pick.over_under &&
      userPick.is_active
    );
  };

  const handleTailPick = async (pick: any) => {
    if (pick.result !== 'pending') {
      return;
    }

    if (isDuplicatePick(pick)) {
      toast.error('You already have this exact pick saved');
      return;
    }

    try {
      const result = await savePickMutation.mutateAsync({
        playerId: pick.player_id.toString(),
        gameId: pick.game_id,
        statName: pick.stat_name,
        lineValue: pick.line_value,
        overUnder: pick.over_under,
        tailedFromPickId: pick.id, 
      });
      toast.success('Pick tailed successfully!');
      
      
      if (pick.id && result?.id) {
        
        
        
      }
    } catch (error: any) {
      toast.error(error?.message || 'Failed to tail pick');
    }
  };

  const handlePickClick = (pick: any) => {
    if (pick.game?.game_date) {
      
      const dateStr = typeof pick.game.game_date === 'string' 
        ? pick.game.game_date 
        : pick.game.game_date.toISOString().split('T')[0];
      
      const [year, month, day] = dateStr.split('-').map(Number);
      const localDate = new Date(year, month - 1, day); 
      
      const isoString = new Date(Date.UTC(year, month - 1, day)).toISOString();
      sessionStorage.setItem('shared-selected-date', isoString);
    }
    
    localStorage.setItem('player-analysis-selected-game', pick.game_id);
    localStorage.setItem('player-analysis-selected-player', pick.player_id.toString());
    localStorage.setItem('player-analysis-selected-stat', pick.stat_name);
    localStorage.setItem('player-analysis-line-value', pick.line_value.toString());
    
    navigate('/dashboard/player-analysis');
  };

  
  const filteredPicks = useMemo(() => {
    let filtered = [...picks];

    
    if (statusFilter !== 'all') {
      filtered = filtered.filter(pick => pick.result === statusFilter);
    }

    
    if (statFilter !== 'all') {
      filtered = filtered.filter(pick => pick.stat_name === statFilter);
    }

    
    const searchTerm = playerSearch.trim();
    if (searchTerm) {
      const searchLower = searchTerm.toLowerCase();
      
      if (filter === 'groups') {
        
        filtered = filtered.filter(pick => {
          const groupName = pick.shared_group?.name?.toLowerCase() || '';
          const displayName = pick.owner_profile?.display_name?.toLowerCase() || '';
          const username = pick.owner_profile?.username?.toLowerCase() || '';
          
          return groupName.includes(searchLower) || 
                 displayName.includes(searchLower) || 
                 username.includes(searchLower);
        });
      } else {
        
        filtered = filtered.filter(pick => {
          const displayName = pick.owner_profile?.display_name?.toLowerCase() || '';
          const username = pick.owner_profile?.username?.toLowerCase() || '';
          
          return displayName.includes(searchLower) || username.includes(searchLower);
        });
      }
    }

    
    filtered.sort((a, b) => {
      let aValue: any;
      let bValue: any;

      if (sortField === 'created_at') {
        aValue = new Date(a.created_at).getTime();
        bValue = new Date(b.created_at).getTime();
      } else if (sortField === 'player') {
        aValue = a.player?.full_name || '';
        bValue = b.player?.full_name || '';
      } else if (sortField === 'stat') {
        aValue = statLabels[a.stat_name] || '';
        bValue = statLabels[b.stat_name] || '';
      } else if (sortField === 'line') {
        aValue = a.line_value;
        bValue = b.line_value;
      } else {
        aValue = a[sortField as keyof typeof a];
        bValue = b[sortField as keyof typeof b];
      }

      if (sortOrder === 'asc') {
        return aValue > bValue ? 1 : aValue < bValue ? -1 : 0;
      } else {
        return aValue < bValue ? 1 : aValue > bValue ? -1 : 0;
      }
    });

    return filtered;
  }, [picks, statusFilter, statFilter, playerSearch, filter, sortField, sortOrder]);


  if (isLoading) {
    return (
      <div className="space-y-6">
        <div>
          <div className="h-8 w-56 bg-muted animate-pulse rounded mb-2" />
          <div className="h-4 w-80 bg-muted animate-pulse rounded" />
        </div>

        <div className="flex flex-wrap gap-3">
          <div className="h-10 w-32 bg-muted animate-pulse rounded" />
          <div className="h-10 w-48 bg-muted animate-pulse rounded" />
          <div className="h-10 w-32 bg-muted animate-pulse rounded" />
        </div>

        <div className="space-y-3">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="stat-card p-4">
              <div className="flex items-start gap-3 mb-3">
                <div className="h-10 w-10 bg-muted animate-pulse rounded-full" />
                <div className="flex-1 space-y-1">
                  <div className="h-4 w-32 bg-muted animate-pulse rounded" />
                  <div className="h-3 w-24 bg-muted animate-pulse rounded" />
                </div>
              </div>
              <div className="space-y-2">
                <div className="h-5 w-48 bg-muted animate-pulse rounded" />
                <div className="h-4 w-full bg-muted animate-pulse rounded" />
                <div className="flex gap-2 mt-3">
                  <div className="h-8 w-20 bg-muted animate-pulse rounded-full" />
                  <div className="h-8 w-16 bg-muted animate-pulse rounded-full" />
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
      <div className="flex items-center justify-center h-64">
        <div className="text-destructive">Error loading community picks</div>
      </div>
    );
  }

  if (picks.length === 0) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold">Community</h1>
          <p className="text-muted-foreground mt-1">Discover picks shared by the community</p>
        </div>

        <Tabs value={filter} onValueChange={(v) => setFilter(v as CommunityFilter)} className="w-full">
          <TabsList>
            <TabsTrigger value="friends">
              <UserPlus className="h-4 w-4 mr-2 shrink-0" />
              <span className="whitespace-nowrap">Friends</span>
            </TabsTrigger>
            <TabsTrigger value="groups">
              <Users className="h-4 w-4 mr-2 shrink-0" />
              <span className="whitespace-nowrap">Groups</span>
            </TabsTrigger>
            <TabsTrigger value="public">
              <Globe className="h-4 w-4 mr-2 shrink-0" />
              <span className="whitespace-nowrap">Public</span>
            </TabsTrigger>
          </TabsList>
        </Tabs>

        <div className="flex items-center justify-center h-64 border rounded-lg">
          <div className="text-center">
            <Trophy className="h-12 w-12 text-muted-foreground mx-auto mb-4 shrink-0" />
            <p className="text-lg font-medium leading-tight">No shared picks yet</p>
            <p className="text-sm text-muted-foreground">
              {filter === 'friends' && "No friends have shared picks yet."}
              {filter === 'groups' && "No group members have shared picks yet."}
              {filter === 'public' && "No public picks available yet."}
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="min-w-0">
        <h1 className="text-3xl font-bold leading-tight truncate">Community</h1>
        <p className="text-muted-foreground mt-1 leading-tight truncate">Discover picks shared by the community</p>
      </div>

      <Tabs value={filter} onValueChange={(v) => setFilter(v as CommunityFilter)} className="w-full">
        <TabsList>
          <TabsTrigger value="friends">
            <UserPlus className="h-4 w-4 mr-2 shrink-0" />
            <span className="whitespace-nowrap">Friends</span>
          </TabsTrigger>
          <TabsTrigger value="groups">
            <Users className="h-4 w-4 mr-2 shrink-0" />
            <span className="whitespace-nowrap">Groups</span>
          </TabsTrigger>
          <TabsTrigger value="public">
            <Globe className="h-4 w-4 mr-2 shrink-0" />
            <span className="whitespace-nowrap">Public</span>
          </TabsTrigger>
        </TabsList>
      </Tabs>

      <div className="flex flex-wrap gap-4 items-center">
        <div className="flex items-center gap-2 shrink-0">
          <Filter className="h-4 w-4 text-muted-foreground shrink-0" />
          <span className="text-sm text-muted-foreground whitespace-nowrap">Filters:</span>
        </div>
        <div className="flex-1">
          <Input
            placeholder={
              filter === 'groups' 
                ? "Search by group name or username..." 
                : "Search by username or display name..."
            }
            value={playerSearch}
            onChange={(e) => setPlayerSearch(e.target.value)}
            className="w-full"
          />
        </div>
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
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[150px]">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            <SelectItem value="pending">Pending</SelectItem>
            <SelectItem value="win">Wins</SelectItem>
            <SelectItem value="loss">Losses</SelectItem>
          </SelectContent>
        </Select>
        <div className="flex items-center gap-2 ml-auto shrink-0">
          <ArrowUpDown className="h-4 w-4 text-muted-foreground shrink-0" />
          <span className="text-sm text-muted-foreground whitespace-nowrap">Sort:</span>
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
              <ArrowUp className="h-4 w-4 text-muted-foreground shrink-0" />
            ) : (
              <ArrowDown className="h-4 w-4 text-muted-foreground shrink-0" />
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

            const displayName = pick.owner_profile?.display_name || pick.owner_profile?.username || 'Unknown User';
            const username = pick.owner_profile?.username || 'unknown';
            const profilePicture = pick.owner_profile?.profile_picture_url;

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
                <div className="flex items-center gap-2 mb-3 pb-3 border-b border-border/50">
                  <Avatar className="h-6 w-6">
                    {profilePicture ? (
                      <AvatarImage src={profilePicture} alt={displayName} />
                    ) : null}
                    <AvatarFallback className="text-xs">
                      {getInitials(displayName)}
                    </AvatarFallback>
                  </Avatar>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setSelectedUserId(pick.owner_id);
                    }}
                    className="text-sm font-medium text-foreground hover:text-primary transition-colors"
                  >
                    {displayName}
                  </button>
                  <span className="text-xs text-muted-foreground">@{username}</span>
                  {pick.shared_group && (
                    <>
                      <span className="text-xs text-muted-foreground">•</span>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedGroupId(pick.shared_group!.id);
                        }}
                        className="text-xs font-medium text-primary hover:underline flex items-center gap-1"
                      >
                        <Users className="h-3 w-3 shrink-0" />
                        <span className="truncate">{pick.shared_group.name}</span>
                      </button>
                    </>
                  )}
                </div>

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
                          <Brain className="h-8 w-8 shrink-0" />
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
                      </div>
                    </div>
                  </div>

                  <div className="flex-shrink-0 ml-2 flex items-center gap-2">
                    <button
                      onClick={() => handlePickClick(pick)}
                      className="text-muted-foreground hover:text-foreground hover:bg-primary/10 rounded-md p-1.5 flex items-center justify-center transition-colors shrink-0"
                      title="View details"
                    >
                      <Eye className="h-4 w-4 shrink-0" />
                    </button>
                    {isPending ? (
                      isDuplicatePick(pick) ? (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button
                              disabled
                              className="text-muted-foreground/50 cursor-not-allowed rounded-md p-1.5 flex items-center justify-center transition-colors shrink-0"
                            >
                              <Copy className="h-4 w-4 shrink-0" />
                            </button>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>You already have this pick saved</p>
                          </TooltipContent>
                        </Tooltip>
                      ) : (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleTailPick(pick);
                          }}
                          disabled={savePickMutation.isPending}
                          className="text-primary hover:text-primary hover:bg-primary/10 rounded-md p-1.5 flex items-center justify-center transition-colors shrink-0"
                          title="Tail this pick"
                        >
                          <Copy className="h-4 w-4 shrink-0" />
                        </button>
                      )
                    ) : (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <button
                            disabled
                            className="text-muted-foreground/50 cursor-not-allowed rounded-md p-1.5 flex items-center justify-center transition-colors shrink-0"
                          >
                            <Copy className="h-4 w-4 shrink-0" />
                          </button>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>You can't tail a settled pick</p>
                        </TooltipContent>
                      </Tooltip>
                    )}
                    
                    <div className="ml-1 shrink-0">
                      {isWin && <CheckCircle2 className="h-5 w-5 text-green-600 shrink-0" />}
                      {isLoss && <XCircle className="h-5 w-5 text-red-600 shrink-0" />}
                      {isPending && <Clock className="h-5 w-5 text-yellow-600 shrink-0" />}
                    </div>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>

      
      {selectedUserId && (
        <UserProfileCard
          userId={selectedUserId}
          open={!!selectedUserId}
          onOpenChange={(open) => !open && setSelectedUserId(null)}
        />
      )}

      
      {selectedGroupId && (
        <GroupDetailModal
          groupId={selectedGroupId}
          open={!!selectedGroupId}
          onOpenChange={(open) => !open && setSelectedGroupId(null)}
        />
      )}
    </div>
  );
}

