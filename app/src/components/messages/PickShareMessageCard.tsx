import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Trophy, Eye, Copy, Brain, CheckCircle2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Tooltip, TooltipTrigger, TooltipContent } from '@/components/ui/tooltip';
import { useSavePick } from '@/hooks/useSavePick';
import { useUserPicks } from '@/hooks/useUserPicks';
import { useSendPickTailNotification } from '@/hooks/useSendPickTailNotification';
import { supabase } from '@/lib/supabase';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';
import { logger } from '@/lib/logger';
import { getTeamLogoUrl } from '@/utils/teamLogos';

interface PickShareMessageCardProps {
  metadata: {
    pick_id?: string;
    player_id?: number;
    game_id?: string;
    stat_name?: string;
    line_value?: number;
    over_under?: 'over' | 'under';
  };
  isOwn: boolean;
}

const statLabels: Record<string, string> = {
  points: 'Points',
  rebounds: 'Rebounds',
  assists: 'Assists',
  steals: 'Steals',
  blocks: 'Blocks',
  turnovers: 'Turnovers',
  three_pointers_made: '3PT Made',
  threePointersMade: '3PT Made',
};

export function PickShareMessageCard({ metadata, isOwn }: PickShareMessageCardProps) {
  const navigate = useNavigate();
  const { data: userPicks = [] } = useUserPicks();
  const savePickMutation = useSavePick();
  const sendTailNotification = useSendPickTailNotification();
  const [pickData, setPickData] = useState<{
    player?: { player_id: number; full_name: string; team_id?: number; team_abbr?: string };
    game?: { 
      game_id: string; 
      game_date: string;
      home_team_id?: number;
      away_team_id?: number;
      game_status?: string;
    };
    homeTeam?: { team_id: number; abbreviation: string };
    awayTeam?: { team_id: number; abbreviation: string };
    stat_name?: string;
    line_value?: number;
    over_under?: 'over' | 'under';
  } | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSettled, setIsSettled] = useState(false);
  const [pickResult, setPickResult] = useState<'win' | 'loss' | 'pending' | null>(null);
  const [actualStat, setActualStat] = useState<number | null>(null);

  
  useEffect(() => {
    const fetchPickData = async () => {
      if (!metadata.player_id || !metadata.game_id) {
        setIsLoading(false);
        return;
      }

      try {
        
        const { data: playerData } = await supabase
          .from('players')
          .select('player_id, full_name, team_id')
          .eq('player_id', metadata.player_id)
          .single();

        
        let playerTeam: { team_id: number; abbreviation: string } | undefined;
        if (playerData?.team_id) {
          const { data: teamData } = await supabase
            .from('teams')
            .select('team_id, abbreviation')
            .eq('team_id', playerData.team_id)
            .single();
          if (teamData) {
            playerTeam = { team_id: teamData.team_id, abbreviation: teamData.abbreviation };
          }
        }

        
        const { data: gameData } = await supabase
          .from('games')
          .select('game_id, game_date, home_team_id, away_team_id, game_status')
          .eq('game_id', metadata.game_id)
          .single();

        
        let homeTeam: { team_id: number; abbreviation: string } | undefined;
        let awayTeam: { team_id: number; abbreviation: string } | undefined;
        
        if (gameData?.home_team_id && gameData?.away_team_id) {
          const { data: teamsData } = await supabase
            .from('teams')
            .select('team_id, abbreviation')
            .in('team_id', [gameData.home_team_id, gameData.away_team_id]);
          
          if (teamsData) {
            homeTeam = teamsData.find(t => t.team_id === gameData.home_team_id);
            awayTeam = teamsData.find(t => t.team_id === gameData.away_team_id);
          }
        }

        
        const gameCompleted = gameData?.game_status === 'completed';
        
        
        let actualStatValue: number | null = null;
        let result: 'win' | 'loss' | 'pending' = 'pending';
        
        if (gameCompleted && metadata.player_id && metadata.game_id && metadata.stat_name && metadata.line_value !== undefined) {
          
          const statColumnMap: Record<string, string> = {
            points: 'points',
            rebounds: 'rebounds_total',
            assists: 'assists',
            steals: 'steals',
            blocks: 'blocks',
            turnovers: 'turnovers',
            threePointersMade: 'three_pointers_made',
            three_pointers_made: 'three_pointers_made',
          };
          
          const statColumn = statColumnMap[metadata.stat_name];
          if (statColumn) {
            const { data: statsData } = await supabase
              .from('player_game_stats')
              .select(statColumn)
              .eq('player_id', metadata.player_id)
              .eq('game_id', metadata.game_id)
              .single();
            
            if (statsData && statsData[statColumn] !== null && statsData[statColumn] !== undefined) {
              actualStatValue = Number(statsData[statColumn]);
              
              
              if (metadata.over_under === 'over') {
                result = actualStatValue > metadata.line_value ? 'win' : 'loss';
              } else {
                result = actualStatValue < metadata.line_value ? 'win' : 'loss';
              }
            }
          }
        }
        
        setPickData({
          player: playerData ? { 
            ...playerData, 
            team_id: playerData.team_id,
            team_abbr: playerTeam?.abbreviation 
          } : undefined,
          game: gameData,
          homeTeam,
          awayTeam,
          stat_name: metadata.stat_name,
          line_value: metadata.line_value,
          over_under: metadata.over_under,
        });
        
        setIsSettled(gameCompleted || false);
        setPickResult(result);
        setActualStat(actualStatValue);
      } catch (error) {
        logger.error('Error fetching pick data', error as Error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchPickData();
  }, [metadata.player_id, metadata.game_id]);

  const playerPhotoUrl = metadata.player_id
    ? `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${metadata.player_id}.png`
    : null;

  const statLabel = metadata.stat_name
    ? statLabels[metadata.stat_name] || metadata.stat_name
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ')
    : 'Unknown';

  
  const getGameMatchup = () => {
    if (!pickData?.homeTeam || !pickData?.awayTeam || !pickData?.player?.team_id) {
      return null;
    }
    
    const isHome = pickData.player.team_id === pickData.game?.home_team_id;
    const playerTeam = isHome ? pickData.homeTeam : pickData.awayTeam;
    const opponentTeam = isHome ? pickData.awayTeam : pickData.homeTeam;
    
    return `${opponentTeam.abbreviation} @ ${playerTeam.abbreviation}`;
  };

  const isDuplicatePick = userPicks.some(
    pick =>
      pick.player_id === metadata.player_id &&
      pick.game_id === metadata.game_id &&
      pick.stat_name === metadata.stat_name &&
      pick.line_value === metadata.line_value &&
      pick.over_under === metadata.over_under &&
      pick.is_active
  );

  const handleViewPick = () => {
    if (pickData?.game?.game_date) {
      
      const dateStr = typeof pickData.game.game_date === 'string' 
        ? pickData.game.game_date 
        : pickData.game.game_date.toISOString().split('T')[0];
      
      const [year, month, day] = dateStr.split('-').map(Number);
      
      const isoString = new Date(Date.UTC(year, month - 1, day)).toISOString();
      sessionStorage.setItem('shared-selected-date', isoString);
    }
    localStorage.setItem('player-analysis-selected-game', metadata.game_id || '');
    localStorage.setItem('player-analysis-selected-player', metadata.player_id?.toString() || '');
    localStorage.setItem('player-analysis-selected-stat', metadata.stat_name || '');
    localStorage.setItem('player-analysis-line-value', metadata.line_value?.toString() || '');
    navigate('/dashboard/player-analysis');
  };

  const handleTailPick = async () => {
    if (isDuplicatePick) {
      toast.error('You already have this exact pick saved');
      return;
    }

    try {
      const result = await savePickMutation.mutateAsync({
        playerId: metadata.player_id?.toString() || '',
        gameId: metadata.game_id || '',
        statName: metadata.stat_name || '',
        lineValue: metadata.line_value || 0,
        overUnder: metadata.over_under || 'over',
        tailedFromPickId: metadata.pick_id, 
      });
      
      toast.success('Pick tailed successfully!');
      
      
      if (metadata.pick_id && result?.id) {
        sendTailNotification.mutate({
          originalPickId: metadata.pick_id,
          tailedPickId: result.id,
        });
      }
    } catch (error: any) {
      toast.error(error?.message || 'Failed to tail pick');
    }
  };

  if (isLoading || !pickData) {
    return (
      <div className={cn('rounded-lg p-4 border', isOwn ? 'bg-primary/10 border-primary/20' : 'bg-secondary border-border')}>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Trophy className="h-4 w-4" />
          <span>Loading pick details...</span>
        </div>
      </div>
    );
  }

  const gameMatchup = getGameMatchup();
  const teamId = pickData?.player?.team_id;

  return (
    <div className={cn('rounded-lg p-4 border max-w-md', isOwn ? 'bg-primary/10 border-primary/20' : 'bg-secondary border-border')}>
      <div className="flex items-center gap-2 mb-3">
        <Trophy className="h-4 w-4 text-primary" />
        <p className="text-xs font-medium text-muted-foreground">
          {isOwn ? 'You' : 'User'} shared a pick
        </p>
      </div>

      <div className="flex items-start gap-3">
        <div className="relative flex-shrink-0">
          <div className="w-14 h-14 rounded-full overflow-hidden ring-2 ring-primary/30 bg-gradient-to-br from-secondary via-muted to-secondary">
            {playerPhotoUrl ? (
              <img
                src={playerPhotoUrl}
                alt={pickData.player?.full_name || 'Player'}
                className="w-full h-full object-cover object-top"
                onError={(e) => {
                  e.currentTarget.src = '/player-placeholder.png';
                }}
              />
            ) : (
              <div className="w-full h-full flex items-center justify-center bg-muted text-muted-foreground">
                <Brain className="h-7 w-7" />
              </div>
            )}
          </div>
          {teamId && pickData.player?.team_abbr && (
            <div className="absolute -bottom-1 -right-1 w-6 h-6 rounded-full bg-background border-2 border-primary/30 overflow-hidden">
              <img
                src={getTeamLogoUrl(pickData.player.team_abbr, teamId).primary}
                alt={pickData.player.team_abbr}
                className="w-full h-full object-contain"
                crossOrigin="anonymous"
                onError={(e) => {
                  const fallback = getTeamLogoUrl(pickData.player.team_abbr, teamId).fallback;
                  if (fallback && e.currentTarget.src !== fallback) {
                    e.currentTarget.src = fallback;
                  }
                }}
              />
            </div>
          )}
        </div>

        <div className="flex-1 min-w-0">
          <div className="mb-1">
            <p className="text-sm font-semibold text-foreground">
              {pickData.player?.full_name || 'Unknown Player'}
            </p>
          </div>
          <p className="text-sm font-medium text-foreground mb-1">
            <span className="text-muted-foreground">{statLabel}</span>{' '}
            {metadata.over_under === 'over' ? 'Over' : 'Under'}{' '}
            <span className="text-primary">{metadata.line_value}</span>
            {pickResult && pickResult !== 'pending' && actualStat !== null && (
              <span className={cn(
                "ml-2 inline-flex items-center gap-1 text-xs font-medium",
                pickResult === 'win' ? 'text-green-600' : 'text-red-600'
              )}>
                {pickResult === 'win' ? (
                  <>
                    <CheckCircle2 className="h-3.5 w-3.5" />
                    Won ({actualStat})
                  </>
                ) : (
                  <>
                    <XCircle className="h-3.5 w-3.5" />
                    Lost ({actualStat})
                  </>
                )}
              </span>
            )}
          </p>
          {gameMatchup && (
            <p className="text-xs text-muted-foreground mb-3">
              {gameMatchup}
            </p>
          )}

          <div className="flex gap-2 mt-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={handleViewPick}
                  className="h-8 text-xs"
                >
                  <Eye className="h-3 w-3 mr-1" />
                  View
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>View in Player Analysis</p>
              </TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <span>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={handleTailPick}
                    disabled={isDuplicatePick || isSettled || savePickMutation.isPending}
                    className="h-8 text-xs"
                  >
                    <Copy className="h-3 w-3 mr-1" />
                    Tail
                  </Button>
                </span>
              </TooltipTrigger>
              <TooltipContent>
                {isSettled ? (
                  <p>You can't tail a settled pick</p>
                ) : isDuplicatePick ? (
                  <p>You already have this pick saved</p>
                ) : (
                  <p>Save this pick to your picks</p>
                )}
              </TooltipContent>
            </Tooltip>
          </div>
        </div>
      </div>
    </div>
  );
}

