import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { format } from 'date-fns';
import { logger } from '@/lib/logger';


function mapPositionToDefensePosition(position: string | null | undefined): string {
  if (!position) return 'G';
  const posStr = position.toUpperCase();
  if (posStr.includes('C') && !posStr.includes('G') && !posStr.includes('F')) {
    return 'C';
  } else if (posStr.includes('F') && !posStr.includes('G')) {
    return 'F';
  }
  return 'G';
}


function getOrdinalSuffix(n: number): string {
  if (11 <= n % 100 && n % 100 <= 13) return 'th';
  return { 1: 'st', 2: 'nd', 3: 'rd' }[n % 10] || 'th';
}

interface OpponentDefenseData {
  rank: number;
  value: number;
  totalTeams: number;
  teamAbbr: string;
  rankDirection: string;
}

interface StarPlayersOutData {
  starsOutNames: string[];
  totalChange?: number;
  totalGames?: number;
  insufficientData?: boolean;
}

interface RestDaysData {
  teamRest: number | null;
  opponentRest: number | null;
}

interface PlayoffExperienceData {
  isPlayoff: boolean;
  change?: number;
  playoffAvg?: number;
  regularAvg?: number;
  playoffGames?: number;
  insufficientData?: boolean;
}

interface PaceComparisonData {
  teamPace: number;
  oppPace: number;
  teamName: string;
  oppName: string;
  avgPace: number;
  leagueAvg: number;
  pctDiff: number;
}


export function useOpponentDefense(
  opponentId: number | null,
  season: string | null,
  position: string | null,
  statName: string
) {
  return useQuery<OpponentDefenseData | null, Error>({
    queryKey: ['opponentDefense', opponentId, season, position, statName],
    enabled: !!opponentId && !!season && !!position,
    queryFn: async () => {
      try {
        if (!opponentId || !season || !position) return null;

      const statColumnMap: Record<string, string> = {
        points: 'points_allowed_per_game',
        rebounds: 'rebounds_allowed_per_game',
        assists: 'assists_allowed_per_game',
        steals: 'steals_allowed_per_game',
        blocks: 'blocks_allowed_per_game',
        turnovers: 'turnovers_forced_per_game',
        threePointersMade: 'three_pointers_made_allowed_per_game',
      };

      const statColumn = statColumnMap[statName];
      if (!statColumn) return null;

      const defensePosition = mapPositionToDefensePosition(position);

      
      const { data, error } = await supabase
        .from('position_defense_stats')
        .select(`${statColumn}, team_id, teams(abbreviation)`)
        .eq('season', season)
        .eq('position', defensePosition);

      if (error) {
        logger.error('Error fetching opponent defense', error as Error);
        return null;
      }
      if (!data || data.length === 0) return null;

      
      const sorted = [...data].sort((a: any, b: any) => {
        const aVal = Number(a[statColumn]) || 0;
        const bVal = Number(b[statColumn]) || 0;
        if (statName === 'turnovers') {
          return bVal - aVal; 
        }
        return aVal - bVal; 
      });

      const opponentRow = sorted.find((row: any) => row.team_id === opponentId);
      if (!opponentRow) return null;

      const rank = sorted.findIndex((row: any) => row.team_id === opponentId) + 1;
      const totalTeams = sorted.length;
      const value = Number(opponentRow[statColumn]) || 0;
      const teamAbbr = (opponentRow.teams as any)?.abbreviation || 'OPP';

      let rankDirection: string;
      if (statName === 'turnovers') {
        if (rank === 1) {
          rankDirection = 'forces the most';
        } else if (rank === totalTeams) {
          rankDirection = 'forces the least';
        } else {
          rankDirection = `forces the ${rank}${getOrdinalSuffix(rank)} most`;
        }
      } else {
        if (rank === 1) {
          rankDirection = 'allows the least';
        } else if (rank === totalTeams) {
          rankDirection = 'allows the most';
        } else {
          rankDirection = `allows the ${rank}${getOrdinalSuffix(rank)} most`;
        }
      }

      return {
        rank,
        value,
        totalTeams,
        teamAbbr,
        rankDirection,
      };
      } catch (error) {
        logger.error('Error in useOpponentDefense', error as Error);
        return null;
      }
    },
  });
}


export function useStarPlayersOut(
  playerId: number | null,
  teamId: number | null,
  season: string | null,
  targetDate: string | null,
  statName: string
) {
  return useQuery<StarPlayersOutData | null, Error>({
    queryKey: ['starPlayersOut', playerId, teamId, season, targetDate, statName],
    enabled: !!playerId && !!teamId && !!season && !!targetDate,
    queryFn: async () => {
      try {
        if (!playerId || !teamId || !season || !targetDate) return null;

      
      const { data: gamesData } = await supabase
        .from('games')
        .select('game_id')
        .eq('season', season)
        .lt('game_date', targetDate)
        .eq('game_status', 'completed');

      if (!gamesData || gamesData.length === 0) return null;

      const gameIds = gamesData.map(g => g.game_id);

      
      const { data: teamStats } = await supabase
        .from('player_game_stats')
        .select('player_id, points, game_id')
        .eq('team_id', teamId)
        .in('game_id', gameIds)
        .neq('player_id', playerId)
        .gte('minutes_played', 15);

      if (!teamStats || teamStats.length === 0) return null;

      
      const playerPPG = new Map<number, { points: number; games: number }>();
      teamStats.forEach((stat: any) => {
        const existing = playerPPG.get(stat.player_id) || { points: 0, games: 0 };
        playerPPG.set(stat.player_id, {
          points: existing.points + (stat.points || 0),
          games: existing.games + 1,
        });
      });

      
      const starPlayers: number[] = [];
      playerPPG.forEach((stats, playerId) => {
        const ppg = stats.points / stats.games;
        if (ppg >= 20) {
          starPlayers.push(playerId);
        }
      });

      if (starPlayers.length === 0) return null;

      
      const { data: injuriesData } = await supabase
        .from('injuries')
        .select('player_id, players!inner(full_name)')
        .in('player_id', starPlayers)
        .eq('injury_status', 'Out')
        .lte('report_date', targetDate)
        .or('return_date.is.null,return_date.gt.' + targetDate);

      if (!injuriesData || injuriesData.length === 0) return null;

      const starsOutNames = injuriesData.map((inj: any) => 
        (inj.players as any)?.full_name || `Player ${inj.player_id}`
      );

      
      return {
        starsOutNames,
        insufficientData: true,
      };
      } catch (error) {
        logger.error('Error in useStarPlayersOut', error as Error);
        return null;
      }
    },
  });
}


export function useRestDays(
  teamId: number | null,
  opponentId: number | null,
  targetDate: string | null,
  season: string | null
) {
  return useQuery<RestDaysData, Error>({
    queryKey: ['restDays', teamId, opponentId, targetDate, season],
    enabled: !!teamId && !!opponentId && !!targetDate && !!season,
    queryFn: async () => {
      try {
        if (!teamId || !opponentId || !targetDate || !season) {
          return { teamRest: null, opponentRest: null };
        }

      
      const { data: teamLastGame, error: teamError } = await supabase
        .from('games')
        .select('game_date')
        .or(`home_team_id.eq.${teamId},away_team_id.eq.${teamId}`)
        .eq('season', season)
        .eq('game_status', 'completed')
        .lt('game_date', targetDate)
        .order('game_date', { ascending: false })
        .limit(1)
        .maybeSingle();

      
      const { data: oppLastGame, error: oppError } = await supabase
        .from('games')
        .select('game_date')
        .or(`home_team_id.eq.${opponentId},away_team_id.eq.${opponentId}`)
        .eq('season', season)
        .eq('game_status', 'completed')
        .lt('game_date', targetDate)
        .order('game_date', { ascending: false })
        .limit(1)
        .maybeSingle();

      const target = new Date(targetDate);
      
      const parseDateSafe = (dateStr: string | Date): Date => {
        if (dateStr instanceof Date) return dateStr;
        if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
          const [year, month, day] = dateStr.split('-').map(Number);
          return new Date(year, month - 1, day);
        }
        return new Date(dateStr);
      };
      
      const teamRest = teamLastGame?.game_date
        ? Math.floor((target.getTime() - parseDateSafe(teamLastGame.game_date).getTime()) / (1000 * 60 * 60 * 24))
        : null;
      const opponentRest = oppLastGame?.game_date
        ? Math.floor((target.getTime() - parseDateSafe(oppLastGame.game_date).getTime()) / (1000 * 60 * 60 * 24))
        : null;

      return { teamRest, opponentRest };
      } catch (error) {
        logger.error('Error in useRestDays', error as Error);
        return { teamRest: null, opponentRest: null };
      }
    },
  });
}


export function usePlayoffExperience(
  playerId: number | null,
  statName: string,
  gameType: string | null
) {
  return useQuery<PlayoffExperienceData, Error>({
    queryKey: ['playoffExperience', playerId, statName, gameType],
    enabled: !!playerId,
    queryFn: async () => {
      try {
        if (!playerId) {
          return { isPlayoff: false };
        }

      if (gameType !== 'playoff') {
        return { isPlayoff: false };
      }

      const statColumnMap: Record<string, string> = {
        points: 'points',
        rebounds: 'rebounds_total',
        assists: 'assists',
        steals: 'steals',
        blocks: 'blocks',
        turnovers: 'turnovers',
        threePointersMade: 'three_pointers_made',
      };

      const statColumn = statColumnMap[statName];
      if (!statColumn) {
        return { isPlayoff: true, insufficientData: true };
      }

      
      const { data: playerStats, error: statsError } = await supabase
        .from('player_game_stats')
        .select(`${statColumn}, game_id`)
        .eq('player_id', playerId);

      if (statsError || !playerStats || playerStats.length === 0) {
        return { isPlayoff: true, insufficientData: true };
      }

      const gameIds = (playerStats as any[]).map((s: any) => s.game_id).filter(Boolean);
      if (gameIds.length === 0) {
        return { isPlayoff: true, insufficientData: true };
      }

      
      const { data: gamesData, error: gamesError } = await supabase
        .from('games')
        .select('game_id, game_type, game_status')
        .in('game_id', gameIds);

      if (gamesError || !gamesData) {
        return { isPlayoff: true, insufficientData: true };
      }

      const gamesMap = new Map((gamesData as any[]).map((g: any) => [g.game_id, g]));

      
      const playoffStats = (playerStats as any[]).filter((s: any) => {
        const game = gamesMap.get(s.game_id);
        return game?.game_type === 'playoff' && game?.game_status === 'completed';
      });
      const regularStats = (playerStats as any[]).filter((s: any) => {
        const game = gamesMap.get(s.game_id);
        return game?.game_type === 'regular_season' && game?.game_status === 'completed';
      });

      if (playoffStats.length < 3 || regularStats.length === 0) {
        return { isPlayoff: true, insufficientData: true };
      }

      const playoffAvg = playoffStats.length > 0
        ? playoffStats.reduce((sum, s) => sum + (Number(s[statColumn]) || 0), 0) / playoffStats.length
        : 0;
      const regularAvg = regularStats.length > 0
        ? regularStats.reduce((sum, s) => sum + (Number(s[statColumn]) || 0), 0) / regularStats.length
        : 0;
      const change = playoffAvg - regularAvg;

      return {
        isPlayoff: true,
        change,
        playoffAvg,
        regularAvg,
        playoffGames: playoffStats.length,
      };
      } catch (error) {
        logger.error('Error in usePlayoffExperience', error as Error);
        return { isPlayoff: false };
      }
    },
  });
}


export function usePaceComparison(
  teamId: number | null,
  opponentId: number | null,
  season: string | null
) {
  return useQuery<PaceComparisonData | null, Error>({
    queryKey: ['paceComparison', teamId, opponentId, season],
    enabled: !!teamId && !!opponentId && !!season,
    queryFn: async () => {
      try {
        if (!teamId || !opponentId || !season) return null;

      
      const { data: teamPaceData } = await supabase
        .from('team_ratings')
        .select('pace, teams(full_name)')
        .eq('team_id', teamId)
        .eq('season', season)
        .single();

      
      const { data: oppPaceData } = await supabase
        .from('team_ratings')
        .select('pace, teams(full_name)')
        .eq('team_id', opponentId)
        .eq('season', season)
        .single();

      
      const { data: leagueData } = await supabase
        .from('team_ratings')
        .select('pace')
        .eq('season', season);

      if (!teamPaceData || !oppPaceData || !leagueData || leagueData.length === 0) {
        return null;
      }

      const teamPace = Number(teamPaceData.pace) || 0;
      const oppPace = Number(oppPaceData.pace) || 0;
      const leagueAvg = leagueData.length > 0
        ? leagueData.reduce((sum, r) => sum + (Number(r.pace) || 0), 0) / leagueData.length
        : 0;

      if (teamPace === 0 || oppPace === 0 || leagueAvg === 0) {
        return null;
      }

      const avgPace = (teamPace + oppPace) / 2;
      const pctDiff = ((avgPace - leagueAvg) / leagueAvg) * 100;

      return {
        teamPace,
        oppPace,
        teamName: (teamPaceData.teams as any)?.full_name || 'Team',
        oppName: (oppPaceData.teams as any)?.full_name || 'Opponent',
        avgPace,
        leagueAvg,
        pctDiff,
      };
      } catch (error) {
        logger.error('Error in usePaceComparison', error as Error);
        return null;
      }
    },
  });
}

