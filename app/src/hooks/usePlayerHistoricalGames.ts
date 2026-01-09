import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import type { HistoricalGame, PlayerStats } from '@/types/nba';
import { logger } from '@/lib/logger';

interface PlayerStatRow {
  game_id: string;
  minutes_played: number | null;
  points: number | null;
  rebounds_total: number | null;
  assists: number | null;
  steals: number | null;
  blocks: number | null;
  turnovers: number | null;
  three_pointers_made: number | null;
  team_id: number;
}

interface TeamRow {
  team_id: number;
  abbreviation: string;
}

interface GameRow {
  game_id: string;
  game_date: string;
  home_team_id: number;
  away_team_id: number;
  game_status: string;
  season: string;
  home_score: number | null;
  away_score: number | null;
}

function toStats(row: PlayerStatRow): PlayerStats {
  return {
    points: Number(row.points ?? 0),
    rebounds: Number(row.rebounds_total ?? 0),
    assists: Number(row.assists ?? 0),
    steals: Number(row.steals ?? 0),
    blocks: Number(row.blocks ?? 0),
    turnovers: Number(row.turnovers ?? 0),
    threePointersMade: Number(row.three_pointers_made ?? 0),
  };
}

export function usePlayerHistoricalGames(playerId: string | null, maxGames = 50, beforeDate?: Date) {
  return useQuery<HistoricalGame[], Error>({
    queryKey: ['playerHistoricalGames', playerId, maxGames, beforeDate?.toISOString()],
    enabled: !!playerId,
    queryFn: async () => {
      if (!playerId) return [];

      const numericId = Number(playerId);
      if (Number.isNaN(numericId)) return [];

      
      const { data, error } = await supabase
        .from('player_game_stats')
        .select(
          'game_id, minutes_played, points, rebounds_total, assists, steals, blocks, turnovers, three_pointers_made, team_id'
        )
        .eq('player_id', numericId)
        .limit(1000); 

      if (error) {
        logger.error('Error fetching player_game_stats for historical games', error as Error);
      }

      let rows: PlayerStatRow[] = (data as unknown as PlayerStatRow[]) || [];

      
      if (!rows.length) {
        const { data: predData, error: predError } = await supabase
          .from('predictions')
          .select(
            'game_id, actual_points, actual_rebounds, actual_assists, actual_steals, actual_blocks, actual_turnovers, actual_three_pointers_made'
          )
          .eq('player_id', numericId)
          .not('actual_points', 'is', null)
          .limit(maxGames * 3);

        if (predError) {
          logger.error('Error fetching predictions fallback for historical games', predError as Error);
          throw predError;
        }

        if (!predData || !predData.length) {
          logger.debug('No historical games found for player in stats or predictions', { playerId: numericId });
          return [];
        }

        rows = (predData as any[]).map((r) => ({
          game_id: r.game_id,
          minutes_played: null,
          points: r.actual_points,
          rebounds_total: r.actual_rebounds,
          assists: r.actual_assists,
          steals: r.actual_steals,
          blocks: r.actual_blocks,
          turnovers: r.actual_turnovers,
          three_pointers_made: r.actual_three_pointers_made,
          team_id: 0,
        }));
      }

      
      const gameIds = Array.from(new Set(rows.map((r) => r.game_id))).filter(Boolean);
      if (!gameIds.length) {
        return [];
      }

      const { data: gameRows, error: gameError } = await supabase
        .from('games')
        .select('game_id, game_date, home_team_id, away_team_id, game_status, season, home_score, away_score')
        .in('game_id', gameIds);

      if (gameError) {
        logger.error('Error fetching games for historical games', gameError as Error);
        throw gameError;
      }

      if (gameError) {
        logger.error('Error fetching games for historical games', gameError as Error);
        throw gameError;
      }

      const gamesById = new Map<string, GameRow>(
        (gameRows as GameRow[]).map((g) => [g.game_id, g])
      );

      
      const completedRows: PlayerStatRow[] = [];
      const teamIds = new Set<number>();
      
      
      const beforeDateStr = beforeDate ? beforeDate.toISOString().split('T')[0] : null;

      for (const row of rows) {
        const g = gamesById.get(row.game_id);
        if (!g || g.game_status !== 'completed') continue;
        
        
        if (beforeDateStr) {
          const gameDateStr = g.game_date.split('T')[0];
          if (gameDateStr >= beforeDateStr) continue; 
        }
        
        completedRows.push(row);
        teamIds.add(g.home_team_id);
        teamIds.add(g.away_team_id);
      }

      if (!completedRows.length) {
        logger.debug('No completed games found for player', { playerId: numericId });
        return [];
      }

      
      const { data: teamRows, error: teamError } = await supabase
        .from('teams')
        .select('team_id, abbreviation')
        .in('team_id', Array.from(teamIds));

      if (teamError) {
        logger.error('Error fetching teams for historical games', teamError as Error);
        throw teamError;
      }

      const teamsById = new Map<number, TeamRow>(
        (teamRows as TeamRow[]).map((t) => [t.team_id, t])
      );

      
      const sortedRows = completedRows.sort((a, b) => {
        const gA = gamesById.has(a.game_id)
          ? (gamesById.get(a.game_id) as GameRow)
          : null;
        const gB = gamesById.has(b.game_id)
          ? (gamesById.get(b.game_id) as GameRow)
          : null;
        if (!gA || !gB) return 0;
        
        const parseDateSafe = (dateStr: string | Date): Date => {
          if (dateStr instanceof Date) return dateStr;
          if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
            const [year, month, day] = dateStr.split('-').map(Number);
            return new Date(year, month - 1, day);
          }
          return new Date(dateStr);
        };
        return parseDateSafe(gB.game_date).getTime() - parseDateSafe(gA.game_date).getTime();
      });

      
      const games: HistoricalGame[] = sortedRows.map((row, index) => {
        const g = gamesById.get(row.game_id) as GameRow;
        const isHome = row.team_id !== 0 ? row.team_id === g.home_team_id : true;
        const opponentId = isHome ? g.away_team_id : g.home_team_id;
        const opponent = teamsById.get(opponentId);

        const homeScore = g.home_score ?? null;
        const awayScore = g.away_score ?? null;
        let result: 'W' | 'L' = 'W';
        let score = '';

        if (homeScore !== null && awayScore !== null) {
          const didWin = isHome ? homeScore > awayScore : awayScore > homeScore;
          result = didWin ? 'W' : 'L';
          score = `${homeScore}-${awayScore}`;
        }

        return {
          id: row.game_id ?? `game-${index}`,
          date: g.game_date,
          opponent: opponent?.abbreviation ?? 'OPP',
          opponentAbbr: opponent?.abbreviation ?? 'OPP',
          isHome,
          result,
          score,
          stats: toStats(row),
          minutesPlayed: Number(row.minutes_played ?? 0),
          season: g.season,
          teamId: row.team_id !== 0 ? row.team_id : undefined,
          opponentTeamId: opponentId,
        };
      });

      
      games.sort(
        (a, b) =>
          new Date(b.date).getTime() - new Date(a.date).getTime()
      );

      return games;
    },
  });
}


