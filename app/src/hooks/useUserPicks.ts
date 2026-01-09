import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId } from '@/lib/security';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { logger } from '@/lib/logger';
import { useEffect, useRef } from 'react';

export interface UserPick {
  id: string; 
  owner_id: string;
  player_id: number;
  game_id: string;
  stat_name: string;
  line_value: number;
  over_under: 'over' | 'under';
  prediction_id?: number | null;
  visibility?: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  
  player?: {
    player_id: number;
    full_name: string;
    team_id: number | null;
    team_abbr?: string; 
  };
  game?: {
    game_id: string;
    game_date: string;
    home_team_id: number;
    away_team_id: number;
    game_status: string;
    home_score: number | null;
    away_score: number | null;
  };
  actual_stat?: number | null; 
  result?: 'win' | 'loss' | 'pending'; 
}

export function useUserPicks() {
  const { user } = useAuth();
  const { notify } = useNotifications();
  const { isEnabled: doNotDisturb } = useDoNotDisturb();
  const previousPickResultsRef = useRef<Map<string, 'win' | 'loss' | 'pending'>>(new Map());
  const previousTailedPickIdsRef = useRef<Set<string>>(new Set());
  const hasInitializedRef = useRef(false);

  
  useEffect(() => {
    if (!hasInitializedRef.current && typeof window !== 'undefined' && user?.id) {
      try {
        const storedPickResults = localStorage.getItem(`courtvision-notified-pick-results-${user.id}`);
        if (storedPickResults) {
          previousPickResultsRef.current = new Map(JSON.parse(storedPickResults));
        }
        const storedTailedPicks = localStorage.getItem(`courtvision-notified-tailed-picks-${user.id}`);
        if (storedTailedPicks) {
          previousTailedPickIdsRef.current = new Set(JSON.parse(storedTailedPicks));
        }
      } catch (e) {
        logger.warn('Error loading pick notification state from localStorage', { error: e });
      }
      hasInitializedRef.current = true;
    }
  }, [user?.id]);

  const query = useQuery<UserPick[], Error>({
    queryKey: ['userPicks', user?.id],
    queryFn: async () => {
      if (!user) {
        throw new Error('You must be logged in to view picks.');
      }

      
      const validatedUserId = validateUserId(user.id);

      
      const { data: picksData, error: picksError } = await supabase
        .from('user_picks')
        .select(`
          id,
          owner_id,
          player_id,
          game_id,
          stat_name,
          line_value,
          over_under,
          prediction_id,
          visibility,
          shared_group_id,
          is_active,
          created_at,
          updated_at
        `)
        .eq('owner_id', validatedUserId)
        .eq('is_active', true)
        .order('created_at', { ascending: false });

      if (picksError) {
        logger.error('Error fetching user picks', picksError as Error);
        throw picksError;
      }
      if (!picksData || picksData.length === 0) {
        return [];
      }

      
      const playerIds = Array.from(new Set(picksData.map(p => p.player_id)));
      const gameIds = Array.from(new Set(picksData.map(p => p.game_id)));

      
      const { data: playersData, error: playersError } = await supabase
        .from('players')
        .select('player_id, full_name, team_id')
        .in('player_id', playerIds);

      if (playersError) throw playersError;

      
      const teamIds = Array.from(new Set((playersData || []).map(p => p.team_id).filter(Boolean)));
      const { data: teamsData, error: teamsError } = await supabase
        .from('teams')
        .select('team_id, abbreviation')
        .in('team_id', teamIds);

      if (teamsError) {
        logger.warn('Error fetching teams for user picks', teamsError as Error);
        
      }

      
      const { data: gamesData, error: gamesError } = await supabase
        .from('games')
        .select('game_id, game_date, home_team_id, away_team_id, game_status, home_score, away_score')
        .in('game_id', gameIds);

      if (gamesError) throw gamesError;

      
      
      const statsMap = new Map<string, any>();
      
      
      const { data: playerGameStatsData, error: statsError } = await supabase
        .from('player_game_stats')
        .select('player_id, game_id, points, rebounds_total, assists, steals, blocks, turnovers, three_pointers_made')
        .in('player_id', playerIds)
        .in('game_id', gameIds);

      if (statsError) {
        logger.warn('Error fetching player_game_stats, will try predictions fallback', statsError as Error);
      } else if (playerGameStatsData && playerGameStatsData.length > 0) {
        logger.debug(`Found ${playerGameStatsData.length} rows in player_game_stats`);
        
        playerGameStatsData.forEach(s => {
          const key = `${Number(s.player_id)}-${String(s.game_id)}`;
          statsMap.set(key, s);
          logger.debug(`Mapped stats for key: ${key}`, { playerId: s.player_id, gameId: s.game_id, points: s.points });
        });
      } else {
        logger.debug('No data found in player_game_stats');
      }

      
      
      const picksNeedingStats = picksData.filter(p => {
        const key = `${Number(p.player_id)}-${String(p.game_id)}`;
        const hasStats = statsMap.has(key);
        if (!hasStats) {
          logger.debug(`Pick ${p.id} missing stats - key: ${key}`, { 
            pickId: p.id, 
            playerId: p.player_id, 
            gameId: p.game_id,
            availableKeys: Array.from(statsMap.keys()).slice(0, 5) 
          });
        }
        return !hasStats;
      });

      if (picksNeedingStats.length > 0) {
        logger.debug(`Found ${picksNeedingStats.length} picks missing stats, trying predictions.actual_* fallback`);
        
        
        const predictionPlayerIds = Array.from(new Set(picksNeedingStats.map(p => p.player_id)));
        const predictionGameIds = Array.from(new Set(picksNeedingStats.map(p => p.game_id)));

        const { data: predictionsData, error: predError } = await supabase
          .from('predictions')
          .select('player_id, game_id, actual_points, actual_rebounds, actual_assists, actual_steals, actual_blocks, actual_turnovers, actual_three_pointers_made')
          .in('player_id', predictionPlayerIds)
          .in('game_id', predictionGameIds)
          .not('actual_points', 'is', null); 

        if (predError) {
          logger.warn('Error fetching predictions fallback for actual stats', predError as Error);
        } else if (predictionsData && predictionsData.length > 0) {
          logger.debug(`Found ${predictionsData.length} predictions with actual stats to use as fallback`);
          
          predictionsData.forEach(pred => {
            const key = `${Number(pred.player_id)}-${String(pred.game_id)}`;
            if (!statsMap.has(key)) {
              
              statsMap.set(key, {
                player_id: pred.player_id,
                game_id: pred.game_id,
                points: pred.actual_points,
                rebounds_total: pred.actual_rebounds,
                assists: pred.actual_assists,
                steals: pred.actual_steals,
                blocks: pred.actual_blocks,
                turnovers: pred.actual_turnovers,
                three_pointers_made: pred.actual_three_pointers_made,
              });
              logger.debug(`Added fallback stats for key: ${key}`, { 
                playerId: pred.player_id, 
                gameId: pred.game_id, 
                points: pred.actual_points 
              });
            }
          });
        }
      }

      
      const teamsMap = new Map(
        (teamsData || []).map(t => [t.team_id, t.abbreviation])
      );
      const playersMap = new Map(
        (playersData || []).map(p => [p.player_id, {
          ...p,
          team_abbr: p.team_id ? teamsMap.get(p.team_id) : undefined
        }])
      );
      const gamesMap = new Map(
        (gamesData || []).map(g => [g.game_id, g])
      );

      
      const statColumnMap: Record<string, string> = {
        points: 'points',
        rebounds: 'rebounds_total',
        assists: 'assists',
        steals: 'steals',
        blocks: 'blocks',
        turnovers: 'turnovers',
        threePointersMade: 'three_pointers_made',
      };

      
      const picks: UserPick[] = picksData.map(pick => {
        const player = playersMap.get(pick.player_id);
        const game = gamesMap.get(pick.game_id);
        
        const statsKey = `${Number(pick.player_id)}-${String(pick.game_id)}`;
        const stats = statsMap.get(statsKey);
        
        const statColumn = statColumnMap[pick.stat_name];
        if (!statColumn) {
          logger.warn(`Unknown stat_name in pick: ${pick.stat_name}`, { pickId: pick.id });
        }
        const actualStat = stats && statColumn ? (stats[statColumn] as number | null | undefined) : null;

        
        if (actualStat === null && stats) {
          logger.debug(`Pick ${pick.id} has stats object but stat column '${statColumn}' not found`, {
            pickId: pick.id,
            playerId: pick.player_id,
            gameId: pick.game_id,
            statsKey,
            statName: pick.stat_name,
            statColumn,
            availableColumns: stats ? Object.keys(stats) : [],
            statsObject: stats,
          });
        } else if (actualStat === null && !stats) {
          logger.debug(`Pick ${pick.id} has no stats in statsMap`, {
            pickId: pick.id,
            playerId: pick.player_id,
            gameId: pick.game_id,
            statsKey,
            allStatsKeys: Array.from(statsMap.keys()),
          });
        }

        
        
        
        let result: 'win' | 'loss' | 'pending' = 'pending';
        if (actualStat !== null && actualStat !== undefined) {
          
          if (pick.over_under === 'over') {
            result = actualStat > pick.line_value ? 'win' : 'loss';
          } else {
            result = actualStat < pick.line_value ? 'win' : 'loss';
          }
        } else if (stats) {
          
          logger.debug(`Pick ${pick.id} has stats but actualStat is null/undefined`, {
            pickId: pick.id,
            statName: pick.stat_name,
            statColumn,
            statsObject: stats,
          });
        }

        return {
          ...pick,
          player,
          game,
          actual_stat: actualStat !== null && actualStat !== undefined ? Number(actualStat) : null,
          result,
        };
      });

      
      
      
      const isFirstLoad = previousPickResultsRef.current.size === 0 && picks.length > 0;
      
      picks.forEach(pick => {
        const currentResult = pick.result || 'pending';
        
        if (isFirstLoad) {
          
          previousPickResultsRef.current.set(pick.id, currentResult);
        } else {
          
          const previousResult = previousPickResultsRef.current.get(pick.id);
          
          
          
          if (previousResult === 'pending' && (currentResult === 'win' || currentResult === 'loss')) {
            const playerName = pick.player?.full_name || 'Player';
            const statLabel = pick.stat_name
              .split('_')
              .map(word => word.charAt(0).toUpperCase() + word.slice(1))
              .join(' ');
            const resultText = currentResult === 'win' ? 'Won' : 'Lost';
            const actualStat = pick.actual_stat !== null && pick.actual_stat !== undefined 
              ? pick.actual_stat 
              : 'N/A';
            
            notify('pickStatus', 'Pick Settled', `${playerName} ${resultText}: ${statLabel} ${pick.over_under === 'over' ? 'Over' : 'Under'} ${pick.line_value} (${actualStat})`, {
              tag: `pick-${pick.id}`, 
            });
          }
          
          
          previousPickResultsRef.current.set(pick.id, currentResult);
        }
      });

      
      const ourPickIds = new Set(picks.map(p => p.id));
      if (ourPickIds.size > 0) {
        const { data: tailedPicksData } = await supabase
          .from('user_picks')
          .select('id, tailed_from_pick_id, owner_id, created_at') 
          .not('tailed_from_pick_id', 'is', null)
          .in('tailed_from_pick_id', Array.from(ourPickIds))
          .eq('is_active', true)
          .order('created_at', { ascending: false })
          .limit(50); 

        if (tailedPicksData && tailedPicksData.length > 0) {
          
          const tailerIds = Array.from(new Set(tailedPicksData.map(tp => tp.owner_id)));
          
          
          const { data: tailerProfiles } = await supabase
            .from('user_profiles')
            .select('user_id, username, display_name')
            .in('user_id', tailerIds);

          const tailerProfilesMap = new Map(
            (tailerProfiles || []).map(p => [p.user_id, p])
          );

          
          
          const currentTailedPickIds = new Set(tailedPicksData.map(tp => tp.id));
          if (previousTailedPickIdsRef.current.size === 0 && currentTailedPickIds.size > 0) {
            
            previousTailedPickIdsRef.current = new Set(currentTailedPickIds);
            
            if (typeof window !== 'undefined' && user?.id) {
              localStorage.setItem(
                `courtvision-notified-tailed-picks-${user.id}`,
                JSON.stringify(Array.from(currentTailedPickIds))
              );
            }
          } else {
            
            tailedPicksData.forEach(tailedPick => {
              if (!previousTailedPickIdsRef.current.has(tailedPick.id)) {
                
                const ourPick = picks.find(p => p.id === tailedPick.tailed_from_pick_id);
                if (ourPick) {
                  const tailerProfile = tailerProfilesMap.get(tailedPick.owner_id);
                  const tailerName = tailerProfile?.display_name || tailerProfile?.username || 'Someone';
                  const playerName = ourPick.player?.full_name || 'Player';
                  
                  notify('pickTailed', 'Pick Tailed', `${tailerName} tailed your ${playerName} pick`, {
                    tag: `pick-tailed-${tailedPick.id}`,
                  });
                }
              }
            });

            
            previousTailedPickIdsRef.current = new Set(currentTailedPickIds);
          }
          
          
          if (typeof window !== 'undefined' && user?.id) {
            localStorage.setItem(
              `courtvision-notified-tailed-picks-${user.id}`,
              JSON.stringify(Array.from(previousTailedPickIdsRef.current))
            );
          }
        }
      }

      
      if (typeof window !== 'undefined' && user?.id) {
        localStorage.setItem(
          `courtvision-notified-pick-results-${user.id}`,
          JSON.stringify(Array.from(previousPickResultsRef.current.entries()))
        );
      }

      return picks;
    },
    enabled: !!user && !doNotDisturb, 
    refetchInterval: 300000, 
    
    refetchIntervalInBackground: true, 
    staleTime: 60000, 
  });
  
  return query;
}

