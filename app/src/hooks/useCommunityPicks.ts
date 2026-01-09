import { useQuery } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { useFriends } from './useFriends';
import { useGroups } from './useGroups';
import { logger } from '@/lib/logger';

export interface CommunityPick {
  id: string;
  owner_id: string;
  player_id: number;
  game_id: string;
  stat_name: string;
  line_value: number;
  over_under: 'over' | 'under';
  prediction_id?: number | null;
  visibility: string | null;
  shared_group_id?: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  
  owner_profile?: {
    user_id: string;
    username: string;
    display_name: string | null;
    profile_picture_url: string | null;
  };
  
  shared_group?: {
    id: string;
    name: string;
    description: string | null;
  };
  
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

export type CommunityFilter = 'friends' | 'groups' | 'public';

export function useCommunityPicks(filter: CommunityFilter) {
  const { user } = useAuth();
  const { data: friends = [] } = useFriends();
  const { data: groups = [] } = useGroups();

  return useQuery<CommunityPick[], Error>({
    queryKey: ['communityPicks', filter, user?.id],
    queryFn: async () => {
      if (!user) {
        throw new Error('You must be logged in to view community picks.');
      }

      let query = supabase
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
        .eq('is_active', true)
        .not('visibility', 'is', null); 

      
      if (filter === 'friends') {
        const friendIds = friends.map(f =>
          f.requester_id === user.id ? f.addressee_id : f.requester_id
        );
        
        
        if (friendIds.length === 0) {
          
          query = query
            .in('visibility', ['friends', 'custom', 'public'])
            .eq('owner_id', user.id);
        } else {
          
          
          
          
          query = query
            .in('visibility', ['friends', 'custom', 'public'])
            .in('owner_id', [...friendIds, user.id])
            .or(`shared_friend_ids.cs.{},shared_friend_ids.cs.{${user.id}}`);
        }
      } else if (filter === 'groups') {
        const groupIds = groups.map(g => g.id);
        if (groupIds.length === 0) return [];
        
        
        
        query = query
          .in('visibility', ['group', 'custom', 'public'])
          .overlaps('shared_group_ids', groupIds);
      } else if (filter === 'public') {
        
        
        query = query.eq('visibility', 'public');
      }

      const { data: picksData, error: picksError } = await query.order('created_at', { ascending: false });

      if (picksError) {
        logger.error('Error fetching community picks', picksError as Error);
        throw picksError;
      }
      if (!picksData || picksData.length === 0) return [];

      
      const ownerIds = Array.from(new Set(picksData.map(p => p.owner_id)));
      const { data: profilesData, error: profilesError } = await supabase
        .from('user_profiles')
        .select('user_id, username, display_name, profile_picture_url')
        .in('user_id', ownerIds);

      if (profilesError) {
        logger.error('Error fetching owner profiles', profilesError as Error);
        throw profilesError;
      }

      
      const profilesMap = new Map(
        (profilesData || []).map(p => [p.user_id, p])
      );

      
      const groupIds = Array.from(new Set(
        picksData
          .filter(p => p.shared_group_id)
          .map(p => p.shared_group_id!)
      ));
      const groupsMap = new Map<string, { id: string; name: string; description: string | null }>();
      
      if (groupIds.length > 0) {
        const { data: groupsData } = await supabase
          .from('user_groups')
          .select('id, name, description')
          .in('id', groupIds);
        
        if (groupsData) {
          groupsData.forEach(g => groupsMap.set(g.id, g));
        }
      }

      
      const playerIds = Array.from(new Set(picksData.map(p => p.player_id)));
      const gameIds = Array.from(new Set(picksData.map(p => p.game_id)));

      
      const { data: playersData, error: playersError } = await supabase
        .from('players')
        .select('player_id, full_name, team_id')
        .in('player_id', playerIds);

      if (playersError) throw playersError;

      
      const teamIds = Array.from(new Set((playersData || []).map(p => p.team_id).filter(Boolean)));
      const { data: teamsData } = await supabase
        .from('teams')
        .select('team_id, abbreviation')
        .in('team_id', teamIds);

      
      const { data: gamesData, error: gamesError } = await supabase
        .from('games')
        .select('game_id, game_date, home_team_id, away_team_id, game_status, home_score, away_score')
        .in('game_id', gameIds);

      if (gamesError) throw gamesError;

      
      const { data: statsData } = await supabase
        .from('player_game_stats')
        .select('player_id, game_id, points, rebounds_total, assists, steals, blocks, turnovers, three_pointers_made')
        .in('player_id', playerIds)
        .in('game_id', gameIds);

      
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
      const statsMap = new Map(
        (statsData || []).map(s => [`${s.player_id}-${s.game_id}`, s])
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

      
      const picks: CommunityPick[] = picksData.map((pick: any) => {
        const player = playersMap.get(pick.player_id);
        const game = gamesMap.get(pick.game_id);
        const stats = statsMap.get(`${pick.player_id}-${pick.game_id}`);
        const ownerProfile = profilesMap.get(pick.owner_id);
        const sharedGroup = pick.shared_group_id ? groupsMap.get(pick.shared_group_id) : undefined;

        const statColumn = statColumnMap[pick.stat_name] as keyof typeof stats | undefined;
        const actualStat = stats && statColumn ? (stats[statColumn] as number | null) : null;

        
        let result: 'win' | 'loss' | 'pending' = 'pending';
        if (actualStat !== null && actualStat !== undefined) {
          if (pick.over_under === 'over') {
            result = actualStat > pick.line_value ? 'win' : 'loss';
          } else {
            result = actualStat < pick.line_value ? 'win' : 'loss';
          }
        }

        return {
          ...pick,
          owner_profile: ownerProfile,
          shared_group: sharedGroup,
          player,
          game,
          actual_stat: actualStat !== null && actualStat !== undefined ? Number(actualStat) : null,
          result,
        };
      });

      return picks;
    },
    enabled: !!user,
  });
}

