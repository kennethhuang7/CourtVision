import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime, timedelta

def update_position_defense_stats_for_yesterday(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Updating position defense stats for teams that played on {target_date}...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    season_year = target_date.year
    season_month = target_date.month
    
    if season_month >= 10:
        season = f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        season = f"{season_year-1}-{str(season_year)[-2:]}"
    
    cur.execute("""
        SELECT DISTINCT home_team_id as team_id FROM games WHERE game_date = %s
        UNION
        SELECT DISTINCT away_team_id as team_id FROM games WHERE game_date = %s
    """, (target_date, target_date))
    
    teams = [row[0] for row in cur.fetchall()]
    
    if len(teams) == 0:
        print(f"No teams found for {target_date}")
        cur.close()
        conn.close()
        return
    
    print(f"Found {len(teams)} teams to update for season {season}\n")
    
    positions = ['G', 'F', 'C']
    
    total_inserts = 0
    
    for team_id in teams:
        for position in positions:
            cur.execute("""
                SELECT games_played, points_allowed_per_game, rebounds_allowed_per_game,
                       assists_allowed_per_game, steals_allowed_per_game, blocks_allowed_per_game,
                       turnovers_forced_per_game, three_pointers_made_allowed_per_game, fg_pct_allowed
                FROM position_defense_stats
                WHERE team_id = %s AND season = %s AND position = %s
            """, (team_id, season, position))
            
            existing = cur.fetchone()
            
            if existing:
                old_games = existing[0] or 0
                old_avg_points = existing[1] or 0
                old_avg_rebounds = existing[2] or 0
                old_avg_assists = existing[3] or 0
                old_avg_steals = existing[4] or 0
                old_avg_blocks = existing[5] or 0
                old_avg_turnovers = existing[6] or 0
                old_avg_3pm = existing[7] or 0
                old_fg_pct = existing[8] or 0
                
                cur.execute("""
                    SELECT 
                        pgs.game_id,
                        SUM(pgs.points) as points,
                        SUM(pgs.rebounds_total) as rebounds,
                        SUM(pgs.assists) as assists,
                        SUM(pgs.steals) as steals,
                        SUM(pgs.blocks) as blocks,
                        SUM(pgs.turnovers) as turnovers,
                        SUM(pgs.three_pointers_made) as 3pm,
                        SUM(pgs.field_goals_made) as fgm,
                        SUM(pgs.field_goals_attempted) as fga,
                        SUM(pgs.three_pointers_attempted) as 3pa
                    FROM player_game_stats pgs
                    JOIN games g ON pgs.game_id = g.game_id
                    JOIN players p ON pgs.player_id = p.player_id
                    WHERE g.season = %s
                        AND g.game_status = 'completed'
                        AND g.game_type = 'regular_season'
                        AND pgs.team_id != %s
                        AND (g.home_team_id = %s OR g.away_team_id = %s)
                        AND p.position LIKE %s
                        AND g.game_date < %s
                    GROUP BY pgs.game_id
                """, (season, team_id, team_id, team_id, f'%{position}%', target_date))
                
                old_game_stats = cur.fetchall()
                if old_game_stats:
                    old_total_points = sum(row[1] or 0 for row in old_game_stats)
                    old_total_rebounds = sum(row[2] or 0 for row in old_game_stats)
                    old_total_assists = sum(row[3] or 0 for row in old_game_stats)
                    old_total_steals = sum(row[4] or 0 for row in old_game_stats)
                    old_total_blocks = sum(row[5] or 0 for row in old_game_stats)
                    old_total_turnovers = sum(row[6] or 0 for row in old_game_stats)
                    old_total_3pm = sum(row[7] or 0 for row in old_game_stats)
                    old_total_fgm = sum(row[8] or 0 for row in old_game_stats)
                    old_total_fga = sum(row[9] or 0 for row in old_game_stats)
                    old_total_3pa = sum(row[10] or 0 for row in old_game_stats)
                    old_games = len(old_game_stats)
                else:
                    old_total_points = old_avg_points * old_games if old_games > 0 else 0
                    old_total_rebounds = old_avg_rebounds * old_games if old_games > 0 else 0
                    old_total_assists = old_avg_assists * old_games if old_games > 0 else 0
                    old_total_steals = old_avg_steals * old_games if old_games > 0 else 0
                    old_total_blocks = old_avg_blocks * old_games if old_games > 0 else 0
                    old_total_turnovers = old_avg_turnovers * old_games if old_games > 0 else 0
                    old_total_3pm = old_avg_3pm * old_games if old_games > 0 else 0
                    old_total_fga = (old_total_points / 2) / (old_fg_pct / 100) if old_fg_pct > 0 and old_total_points > 0 else 0
                    old_total_fgm = (old_fg_pct / 100) * old_total_fga if old_total_fga > 0 else 0
                    old_total_3pa = old_total_3pm / 0.35 if old_total_3pm > 0 else 0
            else:
                old_games = 0
                old_total_points = 0
                old_total_rebounds = 0
                old_total_assists = 0
                old_total_steals = 0
                old_total_blocks = 0
                old_total_turnovers = 0
                old_total_3pm = 0
                old_total_fgm = 0
                old_total_fga = 0
                old_total_3pa = 0
            
            cur.execute("""
                SELECT 
                    pgs.game_id,
                    SUM(pgs.points) as points,
                    SUM(pgs.rebounds_total) as rebounds,
                    SUM(pgs.assists) as assists,
                    SUM(pgs.steals) as steals,
                    SUM(pgs.blocks) as blocks,
                    SUM(pgs.turnovers) as turnovers,
                    SUM(pgs.three_pointers_made) as three_pointers_made,
                    SUM(pgs.field_goals_made) as field_goals_made,
                    SUM(pgs.field_goals_attempted) as field_goals_attempted,
                    SUM(pgs.three_pointers_attempted) as three_pointers_attempted
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                JOIN players p ON pgs.player_id = p.player_id
                WHERE g.season = %s
                    AND g.game_status = 'completed'
                    AND g.game_type = 'regular_season'
                    AND pgs.team_id != %s
                    AND (g.home_team_id = %s OR g.away_team_id = %s)
                    AND p.position LIKE %s
                    AND g.game_date = %s
                GROUP BY pgs.game_id
            """, (season, team_id, team_id, team_id, f'%{position}%', target_date))
            
            new_game_stats = cur.fetchall()
            
            if len(new_game_stats) == 0:
                continue
            
            new_total_points = sum(row[1] or 0 for row in new_game_stats)
            new_total_rebounds = sum(row[2] or 0 for row in new_game_stats)
            new_total_assists = sum(row[3] or 0 for row in new_game_stats)
            new_total_steals = sum(row[4] or 0 for row in new_game_stats)
            new_total_blocks = sum(row[5] or 0 for row in new_game_stats)
            new_total_turnovers = sum(row[6] or 0 for row in new_game_stats)
            new_total_3pm = sum(row[7] or 0 for row in new_game_stats)
            new_total_fgm = sum(row[8] or 0 for row in new_game_stats)
            new_total_fga = sum(row[9] or 0 for row in new_game_stats)
            new_total_3pa = sum(row[10] or 0 for row in new_game_stats)
            new_games = len(new_game_stats)
            
            total_points = old_total_points + new_total_points
            total_rebounds = old_total_rebounds + new_total_rebounds
            total_assists = old_total_assists + new_total_assists
            total_steals = old_total_steals + new_total_steals
            total_blocks = old_total_blocks + new_total_blocks
            total_turnovers = old_total_turnovers + new_total_turnovers
            total_3pm = old_total_3pm + new_total_3pm
            total_fgm = old_total_fgm + new_total_fgm
            total_fga = old_total_fga + new_total_fga
            total_3pa = old_total_3pa + new_total_3pa
            games_count = old_games + new_games
            
            avg_points = round(total_points / games_count, 1) if games_count > 0 else 0.0
            avg_rebounds = round(total_rebounds / games_count, 1) if games_count > 0 else 0.0
            avg_assists = round(total_assists / games_count, 1) if games_count > 0 else 0.0
            avg_steals = round(total_steals / games_count, 1) if games_count > 0 else 0.0
            avg_blocks = round(total_blocks / games_count, 1) if games_count > 0 else 0.0
            avg_turnovers = round(total_turnovers / games_count, 1) if games_count > 0 else 0.0
            avg_3pm = total_3pm / games_count if games_count > 0 else 0
            avg_fgm = total_fgm / games_count if games_count > 0 else 0
            avg_fga = total_fga / games_count if games_count > 0 else 0
            avg_3pa = total_3pa / games_count if games_count > 0 else 0
            
            opp_fg_pct = round((avg_fgm / avg_fga) * 100, 1) if avg_fga > 0 else 0
            opp_3p_pct = round((avg_3pm / avg_3pa) * 100, 1) if avg_3pa > 0 else 0
            avg_3pm_allowed = round(avg_3pm, 1) if avg_3pm else 0.0
            
            cur.execute("""
                INSERT INTO position_defense_stats (
                    team_id, season, position, games_played,
                    points_allowed_per_game, rebounds_allowed_per_game, assists_allowed_per_game,
                    steals_allowed_per_game, blocks_allowed_per_game, turnovers_forced_per_game,
                    three_pointers_made_allowed_per_game, fg_pct_allowed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, season, position) DO UPDATE SET
                    games_played = EXCLUDED.games_played,
                    points_allowed_per_game = EXCLUDED.points_allowed_per_game,
                    rebounds_allowed_per_game = EXCLUDED.rebounds_allowed_per_game,
                    assists_allowed_per_game = EXCLUDED.assists_allowed_per_game,
                    steals_allowed_per_game = EXCLUDED.steals_allowed_per_game,
                    blocks_allowed_per_game = EXCLUDED.blocks_allowed_per_game,
                    turnovers_forced_per_game = EXCLUDED.turnovers_forced_per_game,
                    three_pointers_made_allowed_per_game = EXCLUDED.three_pointers_made_allowed_per_game,
                    fg_pct_allowed = EXCLUDED.fg_pct_allowed
            """, (team_id, season, position, games_count, avg_points, avg_rebounds, avg_assists,
                  avg_steals, avg_blocks, avg_turnovers, avg_3pm_allowed, opp_fg_pct))
            
            try:
                cur.execute("""
                    UPDATE position_defense_stats
                    SET opp_points_per_game = %s,
                        opp_field_goal_pct = %s,
                        opp_three_point_pct = %s
                    WHERE team_id = %s AND season = %s AND position = %s
                """, (avg_points, opp_fg_pct, opp_3p_pct, team_id, season, position))
            except Exception:
                pass
            
            total_inserts += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("="*50)
    print(f"Updated {total_inserts} team-position combinations for {season}")
    print("="*50)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        update_position_defense_stats_for_yesterday(target_date)
    else:
        update_position_defense_stats_for_yesterday()