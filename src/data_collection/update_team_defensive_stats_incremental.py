import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime, timedelta

def update_team_defensive_stats_for_yesterday(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Updating team defensive stats for teams that played on {target_date}...\n")
    
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
    
    for team_id in teams:
        cur.execute("""
            SELECT games_played, opp_points_per_game, opp_rebounds_per_game, opp_assists_per_game,
                   opp_steals_per_game, opp_blocks_per_game, opp_turnovers_per_game,
                   opp_field_goal_pct, opp_three_point_pct, opp_two_point_pct,
                   opp_free_throw_pct, opp_three_point_attempts_pg, opp_free_throw_attempts_pg, opp_free_throw_rate
            FROM team_defensive_stats
            WHERE team_id = %s AND season = %s
        """, (team_id, season))
        
        existing = cur.fetchone()
        
        if existing:
            old_games = existing[0] or 0
            old_ppg = existing[1] or 0
            old_rpg = existing[2] or 0
            old_apg = existing[3] or 0
            old_spg = existing[4] or 0
            old_bpg = existing[5] or 0
            old_tpg = existing[6] or 0
            old_fg_pct = existing[7] or 0
            old_3p_pct = existing[8] or 0
            old_2p_pct = existing[9] or 0
            old_ft_pct = existing[10] or 0
            old_3pa_pg = existing[11] or 0
            old_fta_pg = existing[12] or 0
            old_ft_rate = existing[13] or 0
            
            cur.execute("""
                SELECT 
                    SUM(pgs.points) as points,
                    SUM(pgs.rebounds_total) as rebounds,
                    SUM(pgs.assists) as assists,
                    SUM(pgs.steals) as steals,
                    SUM(pgs.blocks) as blocks,
                    SUM(pgs.turnovers) as turnovers,
                    SUM(pgs.field_goals_made) as fgm,
                    SUM(pgs.field_goals_attempted) as fga,
                    SUM(pgs.three_pointers_made) as "3pm",
                    SUM(pgs.three_pointers_attempted) as "3pa",
                    SUM(pgs.free_throws_made) as ftm,
                    SUM(pgs.free_throws_attempted) as fta
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE g.season = %s
                    AND g.game_status = 'completed'
                    AND g.game_type = 'regular_season'
                    AND pgs.team_id != %s
                    AND (g.home_team_id = %s OR g.away_team_id = %s)
                    AND g.game_date < %s
            """, (season, team_id, team_id, team_id, target_date))
            
            old_opp_stats = cur.fetchone()
            if old_opp_stats and old_opp_stats[7]:
                old_total_points = old_opp_stats[0] or 0
                old_total_rebounds = old_opp_stats[1] or 0
                old_total_assists = old_opp_stats[2] or 0
                old_total_steals = old_opp_stats[3] or 0
                old_total_blocks = old_opp_stats[4] or 0
                old_total_turnovers = old_opp_stats[5] or 0
                old_total_fgm = old_opp_stats[6] or 0
                old_total_fga = old_opp_stats[7] or 0
                old_total_3pm = old_opp_stats[8] or 0
                old_total_3pa = old_opp_stats[9] or 0
                old_total_ftm = old_opp_stats[10] or 0
                old_total_fta = old_opp_stats[11] or 0
            else:
                old_total_points = old_ppg * old_games if old_games > 0 else 0
                old_total_rebounds = old_rpg * old_games if old_games > 0 else 0
                old_total_assists = old_apg * old_games if old_games > 0 else 0
                old_total_steals = old_spg * old_games if old_games > 0 else 0
                old_total_blocks = old_bpg * old_games if old_games > 0 else 0
                old_total_turnovers = old_tpg * old_games if old_games > 0 else 0
                old_total_3pa = old_3pa_pg * old_games if old_games > 0 else 0
                old_total_fta = old_fta_pg * old_games if old_games > 0 else 0
                old_total_fga = old_total_fta / old_ft_rate if old_ft_rate > 0 else (old_total_3pa / 0.35) if old_total_3pa > 0 else 0
                old_total_fgm = (old_fg_pct / 100) * old_total_fga if old_total_fga > 0 else 0
                old_total_3pm = (old_3p_pct / 100) * old_total_3pa if old_total_3pa > 0 else 0
                old_total_ftm = (old_ft_pct / 100) * old_total_fta if old_total_fta > 0 else 0
        else:
            old_games = 0
            old_total_points = 0
            old_total_rebounds = 0
            old_total_assists = 0
            old_total_steals = 0
            old_total_blocks = 0
            old_total_turnovers = 0
            old_total_fgm = 0
            old_total_fga = 0
            old_total_3pm = 0
            old_total_3pa = 0
            old_total_ftm = 0
            old_total_fta = 0
        
        cur.execute("""
            SELECT 
                SUM(pgs.points) as points,
                SUM(pgs.rebounds_total) as rebounds,
                SUM(pgs.assists) as assists,
                SUM(pgs.steals) as steals,
                SUM(pgs.blocks) as blocks,
                SUM(pgs.turnovers) as turnovers,
                SUM(pgs.field_goals_made) as fgm,
                SUM(pgs.field_goals_attempted) as fga,
                SUM(pgs.three_pointers_made) as "3pm",
                SUM(pgs.three_pointers_attempted) as "3pa",
                SUM(pgs.free_throws_made) as ftm,
                SUM(pgs.free_throws_attempted) as fta
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND pgs.team_id != %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_date = %s
        """, (season, team_id, team_id, team_id, target_date))
        
        new_opp_stats = cur.fetchone()
        
        if not new_opp_stats:
            continue
        
        new_total_points = new_opp_stats[0] or 0
        new_total_rebounds = new_opp_stats[1] or 0
        new_total_assists = new_opp_stats[2] or 0
        new_total_steals = new_opp_stats[3] or 0
        new_total_blocks = new_opp_stats[4] or 0
        new_total_turnovers = new_opp_stats[5] or 0
        new_total_fgm = new_opp_stats[6] or 0
        new_total_fga = new_opp_stats[7] or 0
        new_total_3pm = new_opp_stats[8] or 0
        new_total_3pa = new_opp_stats[9] or 0
        new_total_ftm = new_opp_stats[10] or 0
        new_total_fta = new_opp_stats[11] or 0
        
        cur.execute("""
            SELECT COUNT(DISTINCT g.game_id)
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date = %s
        """, (season, team_id, team_id, target_date))
        
        new_games = cur.fetchone()[0] or 0
        
        if new_games == 0:
            continue
        
        total_points = old_total_points + new_total_points
        total_rebounds = old_total_rebounds + new_total_rebounds
        total_assists = old_total_assists + new_total_assists
        total_steals = old_total_steals + new_total_steals
        total_blocks = old_total_blocks + new_total_blocks
        total_turnovers = old_total_turnovers + new_total_turnovers
        total_fgm = old_total_fgm + new_total_fgm
        total_fga = old_total_fga + new_total_fga
        total_3pm = old_total_3pm + new_total_3pm
        total_3pa = old_total_3pa + new_total_3pa
        total_ftm = old_total_ftm + new_total_ftm
        total_fta = old_total_fta + new_total_fta
        games_played = old_games + new_games
        
        opp_ppg = round(total_points / games_played, 1) if games_played > 0 else 0
        opp_rpg = round(total_rebounds / games_played, 1) if games_played > 0 else 0
        opp_apg = round(total_assists / games_played, 1) if games_played > 0 else 0
        opp_spg = round(total_steals / games_played, 1) if games_played > 0 else 0
        opp_bpg = round(total_blocks / games_played, 1) if games_played > 0 else 0
        opp_tpg = round(total_turnovers / games_played, 1) if games_played > 0 else 0
        
        opp_fg_pct = round((total_fgm / total_fga) * 100, 1) if total_fga > 0 else 0
        opp_3p_pct = round((total_3pm / total_3pa) * 100, 1) if total_3pa > 0 else 0
        opp_ft_pct = round((total_ftm / total_fta) * 100, 1) if total_fta > 0 else 0
        
        opp_3pa_pg = round(total_3pa / games_played, 1) if games_played > 0 else 0
        opp_fta_pg = round(total_fta / games_played, 1) if games_played > 0 else 0
        
        two_pm = total_fgm - total_3pm
        two_pa = total_fga - total_3pa
        opp_2p_pct = round((two_pm / two_pa) * 100, 1) if two_pa > 0 else 0
        
        opp_ft_rate = round((total_fta / total_fga), 3) if total_fga > 0 else 0
        
        cur.execute("""
            INSERT INTO team_defensive_stats (
                team_id, season, games_played,
                opp_points_per_game, opp_rebounds_per_game, opp_assists_per_game,
                opp_steals_per_game, opp_blocks_per_game, opp_turnovers_per_game,
                opp_field_goal_pct, opp_three_point_pct, opp_two_point_pct,
                opp_free_throw_pct, opp_three_point_attempts_pg,
                opp_free_throw_attempts_pg, opp_free_throw_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id, season) DO UPDATE SET
                games_played = EXCLUDED.games_played,
                opp_points_per_game = EXCLUDED.opp_points_per_game,
                opp_rebounds_per_game = EXCLUDED.opp_rebounds_per_game,
                opp_assists_per_game = EXCLUDED.opp_assists_per_game,
                opp_steals_per_game = EXCLUDED.opp_steals_per_game,
                opp_blocks_per_game = EXCLUDED.opp_blocks_per_game,
                opp_turnovers_per_game = EXCLUDED.opp_turnovers_per_game,
                opp_field_goal_pct = EXCLUDED.opp_field_goal_pct,
                opp_three_point_pct = EXCLUDED.opp_three_point_pct,
                opp_two_point_pct = EXCLUDED.opp_two_point_pct,
                opp_free_throw_pct = EXCLUDED.opp_free_throw_pct,
                opp_three_point_attempts_pg = EXCLUDED.opp_three_point_attempts_pg,
                opp_free_throw_attempts_pg = EXCLUDED.opp_free_throw_attempts_pg,
                opp_free_throw_rate = EXCLUDED.opp_free_throw_rate
        """, (
            team_id, season, games_played,
            opp_ppg, opp_rpg, opp_apg, opp_spg, opp_bpg, opp_tpg,
            opp_fg_pct, opp_3p_pct, opp_2p_pct, opp_ft_pct,
            opp_3pa_pg, opp_fta_pg, opp_ft_rate
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("="*50)
    print(f"Updated {len(teams)} teams defensive stats for {season}")
    print("="*50)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        update_team_defensive_stats_for_yesterday(target_date)
    else:
        update_team_defensive_stats_for_yesterday()