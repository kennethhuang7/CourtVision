from utils import get_db_connection
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def calculate_team_defensive_stats():
    print("Calculating team defensive stats (opponent stats allowed)...")
    print("This shows what teams ALLOW opponents to do against them\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT g.season, g.home_team_id as team_id
        FROM games g
        WHERE g.game_status = 'completed'
        UNION
        SELECT DISTINCT g.season, g.away_team_id as team_id
        FROM games g
        WHERE g.game_status = 'completed'
        ORDER BY season, team_id
    """)
    
    season_teams = cur.fetchall()
    print(f"Found {len(season_teams)} season-team combinations to process\n")
    
    count = 0
    
    for season, team_id in season_teams:
        cur.execute("""
            SELECT 
                pgs.points,
                pgs.rebounds_total,
                pgs.assists,
                pgs.steals,
                pgs.blocks,
                pgs.turnovers,
                pgs.field_goals_made,
                pgs.field_goals_attempted,
                pgs.three_pointers_made,
                pgs.three_pointers_attempted,
                pgs.free_throws_made,
                pgs.free_throws_attempted
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND pgs.team_id != %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
        """, (season, team_id, team_id, team_id))
        
        opponent_stats = cur.fetchall()
        
        if len(opponent_stats) == 0:
            continue
        
        cur.execute("""
            SELECT COUNT(DISTINCT g.game_id)
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
        """, (season, team_id, team_id))
        games_played = cur.fetchone()[0]
        
        total_points = sum(row[0] or 0 for row in opponent_stats)
        total_rebounds = sum(row[1] or 0 for row in opponent_stats)
        total_assists = sum(row[2] or 0 for row in opponent_stats)
        total_steals = sum(row[3] or 0 for row in opponent_stats)
        total_blocks = sum(row[4] or 0 for row in opponent_stats)
        total_turnovers = sum(row[5] or 0 for row in opponent_stats)
        total_fgm = sum(row[6] or 0 for row in opponent_stats)
        total_fga = sum(row[7] or 0 for row in opponent_stats)
        total_3pm = sum(row[8] or 0 for row in opponent_stats)
        total_3pa = sum(row[9] or 0 for row in opponent_stats)
        total_ftm = sum(row[10] or 0 for row in opponent_stats)
        total_fta = sum(row[11] or 0 for row in opponent_stats)
        
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
        
        try:
            cur.execute("""
                INSERT INTO team_defensive_stats (
                    team_id,
                    season,
                    games_played,
                    opp_points_per_game,
                    opp_rebounds_per_game,
                    opp_assists_per_game,
                    opp_steals_per_game,
                    opp_blocks_per_game,
                    opp_turnovers_per_game,
                    opp_field_goal_pct,
                    opp_three_point_pct,
                    opp_two_point_pct,
                    opp_free_throw_pct,
                    opp_three_point_attempts_pg,
                    opp_free_throw_attempts_pg,
                    opp_free_throw_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                team_id,
                season,
                games_played,
                opp_ppg,
                opp_rpg,
                opp_apg,
                opp_spg,
                opp_bpg,
                opp_tpg,
                opp_fg_pct,
                opp_3p_pct,
                opp_2p_pct,
                opp_ft_pct,
                opp_3pa_pg,
                opp_fta_pg,
                opp_ft_rate
            ))
            
            count += 1
            
            if count % 30 == 0:
                print(f"Processed {count}/{len(season_teams)} season-teams...")
                conn.commit()
                
        except Exception as e:
            print(f"Error inserting team {team_id} season {season}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("\n" + "="*50)
    print("TEAM DEFENSIVE STATS CALCULATION COMPLETE!")
    print("="*50)
    print(f"Calculated defensive stats for {count} season-teams")
    
    print("\nTop 5 Defenses by Points Allowed (2024-25):")
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT t.full_name, tds.opp_points_per_game, tds.opp_field_goal_pct, tds.opp_three_point_pct
        FROM team_defensive_stats tds
        JOIN teams t ON tds.team_id = t.team_id
        WHERE tds.season = '2024-25'
        ORDER BY tds.opp_points_per_game ASC
        LIMIT 5
    """)
    
    for i, row in enumerate(cur.fetchall(), 1):
        print(f"{i}. {row[0]}: {row[1]} PPG allowed (FG%: {row[2]}, 3P%: {row[3]})")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    calculate_team_defensive_stats()