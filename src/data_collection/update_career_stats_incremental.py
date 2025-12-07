import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_api.stats.endpoints import playercareerstats
from utils import get_db_connection, ensure_connection
import time
from datetime import datetime, timedelta

def update_career_stats_for_yesterday(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Updating career stats for players who played on {target_date}...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT pgs.player_id, p.full_name
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        JOIN players p ON pgs.player_id = p.player_id
        WHERE g.game_date = %s
    """, (target_date,))
    
    players = cur.fetchall()
    
    if len(players) == 0:
        print(f"No players found for {target_date}")
        cur.close()
        conn.close()
        return
    
    print(f"Found {len(players)} players to update\n")
    
    updated = 0
    errors = 0
    warnings = 0
    
    for player_id, full_name in players:
        try:
            conn, cur = ensure_connection(conn, cur)
            
            cur.execute("""
                SELECT career_points, career_rebounds, career_assists, career_games
                FROM player_career_stats
                WHERE player_id = %s
            """, (player_id,))
            
            old_stats = cur.fetchone()
            
            time.sleep(0.6)
            
            career = playercareerstats.PlayerCareerStats(player_id=player_id, per_mode36='Totals')
            career_totals = career.get_data_frames()[0]
            
            if len(career_totals) == 0:
                errors += 1
                continue
            
            regular_season = career_totals[career_totals['SEASON_ID'].str.contains('2', na=False)]
            
            if len(regular_season) == 0:
                errors += 1
                continue
            
            total_points = int(regular_season['PTS'].sum())
            total_rebounds = int(regular_season['REB'].sum())
            total_assists = int(regular_season['AST'].sum())
            total_games = int(regular_season['GP'].sum())
            total_steals = int(regular_season['STL'].sum())
            total_blocks = int(regular_season['BLK'].sum())
            
            conn, cur = ensure_connection(conn, cur)
            
            cur.execute("""
                INSERT INTO player_career_stats (
                    player_id, career_points, career_rebounds, career_assists,
                    career_steals, career_blocks, career_games
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    career_points = EXCLUDED.career_points,
                    career_rebounds = EXCLUDED.career_rebounds,
                    career_assists = EXCLUDED.career_assists,
                    career_steals = EXCLUDED.career_steals,
                    career_blocks = EXCLUDED.career_blocks,
                    career_games = EXCLUDED.career_games,
                    updated_date = CURRENT_DATE
            """, (
                player_id,
                total_points,
                total_rebounds,
                total_assists,
                total_steals,
                total_blocks,
                total_games
            ))
            
            if old_stats:
                old_points = old_stats[0] or 0
                difference = total_points - old_points
                
                if abs(difference) > 100:
                    status = 'WARNING'
                    warnings += 1
                    print(f"  WARNING: {full_name} - difference of {difference} points")
                else:
                    status = 'OK'
                
                conn, cur = ensure_connection(conn, cur)
                
                cur.execute("""
                    INSERT INTO career_stats_validation (
                        player_id, check_date, our_total, nba_api_total, difference, status
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (player_id, target_date, old_points, total_points, difference, status))
            
            updated += 1
            
            if updated % 10 == 0:
                print(f"Progress: {updated}/{len(players)} players updated")
                try:
                    conn.commit()
                except Exception as commit_error:
                    print(f"  Commit error, reconnecting: {commit_error}")
                    conn, cur = ensure_connection(conn, cur)
                    conn.commit()
            
        except Exception as e:
            print(f"Error processing {full_name}: {e}")
            errors += 1
            if "connection" in str(e).lower() or "cursor" in str(e).lower():
                conn, cur = ensure_connection(conn, cur)
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("\n" + "="*50)
    print("CAREER STATS UPDATE COMPLETE!")
    print("="*50)
    print(f"Updated: {updated}")
    print(f"Errors: {errors}")
    print(f"Warnings: {warnings}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        update_career_stats_for_yesterday(target_date)
    else:
        update_career_stats_for_yesterday()