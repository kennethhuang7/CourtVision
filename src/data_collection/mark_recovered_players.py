import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime, timedelta

def mark_recovered_players(target_date=None):
    print("Checking for recovered players...\n")
    
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Checking players who played on {target_date}\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT ON (i.player_id) 
            i.injury_id, i.player_id, p.full_name, i.report_date, i.injury_status
        FROM injuries i
        JOIN players p ON i.player_id = p.player_id
        WHERE i.injury_status IN ('Out', 'Day-To-Day', 'Questionable')
        ORDER BY i.player_id, i.report_date DESC, i.injury_id DESC
    """)
    
    injured_players = cur.fetchall()
    
    if len(injured_players) == 0:
        print("No active injuries to check")
        cur.close()
        conn.close()
        return
    
    print(f"Found {len(injured_players)} players with active injuries\n")
    
    recovered = 0
    still_injured = 0
    
    for injury_id, player_id, player_name, injury_date, injury_status in injured_players:
        cur.execute("""
            SELECT COUNT(*) 
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.player_id = %s
            AND g.game_date = %s
            AND g.game_status = 'completed'
            AND pgs.minutes_played > 0
        """, (player_id, target_date))
        
        played = cur.fetchone()[0]
        
        if played > 0:
            cur.execute("""
                SELECT COUNT(DISTINCT g.game_date)
                FROM games g
                WHERE g.game_date >= %s
                AND g.game_date < %s
                AND g.game_status = 'completed'
                AND (g.home_team_id IN (SELECT team_id FROM players WHERE player_id = %s)
                     OR g.away_team_id IN (SELECT team_id FROM players WHERE player_id = %s))
            """, (injury_date, target_date, player_id, player_id))
            
            games_missed = cur.fetchone()[0]
            
            cur.execute("""
                UPDATE injuries SET
                    injury_status = 'Healthy',
                    return_date = %s,
                    games_missed = %s
                WHERE injury_id = %s
            """, (target_date, games_missed, injury_id))
            
            recovered += 1
            try:
                print(f"  [RECOVERED] {player_name} (missed {games_missed} games, was {injury_status})")
            except UnicodeEncodeError:
                sys.stdout.buffer.write(f"  [RECOVERED] {player_name} (missed {games_missed} games, was {injury_status})\n".encode('utf-8'))
        else:
            still_injured += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\n{'='*50}")
    print("RECOVERY CHECK COMPLETE!")
    print(f"{'='*50}")
    print(f"Recovered: {recovered}")
    print(f"Still injured: {still_injured}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        mark_recovered_players(target_date)
    else:
        mark_recovered_players()