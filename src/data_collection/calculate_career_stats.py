from nba_api.stats.endpoints import playercareerstats # type: ignore
from utils import get_db_connection, rate_limit
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_career_totals_from_nba_api():
    print("Getting REAL career stats from NBA API...")
    print("This fetches true career totals (entire NBA career)\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT player_id 
        FROM player_game_stats
        ORDER BY player_id
    """)
    
    player_ids = [row[0] for row in cur.fetchall()]
    print(f"Found {len(player_ids)} players to process\n")
    
    inserted = 0
    errors = 0
    
    for i, player_id in enumerate(player_ids, 1):
        try:
            rate_limit(3.0)  
            
            career = playercareerstats.PlayerCareerStats(player_id=player_id)
            career_df = career.get_data_frames()[0]
            
            if len(career_df) == 0:
                print(f"No career data for player {player_id}")
                errors += 1
                continue
            
            total_games = int(career_df['GP'].sum())
            total_points = int(career_df['PTS'].sum())
            total_rebounds = int(career_df['REB'].sum())
            total_assists = int(career_df['AST'].sum())
            total_steals = int(career_df['STL'].sum())
            total_blocks = int(career_df['BLK'].sum())
            
            cur.execute("""
                INSERT INTO player_career_stats (
                    player_id,
                    career_games,
                    career_points,
                    career_rebounds,
                    career_assists,
                    career_steals,
                    career_blocks,
                    updated_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
            """, (
                player_id,
                total_games,
                total_points,
                total_rebounds,
                total_assists,
                total_steals,
                total_blocks
            ))
            
            conn.commit()
            inserted += 1
            
            if i % 50 == 0:
                print(f"Progress: {i}/{len(player_ids)} players processed")
                print(f"Inserted: {inserted}, Errors: {errors}")
            
        except Exception as e:
            errors += 1
            print(f"Error processing player {player_id}: {e}")
            continue
    
    cur.close()
    conn.close()
    
    print("\n" + "="*50)
    print("REAL CAREER STATS COLLECTION COMPLETE!")
    print("="*50)
    print(f"Successfully inserted: {inserted} players")
    print(f"Errors: {errors}")
    
    print("\nTop 10 REAL Career Points:")
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT p.full_name, pcs.career_points, pcs.career_games
        FROM player_career_stats pcs
        JOIN players p ON pcs.player_id = p.player_id
        ORDER BY pcs.career_points DESC
        LIMIT 10
    """)
    
    for i, row in enumerate(cur.fetchall(), 1):
        print(f"{i}. {row[0]}: {row[1]:,} points ({row[2]} games)")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    get_career_totals_from_nba_api()