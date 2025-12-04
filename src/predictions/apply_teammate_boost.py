import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection

def get_injured_stars(game_date, season='2025-26'):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT i.player_id, p.full_name
        FROM injuries i
        JOIN players p ON i.player_id = p.player_id
        WHERE i.injury_status = 'Out'
        AND i.report_date = (
            SELECT MAX(report_date)
            FROM injuries i2
            WHERE i2.player_id = i.player_id
        )
        AND EXISTS (
            SELECT 1 
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.player_id = i.player_id
            AND g.season = %s
            GROUP BY pgs.player_id
            HAVING AVG(pgs.points) >= 20
        )
    """, (season,))
    
    injured_stars = cur.fetchall()
    cur.close()
    conn.close()
    
    return injured_stars

def get_teammate_boosts(player_id, season='2025-26'):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            player_id,
            ppg_boost,
            rpg_boost,
            apg_boost
        FROM teammate_dependency
        WHERE teammate_id = %s
        AND season = %s
    """, (player_id, season))
    
    boosts = {}
    for row in cur.fetchall():
        teammate_id, ppg_boost, rpg_boost, apg_boost = row
        boosts[teammate_id] = {
            'ppg_boost': float(ppg_boost),
            'rpg_boost': float(rpg_boost),
            'apg_boost': float(apg_boost)
        }
    
    cur.close()
    conn.close()
    
    return boosts

def apply_boosts_to_predictions(predictions_df, game_date, season='2025-26'):
    print("\nChecking for injured stars and applying teammate boosts...")
    
    injured_stars = get_injured_stars(game_date, season)
    
    if not injured_stars:
        print("No injured star players found")
        return predictions_df
    
    print(f"Found {len(injured_stars)} injured star players:")
    
    total_adjustments = 0
    
    for star_id, star_name in injured_stars:
        print(f"  {star_name} is out")
        
        boosts = get_teammate_boosts(star_id, season)
        
        if not boosts:
            print(f"    No teammate dependencies found")
            continue
        
        print(f"    Found {len(boosts)} teammates with usage boosts")
        
        for idx, row in predictions_df.iterrows():
            player_id = row['player_id']
            
            if player_id in boosts:
                boost = boosts[player_id]
                
                predictions_df.at[idx, 'predicted_points'] += boost['ppg_boost']
                predictions_df.at[idx, 'predicted_rebounds'] += boost['rpg_boost']
                predictions_df.at[idx, 'predicted_assists'] += boost['apg_boost']
                
                total_adjustments += 1
                
                print(f"      Adjusted {row['player_name']}: {boost['ppg_boost']:+.1f} PPG, {boost['rpg_boost']:+.1f} RPG, {boost['apg_boost']:+.1f} APG")
    
    print(f"\nApplied {total_adjustments} teammate boost adjustments\n")
    
    return predictions_df