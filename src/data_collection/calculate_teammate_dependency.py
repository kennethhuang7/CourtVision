import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime

def calculate_dependency_for_player(star_player_id, season='2025-26'):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT p.full_name, AVG(pgs.points) as ppg
        FROM players p
        JOIN player_game_stats pgs ON p.player_id = pgs.player_id
        JOIN games g ON pgs.game_id = g.game_id
        WHERE p.player_id = %s 
        AND g.season = %s
        AND pgs.minutes_played >= 15
        GROUP BY p.player_id, p.full_name
    """, (star_player_id, season))
    
    result = cur.fetchone()
    if not result:
        print(f"Player {star_player_id} not found")
        cur.close()
        conn.close()
        return
    
    star_name, star_ppg = result
    star_ppg = float(star_ppg)
    
    if star_ppg < 20:
        print(f"{star_name} ({star_ppg:.1f} PPG) is not a star player (< 20 PPG)")
        cur.close()
        conn.close()
        return
    
    print(f"\nCalculating dependencies for {star_name} ({star_ppg:.1f} PPG)...")
    
    cur.execute("""
        SELECT pgs.team_id
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = %s AND g.season = %s
        ORDER BY g.game_date DESC
        LIMIT 1
    """, (star_player_id, season))
    
    team_result = cur.fetchone()
    if not team_result:
        cur.close()
        conn.close()
        return
    
    team_id = team_result[0]
    
    cur.execute("""
        SELECT DISTINCT pgs.player_id, p.full_name
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        JOIN players p ON pgs.player_id = p.player_id
        WHERE pgs.team_id = %s
        AND g.season = %s
        AND pgs.player_id != %s
        AND pgs.minutes_played >= 15
    """, (team_id, season, star_player_id))
    
    teammates = cur.fetchall()
    
    print(f"Found {len(teammates)} teammates")
    
    dependencies_found = 0
    
    for teammate_id, teammate_name in teammates:
        
        cur.execute("""
            SELECT 
                COUNT(*) as games,
                AVG(pgs.points) as ppg,
                AVG(pgs.rebounds_total) as rpg,
                AVG(pgs.assists) as apg
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.player_id = %s
            AND g.season = %s
            AND pgs.minutes_played >= 15
            AND EXISTS (
                SELECT 1 FROM player_game_stats pgs2
                WHERE pgs2.game_id = pgs.game_id
                AND pgs2.player_id = %s
                AND pgs2.minutes_played >= 15
            )
        """, (teammate_id, season, star_player_id))
        
        with_stats = cur.fetchone()
        games_with = with_stats[0] if with_stats else 0
        ppg_with = float(with_stats[1]) if with_stats and with_stats[1] else 0.0
        rpg_with = float(with_stats[2]) if with_stats and with_stats[2] else 0.0
        apg_with = float(with_stats[3]) if with_stats and with_stats[3] else 0.0
        
        cur.execute("""
            SELECT 
                COUNT(*) as games,
                AVG(pgs.points) as ppg,
                AVG(pgs.rebounds_total) as rpg,
                AVG(pgs.assists) as apg
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.player_id = %s
            AND g.season = %s
            AND pgs.minutes_played >= 15
            AND NOT EXISTS (
                SELECT 1 FROM player_game_stats pgs2
                WHERE pgs2.game_id = pgs.game_id
                AND pgs2.player_id = %s
                AND pgs2.minutes_played >= 15
            )
        """, (teammate_id, season, star_player_id))
        
        without_stats = cur.fetchone()
        games_without = without_stats[0] if without_stats else 0
        ppg_without = float(without_stats[1]) if without_stats and without_stats[1] else 0.0
        rpg_without = float(without_stats[2]) if without_stats and without_stats[2] else 0.0
        apg_without = float(without_stats[3]) if without_stats and without_stats[3] else 0.0
        
        if games_without < 3:
            continue
        
        ppg_boost = ppg_without - ppg_with
        rpg_boost = rpg_without - rpg_with
        apg_boost = apg_without - apg_with
        
        if abs(ppg_boost) < 2.0:
            continue
        
        cur.execute("""
            INSERT INTO teammate_dependency (
                player_id, teammate_id, season,
                games_with_teammate, games_without_teammate,
                ppg_with, ppg_without, ppg_boost,
                rpg_with, rpg_without, rpg_boost,
                apg_with, apg_without, apg_boost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id, teammate_id, season)
            DO UPDATE SET
                games_with_teammate = EXCLUDED.games_with_teammate,
                games_without_teammate = EXCLUDED.games_without_teammate,
                ppg_with = EXCLUDED.ppg_with,
                ppg_without = EXCLUDED.ppg_without,
                ppg_boost = EXCLUDED.ppg_boost,
                rpg_with = EXCLUDED.rpg_with,
                rpg_without = EXCLUDED.rpg_without,
                rpg_boost = EXCLUDED.rpg_boost,
                apg_with = EXCLUDED.apg_with,
                apg_without = EXCLUDED.apg_without,
                apg_boost = EXCLUDED.apg_boost
        """, (
            teammate_id, star_player_id, season,
            games_with, games_without,
            ppg_with, ppg_without, ppg_boost,
            rpg_with, rpg_without, rpg_boost,
            apg_with, apg_without, apg_boost
        ))
        
        dependencies_found += 1
        print(f"  {teammate_name}: {ppg_boost:+.1f} PPG, {rpg_boost:+.1f} RPG, {apg_boost:+.1f} APG (without: {games_without} games)")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\nFound {dependencies_found} significant dependencies")
    return dependencies_found

if __name__ == "__main__":
    if len(sys.argv) > 1:
        player_id = int(sys.argv[1])
        season = sys.argv[2] if len(sys.argv) > 2 else '2025-26'
        calculate_dependency_for_player(player_id, season)
    else:
        print("Usage: python calculate_teammate_dependency.py <player_id> [season]")