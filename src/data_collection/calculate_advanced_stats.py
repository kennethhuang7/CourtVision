from utils import get_db_connection
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def calculate_advanced_stats():
    print("Calculating advanced stats for all player game stats...")
    print("This may take 2-5 minutes for 90,000+ records...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("Step 1/3: Calculating True Shooting %...")
    cur.execute("""
        UPDATE player_game_stats
        SET true_shooting_pct = CASE 
            WHEN (field_goals_attempted + 0.44 * free_throws_attempted) > 0 
            THEN ROUND(
                (points::decimal / (2 * (field_goals_attempted + 0.44 * free_throws_attempted))) * 100, 
                1
            )
            ELSE NULL
        END
        WHERE true_shooting_pct IS NULL
    """)
    conn.commit()
    print(f"True Shooting % calculated for {cur.rowcount} records")
    
    print("\nStep 2/3: Calculating Usage Rate...")
    cur.execute("""
        WITH team_totals AS (
            SELECT 
                game_id,
                team_id,
                SUM(field_goals_attempted) as team_fga,
                SUM(free_throws_attempted) as team_fta,
                SUM(turnovers) as team_to,
                SUM(minutes_played) as team_minutes
            FROM player_game_stats
            GROUP BY game_id, team_id
        )
        UPDATE player_game_stats pgs
        SET usage_rate = CASE
            WHEN pgs.minutes_played > 0 AND tt.team_minutes > 0 
                AND (tt.team_fga + 0.44 * tt.team_fta + tt.team_to) > 0
            THEN ROUND(
                (100.0 * ((pgs.field_goals_attempted + 0.44 * pgs.free_throws_attempted + pgs.turnovers) 
                    * (tt.team_minutes / 5.0)) 
                / (pgs.minutes_played * (tt.team_fga + 0.44 * tt.team_fta + tt.team_to))),
                1
            )
            ELSE NULL
        END
        FROM team_totals tt
        WHERE pgs.game_id = tt.game_id 
            AND pgs.team_id = tt.team_id
            AND pgs.usage_rate IS NULL
    """)
    conn.commit()
    print(f"Usage Rate calculated for {cur.rowcount} records")
    
    print("\nStep 3/3: Calculating Offensive & Defensive Ratings...")
    cur.execute("""
        WITH game_possessions AS (
            SELECT 
                g.game_id,
                g.home_team_id,
                g.away_team_id,
                (SUM(pgs.field_goals_attempted) - SUM(pgs.rebounds_offensive) + SUM(pgs.turnovers) 
                    + 0.44 * SUM(pgs.free_throws_attempted)) as possessions
            FROM games g
            JOIN player_game_stats pgs ON g.game_id = pgs.game_id
            GROUP BY g.game_id, g.home_team_id, g.away_team_id
        ),
        team_stats AS (
            SELECT 
                pgs.game_id,
                pgs.team_id,
                SUM(pgs.points) as team_points,
                gp.possessions
            FROM player_game_stats pgs
            JOIN game_possessions gp ON pgs.game_id = gp.game_id
            GROUP BY pgs.game_id, pgs.team_id, gp.possessions
        ),
        opponent_stats AS (
            SELECT 
                ts1.game_id,
                ts1.team_id,
                ts2.team_points as opp_points,
                ts1.possessions
            FROM team_stats ts1
            JOIN team_stats ts2 ON ts1.game_id = ts2.game_id AND ts1.team_id != ts2.team_id
        )
        UPDATE player_game_stats pgs
        SET 
            offensive_rating = CASE 
                WHEN pgs.minutes_played > 0 AND ts.possessions > 0
                THEN ROUND((pgs.points / (ts.possessions / 240.0 * pgs.minutes_played)) * 100, 1)
                ELSE NULL
            END,
            defensive_rating = CASE
                WHEN pgs.minutes_played > 0 AND os.possessions > 0
                THEN ROUND((os.opp_points / (os.possessions / 240.0 * pgs.minutes_played)) * 100, 1)
                ELSE NULL
            END
        FROM team_stats ts
        JOIN opponent_stats os ON ts.game_id = os.game_id AND ts.team_id = os.team_id
        WHERE pgs.game_id = ts.game_id 
            AND pgs.team_id = ts.team_id
            AND (pgs.offensive_rating IS NULL OR pgs.defensive_rating IS NULL)
    """)
    conn.commit()
    print(f"Offensive & Defensive Ratings calculated for {cur.rowcount} records")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*50)
    print("ADVANCED STATS CALCULATION COMPLETE!")
    print("="*50)
    
    print("\nVerifying results...")
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(true_shooting_pct) as has_ts,
            COUNT(usage_rate) as has_usg,
            COUNT(offensive_rating) as has_ortg,
            COUNT(defensive_rating) as has_drtg
        FROM player_game_stats
    """)
    
    result = cur.fetchone()
    total, has_ts, has_usg, has_ortg, has_drtg = result
    
    print(f"\nTotal records: {total:,}")
    print(f"True Shooting %: {has_ts:,} ({100*has_ts/total:.1f}%)")
    print(f"Usage Rate: {has_usg:,} ({100*has_usg/total:.1f}%)")
    print(f"Offensive Rating: {has_ortg:,} ({100*has_ortg/total:.1f}%)")
    print(f"Defensive Rating: {has_drtg:,} ({100*has_drtg/total:.1f}%)")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    calculate_advanced_stats()