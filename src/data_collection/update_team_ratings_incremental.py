import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime, timedelta

def update_team_ratings_for_yesterday(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Updating team ratings for teams that played on {target_date}...\n")
    
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
            SELECT games_played, wins, losses, offensive_rating, defensive_rating, pace
            FROM team_ratings
            WHERE team_id = %s AND season = %s
        """, (team_id, season))
        
        existing = cur.fetchone()
        
        if existing:
            old_games = existing[0] or 0
            old_wins = existing[1] or 0
            
            cur.execute("""
                SELECT 
                    SUM(CASE WHEN g.home_team_id = %s THEN g.home_score ELSE g.away_score END) as points_for,
                    SUM(CASE WHEN g.home_team_id = %s THEN g.away_score ELSE g.home_score END) as points_against,
                    SUM(CASE WHEN (g.home_team_id = %s AND g.home_score > g.away_score) OR 
                                  (g.away_team_id = %s AND g.away_score > g.home_score) THEN 1 ELSE 0 END) as wins
                FROM games g
                WHERE g.season = %s
                    AND (g.home_team_id = %s OR g.away_team_id = %s)
                    AND g.game_status = 'completed'
                    AND g.game_type = 'regular_season'
                    AND g.game_date < %s
            """, (team_id, team_id, team_id, team_id, season, team_id, team_id, target_date))
            
            old_game_totals = cur.fetchone()
            if old_game_totals and old_game_totals[0] is not None:
                old_points_for = old_game_totals[0] or 0
                old_points_against = old_game_totals[1] or 0
                old_wins = old_game_totals[2] or 0
            else:
                old_points_for = 0
                old_points_against = 0
                old_wins = 0
            
            cur.execute("""
                SELECT 
                    SUM(pgs.field_goals_attempted) as fga,
                    SUM(pgs.rebounds_offensive) as oreb,
                    SUM(pgs.turnovers) as tov,
                    SUM(pgs.free_throws_attempted) as fta
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.team_id = %s
                    AND g.season = %s
                    AND g.game_status = 'completed'
                    AND g.game_type = 'regular_season'
                    AND g.game_date < %s
            """, (team_id, season, target_date))
            
            old_team_stats = cur.fetchone()
            old_fga = old_team_stats[0] or 0 if old_team_stats else 0
            old_oreb = old_team_stats[1] or 0 if old_team_stats else 0
            old_tov = old_team_stats[2] or 0 if old_team_stats else 0
            old_fta = old_team_stats[3] or 0 if old_team_stats else 0
        else:
            old_games = 0
            old_wins = 0
            old_points_for = 0
            old_points_against = 0
            old_fga = 0
            old_oreb = 0
            old_tov = 0
            old_fta = 0
        
        cur.execute("""
            SELECT 
                CASE WHEN g.home_team_id = %s THEN g.home_score ELSE g.away_score END as points_for,
                CASE WHEN g.home_team_id = %s THEN g.away_score ELSE g.home_score END as points_against,
                CASE WHEN (g.home_team_id = %s AND g.home_score > g.away_score) OR 
                          (g.away_team_id = %s AND g.away_score > g.home_score) THEN 1 ELSE 0 END as win
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date = %s
        """, (team_id, team_id, team_id, team_id, season, team_id, team_id, target_date))
        
        new_games = cur.fetchall()
        
        if len(new_games) == 0:
            continue
        
        new_points_for = sum(row[0] or 0 for row in new_games)
        new_points_against = sum(row[1] or 0 for row in new_games)
        new_wins = sum(row[2] or 0 for row in new_games)
        new_game_count = len(new_games)
        
        cur.execute("""
            SELECT 
                SUM(pgs.field_goals_attempted) as fga,
                SUM(pgs.rebounds_offensive) as oreb,
                SUM(pgs.turnovers) as tov,
                SUM(pgs.free_throws_attempted) as fta
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.team_id = %s
                AND g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date = %s
        """, (team_id, season, target_date))
        
        new_team_stats = cur.fetchone()
        
        if not new_team_stats or not new_team_stats[0]:
            continue
        
        new_fga = new_team_stats[0] or 0
        new_oreb = new_team_stats[1] or 0
        new_tov = new_team_stats[2] or 0
        new_fta = new_team_stats[3] or 0
        
        total_points_for = old_points_for + new_points_for
        total_points_against = old_points_against + new_points_against
        total_fga = old_fga + new_fga
        total_oreb = old_oreb + new_oreb
        total_tov = old_tov + new_tov
        total_fta = old_fta + new_fta
        total_possessions = total_fga - total_oreb + total_tov + 0.44 * total_fta
        games_played = old_games + new_game_count
        wins = old_wins + new_wins
        losses = games_played - wins
        
        if total_possessions == 0:
            continue
        
        offensive_rating = round((total_points_for / total_possessions) * 100, 1)
        defensive_rating = round((total_points_against / total_possessions) * 100, 1)
        net_rating = round(offensive_rating - defensive_rating, 1)
        pace = round(total_possessions / games_played, 1)
        
        cur.execute("""
            INSERT INTO team_ratings (
                team_id, season, games_played, wins, losses,
                offensive_rating, defensive_rating, net_rating, pace
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id, season) DO UPDATE SET
                games_played = EXCLUDED.games_played,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                offensive_rating = EXCLUDED.offensive_rating,
                defensive_rating = EXCLUDED.defensive_rating,
                net_rating = EXCLUDED.net_rating,
                pace = EXCLUDED.pace
        """, (team_id, season, games_played, wins, losses, 
              offensive_rating, defensive_rating, net_rating, pace))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("="*50)
    print(f"Updated {len(teams)} teams for {season}")
    print("="*50)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        update_team_ratings_for_yesterday(target_date)
    else:
        update_team_ratings_for_yesterday()