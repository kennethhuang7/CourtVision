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
            SELECT COUNT(DISTINCT g.game_id) as games_played,
                   SUM(CASE WHEN g.home_team_id = %s THEN 1 WHEN g.home_score > g.away_score THEN 1 
                            WHEN g.away_team_id = %s THEN 1 WHEN g.away_score > g.home_score THEN 1 
                            ELSE 0 END) as wins
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
        """, (team_id, team_id, season, team_id, team_id))
        
        games_wins = cur.fetchone()
        games_played = games_wins[0]
        wins = games_wins[1]
        losses = games_played - wins
        
        cur.execute("""
            SELECT 
                SUM(CASE WHEN g.home_team_id = %s THEN g.home_score ELSE g.away_score END) as points_for,
                SUM(CASE WHEN g.home_team_id = %s THEN g.away_score ELSE g.home_score END) as points_against,
                COUNT(*) as game_count
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
        """, (team_id, team_id, season, team_id, team_id))
        
        result = cur.fetchone()
        points_for = result[0] or 0
        points_against = result[1] or 0
        game_count = result[2] or 0
        
        if game_count == 0:
            continue
        
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
        """, (team_id, season))
        
        team_stats = cur.fetchone()
        
        if not team_stats or not team_stats[0]:
            continue
        
        fga = team_stats[0] or 0
        oreb = team_stats[1] or 0
        tov = team_stats[2] or 0
        fta = team_stats[3] or 0
        
        possessions = fga - oreb + tov + 0.44 * fta
        
        if possessions == 0:
            continue
        
        offensive_rating = round((points_for / possessions) * 100, 1)
        defensive_rating = round((points_against / possessions) * 100, 1)
        net_rating = round(offensive_rating - defensive_rating, 1)
        pace = round(possessions / game_count, 1)
        
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