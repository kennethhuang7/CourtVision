from nba_api.stats.endpoints import leaguegamefinder
from utils import get_db_connection, rate_limit
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def collect_games(season='2024-25'):
    print(f"Collecting games for {season} season...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT team_id FROM teams")
    valid_team_ids = set([row[0] for row in cur.fetchall()])
    
    rate_limit()
    
    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable='00'
    )
    
    games_df = gamefinder.get_data_frames()[0]
    
    games_dict = {}
    
    for idx, row in games_df.iterrows():
        game_id = str(row['GAME_ID'])
        game_date = str(row['GAME_DATE'])
        team_id = int(row['TEAM_ID'])
        matchup = str(row['MATCHUP'])
        
        if team_id not in valid_team_ids:
            continue
        
        if game_id not in games_dict:
            games_dict[game_id] = {
                'game_date': game_date,
                'season': season,
                'teams': []
            }
        
        games_dict[game_id]['teams'].append({
            'team_id': team_id,
            'matchup': matchup,
            'score': int(row['PTS']) if pd.notna(row['PTS']) else None
        })
    
    count = 0
    skipped = 0
    
    for game_id, game_data in games_dict.items():
        teams = game_data['teams']
        
        if len(teams) != 2:
            skipped += 1
            continue
        
        team1 = teams[0]
        team2 = teams[1]
        
        if team1['team_id'] not in valid_team_ids or team2['team_id'] not in valid_team_ids:
            skipped += 1
            continue
        
        if 'vs.' in team1['matchup']:
            home_team_id = team1['team_id']
            away_team_id = team2['team_id']
            home_score = team1['score']
            away_score = team2['score']
        else:
            home_team_id = team2['team_id']
            away_team_id = team1['team_id']
            home_score = team2['score']
            away_score = team1['score']
        
        game_status = 'completed' if home_score is not None else 'scheduled'
        
        if game_id.startswith('004'):
            game_type = 'playoffs'
        elif game_id.startswith('005'):
            game_type = 'play_in'
        else:
            game_type = 'regular_season'
        
        try:
            cur.execute("""
                INSERT INTO games (game_id, game_date, season, home_team_id, away_team_id,
                                 home_score, away_score, game_status, game_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    game_status = EXCLUDED.game_status,
                    game_type = EXCLUDED.game_type
            """, (
                game_id, game_data['game_date'], season,
                home_team_id, away_team_id,
                home_score, away_score, game_status, game_type
            ))
            
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} games...")
                conn.commit()
                
        except Exception as e:
            print(f"Error inserting game {game_id}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Successfully inserted {count} games for {season}!")
    print(f"Skipped {skipped} special games (All-Star, etc.)")

if __name__ == "__main__":
    seasons = ['2022-23', '2023-24', '2024-25']
    
    for season in seasons:
        collect_games(season)
        print(f"Completed {season}\n")