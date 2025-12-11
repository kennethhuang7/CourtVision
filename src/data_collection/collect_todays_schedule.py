import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection, rate_limit
from datetime import datetime
import requests
import time
from nba_api.stats.endpoints import leaguegamefinder
import pandas as pd

def collect_schedule(target_date=None):
    if target_date is None:
        target_date = datetime.now().date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Collecting schedule for {target_date}...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    season_year = target_date.year
    season_month = target_date.month
    
    if season_month >= 10:
        season = f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        season = f"{season_year-1}-{str(season_year)[-2:]}"
    
    try:
        time.sleep(1)
        
        formatted_date = target_date.strftime('%m/%d/%Y')
        print(f"Fetching schedule for {formatted_date}...")
        
        url = "https://stats.nba.com/stats/scoreboardv2"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nba.com/',
            'Origin': 'https://www.nba.com'
        }
        
        params = {
            'GameDate': formatted_date,
            'LeagueID': '00',
            'DayOffset': '0'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        game_headers = data['resultSets'][0]  
        headers_list = game_headers['headers']
        rows = game_headers['rowSet']
        
        if len(rows) > 0:
            print(f"Sample game data (first game):")
            sample_row = rows[0]
            for header, idx in zip(headers_list, range(len(headers_list))):
                print(f"  {header}: {sample_row[idx]}")
            print()
        
        if len(rows) == 0:
            print(f"No games found for {target_date}")
            print("This likely means it's an off-day for the NBA")
            cur.close()
            conn.close()
            return
        
        print(f"Found {len(rows)} games\n")
        
        scheduled_count = 0
        completed_count = 0
        in_progress_count = 0
        
        header_map = {header: idx for idx, header in enumerate(headers_list)}
        
        for row in rows:
            game_id = str(row[header_map['GAME_ID']])
            
            home_team_id_val = row[header_map['HOME_TEAM_ID']]
            away_team_id_val = row[header_map['VISITOR_TEAM_ID']]
            
            game_status_text = str(row[header_map['GAME_STATUS_TEXT']])
            
            if home_team_id_val is None or away_team_id_val is None:
                if game_status_text == 'TBD':
                    print(f"  Game {game_id} has status 'TBD' with no team IDs - NBA API hasn't populated this game yet")
                    print(f"    This game will be collected automatically when the NBA API updates (usually closer to game time)")
                    continue
                else:
                    print(f"  Game {game_id} has None team IDs but status is '{game_status_text}', trying LeagueGameFinder...")
                    try:
                        rate_limit()
                        gamefinder = leaguegamefinder.LeagueGameFinder(
                            game_id_nullable=game_id,
                            league_id_nullable='00'
                        )
                        games_df = gamefinder.get_data_frames()[0]
                        
                        if len(games_df) >= 2:
                            teams = games_df['TEAM_ID'].unique()
                            if len(teams) == 2:
                                matchup1 = games_df.iloc[0]['MATCHUP']
                                if 'vs.' in matchup1:
                                    home_team_id = int(games_df.iloc[0]['TEAM_ID'])
                                    away_team_id = int(games_df.iloc[1]['TEAM_ID'])
                                else:
                                    home_team_id = int(games_df.iloc[1]['TEAM_ID'])
                                    away_team_id = int(games_df.iloc[0]['TEAM_ID'])
                                print(f"    Found teams: {away_team_id} @ {home_team_id}")
                            else:
                                print(f"    Could not determine teams, skipping")
                                continue
                        else:
                            print(f"    LeagueGameFinder returned insufficient data, skipping")
                            continue
                    except Exception as e:
                        print(f"    LeagueGameFinder failed, skipping game")
                        continue
            else:
                home_team_id = int(home_team_id_val)
                away_team_id = int(away_team_id_val)
            
            home_abbr = 'HOME'
            away_abbr = 'AWAY'
            if 'HOME_TEAM_ABBREVIATION' in header_map:
                home_abbr = str(row[header_map['HOME_TEAM_ABBREVIATION']])
            if 'VISITOR_TEAM_ABBREVIATION' in header_map:
                away_abbr = str(row[header_map['VISITOR_TEAM_ABBREVIATION']])
            
            if 'pm' in game_status_text.lower() or 'am' in game_status_text.lower():
                status = 'scheduled'
                scheduled_count += 1
                home_score = None
                away_score = None
            elif 'final' in game_status_text.lower():
                status = 'completed'
                completed_count += 1
                home_score = None
                away_score = None
                if 'PTS_HOME' in header_map:
                    try:
                        home_score = int(row[header_map['PTS_HOME']])
                    except (ValueError, TypeError):
                        pass
                if 'PTS_AWAY' in header_map:
                    try:
                        away_score = int(row[header_map['PTS_AWAY']])
                    except (ValueError, TypeError):
                        pass
            else:
                status = 'in_progress'
                in_progress_count += 1
                home_score = None
                away_score = None
            
            cur.execute("""
                INSERT INTO games (
                    game_id, game_date, season, home_team_id, away_team_id,
                    home_score, away_score, game_status, game_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    game_status = EXCLUDED.game_status,
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score
            """, (
                game_id,
                target_date,
                season,
                home_team_id,
                away_team_id,
                home_score,
                away_score,
                status,
                'regular_season'
            ))
            
            print(f"  {away_abbr} @ {home_abbr} - {status} ({game_status_text})")
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"\n{'='*50}")
        print(f"Scheduled: {scheduled_count}")
        print(f"In Progress: {in_progress_count}")
        print(f"Completed: {completed_count}")
        print(f"Total: {len(rows)}")
        print(f"{'='*50}")
        
        if scheduled_count > 0:
            print(f"\nFound {scheduled_count} games ready for predictions")
        
    except Exception as e:
        print(f"Error collecting schedule: {e}")
        import traceback
        traceback.print_exc()
        try:
            cur.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        collect_schedule(target_date)
    else:
        collect_schedule()
