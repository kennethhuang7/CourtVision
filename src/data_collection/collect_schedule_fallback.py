import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection, rate_limit
from datetime import datetime
import time
from nba_api.stats.endpoints import scoreboard
import pandas as pd

def collect_schedule_fallback(target_date=None):
    if target_date is None:
        target_date = datetime.now().date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Collecting schedule for {target_date} using nba-api Scoreboard endpoint...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    season_year = target_date.year
    season_month = target_date.month
    
    if season_month >= 10:
        season = f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        season = f"{season_year-1}-{str(season_year)[-2:]}"
    
    try:
        formatted_date = target_date.strftime('%m/%d/%Y')
        print(f"Fetching schedule for {formatted_date}...")
        
        rate_limit()
        time.sleep(1)
        
        max_retries = 3
        scoreboard_data = None
        
        for attempt in range(max_retries):
            try:
                print(f"  Attempt {attempt + 1}/{max_retries}...")
                scoreboard_endpoint = scoreboard.Scoreboard(
                    game_date=formatted_date,
                    league_id='00',
                    timeout=60
                )
                scoreboard_data = scoreboard_endpoint.get_dict()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"  Error, retrying in 10 seconds... (attempt {attempt + 1}/{max_retries})")
                    print(f"  Error: {e}")
                    time.sleep(10)
                else:
                    print(f"  Failed after {max_retries} attempts: {e}")
                    raise
        
        if scoreboard_data is None:
            print("No scoreboard data received")
            cur.close()
            conn.close()
            return
        
        game_header = scoreboard_data['resultSets'][0]
        headers_list = game_header['headers']
        rows = game_header['rowSet']
        
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
            
            home_team_id_val = row[header_map.get('HOME_TEAM_ID', None)]
            away_team_id_val = row[header_map.get('VISITOR_TEAM_ID', None)]
            
            if home_team_id_val is None or away_team_id_val is None:
                game_status_text = str(row[header_map.get('GAME_STATUS_TEXT', 'TBD')])
                if game_status_text == 'TBD':
                    print(f"  Game {game_id} has status 'TBD' - NBA API hasn't populated this game yet")
                    print(f"    This game will be collected automatically when the NBA API updates")
                    continue
                else:
                    print(f"  Game {game_id} has None team IDs, skipping")
                    continue
            
            home_team_id = int(home_team_id_val)
            away_team_id = int(away_team_id_val)
            
            game_status_text = str(row[header_map.get('GAME_STATUS_TEXT', '')])
            
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
        collect_schedule_fallback(target_date)
    else:
        collect_schedule_fallback()

