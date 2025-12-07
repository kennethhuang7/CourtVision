import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection, rate_limit
from datetime import datetime
import requests
import time

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
            home_team_id = int(row[header_map['HOME_TEAM_ID']])
            away_team_id = int(row[header_map['VISITOR_TEAM_ID']])
            game_status_text = str(row[header_map['GAME_STATUS_TEXT']])
            
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