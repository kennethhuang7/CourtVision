import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection, rate_limit
from datetime import datetime
import requests
import time
from bs4 import BeautifulSoup
import re

# RUN THIS:
# python src/data_collection/collect_schedule_html.py

ESPN_TO_DB_ABBREV = {
    'GS': 'GSW', 'gs': 'GSW',
    'WSH': 'WAS', 'wsh': 'WAS',
    'NO': 'NOP', 'no': 'NOP',
    'NY': 'NYK', 'ny': 'NYK',
    'SA': 'SAS', 'sa': 'SAS',
    'UTAH': 'UTA', 'utah': 'UTA',
    'SUNS': 'PHX', 'suns': 'PHX',
    'KINGS': 'SAC', 'kings': 'SAC',
    'WARRIORS': 'GSW', 'warriors': 'GSW',
    'LAKERS': 'LAL', 'lakers': 'LAL',
    'CLIPPERS': 'LAC', 'clippers': 'LAC',
    'HEAT': 'MIA', 'heat': 'MIA',
    'BUCKS': 'MIL', 'bucks': 'MIL',
    'TIMBERWOLVES': 'MIN', 'timberwolves': 'MIN',
    'PELICANS': 'NOP', 'pelicans': 'NOP',
    'THUNDER': 'OKC', 'thunder': 'OKC',
    'MAGIC': 'ORL', 'magic': 'ORL',
    'SIXERS': 'PHI', 'sixers': 'PHI',
    'BLAZERS': 'POR', 'blazers': 'POR',
    'SPURS': 'SAS', 'spurs': 'SAS',
    'RAPTORS': 'TOR', 'raptors': 'TOR',
    'JAZZ': 'UTA', 'jazz': 'UTA',
    'WIZARDS': 'WAS', 'wizards': 'WAS',
    'HAWKS': 'ATL', 'hawks': 'ATL',
    'NETS': 'BKN', 'nets': 'BKN',
    'CELTICS': 'BOS', 'celtics': 'BOS',
    'HORNETS': 'CHA', 'hornets': 'CHA',
    'BULLS': 'CHI', 'bulls': 'CHI',
    'CAVALIERS': 'CLE', 'cavaliers': 'CLE',
    'MAVERICKS': 'DAL', 'mavericks': 'DAL',
    'NUGGETS': 'DEN', 'nuggets': 'DEN',
    'PISTONS': 'DET', 'pistons': 'DET',
    'ROCKETS': 'HOU', 'rockets': 'HOU',
    'PACERS': 'IND', 'pacers': 'IND',
    'GRIZZLIES': 'MEM', 'grizzlies': 'MEM'
}

DB_ABBREVS = {'ATL', 'BKN', 'BOS', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'}

def get_team_id_from_abbreviation(cur, abbreviation):
    abbrev = abbreviation.upper()
    
    if abbrev in ESPN_TO_DB_ABBREV:
        abbrev = ESPN_TO_DB_ABBREV[abbrev]
    
    if abbrev not in DB_ABBREVS:
        return None
    
    cur.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (abbrev,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    
    return None

def construct_nba_game_id(target_date, home_team_id, away_team_id, cur, used_ids=None):
    if used_ids is None:
        used_ids = set()
    
    cur.execute("""
        SELECT game_id FROM games 
        WHERE game_date = %s 
        AND home_team_id = %s 
        AND away_team_id = %s
    """, (target_date, home_team_id, away_team_id))
    
    result = cur.fetchone()
    if result:
        return result[0]
    
    year_str = target_date.strftime('%y')
    date_prefix = f"002{year_str}"
    
    cur.execute("""
        SELECT game_id FROM games 
        WHERE game_id LIKE %s
        ORDER BY game_id DESC
        LIMIT 1
    """, (f"{date_prefix}%",))
    
    result = cur.fetchone()
    if result:
        last_id = result[0]
        sequence = int(last_id[-3:]) + 1
    else:
        sequence = 1
    
    while True:
        game_id = f"{date_prefix}{sequence:03d}"
        if game_id not in used_ids:
            used_ids.add(game_id)
            return game_id
        sequence += 1

def collect_schedule_html(target_date=None):
    if target_date is None:
        target_date = datetime.now().date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Collecting schedule for {target_date} by scraping ESPN HTML...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    season_year = target_date.year
    season_month = target_date.month
    
    if season_month >= 10:
        season = f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        season = f"{season_year-1}-{str(season_year)[-2:]}"
    
    try:
        formatted_date = target_date.strftime('%Y%m%d')
        url = f"https://www.espn.com/nba/scoreboard/_/date/{formatted_date}"
        
        print(f"Fetching schedule from ESPN for {target_date.strftime('%m/%d/%Y')}...")
        print(f"URL: {url}\n")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
        max_retries = 3
        response = None
        
        for attempt in range(max_retries):
            try:
                print(f"  Attempt {attempt + 1}/{max_retries}...")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"  Error, retrying in 5 seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(5)
                else:
                    print(f"  Failed after {max_retries} attempts: {e}")
                    raise
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        games_found = []
        
        game_links = soup.find_all('a', href=re.compile(r'/nba/game/_/gameId/(\d+)'))
        
        if not game_links:
            all_text = soup.get_text()
            if 'No games scheduled' in all_text or 'no games' in all_text.lower():
                print(f"No games found for {target_date}")
                print("This likely means it's an off-day for the NBA")
                cur.close()
                conn.close()
                return
            else:
                print("Could not find game links on ESPN page")
                print("Trying to extract from page text...")
                
                game_id_pattern = re.compile(r'gameId/(\d{10})')
                found_ids = set(game_id_pattern.findall(response.text))
                for game_id in found_ids:
                    if game_id.startswith('002'):
                        games_found.append({'game_id': game_id})
        
        for link in game_links:
            href = link.get('href', '')
            espn_game_id_match = re.search(r'gameId/(\d+)', href)
            if espn_game_id_match:
                espn_game_id = espn_game_id_match.group(1)
                
                if any(g.get('espn_game_id') == espn_game_id for g in games_found):
                    continue
                
                games_found.append({'espn_game_id': espn_game_id, 'game_url': f"https://www.espn.com{href}"})
        
        if len(games_found) == 0:
            print(f"No games found for {target_date}")
            print("This likely means it's an off-day for the NBA")
            cur.close()
            conn.close()
            return
        
        print(f"Found {len(games_found)} games, extracting team info...\n")
        
        scheduled_count = 0
        completed_count = 0
        in_progress_count = 0
        used_game_ids = set()
        
        for game in games_found:
            game_url = game.get('game_url', f"https://www.espn.com/nba/game/_/gameId/{game.get('espn_game_id')}")
            
            try:
                game_response = requests.get(game_url, headers=headers, timeout=15)
                game_soup = BeautifulSoup(game_response.content, 'html.parser')
                
                team_abbrs = []
                
                team_links = game_soup.find_all('a', href=re.compile(r'/nba/team/_/name/(\w+)'))
                for link in team_links:
                    href = link.get('href', '')
                    match = re.search(r'/name/(\w+)', href)
                    if match:
                        abbrev = match.group(1).upper()
                        if abbrev not in team_abbrs:
                            team_abbrs.append(abbrev)
                            if len(team_abbrs) >= 2:
                                break
                
                if len(team_abbrs) < 2:
                    team_elements = game_soup.find_all(['span', 'div'], class_=re.compile(r'TeamName|team-name', re.I))
                    for elem in team_elements:
                        text = elem.get_text(strip=True)
                        if text and len(text) <= 5 and text.upper() not in team_abbrs:
                            team_abbrs.append(text.upper())
                            if len(team_abbrs) >= 2:
                                break
                
                if len(team_abbrs) < 2:
                    title = game_soup.find('title')
                    if title:
                        title_text = title.get_text()
                        team_pattern = re.compile(r'\b([A-Z]{2,5})\b')
                        potential_teams = team_pattern.findall(title_text)
                        for team in potential_teams:
                            if team not in team_abbrs and len(team) >= 2:
                                team_abbrs.append(team)
                                if len(team_abbrs) >= 2:
                                    break
                
                if len(team_abbrs) >= 2:
                    away_abbr_raw = team_abbrs[0]
                    home_abbr_raw = team_abbrs[1]
                    
                    away_abbr = away_abbr_raw
                    home_abbr = home_abbr_raw
                    
                    if away_abbr in ESPN_TO_DB_ABBREV:
                        away_abbr = ESPN_TO_DB_ABBREV[away_abbr]
                    
                    if home_abbr in ESPN_TO_DB_ABBREV:
                        home_abbr = ESPN_TO_DB_ABBREV[home_abbr]
                    
                    away_team_id = get_team_id_from_abbreviation(cur, away_abbr)
                    home_team_id = get_team_id_from_abbreviation(cur, home_abbr)
                    
                    if not away_team_id or not home_team_id:
                        espn_id = game.get('espn_game_id', 'unknown')
                        print(f"  Game {espn_id} - Could not find team IDs for {away_abbr_raw} ({away_abbr}) or {home_abbr_raw} ({home_abbr})")
                        if not away_team_id:
                            cur.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (away_abbr,))
                            result = cur.fetchone()
                            if result:
                                away_team_id = result[0]
                        if not home_team_id:
                            cur.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (home_abbr,))
                            result = cur.fetchone()
                            if result:
                                home_team_id = result[0]
                        
                        if not away_team_id or not home_team_id:
                            continue
                    
                    if away_team_id and home_team_id:
                        status_text = game_soup.get_text()
                        
                        if 'final' in status_text.lower()[:500]:
                            status = 'completed'
                            home_score = None
                            away_score = None
                            
                            score_pattern = re.compile(r'(\d+)\s*-\s*(\d+)')
                            scores = score_pattern.findall(status_text[:1000])
                            if scores:
                                try:
                                    home_score = int(scores[-1][1])
                                    away_score = int(scores[-1][0])
                                except:
                                    pass
                        elif 'pm' in status_text.lower()[:500] or 'am' in status_text.lower()[:500]:
                            status = 'scheduled'
                            home_score = None
                            away_score = None
                        else:
                            status = 'in_progress'
                            home_score = None
                            away_score = None
                        
                        nba_game_id = construct_nba_game_id(target_date, home_team_id, away_team_id, cur, used_game_ids)
                        
                        games_found[games_found.index(game)]['game_id'] = nba_game_id
                        games_found[games_found.index(game)]['away_team_id'] = away_team_id
                        games_found[games_found.index(game)]['home_team_id'] = home_team_id
                        games_found[games_found.index(game)]['away_abbr'] = away_abbr
                        games_found[games_found.index(game)]['home_abbr'] = home_abbr
                        games_found[games_found.index(game)]['status'] = status
                        games_found[games_found.index(game)]['home_score'] = home_score
                        games_found[games_found.index(game)]['away_score'] = away_score
                    else:
                        espn_id = game.get('espn_game_id', 'unknown')
                        print(f"  Game {espn_id} - Could not find team IDs for {away_abbr} or {home_abbr}")
                        continue
                else:
                    espn_id = game.get('espn_game_id', 'unknown')
                    print(f"  Game {espn_id} - Could not determine teams")
                    continue
                
                time.sleep(1)
            except Exception as e:
                espn_id = game.get('espn_game_id', 'unknown')
                print(f"  Game {espn_id} - Error fetching details: {e}")
                continue
        
        valid_games = [g for g in games_found if 'away_team_id' in g]
        
        if len(valid_games) == 0:
            print("Could not extract team information for any games")
            cur.close()
            conn.close()
            return
        
        for game in valid_games:
            game_id = game['game_id']
            home_team_id = game['home_team_id']
            away_team_id = game['away_team_id']
            status = game.get('status', 'scheduled')
            home_score = game.get('home_score')
            away_score = game.get('away_score')
            home_abbr = game.get('home_abbr', 'HOME')
            away_abbr = game.get('away_abbr', 'AWAY')
            
            if status == 'scheduled':
                scheduled_count += 1
            elif status == 'completed':
                completed_count += 1
            else:
                in_progress_count += 1
            
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
            
            print(f"  {away_abbr} @ {home_abbr} - {status}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"\n{'='*50}")
        print(f"Scheduled: {scheduled_count}")
        print(f"In Progress: {in_progress_count}")
        print(f"Completed: {completed_count}")
        print(f"Total: {len(valid_games)}")
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
        collect_schedule_html(target_date)
    else:
        collect_schedule_html()
