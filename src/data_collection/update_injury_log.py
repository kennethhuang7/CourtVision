import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re

def normalize_name(name):
    char_map = {
        'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ã': 'a', 'å': 'a', 'ā': 'a', 'ă': 'a', 'ą': 'a', 'ǎ': 'a',
        'Á': 'a', 'À': 'a', 'Ä': 'a', 'Â': 'a', 'Ã': 'a', 'Å': 'a', 'Ā': 'a', 'Ă': 'a', 'Ą': 'a', 'Ǎ': 'a',
        'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e', 'ē': 'e', 'ė': 'e', 'ę': 'e', 'ě': 'e',
        'É': 'e', 'È': 'e', 'Ë': 'e', 'Ê': 'e', 'Ē': 'e', 'Ė': 'e', 'Ę': 'e', 'Ě': 'e',
        'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i', 'ī': 'i', 'į': 'i', 'ı': 'i',
        'Í': 'i', 'Ì': 'i', 'Ï': 'i', 'Î': 'i', 'Ī': 'i', 'Į': 'i', 'İ': 'i',
        'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'õ': 'o', 'ø': 'o', 'ō': 'o', 'ő': 'o', 'ǫ': 'o',
        'Ó': 'o', 'Ò': 'o', 'Ö': 'o', 'Ô': 'o', 'Õ': 'o', 'Ø': 'o', 'Ō': 'o', 'Ő': 'o', 'Ǫ': 'o',
        'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u', 'ū': 'u', 'ů': 'u', 'ű': 'u', 'ų': 'u',
        'Ú': 'u', 'Ù': 'u', 'Ü': 'u', 'Û': 'u', 'Ū': 'u', 'Ů': 'u', 'Ű': 'u', 'Ų': 'u',
        'ý': 'y', 'ÿ': 'y', 'ŷ': 'y', 'Ý': 'y', 'Ÿ': 'y', 'Ŷ': 'y',
        'ç': 'c', 'ć': 'c', 'č': 'c', 'ĉ': 'c', 'ċ': 'c',
        'Ç': 'c', 'Ć': 'c', 'Č': 'c', 'Ĉ': 'c', 'Ċ': 'c',
        'đ': 'd', 'Đ': 'd',
        'ñ': 'n', 'ń': 'n', 'ň': 'n', 'ņ': 'n',
        'Ñ': 'n', 'Ń': 'n', 'Ň': 'n', 'Ņ': 'n',
        'š': 's', 'ś': 's', 'ş': 's',
        'Š': 's', 'Ś': 's', 'Ş': 's',
        'ž': 'z', 'ź': 'z', 'ż': 'z',
        'Ž': 'z', 'Ź': 'z', 'Ż': 'z',
        'ģ': 'g', 'Ģ': 'g',
        'ķ': 'k', 'Ķ': 'k',
        'ļ': 'l', 'Ļ': 'l',
    }
    return ''.join(char_map.get(c, c) for c in name)

def clean_name_for_matching(name):
    name = normalize_name(name)
    suffix_pattern = r'\s+(Jr\.?|Sr\.?|II|III|IV)$'
    name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
    name = name.replace("'", "").replace("-", " ")
    return name.strip()

def safe_print(message):
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode('ascii', 'replace').decode('ascii'))

def calculate_games_missed(conn, cur, player_id, report_date, recovery_date):
    cur.execute("""
        WITH player_teams AS (
            SELECT DISTINCT pgs.team_id
            FROM player_game_stats pgs
            JOIN games g2 ON pgs.game_id = g2.game_id
            WHERE pgs.player_id = %s
            AND g2.game_date < %s
            AND g2.game_status = 'completed'
        ),
        all_player_teams AS (
            SELECT team_id FROM player_teams
            UNION
            SELECT team_id FROM players 
            WHERE player_id = %s 
            AND NOT EXISTS (SELECT 1 FROM player_teams)
        )
        SELECT COUNT(DISTINCT g.game_date)
        FROM games g
        WHERE g.game_date >= %s
        AND g.game_date < %s
        AND g.game_status = 'completed'
        AND (
            g.home_team_id IN (SELECT team_id FROM all_player_teams)
            OR g.away_team_id IN (SELECT team_id FROM all_player_teams)
        )
        AND NOT EXISTS (
            SELECT 1 FROM player_game_stats pgs2
            WHERE pgs2.game_id = g.game_id
            AND pgs2.player_id = %s
            AND pgs2.minutes_played > 0
        )
    """, (player_id, report_date, player_id, report_date, recovery_date, player_id))
    
    result = cur.fetchone()
    return result[0] if result else 0

def scrape_espn_injuries(conn, cur, today):
    safe_print("Scraping NBA injury reports from ESPN...\n")
    
    url = "https://www.espn.com/nba/injuries"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        injuries_added = 0
        injuries_updated = 0
        scraped_player_ids = set()
        
        injury_tables = soup.find_all('div', class_='ResponsiveTable')
        
        if not injury_tables:
            safe_print("Could not find injury tables on ESPN")
            return injuries_added, injuries_updated, scraped_player_ids
        
        for table in injury_tables:
            rows = table.find_all('tr')[1:]
            
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if len(cols) < 5:
                        continue
                    
                    player_name = cols[0].text.strip()
                    expected_return = cols[2].text.strip() 
                    status = cols[3].text.strip() 
                    comment = cols[4].text.strip() 
                    
                    if not player_name:
                        continue
                    
                    cleaned_name = clean_name_for_matching(player_name)
                    
                    cur.execute("""
                        SELECT player_id, full_name FROM players
                    """)
                    
                    all_players = cur.fetchall()
                    player_result = None
                    
                    for player_id, full_name in all_players:
                        db_cleaned = clean_name_for_matching(full_name)
                        if db_cleaned.lower() == cleaned_name.lower():
                            player_result = (player_id, full_name)
                            break
                    
                    if not player_result:
                        parts = player_name.split()
                        if len(parts) >= 2:
                            last_name = clean_name_for_matching(parts[-1])
                            first_name = clean_name_for_matching(parts[0])
                            
                            for player_id, full_name in all_players:
                                db_full_cleaned = clean_name_for_matching(full_name)
                                db_parts = db_full_cleaned.split()
                                if len(db_parts) >= 2:
                                    db_first = db_parts[0]
                                    db_last = db_parts[-1]
                                    if (first_name.lower() in db_first.lower() or db_first.lower() in first_name.lower()) and \
                                       (last_name.lower() in db_last.lower() or db_last.lower() in last_name.lower()):
                                        player_result = (player_id, full_name)
                                        safe_print(f"  Matched '{player_name}' to player ID {player_id}")
                                        break
                        
                        if not player_result:
                            safe_print(f"  Player not found: {player_name}")
                            continue
                    else:
                        player_id = player_result[0]
                        safe_print(f"  Matched '{player_name}' to player ID {player_id}")
                    
                    scraped_player_ids.add(player_id)
                    
                    cur.execute("""
                        SELECT injury_id, report_date FROM injuries
                        WHERE player_id = %s 
                        AND return_date IS NULL
                        AND injury_status IN ('Out', 'Day-To-Day', 'Questionable')
                        ORDER BY report_date DESC, injury_id DESC
                        LIMIT 1
                    """, (player_id,))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        injury_id, existing_report_date = existing
                        cur.execute("""
                            UPDATE injuries SET
                                injury_status = %s,
                                injury_description = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE injury_id = %s
                        """, (status, comment, injury_id))
                        injuries_updated += 1
                    else:
                        cur.execute("""
                            INSERT INTO injuries (
                                player_id, report_date, injury_status, 
                                injury_description, source
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (player_id, today, status, comment, 'ESPN'))
                        injuries_added += 1
                    
                    safe_print(f"  {player_name}: {expected_return} - {status}")
                    
                except Exception as e:
                    safe_print(f"  Error processing row: {e}")
                    continue
        
        safe_print(f"\nESPN Scraping: Added {injuries_added}, Updated {injuries_updated}")
        return injuries_added, injuries_updated, scraped_player_ids
        
    except Exception as e:
        safe_print(f"Error scraping ESPN: {e}")
        return 0, 0, set()

def update_injury_log():
    safe_print("Updating injury log: scraping ESPN, detecting recoveries, and calculating return dates...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    try:
        injuries_added, injuries_updated, scraped_player_ids = scrape_espn_injuries(conn, cur, today)
        
        safe_print("\n" + "="*50)
        safe_print("DETECTING RECOVERIES")
        safe_print("="*50 + "\n")
        
        if scraped_player_ids:
            placeholders = ','.join(['%s'] * len(scraped_player_ids))
            cur.execute(f"""
                SELECT DISTINCT ON (i.player_id)
                    i.injury_id, i.player_id, p.full_name, i.report_date, i.injury_status
                FROM injuries i
                JOIN players p ON i.player_id = p.player_id
                WHERE i.injury_status IN ('Out', 'Day-To-Day', 'Questionable')
                AND i.return_date IS NULL
                AND i.player_id NOT IN ({placeholders})
                ORDER BY i.player_id, i.report_date DESC, i.injury_id DESC
            """, tuple(scraped_player_ids))
        else:
            cur.execute("""
                SELECT DISTINCT ON (i.player_id)
                    i.injury_id, i.player_id, p.full_name, i.report_date, i.injury_status
                FROM injuries i
                JOIN players p ON i.player_id = p.player_id
                WHERE i.injury_status IN ('Out', 'Day-To-Day', 'Questionable')
                AND i.return_date IS NULL
                ORDER BY i.player_id, i.report_date DESC, i.injury_id DESC
            """)
        
        active_injuries_not_on_report = cur.fetchall()
        
        recovered_count = 0
        
        if len(active_injuries_not_on_report) == 0:
            safe_print("No players recovered (all active injuries still on report)")
        else:
            safe_print(f"Found {len(active_injuries_not_on_report)} player(s) off the injury report (recovered)...\n")
        
        for injury_id, player_id, player_name, report_date, injury_status in active_injuries_not_on_report:
            games_missed = calculate_games_missed(conn, cur, player_id, report_date, today)
            cur.execute("""
                UPDATE injuries SET
                    injury_status = 'Healthy',
                    return_date = %s,
                    games_missed = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE injury_id = %s
            """, (today, games_missed, injury_id))
            recovered_count += 1
            try:
                safe_print(f"  [RECOVERED] {player_name} (missed {games_missed} games, cleared {today}, was {injury_status})")
            except UnicodeEncodeError:
                safe_print(f"  [RECOVERED] {player_name} (missed {games_missed} games, cleared {today})")
        
        conn.commit()
        cur.close()
        conn.close()
        
        safe_print(f"\n{'='*50}")
        safe_print("INJURY LOG UPDATE COMPLETE!")
        safe_print(f"{'='*50}")
        safe_print(f"ESPN: Added {injuries_added}, Updated {injuries_updated}")
        safe_print(f"Recovered: {recovered_count}")
        safe_print(f"Total processed: {injuries_added + injuries_updated + recovered_count}")
        
    except Exception as e:
        safe_print(f"Error: {e}")
        try:
            cur.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    update_injury_log()

