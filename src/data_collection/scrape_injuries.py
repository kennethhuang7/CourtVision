import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime
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

def scrape_injuries():
    safe_print("Scraping NBA injury reports from ESPN...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    today = datetime.now().date()
    
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
        
        injury_tables = soup.find_all('div', class_='ResponsiveTable')
        
        if not injury_tables:
            safe_print("Could not find injury tables on ESPN")
            cur.close()
            conn.close()
            return
        
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
                            safe_print(f"  Player not found: {player_name}")
                            continue
                    else:
                        player_id = player_result[0]
                        safe_print(f"  Matched '{player_name}' to player ID {player_id}")
                    
                    cur.execute("""
                        SELECT injury_id FROM injuries
                        WHERE player_id = %s 
                        AND report_date = %s
                    """, (player_id, today))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        cur.execute("""
                            UPDATE injuries SET
                                injury_status = %s,
                                injury_description = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE injury_id = %s
                        """, (status, comment, existing[0]))
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
        
        conn.commit()
        cur.close()
        conn.close()
        
        safe_print(f"\n{'='*50}")
        safe_print("INJURY SCRAPING COMPLETE!")
        safe_print(f"{'='*50}")
        safe_print(f"Added: {injuries_added}")
        safe_print(f"Updated: {injuries_updated}")
        safe_print(f"Total processed: {injuries_added + injuries_updated}")
        
    except Exception as e:
        safe_print(f"Error: {e}")
        try:
            cur.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    scrape_injuries()