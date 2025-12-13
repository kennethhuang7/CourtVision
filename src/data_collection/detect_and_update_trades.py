# RUN THIS:
# python src/data_collection/detect_and_update_trades.py [date]
# or (to update all players):
# python src/data_collection/detect_and_update_trades.py [date] --all
# or (to update from recent games):
# python src/data_collection/detect_and_update_trades.py [date] --from-games
# or (to backfill missing transactions):
# python src/data_collection/detect_and_update_trades.py --backfill [--date=date] [--season=season]

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection, rate_limit
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.static import players as nba_players
import pandas as pd
from datetime import datetime, date, timedelta
import requests
from bs4 import BeautifulSoup
import re

def safe_int(val):
    if pd.isna(val) or val == '' or val == 'Undrafted' or val == 0:
        return None
    try:
        return int(val)
    except:
        return None

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

def scrape_recent_transactions_from_espn(days_back=7):
    print(f"Scraping ESPN transactions for trades, signings, and waivers in last {days_back} days...")
    
    url = "https://www.espn.com/nba/transactions"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        transactions = []
        today = datetime.now().date()
        
        all_text = soup.get_text()
        
        date_pattern = r'(\w+day),?\s+(\w+)\s+(\d{1,2})'
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        
        skip_terms = ['Team Transactions', 'TRANSACTION', 'Golden State', 'Houston Rockets', 
                     'Indiana Pacers', 'Memphis Grizzlies', 'Phoenix Suns', 'Dallas Mavericks',
                     'Detroit Pistons', 'Charlotte Hornets', 'New Orleans Pelicans', 'Portland Trail Blazers',
                     'Brooklyn Nets', 'Milwaukee Bucks', 'Denver Nuggets', 'Atlanta Hawks',
                     'Minnesota Timberwolves', 'Utah Jazz', 'Oklahoma City Thunder', 'Toronto Raptors',
                     'Sacramento Kings', 'Miami Heat', 'Cleveland Cavaliers', 'Boston Celtics',
                     'Los Angeles Lakers', 'New York Knicks', 'Chicago Bulls', 'San Antonio Spurs',
                     'Los Angeles Clippers', 'LA Clippers', 'Washington Wizards', 'Orlando Magic',
                     'Philadelphia 76ers', 'Los Angeles', 'New York', 'LA', 'Chicago', 'San Antonio',
                     'Washington', 'Orlando', 'Philadelphia', 'New Orleans', 'Oklahoma City',
                     'Portland Trail', 'Trail Blazers', 'Skip to main', 'ESPN', 'NFL', 'NBA', 'MLB', 'NCAA', 'NHL', 'Soccer', 'WNBA']
        
        date_matches = list(re.finditer(date_pattern, all_text, re.IGNORECASE))
        
        for i, date_match in enumerate(date_matches):
            _, month_name, day = date_match.groups()
            month = month_map.get(month_name.lower(), None)
            if not month:
                continue
            
            try:
                year = today.year
                parsed_date = date(year, month, int(day))
                if parsed_date > today:
                    parsed_date = date(year - 1, month, int(day))
                if (today - parsed_date).days > days_back:
                    continue
            except:
                continue
            
            start_pos = date_match.end()
            end_pos = date_matches[i + 1].start() if i + 1 < len(date_matches) else len(all_text)
            
            section_text = all_text[start_pos:end_pos]
            
            transaction_sentences = re.split(r'\.(?=\s*[A-Z])', section_text)
            
            for sentence in transaction_sentences:
                sentence = sentence.strip()
                if not sentence or len(sentence) < 10:
                    continue
                
                sentence_lower = sentence.lower()
                transaction_type = None
                
                if 'traded' in sentence_lower and ('to' in sentence_lower or 'from' in sentence_lower or 'acquired' in sentence_lower):
                    transaction_type = 'trade'
                elif 'signed' in sentence_lower and ('contract' in sentence_lower or 'two-way' in sentence_lower or 'rest-of-season' in sentence_lower or 'two-year' in sentence_lower or '10-day' in sentence_lower):
                    transaction_type = 'signing'
                elif 'waived' in sentence_lower:
                    transaction_type = 'waiver'
                
                if transaction_type:
                    name_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}(?:\s+[A-Z][a-z]+)?)\b'
                    names = re.findall(name_pattern, sentence)
                    for name in names:
                        name_clean = name.strip()
                        if len(name_clean.split()) >= 2 and len(name_clean) > 5:
                            if name_clean not in skip_terms:
                                transactions.append((name_clean, parsed_date, transaction_type))
        
        if not transactions:
            print("  No recent transactions found on ESPN")
            return []
        
        unique_transactions = {}
        for name, trans_date, trans_type in transactions:
            key = (name, trans_type)
            if key not in unique_transactions or trans_date > unique_transactions[key][1]:
                unique_transactions[key] = (trans_date, trans_type)
        
        result = [(name, trans_date, trans_type) for (name, trans_type), (trans_date, _) in unique_transactions.items()]
        
        trades = sum(1 for _, _, t in result if t == 'trade')
        signings = sum(1 for _, _, t in result if t == 'signing')
        waivers = sum(1 for _, _, t in result if t == 'waiver')
        
        print(f"  Found {len(result)} unique transactions: {trades} trades, {signings} signings, {waivers} waivers")
        return result
        
    except Exception as e:
        print(f"  Error scraping ESPN: {e}")
        return []

def safe_str(val):
    if pd.isna(val) or val == '':
        return None
    return str(val)

def find_player_by_name(cur, name):
    cleaned_search = clean_name_for_matching(name)
    name_parts = cleaned_search.lower().split()
    if len(name_parts) < 2:
        return None
    
    first_name = name_parts[0]
    last_name = name_parts[-1]
    
    cur.execute("""
        SELECT player_id, full_name, team_id 
        FROM players 
        WHERE is_active = TRUE
        AND LOWER(full_name) LIKE %s
        AND LOWER(full_name) LIKE %s
    """, (f'%{first_name}%', f'%{last_name}%'))
    
    candidates = cur.fetchall()
    if not candidates:
        return None
    
    for player_id, full_name, team_id in candidates:
        cleaned_db = clean_name_for_matching(full_name)
        if cleaned_db.lower() == cleaned_search.lower():
            return (player_id, full_name, team_id)
    
    return None

def add_new_player_from_api(cur, player_name):
    try:
        all_nba_players = nba_players.get_players()
        for nba_player in all_nba_players:
            if clean_name_for_matching(nba_player['full_name']).lower() == clean_name_for_matching(player_name).lower():
                player_id = int(nba_player['id'])
                
                rate_limit()
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                info_df = player_info.get_data_frames()[0]
                
                if len(info_df) == 0:
                    return None
                
                info = info_df.iloc[0]
                team_id = safe_int(info.get('TEAM_ID'))
                jersey = safe_str(info.get('JERSEY'))
                position = safe_str(info.get('POSITION'))
                height = safe_str(info.get('HEIGHT'))
                weight = safe_int(info.get('WEIGHT'))
                birthdate = safe_str(info.get('BIRTHDATE'))
                draft_year = safe_str(info.get('DRAFT_YEAR'))
                draft_round = safe_str(info.get('DRAFT_ROUND'))
                draft_number = safe_str(info.get('DRAFT_NUMBER'))
                
                height_inches = None
                if height and '-' in height:
                    try:
                        parts = height.split('-')
                        if len(parts) == 2:
                            feet = int(parts[0])
                            inches = int(parts[1])
                            height_inches = (feet * 12) + inches
                    except:
                        pass
                
                if draft_year and draft_year.lower() == 'undrafted':
                    draft_year = None
                    draft_round = None
                    draft_number = None
                else:
                    draft_year = safe_int(draft_year)
                    draft_round = safe_int(draft_round)
                    draft_number = safe_int(draft_number)
                
                cur.execute("""
                    INSERT INTO players (player_id, full_name, first_name, last_name, is_active,
                                       team_id, jersey_number, position, height_inches, weight_lbs,
                                       birth_date, draft_year, draft_round, draft_number)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player_id) DO UPDATE SET
                        team_id = EXCLUDED.team_id,
                        is_active = TRUE
                    RETURNING player_id
                """, (
                    player_id, nba_player['full_name'], 
                    safe_str(nba_player.get('first_name', '')), 
                    safe_str(nba_player.get('last_name', '')),
                    True, team_id, jersey, position, height_inches, weight,
                    birthdate, draft_year, draft_round, draft_number
                ))
                
                return cur.fetchone()[0]
    except Exception as e:
        print(f"    Error adding new player {player_name}: {e}")
        return None
    return None

def get_season_from_date(date_obj):
    season_year = date_obj.year
    season_month = date_obj.month
    if season_month >= 10:
        return f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        return f"{season_year-1}-{str(season_year)[-2:]}"

def detect_and_update_transactions(check_date=None, update_all_players=False):
    if check_date is None:
        check_date = datetime.now().date()
    elif isinstance(check_date, str):
        check_date = datetime.strptime(check_date, '%Y-%m-%d').date()
    
    print("="*60)
    print("DETECTING AND UPDATING PLAYER TRANSACTIONS")
    print("="*60)
    print(f"Check date: {check_date}\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    espn_transactions = scrape_recent_transactions_from_espn(days_back=3)
    
    transactions_processed = 0
    trades_detected = 0
    signings_detected = 0
    waivers_detected = 0
    players_updated = 0
    new_players_added = 0
    errors = 0
    skipped_not_in_nba_api = 0
    skipped_not_in_db = 0
    skipped_already_recorded = 0
    skipped_already_waived = 0
    
    if espn_transactions and len(espn_transactions) > 0:
        print(f"\nProcessing {len(espn_transactions)} transactions from ESPN...")
        print(f"Transactions: {[(name, ttype) for name, _, ttype in espn_transactions]}\n")
        
        for espn_name, trans_date, trans_type in espn_transactions:
            try:
                player_match = find_player_by_name(cur, espn_name)
                
                if not player_match:
                    if trans_type == 'signing':
                        print(f"\n  NEW SIGNING (not in DB): {espn_name}")
                        new_player_id = add_new_player_from_api(cur, espn_name)
                        if new_player_id:
                            player_match = find_player_by_name(cur, espn_name)
                            new_players_added += 1
                            print(f"    [OK] Added new player to database (ID: {new_player_id})")
                        else:
                            print(f"    [X] Could not find player in NBA API (may be G-League or not active)")
                            skipped_not_in_nba_api += 1
                            continue
                    elif trans_type == 'waiver':
                        print(f"\n  WAIVER (not in DB): {espn_name}")
                        print(f"    [INFO] Player not in database - skipping (likely already waived or G-League player)")
                        skipped_not_in_db += 1
                        continue
                    else:
                        print(f"\n  {trans_type.upper()} (not in DB): {espn_name}")
                        print(f"    [INFO] Player not in database - skipping")
                        skipped_not_in_db += 1
                        continue
                
                player_id, player_name, current_team_id = player_match
                
                season = get_season_from_date(trans_date)
                
                rate_limit()
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                info_df = player_info.get_data_frames()[0]
                
                if len(info_df) == 0:
                    skipped_not_in_nba_api += 1
                    continue
                
                info = info_df.iloc[0]
                api_team_id = safe_int(info.get('TEAM_ID'))
                
                if trans_type == 'waiver':
                    if current_team_id is not None:
                        print(f"\n  WAIVER DETECTED: {player_name} (ID: {player_id})")
                        print(f"    Team: {current_team_id} -> NULL (free agent)")
                        if api_team_id is not None:
                            print(f"    Note: NBA API still shows team_id = {api_team_id} (may not have updated yet)")
                        
                        cur.execute("""
                            SELECT transaction_id
                            FROM player_transactions
                            WHERE player_id = %s
                            AND transaction_date >= %s::date - INTERVAL '7 days'
                            AND transaction_type = 'waiver'
                        """, (player_id, trans_date))
                        
                        if not cur.fetchone():
                            try:
                                cur.execute("""
                                    INSERT INTO player_transactions (
                                        player_id, from_team_id, to_team_id,
                                        transaction_type, transaction_date, season,
                                        source, confidence_score
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """, (player_id, current_team_id, None, 'waiver', trans_date, season, 'espn_scrape', 100))
                                
                                cur.execute("UPDATE players SET team_id = NULL WHERE player_id = %s", (player_id,))
                                waivers_detected += 1
                                players_updated += 1
                                transactions_processed += 1
                                print(f"    [OK] Waiver recorded and team_id set to NULL")
                                conn.commit()
                            except Exception as e:
                                conn.rollback()
                                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                                    print(f"    [INFO] Waiver already exists (duplicate prevented)")
                                    conn.commit()
                                else:
                                    raise
                        else:
                            print(f"    [INFO] Waiver already recorded in last 7 days")
                            skipped_already_recorded += 1
                    else:
                        print(f"\n  WAIVER: {player_name} (ID: {player_id})")
                        print(f"    [INFO] Player already has team_id = NULL (already waived)")
                        skipped_already_waived += 1
                
                elif trans_type == 'signing':
                    if api_team_id is not None:
                        cur.execute("""
                            SELECT transaction_id
                            FROM player_transactions
                            WHERE player_id = %s
                            AND to_team_id = %s
                            AND transaction_date >= %s::date - INTERVAL '7 days'
                            AND transaction_type = 'signing'
                        """, (player_id, api_team_id, trans_date))
                        
                        existing_signing = cur.fetchone()
                        
                        if not existing_signing:
                            print(f"\n  SIGNING DETECTED: {player_name} (ID: {player_id})")
                            print(f"    Old team: {current_team_id}")
                            print(f"    New team: {api_team_id}")
                            
                            try:
                                cur.execute("""
                                    INSERT INTO player_transactions (
                                        player_id, from_team_id, to_team_id,
                                        transaction_type, transaction_date, season,
                                        source, confidence_score
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """, (player_id, current_team_id, api_team_id, 'signing', trans_date, season, 'espn_scrape', 100))
                                signings_detected += 1
                                transactions_processed += 1
                                print(f"    [OK] Signing recorded")
                            except Exception as e:
                                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                                    print(f"    [INFO] Signing already exists (duplicate prevented)")
                                    skipped_already_recorded += 1
                                else:
                                    raise
                        else:
                            skipped_already_recorded += 1
                        
                        if current_team_id != api_team_id:
                            cur.execute("UPDATE players SET team_id = %s WHERE player_id = %s", (api_team_id, player_id))
                            players_updated += 1
                            print(f"    [OK] Team_id updated")
                            conn.commit()
                        elif not existing_signing:
                            conn.commit()
                
                elif trans_type == 'trade':
                    if api_team_id is not None and current_team_id != api_team_id:
                        print(f"\n  TRADE DETECTED: {player_name} (ID: {player_id})")
                        print(f"    Old team: {current_team_id}")
                        print(f"    New team: {api_team_id}")
                        
                        cur.execute("""
                            SELECT transaction_id
                            FROM player_transactions
                            WHERE player_id = %s
                            AND to_team_id = %s
                            AND transaction_date >= %s::date - INTERVAL '7 days'
                            AND transaction_type = 'trade'
                        """, (player_id, api_team_id, trans_date))
                        
                        if not cur.fetchone():
                            try:
                                cur.execute("""
                                    INSERT INTO player_transactions (
                                        player_id, from_team_id, to_team_id,
                                        transaction_type, transaction_date, season,
                                        source, confidence_score
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """, (player_id, current_team_id, api_team_id, 'trade', trans_date, season, 'espn_scrape', 100))
                                
                                cur.execute("UPDATE players SET team_id = %s WHERE player_id = %s", (api_team_id, player_id))
                                trades_detected += 1
                                players_updated += 1
                                transactions_processed += 1
                                print(f"    [OK] Trade recorded and team_id updated")
                                conn.commit()
                            except Exception as e:
                                conn.rollback()
                                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                                    print(f"    [INFO] Trade already exists (duplicate prevented)")
                                    skipped_already_recorded += 1
                                    conn.commit()
                                else:
                                    raise
                        else:
                            skipped_already_recorded += 1
            
            except Exception as e:
                errors += 1
                print(f"  Error processing {espn_name}: {e}")
                continue
    
    if update_all_players:
        print("\nChecking all active players for team changes...")
        cur.execute("SELECT player_id, full_name, team_id FROM players WHERE is_active = TRUE")
        all_players = cur.fetchall()
        
        for player_id, player_name, current_team_id in all_players:
            try:
                rate_limit()
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                info_df = player_info.get_data_frames()[0]
                
                if len(info_df) == 0:
                    continue
                
                info = info_df.iloc[0]
                api_team_id = safe_int(info.get('TEAM_ID'))
                
                if api_team_id is None and current_team_id is not None:
                    print(f"  {player_name} (ID: {player_id}): Team changed from {current_team_id} to NULL (free agent)")
                    cur.execute("UPDATE players SET team_id = NULL WHERE player_id = %s", (player_id,))
                    players_updated += 1
                elif current_team_id != api_team_id and api_team_id is not None:
                    print(f"  {player_name} (ID: {player_id}): Team changed from {current_team_id} to {api_team_id}")
                    cur.execute("UPDATE players SET team_id = %s WHERE player_id = %s", (api_team_id, player_id))
                    players_updated += 1
                
                conn.commit()
            except Exception as e:
                errors += 1
                continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    total_found = len(espn_transactions) if espn_transactions else 0
    total_skipped = skipped_not_in_nba_api + skipped_not_in_db + skipped_already_recorded + skipped_already_waived
    
    print("\n" + "="*60)
    print("TRANSACTION DETECTION COMPLETE")
    print("="*60)
    print(f"Total transactions found: {total_found}")
    print(f"Transactions processed: {transactions_processed}")
    print(f"  - Trades detected: {trades_detected}")
    print(f"  - Signings detected: {signings_detected}")
    print(f"  - Waivers detected: {waivers_detected}")
    print(f"Transactions skipped: {total_skipped}")
    if skipped_not_in_nba_api > 0:
        print(f"  - Not in NBA API: {skipped_not_in_nba_api}")
    if skipped_not_in_db > 0:
        print(f"  - Not in database: {skipped_not_in_db}")
    if skipped_already_recorded > 0:
        print(f"  - Already recorded: {skipped_already_recorded}")
    if skipped_already_waived > 0:
        print(f"  - Already waived: {skipped_already_waived}")
    print(f"New players added: {new_players_added}")
    print(f"Players updated: {players_updated}")
    print(f"Errors: {errors}")
    print(f"Total accounted for: {transactions_processed + total_skipped + errors} / {total_found}")
    print("="*60)

def detect_and_update_trades(check_date=None, update_all_players=False):
    detect_and_update_transactions(check_date, update_all_players)

def investigate_missing_transaction(player_id, team_id, check_date, conn, cur):
    confidence = 0
    notes = []
    safe_name = f"Player {player_id}"
    
    try:
        cur.execute("SELECT full_name FROM players WHERE player_id = %s", (player_id,))
        player_name_result = cur.fetchone()
        player_name = player_name_result[0] if player_name_result else f"Player {player_id}"
        
        try:
            safe_name = player_name.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        except:
            safe_name = f"Player {player_id}"
        
        rate_limit()
        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        info_df = player_info.get_data_frames()[0]
        
        if len(info_df) == 0:
            return None, f"  [SKIP] {safe_name}: Not found in NBA API", 0, "Player not in NBA API"
        
        info = info_df.iloc[0]
        api_team_id = safe_int(info.get('TEAM_ID'))
        
        cur.execute("""
            SELECT COUNT(*) as active_injury_count,
                   STRING_AGG(i.injury_status::text, ', ') as injury_statuses,
                   MIN(i.report_date) as earliest_injury_date
            FROM injuries i
            WHERE i.player_id = %s
            AND i.injury_status IN ('Out', 'Day-To-Day', 'Questionable')
            AND i.return_date IS NULL
            AND i.report_date >= %s::date - INTERVAL '30 days'
        """, (player_id, check_date))
        
        injury_result = cur.fetchone()
        active_injury_count = injury_result[0] if injury_result else 0
        
        if api_team_id == team_id:
            if active_injury_count > 0:
                injury_statuses = injury_result[1] if injury_result[1] else 'Unknown'
                earliest_date = injury_result[2] if injury_result[2] else 'Unknown'
                if active_injury_count > 1:
                    return None, f"  [INFO] {safe_name}: Still on roster with {active_injury_count} active injuries ({injury_statuses}, earliest {earliest_date}) - likely prolonged injury, not a transaction", 100, f"Multiple active injuries detected"
                else:
                    return None, f"  [INFO] {safe_name}: Still on roster with active injury ({injury_statuses}, reported {earliest_date}) - likely prolonged injury, not a transaction", 100, f"Active injury: {injury_statuses}"
            else:
                return None, f"  [INFO] {safe_name}: Still on roster, no active injury - likely role player with 0 minutes or transaction not yet recorded", 100, "On roster, no injury"
        elif api_team_id is None and team_id is not None:
            cur.execute("""
                SELECT to_team_id, transaction_date 
                FROM player_transactions 
                WHERE player_id = %s 
                AND transaction_date >= %s::date - INTERVAL '14 days'
                ORDER BY transaction_date DESC 
                LIMIT 1
            """, (player_id, check_date))
            
            recent_trans = cur.fetchone()
            if recent_trans and recent_trans[0] is not None:
                return None, f"  [INFO] {safe_name}: Recently traded/signed to team {recent_trans[0]} on {recent_trans[1]} - awaiting debut", 100, "Transaction exists, awaiting first game"
            
            cur.execute("""
                SELECT DISTINCT ON (pgs.team_id)
                    pgs.team_id,
                    g.game_date
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = %s
                AND g.game_status = 'completed'
                AND g.game_date < %s
                ORDER BY pgs.team_id, g.game_date DESC
                LIMIT 1
            """, (player_id, check_date))
            
            last_game = cur.fetchone()
            
            if last_game and last_game[0] == team_id:
                trans_type = 'waiver'
                from_team_id = team_id
                estimated_date = last_game[1] + timedelta(days=1) if last_game[1] else check_date
                if estimated_date > check_date:
                    estimated_date = check_date
                
                trans_season = get_season_from_date(estimated_date)
                
                cur.execute("""
                    SELECT transaction_id
                    FROM player_transactions
                    WHERE player_id = %s
                    AND to_team_id IS NULL
                    AND transaction_type = 'waiver'
                    AND transaction_date >= %s::date - INTERVAL '7 days'
                """, (player_id, estimated_date))
                
                if not cur.fetchone():
                    confidence = 85 if last_game else 40
                    notes.append(f"Last game: {last_game[1] if last_game else 'None'}")
                    
                    if confidence >= 70:
                        try:
                            cur.execute("""
                                INSERT INTO player_transactions (
                                    player_id, from_team_id, to_team_id,
                                    transaction_type, transaction_date, season,
                                    source, confidence_score
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (player_id, from_team_id, None, trans_type, estimated_date, trans_season, 'backfill_inferred', confidence))
                            conn.commit()
                            return 'waiver', f"  [BACKFILL] {safe_name}: WAIVER on {estimated_date} (confidence: {confidence}%)", confidence, '; '.join(notes)
                        except Exception as e:
                            conn.rollback()
                            if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                                return None, f"  [INFO] {safe_name}: Waiver already exists (duplicate prevented)", 100, "Duplicate prevented"
                            return None, f"  [ERROR] {safe_name}: Failed to insert waiver: {e}", 0, str(e)
                    else:
                        return None, f"  [FLAG] {safe_name}: Possible waiver but low confidence ({confidence}%) - manual review needed", confidence, '; '.join(notes)
                else:
                    return None, f"  [INFO] {safe_name}: Waiver already recorded", 100, "Transaction exists"
            else:
                return None, f"  [SKIP] {safe_name}: Not on roster, but no recent games found - may have been waived earlier", 30, "No game history to confirm"
        elif api_team_id is not None and api_team_id != team_id:
            cur.execute("""
                SELECT DISTINCT ON (pgs.team_id)
                    pgs.team_id,
                    g.game_date
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = %s
                AND g.game_status = 'completed'
                AND g.game_date < %s
                ORDER BY pgs.team_id, g.game_date DESC
                LIMIT 1
            """, (player_id, check_date))
            
            last_game = cur.fetchone()
            
            if last_game:
                last_team_id = last_game[0]
                last_game_date = last_game[1]
                
                cur.execute("""
                    SELECT to_team_id, transaction_date 
                    FROM player_transactions 
                    WHERE player_id = %s 
                    AND transaction_date >= %s::date - INTERVAL '14 days'
                    ORDER BY transaction_date DESC 
                    LIMIT 1
                """, (player_id, check_date))
                
                recent_trans = cur.fetchone()
                if recent_trans and recent_trans[0] == api_team_id:
                    return None, f"  [INFO] {safe_name}: Recently traded/signed to team {api_team_id} on {recent_trans[1]} - awaiting debut", 100, "Transaction exists, awaiting first game"
                
                if last_team_id != api_team_id:
                    if last_team_id is None:
                        trans_type = 'signing'
                        from_team_id = None
                    else:
                        trans_type = 'trade'
                        from_team_id = last_team_id
                        
                    cur.execute("""
                        SELECT MIN(g.game_date) as first_new_team_game
                        FROM player_game_stats pgs
                        JOIN games g ON pgs.game_id = g.game_id
                        WHERE pgs.player_id = %s
                        AND pgs.team_id = %s
                        AND g.game_status = 'completed'
                        AND g.game_date < %s
                    """, (player_id, api_team_id, check_date))
                    
                    first_new_team_result = cur.fetchone()
                    first_new_team_game = first_new_team_result[0] if first_new_team_result else None
                    
                    estimated_date = last_game_date + timedelta(days=1) if last_game_date else check_date
                    if estimated_date > check_date:
                        estimated_date = check_date
                    
                    if first_new_team_game and estimated_date >= first_new_team_game:
                        estimated_date = first_new_team_game - timedelta(days=1)
                        notes.append(f"Bounded by first game with new team: {first_new_team_game}")
                    
                    trans_season = get_season_from_date(estimated_date)
                    
                    confidence = 85 if last_game_date else 60
                    if first_new_team_game:
                        confidence = min(95, confidence + 5)
                    notes.append(f"Last game for old team: {last_game_date if last_game_date else 'None'}")
                    
                    if confidence >= 70:
                        try:
                            cur.execute("""
                                INSERT INTO player_transactions (
                                    player_id, from_team_id, to_team_id,
                                    transaction_type, transaction_date, season,
                                    source, confidence_score
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (player_id, from_team_id, api_team_id, trans_type, estimated_date, trans_season, 'backfill_inferred', confidence))
                            
                            cur.execute("UPDATE players SET team_id = %s WHERE player_id = %s", (api_team_id, player_id))
                            conn.commit()
                            return trans_type, f"  [BACKFILL] {safe_name}: {trans_type.upper()} on {estimated_date} ({from_team_id} -> {api_team_id}, confidence: {confidence}%)", confidence, '; '.join(notes)
                        except Exception as e:
                            conn.rollback()
                            if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                                return None, f"  [INFO] {safe_name}: Transaction already exists (duplicate prevented)", 100, "Duplicate prevented"
                            return None, f"  [ERROR] {safe_name}: Failed to insert {trans_type}: {e}", 0, str(e)
                    else:
                        return None, f"  [FLAG] {safe_name}: Possible {trans_type} but low confidence ({confidence}%) - manual review needed", confidence, '; '.join(notes)
                else:
                    return None, f"  [INFO] {safe_name}: Team mismatch resolved - updating team_id", 100, "Team ID updated"
            else:
                return None, f"  [SKIP] {safe_name}: No game history found - cannot determine transaction", 0, "No game history"
        else:
            return None, f"  [SKIP] {safe_name}: Unable to determine status", 0, "Unknown status"
            
    except Exception as e:
        return None, f"  [ERROR] {safe_name}: {e}", 0, str(e)

def backfill_missing_transactions(check_date=None, season=None):
    if check_date is None:
        check_date = datetime.now().date()
    elif isinstance(check_date, str):
        check_date = datetime.strptime(check_date, '%Y-%m-%d').date()
    
    if season is None:
        season_year = check_date.year
        season_month = check_date.month
        if season_month >= 10:
            season = f"{season_year}-{str(season_year+1)[-2:]}"
        else:
            season = f"{season_year-1}-{str(season_year)[-2:]}"
    
    print("\n" + "="*60)
    print("INVESTIGATING MISSING TRANSACTIONS")
    print("="*60)
    print("Checking players on teams with no recent games...\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT p.player_id, p.full_name, p.team_id as current_team_id
        FROM players p
        WHERE p.is_active = TRUE
        AND p.team_id IS NOT NULL
        AND p.player_id NOT IN (
            SELECT DISTINCT pgs.player_id
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.team_id = p.team_id
            AND g.season = %s
            AND g.game_date >= %s::date - INTERVAL '30 days'
            AND g.game_status = 'completed'
        )
        AND EXISTS (
            SELECT 1
            FROM player_game_stats pgs2
            JOIN games g2 ON pgs2.game_id = g2.game_id
            WHERE pgs2.player_id = p.player_id
            AND g2.season = %s
            AND g2.game_date < %s
            AND g2.game_status = 'completed'
            GROUP BY pgs2.player_id
            HAVING COUNT(*) >= 5
        )
        AND NOT EXISTS (
            SELECT 1
            FROM player_transactions pt
            WHERE pt.player_id = p.player_id
            AND pt.to_team_id = p.team_id
            AND pt.transaction_date >= %s::date - INTERVAL '90 days'
        )
    """, (season, check_date, season, check_date, check_date))
    
    players_to_check = cur.fetchall()
    
    if len(players_to_check) == 0:
        print("No players found needing investigation.")
        cur.close()
        conn.close()
        return
    
    print(f"Found {len(players_to_check)} players to investigate\n")
    
    backfilled = 0
    skipped = 0
    errors = 0
    
    for player_id, player_name, current_team_id in players_to_check:
        result = investigate_missing_transaction(player_id, current_team_id, check_date, conn, cur)
        if len(result) == 4:
            trans_type, message, confidence, notes = result
        else:
            trans_type, message = result
            confidence = 0
            notes = ""
        
        print(message)
        if confidence > 0 and confidence < 70:
            print(f"    → Notes: {notes}")
        
        if trans_type:
            backfilled += 1
        elif 'ERROR' in message or 'SKIP' in message:
            errors += 1
        else:
            skipped += 1
    
    cur.close()
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"INVESTIGATION COMPLETE")
    print(f"{'='*60}")
    print(f"Transactions backfilled: {backfilled}")
    print(f"Skipped (injured/role player/already recorded): {skipped}")
    print(f"Errors: {errors}")
    print(f"{'='*60}")

def update_player_team_from_recent_games(check_date=None):
    if check_date is None:
        check_date = datetime.now().date()
    elif isinstance(check_date, str):
        check_date = datetime.strptime(check_date, '%Y-%m-%d').date()
    
    print("\n" + "="*60)
    print("UPDATING PLAYER TEAMS FROM RECENT GAMES")
    print("="*60)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        WITH recent_team AS (
            SELECT DISTINCT ON (p.player_id)
                p.player_id,
                pgs.team_id as recent_team_id,
                g.game_date
            FROM players p
            JOIN player_game_stats pgs ON p.player_id = pgs.player_id
            JOIN games g ON pgs.game_id = g.game_id
            WHERE p.is_active = TRUE
            AND g.game_status = 'completed'
            AND g.game_date >= %s::date - INTERVAL '60 days'
            ORDER BY p.player_id, g.game_date DESC
        )
        SELECT rt.player_id, p.full_name, p.team_id as current_team_id, rt.recent_team_id
        FROM recent_team rt
        JOIN players p ON rt.player_id = p.player_id
        WHERE rt.recent_team_id IS NOT NULL
        AND (p.team_id IS NULL OR p.team_id != rt.recent_team_id)
    """, (check_date,))
    
    updates = cur.fetchall()
    
    if len(updates) == 0:
        print("No players need team updates from recent games.")
        cur.close()
        conn.close()
        return
    
    print(f"Found {len(updates)} players with team mismatches\n")
    
    updated_count = 0
    for player_id, player_name, current_team_id, recent_team_id in updates:
        try:
            safe_name = player_name.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            print(f"  Updating {safe_name} (ID: {player_id}): {current_team_id} -> {recent_team_id}")
        except Exception:
            print(f"  Updating Player ID {player_id}: {current_team_id} -> {recent_team_id}")
        try:
            cur.execute("""
                UPDATE players
                SET team_id = %s
                WHERE player_id = %s
            """, (recent_team_id, player_id))
            updated_count += 1
        except Exception as e:
            print(f"    Error: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\nUpdated {updated_count} players from recent game data")

if __name__ == "__main__":
    import sys
    
    update_all = '--all' in sys.argv
    
    from_games = '--from-games' in sys.argv
    
    backfill = '--backfill' in sys.argv
    
    check_date = None
    season = None
    for arg in sys.argv[1:]:
        if arg.startswith('--date='):
            check_date = arg.split('=')[1]
        elif arg.startswith('--season='):
            season = arg.split('=')[1]
        elif not arg.startswith('--') and check_date is None:
            check_date = arg
    
    if backfill:
        backfill_missing_transactions(check_date, season)
    elif from_games:
        update_player_team_from_recent_games(check_date)
    else:
        detect_and_update_trades(check_date, update_all_players=update_all)

