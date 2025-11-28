from nba_api.stats.static import players as nba_players
from nba_api.stats.endpoints import commonplayerinfo
from utils import get_db_connection, rate_limit
import sys
import os
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def safe_int(val):
    if pd.isna(val) or val == '' or val == 'Undrafted' or val == 0:
        return None
    try:
        return int(val)
    except:
        return None

def safe_str(val):
    if pd.isna(val) or val == '':
        return None
    return str(val)

def collect_players():
    print("Collecting NBA players data...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    all_players = nba_players.get_players()
    active_players = [p for p in all_players if p['is_active']]
    
    print(f"Found {len(active_players)} active players")
    
    count = 0
    errors = 0
    
    for player in active_players:
        try:
            player_id = int(player['id'])
            full_name = str(player['full_name'])
            first_name = safe_str(player.get('first_name', ''))
            last_name = safe_str(player.get('last_name', ''))
            is_active = bool(player['is_active'])
            
            rate_limit()
            
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            info_df = player_info.get_data_frames()[0]
            
            if len(info_df) > 0:
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
                        full_name = EXCLUDED.full_name,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        is_active = EXCLUDED.is_active,
                        team_id = EXCLUDED.team_id,
                        jersey_number = EXCLUDED.jersey_number,
                        position = EXCLUDED.position,
                        height_inches = EXCLUDED.height_inches,
                        weight_lbs = EXCLUDED.weight_lbs,
                        birth_date = EXCLUDED.birth_date,
                        draft_year = EXCLUDED.draft_year,
                        draft_round = EXCLUDED.draft_round,
                        draft_number = EXCLUDED.draft_number
                """, (
                    player_id, full_name, first_name, last_name, is_active,
                    team_id, jersey, position, height_inches, weight,
                    birthdate, draft_year, draft_round, draft_number
                ))
                
                count += 1
                if count % 50 == 0:
                    print(f"Processed {count}/{len(active_players)} players...")
                    conn.commit()
            
        except Exception as e:
            errors += 1
            print(f"Error processing {player.get('full_name', 'Unknown')}: {e}")
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Successfully inserted {count} players!")
    print(f"Errors: {errors}")

if __name__ == "__main__":
    collect_players()