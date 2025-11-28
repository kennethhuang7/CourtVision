from nba_api.stats.endpoints import boxscoretraditionalv3, commonplayerinfo
from utils import get_db_connection, rate_limit
import sys
import os
import pandas as pd
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def safe_float(val):
    if pd.isna(val) or val == '':
        return None
    try:
        return float(val)
    except:
        return None

def safe_int(val):
    if pd.isna(val) or val == '':
        return None
    try:
        return int(val)
    except:
        return None

def safe_str(val):
    if pd.isna(val) or val == '':
        return None
    return str(val)

def parse_minutes(minutes_str):
    if pd.isna(minutes_str) or minutes_str == '' or minutes_str == 'None':
        return None
    try:
        if ':' in str(minutes_str):
            parts = str(minutes_str).split(':')
            mins = int(parts[0])
            secs = int(parts[1])
            return round(mins + (secs / 60.0), 2)
        else:
            return float(minutes_str)
    except:
        return None

def add_missing_player(player_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        rate_limit(0.5)
        
        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        info_df = player_info.get_data_frames()[0]
        
        if len(info_df) > 0:
            info = info_df.iloc[0]
            
            full_name = safe_str(info.get('DISPLAY_FIRST_LAST', 'Unknown Player'))
            first_name = safe_str(info.get('FIRST_NAME', ''))
            last_name = safe_str(info.get('LAST_NAME', ''))
            team_id = safe_int(info.get('TEAM_ID'))
            
            if team_id == 0:
                team_id = None
            
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
                ON CONFLICT (player_id) DO NOTHING
            """, (
                player_id, full_name, first_name, last_name, False,
                team_id, jersey, position, height_inches, weight,
                birthdate, draft_year, draft_round, draft_number
            ))
            conn.commit()
            cur.close()
            conn.close()
            return True
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return False

def collect_player_stats_for_season(season='2024-25'):
    print(f"Collecting player stats for {season}...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT game_id FROM games 
        WHERE season = %s AND game_status = 'completed'
        ORDER BY game_date
    """, (season,))
    
    games = cur.fetchall()
    game_ids = [g[0] for g in games]
    
    print(f"Found {len(game_ids)} completed games")
    
    count = 0
    total_inserts = 0
    players_added = 0
    failed_games = []
    max_retries = 3
    
    for game_id in game_ids:
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                if retry_count > 0:
                    wait_time = 2 * retry_count
                    time.sleep(wait_time)
                
                rate_limit()
                
                boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
                player_stats = boxscore.get_data_frames()[0]
                
                game_inserts = 0
                
                for idx, row in player_stats.iterrows():
                    player_id = safe_int(row.get('personId'))
                    team_id = safe_int(row.get('teamId'))
                    
                    if not player_id or not team_id:
                        continue
                    
                    cur.execute("SELECT player_id FROM players WHERE player_id = %s", (player_id,))
                    if not cur.fetchone():
                        if add_missing_player(player_id):
                            players_added += 1
                        else:
                            continue
                    
                    start_position = str(row.get('position', ''))
                    is_starter = start_position != '' and start_position != 'None' and start_position != 'nan'
                    
                    minutes = parse_minutes(row.get('minutes'))
                    if minutes == 0 or minutes is None:
                        continue
                    
                    try:
                        cur.execute("""
                            INSERT INTO player_game_stats (
                                player_id, game_id, team_id, is_starter, minutes_played,
                                points, rebounds_offensive, rebounds_defensive, rebounds_total,
                                assists, steals, blocks, turnovers, personal_fouls,
                                field_goals_made, field_goals_attempted,
                                three_pointers_made, three_pointers_attempted,
                                free_throws_made, free_throws_attempted, plus_minus
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (player_id, game_id) DO NOTHING
                        """, (
                            player_id, game_id, team_id, is_starter, minutes,
                            safe_int(row.get('points')),
                            safe_int(row.get('reboundsOffensive')),
                            safe_int(row.get('reboundsDefensive')),
                            safe_int(row.get('reboundsTotal')),
                            safe_int(row.get('assists')),
                            safe_int(row.get('steals')),
                            safe_int(row.get('blocks')),
                            safe_int(row.get('turnovers')),
                            safe_int(row.get('foulsPersonal')),
                            safe_int(row.get('fieldGoalsMade')),
                            safe_int(row.get('fieldGoalsAttempted')),
                            safe_int(row.get('threePointersMade')),
                            safe_int(row.get('threePointersAttempted')),
                            safe_int(row.get('freeThrowsMade')),
                            safe_int(row.get('freeThrowsAttempted')),
                            safe_int(row.get('plusMinusPoints'))
                        ))
                        game_inserts += 1
                        
                    except Exception as e:
                        continue
                
                conn.commit()
                total_inserts += game_inserts
                count += 1
                success = True
                
                if count % 50 == 0:
                    print(f"Processed {count}/{len(game_ids)} games...")
                    print(f"Total player stats inserted: {total_inserts}")
                    print(f"Players added: {players_added}")
                    print(f"Failed games so far: {len(failed_games)}")
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    failed_games.append(game_id)
                    print(f"SKIPPING game {game_id} after {max_retries} failed attempts")
                    break
                continue
    
    cur.close()
    conn.close()
    
    print(f"\n=== {season} COMPLETE ===")
    print(f"Successfully processed: {count} games")
    print(f"Total player stats inserted: {total_inserts}")
    print(f"Players added: {players_added}")
    print(f"Failed games: {len(failed_games)}")
    
    if failed_games:
        print(f"\nFailed game IDs: {failed_games[:20]}")
        if len(failed_games) > 20:
            print(f"... and {len(failed_games) - 20} more")
    
    return failed_games

if __name__ == "__main__":
    seasons = ['2022-23', '2023-24', '2024-25']
    
    all_failed = []
    
    for season in seasons:
        failed = collect_player_stats_for_season(season)
        all_failed.extend(failed)
        print(f"Completed {season}\n")
    
    print(f"\n=== ALL SEASONS COMPLETE ===")
    print(f"Total failed games across all seasons: {len(all_failed)}")
    
    if all_failed:
        with open('failed_games.txt', 'w') as f:
            for game_id in all_failed:
                f.write(f"{game_id}\n")
        print("Failed game IDs saved to failed_games.txt")