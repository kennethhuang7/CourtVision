from nba_api.stats.endpoints import boxscoretraditionalv3, commonplayerinfo
from utils import get_db_connection, rate_limit
import sys
import os
import pandas as pd
import time
from datetime import datetime
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

def get_missing_games(min_players=15):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT g.game_id, g.season, COUNT(pgs.stat_id) as player_count
        FROM games g
        LEFT JOIN player_game_stats pgs ON g.game_id = pgs.game_id
        WHERE g.game_status = 'completed'
        GROUP BY g.game_id, g.season
        HAVING COUNT(pgs.stat_id) < %s
        ORDER BY g.game_date
    """, (min_players,))
    
    missing = cur.fetchall()
    cur.close()
    conn.close()
    
    return missing

def verify_game_complete(conn, game_id, min_players=15):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM player_game_stats
        WHERE game_id = %s
    """, (game_id,))
    
    count = cur.fetchone()[0]
    cur.close()
    
    return count >= min_players

def collect_batch(batch_size=50, max_failures=10, min_players=15):
    print(f"=== BATCH COLLECTION START ===")
    print(f"Time: {datetime.now()}")
    print(f"Batch size: {batch_size} games")
    print(f"Min players required per game: {min_players}")
    
    missing_games = get_missing_games(min_players=min_players)
    
    if len(missing_games) == 0:
        print("No missing games! Collection complete!")
        return True
    
    incomplete_games = [g for g in missing_games if g[2] > 0]
    empty_games = [g for g in missing_games if g[2] == 0]
    
    print(f"\nTotal incomplete/missing games: {len(missing_games)}")
    print(f"  - Empty games (0 players): {len(empty_games)}")
    print(f"  - Incomplete games (1-{min_players-1} players): {len(incomplete_games)}")
    print(f"\nWill collect up to {batch_size} games in this batch\n")
    
    games_to_process = missing_games[:batch_size]
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    count = 0
    total_inserts = 0
    players_added = 0
    failed_games = []
    incomplete_after_collection = []
    consecutive_failures = 0
    max_retries = 3
    
    for game_id, season, existing_count in games_to_process:
        if existing_count > 0:
            print(f"Game {game_id} has {existing_count} players, re-collecting...")
            cur.execute("DELETE FROM player_game_stats WHERE game_id = %s", (game_id,))
            conn.commit()
        
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                if retry_count > 0:
                    wait_time = 5 * retry_count
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
                
                if verify_game_complete(conn, game_id, min_players):
                    total_inserts += game_inserts
                    count += 1
                    success = True
                    consecutive_failures = 0
                    
                    if count % 10 == 0:
                        remaining = len(missing_games) - count
                        print(f"Progress: {count}/{batch_size} games in this batch")
                        print(f"Total stats inserted: {total_inserts}")
                        print(f"Remaining games overall: {remaining}")
                else:
                    print(f"WARNING: Game {game_id} only has {game_inserts} players (expected {min_players}+)")
                    incomplete_after_collection.append((game_id, game_inserts))
                    success = True
                    consecutive_failures = 0
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    failed_games.append(game_id)
                    consecutive_failures += 1
                    print(f"Failed game {game_id} after {max_retries} retries")
                    
                    if consecutive_failures >= max_failures:
                        print(f"\n!!! HIT {max_failures} CONSECUTIVE FAILURES !!!")
                        print("Stopping batch to avoid prolonged rate limiting")
                        print("Wait 2-4 hours and run again")
                        cur.close()
                        conn.close()
                        return False
                    break
                continue
    
    cur.close()
    conn.close()
    
    print(f"\n=== BATCH COMPLETE ===")
    print(f"Games fully completed: {count}")
    print(f"Stats inserted: {total_inserts}")
    print(f"Players added: {players_added}")
    print(f"Failed games: {len(failed_games)}")
    
    if incomplete_after_collection:
        print(f"\nWARNING: {len(incomplete_after_collection)} games still incomplete after collection:")
        for gid, pcount in incomplete_after_collection[:10]:
            print(f"  Game {gid}: {pcount} players")
        if len(incomplete_after_collection) > 10:
            print(f"  ... and {len(incomplete_after_collection) - 10} more")
    
    remaining = get_missing_games(min_players=min_players)
    print(f"\nRemaining games to collect: {len(remaining)}")
    
    if len(remaining) == 0:
        print("\n*** ALL DATA COLLECTION COMPLETE! ***")
        return True
    else:
        print(f"\nRun this script again to collect next batch")
        return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch-size', type=int, default=50, help='Number of games to collect')
    parser.add_argument('--max-failures', type=int, default=10, help='Max consecutive failures before stopping')
    parser.add_argument('--min-players', type=int, default=15, help='Minimum players required per game')
    args = parser.parse_args()
    
    collect_batch(batch_size=args.batch_size, max_failures=args.max_failures, min_players=args.min_players)