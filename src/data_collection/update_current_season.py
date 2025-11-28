from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3, commonplayerinfo
from nba_api.stats.static import players as nba_players
from utils import get_db_connection, rate_limit
import sys
import os
import pandas as pd
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CURRENT_SEASON = '2025-26'

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

def update_players():
    print("Updating active players...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    all_players = nba_players.get_players()
    active_players = [p for p in all_players if p['is_active']]
    
    new_players = 0
    updated_players = 0
    
    for player in active_players:
        try:
            player_id = int(player['id'])
            
            cur.execute("SELECT player_id FROM players WHERE player_id = %s", (player_id,))
            exists = cur.fetchone()
            
            if exists:
                continue
            
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
                        is_active = EXCLUDED.is_active,
                        team_id = EXCLUDED.team_id,
                        position = EXCLUDED.position
                """, (
                    player_id, full_name, first_name, last_name, is_active,
                    team_id, jersey, position, height_inches, weight,
                    birthdate, draft_year, draft_round, draft_number
                ))
                
                if exists:
                    updated_players += 1
                else:
                    new_players += 1
                
                conn.commit()
            
        except Exception as e:
            print(f"Error processing {player.get('full_name', 'Unknown')}: {e}")
            continue
    
    cur.close()
    conn.close()
    
    print(f"New players added: {new_players}")
    print(f"Players updated: {updated_players}")

def update_games():
    print(f"\nUpdating games for {CURRENT_SEASON} season...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT team_id FROM teams")
    valid_team_ids = set([row[0] for row in cur.fetchall()])
    
    rate_limit()
    
    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable=CURRENT_SEASON,
        league_id_nullable='00'
    )
    
    games_df = gamefinder.get_data_frames()[0]
    
    games_dict = {}
    
    for idx, row in games_df.iterrows():
        game_id = str(row['GAME_ID'])
        game_date = str(row['GAME_DATE'])
        team_id = int(row['TEAM_ID'])
        matchup = str(row['MATCHUP'])
        
        if team_id not in valid_team_ids:
            continue
        
        if game_id not in games_dict:
            games_dict[game_id] = {
                'game_date': game_date,
                'season': CURRENT_SEASON,
                'teams': []
            }
        
        games_dict[game_id]['teams'].append({
            'team_id': team_id,
            'matchup': matchup,
            'score': int(row['PTS']) if pd.notna(row['PTS']) else None
        })
    
    count = 0
    updated = 0
    new = 0
    
    for game_id, game_data in games_dict.items():
        teams = game_data['teams']
        
        if len(teams) != 2:
            continue
        
        team1 = teams[0]
        team2 = teams[1]
        
        if team1['team_id'] not in valid_team_ids or team2['team_id'] not in valid_team_ids:
            continue
        
        if 'vs.' in team1['matchup']:
            home_team_id = team1['team_id']
            away_team_id = team2['team_id']
            home_score = team1['score']
            away_score = team2['score']
        else:
            home_team_id = team2['team_id']
            away_team_id = team1['team_id']
            home_score = team2['score']
            away_score = team1['score']
        
        game_status = 'completed' if home_score is not None else 'scheduled'
        
        if game_id.startswith('004'):
            game_type = 'playoffs'
        elif game_id.startswith('005'):
            game_type = 'play_in'
        else:
            game_type = 'regular_season'
        
        try:
            cur.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
            exists = cur.fetchone()
            
            cur.execute("""
                INSERT INTO games (game_id, game_date, season, home_team_id, away_team_id,
                                 home_score, away_score, game_status, game_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    game_status = EXCLUDED.game_status,
                    game_type = EXCLUDED.game_type
            """, (
                game_id, game_data['game_date'], CURRENT_SEASON,
                home_team_id, away_team_id,
                home_score, away_score, game_status, game_type
            ))
            
            if exists:
                updated += 1
            else:
                new += 1
            
            count += 1
            if count % 50 == 0:
                print(f"Processed {count} games...")
                conn.commit()
                
        except Exception as e:
            print(f"Error inserting game {game_id}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Games updated: {updated}")
    print(f"New games added: {new}")
    print(f"Total processed: {count}")

def update_player_stats():
    print(f"\nUpdating player stats for {CURRENT_SEASON}...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT g.game_id 
        FROM games g
        LEFT JOIN player_game_stats pgs ON g.game_id = pgs.game_id
        WHERE g.season = %s AND g.game_status = 'completed'
        GROUP BY g.game_id
        HAVING COUNT(pgs.stat_id) = 0
        ORDER BY g.game_date
    """, (CURRENT_SEASON,))
    
    games = cur.fetchall()
    game_ids = [g[0] for g in games]
    
    print(f"Found {len(game_ids)} games needing stats")
    
    if len(game_ids) == 0:
        print("All games already have stats collected!")
        cur.close()
        conn.close()
        return
    
    count = 0
    errors = 0
    
    for game_id in game_ids:
        try:
            rate_limit()
            
            boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
            player_stats = boxscore.get_data_frames()[0]
            
            for idx, row in player_stats.iterrows():
                player_id = safe_int(row['personId'])
                team_id = safe_int(row['teamId'])
                
                if not player_id or not team_id:
                    continue
                
                start_position = str(row.get('position', ''))
                is_starter = start_position != '' and start_position != 'None'
                
                minutes = safe_float(row.get('minutes'))
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
                    
                except Exception as e:
                    errors += 1
                    continue
            
            count += 1
            if count % 20 == 0:
                print(f"Processed {count}/{len(game_ids)} games...")
                conn.commit()
                
        except Exception as e:
            errors += 1
            print(f"Error processing game {game_id}: {e}")
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Successfully processed {count} games!")
    print(f"Errors: {errors}")

if __name__ == "__main__":
    print(f"=== UPDATING CURRENT SEASON: {CURRENT_SEASON} ===")
    print(f"Run date: {datetime.now()}\n")
    
    update_players()
    update_games()
    update_player_stats()
    
    print("\n=== CURRENT SEASON UPDATE COMPLETE ===")