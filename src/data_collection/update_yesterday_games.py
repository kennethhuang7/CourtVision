import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3
from utils import get_db_connection
from datetime import datetime, timedelta
import time
from requests.exceptions import ReadTimeout

def parse_minutes(min_string):
    if not min_string or min_string == '0' or min_string == '':
        return 0
    try:
        if ':' in str(min_string):
            parts = str(min_string).split(':')
            return int(parts[0]) + (int(parts[1]) / 60)
        return float(min_string)
    except:
        return 0

def add_missing_player(cur, player_id, player_name):
    cur.execute("SELECT player_id FROM players WHERE player_id = %s", (player_id,))
    if not cur.fetchone():
        print(f"  Adding new player: {player_name} (ID: {player_id})")
        cur.execute("""
            INSERT INTO players (player_id, full_name)
            VALUES (%s, %s)
            ON CONFLICT (player_id) DO NOTHING
        """, (player_id, player_name))

def update_yesterday_games(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()
    
    print(f"Updating games for {target_date}...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    season_year = target_date.year
    season_month = target_date.month
    
    if season_month >= 10:
        season = f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        season = f"{season_year-1}-{str(season_year)[-2:]}"
    
    formatted_date = target_date.strftime('%m/%d/%Y')
    
    max_retries = 3
    gamefinder = None
    
    for attempt in range(max_retries):
        try:
            gamefinder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                date_from_nullable=formatted_date,
                date_to_nullable=formatted_date,
                timeout=60
            )
            break
        except (ReadTimeout, Exception) as e:
            if attempt < max_retries - 1:
                print(f"  Timeout/error, retrying in 10 seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(10)
            else:
                print(f"  Failed after {max_retries} attempts: {e}")
                cur.close()
                conn.close()
                sys.exit(1)
    
    games_df = gamefinder.get_data_frames()[0]
    
    unique_game_ids = games_df['GAME_ID'].unique()
    
    valid_game_ids = []
    for game_id in unique_game_ids:
        if str(game_id).startswith('002'):
            valid_game_ids.append(game_id)
        else:
            print(f"Skipping non-NBA game: {game_id}")
    
    unique_game_ids = valid_game_ids
    
    print(f"Found {len(unique_game_ids)} NBA games\n")
    
    for game_id in unique_game_ids:
        print(f"Processing {game_id}...")
        
        time.sleep(1)
        
        cur.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
        game_exists = cur.fetchone()
        
        max_retries_box = 3
        boxscore = None
        
        for attempt in range(max_retries_box):
            try:
                boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=60)
                break
            except (ReadTimeout, Exception) as e:
                if attempt < max_retries_box - 1:
                    print(f"  Box score timeout, retrying in 10 seconds... (attempt {attempt + 1}/{max_retries_box})")
                    time.sleep(10)
                else:
                    print(f"  Failed to get box score after {max_retries_box} attempts: {e}")
                    continue
        
        if boxscore is None:
            continue
        
        player_stats = boxscore.get_data_frames()[0]
        
        home_score = player_stats[player_stats['personId'] != 0].groupby('teamId')['points'].sum().max()
        away_score = player_stats[player_stats['personId'] != 0].groupby('teamId')['points'].sum().min()
        
        game_row = games_df[games_df['GAME_ID'] == game_id].iloc[0]
        matchup = game_row['MATCHUP']
        team_id = game_row['TEAM_ID']
        
        if 'vs.' in matchup:
            home_team_id = team_id
            away_team_id = games_df[(games_df['GAME_ID'] == game_id) & (games_df['TEAM_ID'] != team_id)].iloc[0]['TEAM_ID']
        else:
            away_team_id = team_id
            home_team_id = games_df[(games_df['GAME_ID'] == game_id) & (games_df['TEAM_ID'] != team_id)].iloc[0]['TEAM_ID']
        
        if not game_exists:
            print(f"  Inserting new game {game_id}...")
            
            cur.execute("""
                INSERT INTO games (
                    game_id, game_date, season, home_team_id, away_team_id,
                    home_score, away_score, game_status, game_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                game_id,
                target_date,
                season,
                int(home_team_id),
                int(away_team_id),
                int(home_score),
                int(away_score),
                'completed',
                'regular_season'
            ))
        else:
            print(f"  Game {game_id} already exists, updating stats...")
            cur.execute("""
                UPDATE games SET
                    home_score = %s,
                    away_score = %s,
                    game_status = 'completed'
                WHERE game_id = %s
            """, (int(home_score), int(away_score), game_id))
        
        for _, player in player_stats.iterrows():
            player_id = int(player['personId'])
            
            if player_id == 0:
                continue
            
            player_name = player.get('name', player.get('firstName', '') + ' ' + player.get('familyName', f'Player_{player_id}'))
            add_missing_player(cur, player_id, player_name)
            
            cur.execute("""
                SELECT stat_id FROM player_game_stats 
                WHERE game_id = %s AND player_id = %s
            """, (game_id, player_id))
            
            if cur.fetchone():
                continue
            
            minutes_played = parse_minutes(player['minutes'])
            
            cur.execute("""
                INSERT INTO player_game_stats (
                    game_id, player_id, team_id, minutes_played,
                    points, rebounds_offensive, rebounds_defensive, rebounds_total,
                    assists, steals, blocks, turnovers, personal_fouls,
                    field_goals_made, field_goals_attempted,
                    three_pointers_made, three_pointers_attempted,
                    free_throws_made, free_throws_attempted,
                    plus_minus
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                game_id,
                player_id,
                int(player['teamId']),
                minutes_played,
                int(player['points']),
                int(player['reboundsOffensive']),
                int(player['reboundsDefensive']),
                int(player['reboundsTotal']),
                int(player['assists']),
                int(player['steals']),
                int(player['blocks']),
                int(player['turnovers']),
                int(player['foulsPersonal']),
                int(player['fieldGoalsMade']),
                int(player['fieldGoalsAttempted']),
                int(player['threePointersMade']),
                int(player['threePointersAttempted']),
                int(player['freeThrowsMade']),
                int(player['freeThrowsAttempted']),
                int(player['plusMinusPoints'])
            ))
        
        print(f"  Updated game {game_id}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"Updated {len(unique_game_ids)} games for {target_date}")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        update_yesterday_games(target_date)
    else:
        update_yesterday_games()