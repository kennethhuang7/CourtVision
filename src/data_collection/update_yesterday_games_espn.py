import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection
from datetime import datetime, timedelta
import requests
import re
import time

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
        'đ': 'd', 'Đ': 'd', 'ñ': 'n', 'ń': 'n', 'ň': 'n', 'ņ': 'n',
        'Ñ': 'n', 'Ń': 'n', 'Ň': 'n', 'Ņ': 'n',
        'š': 's', 'ś': 's', 'ş': 's', 'Š': 's', 'Ś': 's', 'Ş': 's',
        'ž': 'z', 'ź': 'z', 'ż': 'z', 'Ž': 'z', 'Ź': 'z', 'Ż': 'z',
        'ģ': 'g', 'Ģ': 'g', 'ķ': 'k', 'Ķ': 'k', 'ļ': 'l', 'Ļ': 'l',
    }
    return ''.join(char_map.get(c, c) for c in name)

def clean_name_for_matching(name):
    name = normalize_name(name)
    suffix_pattern = r'\s+(Jr\.?|Sr\.?|II|III|IV)$'
    name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
    name = name.replace("'", "").replace("-", " ").replace(".", "")
    return name.strip().lower()

def safe_print(message):
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode('ascii', 'replace').decode('ascii'))

def get_request_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9'
    }

def get_team_id_from_abbreviation(cur, abbreviation):
    espn_to_db = {
        'GS': 'GSW', 'gs': 'GSW', 'WSH': 'WAS', 'wsh': 'WAS',
        'NO': 'NOP', 'no': 'NOP', 'NY': 'NYK', 'ny': 'NYK',
        'SA': 'SAS', 'sa': 'SAS', 'UTAH': 'UTA', 'utah': 'UTA',
        'PHX': 'PHX', 'phx': 'PHX', 'PHOE': 'PHX', 'phoe': 'PHX',
    }
    abbrev = abbreviation.upper()
    if abbrev in espn_to_db:
        abbrev = espn_to_db[abbrev]
    cur.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (abbrev,))
    result = cur.fetchone()
    return result[0] if result else None

def find_player_in_db(cur, player_name):
    cleaned_search = clean_name_for_matching(player_name)
    cur.execute("SELECT player_id, full_name FROM players")
    all_players = cur.fetchall()

    for player_id, full_name in all_players:
        if clean_name_for_matching(full_name) == cleaned_search:
            return player_id

    search_parts = cleaned_search.split()
    if len(search_parts) >= 2:
        first_name = search_parts[0]
        last_name = search_parts[-1]
        for player_id, full_name in all_players:
            db_cleaned = clean_name_for_matching(full_name)
            db_parts = db_cleaned.split()
            if len(db_parts) >= 2:
                if db_parts[0] == first_name and db_parts[-1] == last_name:
                    return player_id
                if first_name in db_parts[0] and last_name in db_parts[-1]:
                    return player_id

    return None

def parse_stat(val):
    if not val or val == '--' or val == '':
        return 0
    try:
        return int(val)
    except:
        return 0

def parse_made_attempted(val):
    if not val or val == '--' or val == '':
        return 0, 0
    try:
        if '-' in str(val):
            parts = str(val).split('-')
            return int(parts[0]), int(parts[1])
        return 0, 0
    except:
        return 0, 0

def fetch_espn_scoreboard(target_date):
    formatted_date = target_date.strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={formatted_date}"

    safe_print(f"Fetching ESPN API scoreboard: {url}")

    response = requests.get(url, headers=get_request_headers(), timeout=30)
    response.raise_for_status()

    data = response.json()
    events = data.get('events', [])

    games = []
    for event in events:
        game_id = event.get('id')
        competitions = event.get('competitions', [])

        if not competitions:
            continue

        comp = competitions[0]
        competitors = comp.get('competitors', [])

        if len(competitors) != 2:
            continue

        home = next((c for c in competitors if c.get('homeAway') == 'home'), {})
        away = next((c for c in competitors if c.get('homeAway') == 'away'), {})

        home_team = home.get('team', {}).get('abbreviation', '')
        away_team = away.get('team', {}).get('abbreviation', '')
        home_score = int(home.get('score', 0) or 0)
        away_score = int(away.get('score', 0) or 0)

        status = event.get('status', {}).get('type', {}).get('name', '')

        games.append({
            'espn_game_id': game_id,
            'home_team': home_team,
            'away_team': away_team,
            'home_score': home_score,
            'away_score': away_score,
            'status': status
        })

    return games

def fetch_espn_boxscore(espn_game_id):
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={espn_game_id}"

    response = requests.get(url, headers=get_request_headers(), timeout=30)
    response.raise_for_status()

    data = response.json()

    game_data = {
        'espn_game_id': espn_game_id,
        'away_team': None,
        'home_team': None,
        'away_score': 0,
        'home_score': 0,
        'players': []
    }

    boxscore = data.get('boxscore', {})
    teams = boxscore.get('teams', [])
    players_data = boxscore.get('players', [])

    if len(teams) >= 2:
        for i, team in enumerate(teams):
            team_info = team.get('team', {})
            abbrev = team_info.get('abbreviation', '')
            home_away = team_info.get('homeAway', '')

            if not home_away:
                home_away = 'away' if i == 0 else 'home'

            if home_away == 'home':
                game_data['home_team'] = abbrev
            else:
                game_data['away_team'] = abbrev

    header = data.get('header', {})
    competitions = header.get('competitions', [])
    if competitions:
        comp = competitions[0]
        competitors = comp.get('competitors', [])
        for competitor in competitors:
            home_away = competitor.get('homeAway', '')
            score = int(competitor.get('score', 0) or 0)
            if home_away == 'home':
                game_data['home_score'] = score
            else:
                game_data['away_score'] = score

    for i, team_players in enumerate(players_data):
        team_info = team_players.get('team', {})
        team_abbrev = team_info.get('abbreviation', '')

        is_home = (team_abbrev == game_data['home_team'])

        statistics = team_players.get('statistics', [])
        if not statistics:
            continue

        stats_section = statistics[0]
        athletes = stats_section.get('athletes', [])

        for athlete in athletes:
            player_info = athlete.get('athlete', {})
            player_name = player_info.get('displayName', '')
            stats = athlete.get('stats', [])

            if len(stats) < 14:
                continue

            minutes_str = stats[0]
            if minutes_str == '--' or minutes_str == 'DNP' or minutes_str == '0':
                continue

            try:
                minutes = int(minutes_str)
            except:
                minutes = 0

            if minutes == 0:
                continue

            pts = parse_stat(stats[1])
            fg_made, fg_att = parse_made_attempted(stats[2])
            tp_made, tp_att = parse_made_attempted(stats[3])
            ft_made, ft_att = parse_made_attempted(stats[4])
            reb = parse_stat(stats[5])
            ast = parse_stat(stats[6])
            tov = parse_stat(stats[7])
            stl = parse_stat(stats[8])
            blk = parse_stat(stats[9])
            oreb = parse_stat(stats[10])
            dreb = parse_stat(stats[11])
            pf = parse_stat(stats[12])

            pm = 0
            if len(stats) > 13:
                pm_str = str(stats[13]).replace('+', '')
                try:
                    pm = int(pm_str) if pm_str and pm_str != '--' else 0
                except:
                    pm = 0

            game_data['players'].append({
                'name': player_name,
                'team_abbrev': team_abbrev,
                'is_home': is_home,
                'minutes': minutes,
                'points': pts,
                'rebounds_total': reb,
                'rebounds_offensive': oreb,
                'rebounds_defensive': dreb,
                'assists': ast,
                'steals': stl,
                'blocks': blk,
                'turnovers': tov,
                'personal_fouls': pf,
                'field_goals_made': fg_made,
                'field_goals_attempted': fg_att,
                'three_pointers_made': tp_made,
                'three_pointers_attempted': tp_att,
                'free_throws_made': ft_made,
                'free_throws_attempted': ft_att,
                'plus_minus': pm
            })

    return game_data

def construct_game_id(target_date, sequence):
    year_str = target_date.strftime('%y')
    return f"002{year_str}{sequence:04d}"

def update_yesterday_games_espn(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()

    safe_print(f"Updating games for {target_date} via ESPN API...")

    conn = get_db_connection()
    cur = conn.cursor()

    season_year = target_date.year
    season_month = target_date.month

    if season_month >= 10:
        season = f"{season_year}-{str(season_year+1)[-2:]}"
    else:
        season = f"{season_year-1}-{str(season_year)[-2:]}"

    try:
        games = fetch_espn_scoreboard(target_date)

        if not games:
            safe_print(f"No games found for {target_date}")
            cur.close()
            conn.close()
            return True

        completed_games = [g for g in games if g['status'] == 'STATUS_FINAL']
        safe_print(f"Found {len(games)} games, {len(completed_games)} completed\n")

        if not completed_games:
            safe_print("No completed games to process")
            cur.close()
            conn.close()
            return True

        games_processed = 0
        players_updated = 0
        errors = 0

        cur.execute("SELECT MAX(CAST(SUBSTRING(game_id, 6) AS INTEGER)) FROM games WHERE game_id LIKE %s AND LENGTH(game_id) = 9", (f"002{target_date.strftime('%y')}%",))
        result = cur.fetchone()
        next_sequence = (result[0] or 0) + 1 if result and result[0] else 1

        for game in completed_games:
            espn_game_id = game['espn_game_id']
            safe_print(f"Processing game {espn_game_id}...")

            time.sleep(1)

            try:
                box_data = fetch_espn_boxscore(espn_game_id)

                if not box_data['away_team'] or not box_data['home_team']:
                    box_data['away_team'] = game['away_team']
                    box_data['home_team'] = game['home_team']
                    box_data['away_score'] = game['away_score']
                    box_data['home_score'] = game['home_score']

                away_team_id = get_team_id_from_abbreviation(cur, box_data['away_team'])
                home_team_id = get_team_id_from_abbreviation(cur, box_data['home_team'])

                if not away_team_id or not home_team_id:
                    safe_print(f"  Could not find team IDs: {box_data['away_team']} or {box_data['home_team']}")
                    errors += 1
                    continue

                cur.execute("""
                    SELECT game_id FROM games
                    WHERE game_date = %s
                    AND home_team_id = %s
                    AND away_team_id = %s
                """, (target_date, home_team_id, away_team_id))
                existing = cur.fetchone()

                if existing:
                    game_id = existing[0]
                else:
                    game_id = construct_game_id(target_date, next_sequence)
                    next_sequence += 1

                cur.execute("""
                    INSERT INTO games (
                        game_id, game_date, season, home_team_id, away_team_id,
                        home_score, away_score, game_status, game_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id) DO UPDATE SET
                        home_score = EXCLUDED.home_score,
                        away_score = EXCLUDED.away_score,
                        game_status = EXCLUDED.game_status
                """, (
                    game_id, target_date, season, home_team_id, away_team_id,
                    box_data['home_score'], box_data['away_score'],
                    'completed', 'regular_season'
                ))

                safe_print(f"  {box_data['away_team']} {box_data['away_score']} @ {box_data['home_team']} {box_data['home_score']} -> {game_id}")

                players_in_game = 0
                for player in box_data['players']:
                    player_id = find_player_in_db(cur, player['name'])

                    if not player_id:
                        continue

                    if player['team_abbrev']:
                        team_id = get_team_id_from_abbreviation(cur, player['team_abbrev'])
                    elif player['is_home'] is not None:
                        team_id = home_team_id if player['is_home'] else away_team_id
                    else:
                        team_id = home_team_id

                    if not team_id:
                        team_id = home_team_id

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
                        ON CONFLICT (player_id, game_id) DO UPDATE SET
                            team_id = EXCLUDED.team_id,
                            minutes_played = EXCLUDED.minutes_played,
                            points = EXCLUDED.points,
                            rebounds_offensive = EXCLUDED.rebounds_offensive,
                            rebounds_defensive = EXCLUDED.rebounds_defensive,
                            rebounds_total = EXCLUDED.rebounds_total,
                            assists = EXCLUDED.assists,
                            steals = EXCLUDED.steals,
                            blocks = EXCLUDED.blocks,
                            turnovers = EXCLUDED.turnovers,
                            personal_fouls = EXCLUDED.personal_fouls,
                            field_goals_made = EXCLUDED.field_goals_made,
                            field_goals_attempted = EXCLUDED.field_goals_attempted,
                            three_pointers_made = EXCLUDED.three_pointers_made,
                            three_pointers_attempted = EXCLUDED.three_pointers_attempted,
                            free_throws_made = EXCLUDED.free_throws_made,
                            free_throws_attempted = EXCLUDED.free_throws_attempted,
                            plus_minus = EXCLUDED.plus_minus
                    """, (
                        game_id, player_id, team_id, player['minutes'],
                        player['points'], player['rebounds_offensive'],
                        player['rebounds_defensive'], player['rebounds_total'],
                        player['assists'], player['steals'], player['blocks'],
                        player['turnovers'], player['personal_fouls'],
                        player['field_goals_made'], player['field_goals_attempted'],
                        player['three_pointers_made'], player['three_pointers_attempted'],
                        player['free_throws_made'], player['free_throws_attempted'],
                        player['plus_minus']
                    ))

                    players_in_game += 1
                    players_updated += 1

                    cur.execute("SELECT team_id FROM players WHERE player_id = %s", (player_id,))
                    player_row = cur.fetchone()
                    if player_row and player_row[0] != team_id:
                        cur.execute("UPDATE players SET team_id = %s WHERE player_id = %s", (team_id, player_id))

                safe_print(f"    Players updated: {players_in_game}")
                games_processed += 1
                conn.commit()

            except Exception as e:
                safe_print(f"  Error processing game {espn_game_id}: {e}")
                errors += 1
                conn.rollback()
                continue

        cur.close()
        conn.close()

        safe_print(f"\n{'='*50}")
        safe_print(f"ESPN API UPDATE COMPLETE")
        safe_print(f"{'='*50}")
        safe_print(f"Games processed: {games_processed}")
        safe_print(f"Player stats updated: {players_updated}")
        safe_print(f"Errors: {errors}")
        safe_print(f"{'='*50}\n")

        if errors > 0 and games_processed == 0:
            safe_print("ERROR: No games were successfully processed!")
            return False

        return True

    except Exception as e:
        safe_print(f"ERROR: Failed to update games: {e}")
        import traceback
        traceback.print_exc()
        try:
            cur.close()
            conn.close()
        except:
            pass
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        success = update_yesterday_games_espn(target_date)
    else:
        success = update_yesterday_games_espn()

    sys.exit(0 if success else 1)
