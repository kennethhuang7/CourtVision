import sys
import os

if sys.platform == 'win32':
    import io
    try:
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'buffer'):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

from datetime import datetime, timedelta
import requests
import time

def get_request_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }

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

def test_espn_api(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(str(target_date), '%Y-%m-%d').date()

    print("="*60)
    print("ESPN API TEST (DRY RUN - NO DATABASE WRITES)")
    print("="*60)
    print(f"\nTesting for date: {target_date}\n")

    print("STEP 1: Fetching ESPN Scoreboard API...")
    print("-"*40)

    try:
        formatted_date = target_date.strftime('%Y%m%d')
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={formatted_date}"
        print(f"URL: {url}")

        response = requests.get(url, headers=get_request_headers(), timeout=30)
        print(f"Response status: {response.status_code}")

        if response.status_code != 200:
            print(f"[FAILED] Bad response: {response.status_code}")
            return False

        data = response.json()
        events = data.get('events', [])
        print(f"Games found: {len(events)}")

        if not events:
            print("\nNo games found. This could mean:")
            print("  - It's an off-day (no NBA games)")
            print("  - Date is in the future")
            return True

        games = []
        for event in events:
            game_id = event.get('id')
            status = event.get('status', {}).get('type', {}).get('name', '')
            competitions = event.get('competitions', [])

            if competitions:
                comp = competitions[0]
                competitors = comp.get('competitors', [])
                if len(competitors) == 2:
                    home = next((c for c in competitors if c.get('homeAway') == 'home'), {})
                    away = next((c for c in competitors if c.get('homeAway') == 'away'), {})
                    home_team = home.get('team', {}).get('abbreviation', '?')
                    away_team = away.get('team', {}).get('abbreviation', '?')
                    home_score = home.get('score', '0')
                    away_score = away.get('score', '0')

                    status_display = 'Final' if status == 'STATUS_FINAL' else status
                    print(f"  {game_id}: {away_team} {away_score} @ {home_team} {home_score} ({status_display})")

                    if status == 'STATUS_FINAL':
                        games.append({
                            'espn_game_id': game_id,
                            'home_team': home_team,
                            'away_team': away_team,
                            'home_score': int(home_score or 0),
                            'away_score': int(away_score or 0)
                        })

    except Exception as e:
        print(f"[FAILED] Error fetching scoreboard: {e}")
        return False

    if not games:
        print("\nNo completed games to test box scores.")
        return True

    print(f"\nSTEP 2: Fetching Box Scores for {len(games)} completed games...")
    print("-"*40)

    successful = 0
    failed = 0

    for game in games:
        espn_game_id = game['espn_game_id']
        print(f"\n  Game {espn_game_id} ({game['away_team']} @ {game['home_team']}):")

        try:
            time.sleep(0.5)

            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={espn_game_id}"
            response = requests.get(url, headers=get_request_headers(), timeout=30)

            if response.status_code != 200:
                print(f"    [FAILED] Bad response: {response.status_code}")
                failed += 1
                continue

            data = response.json()
            boxscore = data.get('boxscore', {})
            players_data = boxscore.get('players', [])

            total_players = 0
            team_scores = {}

            for team_players in players_data:
                team_info = team_players.get('team', {})
                team_abbrev = team_info.get('abbreviation', 'UNK')
                team_scores[team_abbrev] = 0

                statistics = team_players.get('statistics', [])
                if not statistics:
                    continue

                stats_section = statistics[0]
                athletes = stats_section.get('athletes', [])

                print(f"    {team_abbrev}: {len(athletes)} players")

                for athlete in athletes[:2]:
                    player_info = athlete.get('athlete', {})
                    player_name = player_info.get('displayName', 'Unknown')
                    stats = athlete.get('stats', [])

                    if len(stats) >= 14:
                        mins = stats[0]
                        pts = stats[1]
                        reb = stats[5]
                        ast = stats[6]
                        print(f"      - {player_name}: {pts} pts, {reb} reb, {ast} ast ({mins} min)")

                        try:
                            team_scores[team_abbrev] += int(pts)
                        except:
                            pass

                        total_players += 1

            if total_players > 0:
                print(f"    [OK] Parsed {total_players} players with stats")
                successful += 1
            else:
                print(f"    [WARNING] No player stats found")
                failed += 1

        except Exception as e:
            print(f"    [FAILED] Error: {e}")
            failed += 1

    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"Total games: {len(games)}")
    print(f"Successfully parsed: {successful}")
    print(f"Failed: {failed}")

    if failed == 0 and successful > 0:
        print("\n[SUCCESS] ESPN API is working correctly!")
        print("You can now run the full pipeline.")
        return True
    elif successful > 0:
        print(f"\n[PARTIAL] {successful}/{len(games)} games parsed.")
        return True
    else:
        print("\n[FAILED] Could not parse any games.")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        success = test_espn_api(target_date)
    else:
        success = test_espn_api()

    sys.exit(0 if success else 1)
