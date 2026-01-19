import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3, commonplayerinfo
from utils import get_db_connection
from datetime import datetime, timedelta
from typing import Optional
import time
from requests.exceptions import ReadTimeout
import math

def _parse_height_inches(height_str):
    if not height_str or not isinstance(height_str, str) or "-" not in height_str:
        return None
    try:
        feet_s, inches_s = height_str.split("-", 1)
        return int(feet_s) * 12 + int(inches_s)
    except Exception:
        return None


def _infer_gfc_from_height_inches(height_inches):
    """
    Fallback when NBA API POSITION is missing.
    This is a heuristic and only used for fringe players without listed positions.
    """
    if height_inches is None:
        return None
    try:
        h = int(height_inches)
    except Exception:
        return None

    # Rough NBA heuristics:
    # <= 77" (6'5") => guard-ish
    # 78-81" => forward-ish
    # >= 82" (6'10") => center-ish
    if h <= 77:
        return "G"
    if h <= 81:
        return "F"
    return "C"


def _relation_exists(cur, table_name: str, schema: str = "public") -> bool:
    """
    Returns True if schema.table_name exists (as a table/view/etc).
    Uses to_regclass to avoid raising on missing relations.
    """
    try:
        cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table_name}",))
        return cur.fetchone()[0] is not None
    except Exception:
        # If permissions/search_path are weird, treat as missing and let callers skip.
        return False


def _exec_with_savepoint(cur, sql: str, params: Optional[tuple] = None, sp_name: str = "sp_tmp") -> int:
    """
    Execute one statement inside a savepoint, so failures don't abort the whole transaction.
    Returns rowcount on success, 0 on failure (and clears the aborted state via rollback to savepoint).
    """
    try:
        cur.execute(f"SAVEPOINT {sp_name}")
        cur.execute(sql, params)
        affected = cur.rowcount
        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
        return affected
    except Exception:
        # In an aborted transaction, ROLLBACK TO SAVEPOINT is one of the only allowed commands.
        try:
            cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception:
            # Worst case: caller may need to do a full rollback.
            pass
        return 0

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
    row = cur.fetchone()
    if not row:
        print(f"  Adding new player: {player_name} (ID: {player_id})")
        cur.execute("""
            INSERT INTO players (player_id, full_name)
            VALUES (%s, %s)
            ON CONFLICT (player_id) DO NOTHING
        """, (player_id, player_name))

    # If we have the player but are missing a position, try to enrich from NBA API once.
    cur.execute("SELECT position, height_inches FROM players WHERE player_id = %s", (player_id,))
    pos_row = cur.fetchone()
    pos_val = pos_row[0] if pos_row else None
    height_inches = pos_row[1] if pos_row else None
    if not pos_val:
        try:
            info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=30)
            info_df = info.get_data_frames()[0]
            if len(info_df) > 0:
                position = info_df.iloc[0].get("POSITION")
                height = info_df.iloc[0].get("HEIGHT")

                if height_inches is None and height:
                    parsed_hi = _parse_height_inches(str(height))
                    if parsed_hi is not None:
                        cur.execute(
                            "UPDATE players SET height_inches = %s WHERE player_id = %s AND height_inches IS NULL",
                            (parsed_hi, player_id),
                        )
                        height_inches = parsed_hi

                # Prefer NBA API position; fallback to height-based G/F/C inference.
                new_pos = str(position).strip() if position else None
                if not new_pos:
                    new_pos = _infer_gfc_from_height_inches(height_inches)

                if new_pos:
                    cur.execute(
                        "UPDATE players SET position = %s WHERE player_id = %s AND (position IS NULL OR btrim(position) = '')",
                        (new_pos, player_id),
                    )
        except Exception:
            # Non-fatal; position_defense_stats will be slightly incomplete until next successful enrichment.
            pass

def merge_predictions_to_real_game_id(cur, conn, real_game_id, target_date, home_team_id, away_team_id):
    cur.execute("""
        SELECT DISTINCT game_id
        FROM games
        WHERE game_date = %s
        AND home_team_id = %s
        AND away_team_id = %s
        AND game_id != %s
    """, (target_date, home_team_id, away_team_id, real_game_id))
    
    old_game_ids = [row[0] for row in cur.fetchall()]
    
    if not old_game_ids:
        return 0
    
    total_updated = 0
    
    for old_game_id in old_game_ids:
        cur.execute("""
            SELECT COUNT(*) FROM predictions
            WHERE game_id = %s
        """, (old_game_id,))
        pred_count = cur.fetchone()[0]
        
        cur.execute("""
            UPDATE player_game_stats
            SET game_id = %s
            WHERE game_id = %s
            AND NOT EXISTS (
                SELECT 1 FROM player_game_stats pgs2
                WHERE pgs2.player_id = player_game_stats.player_id
                AND pgs2.game_id = %s
            )
        """, (real_game_id, old_game_id, real_game_id))
        
        stats_merged = cur.rowcount
        if stats_merged > 0:
            print(f"    Merged {stats_merged} player_game_stats records from {old_game_id} to {real_game_id}")
            conn.commit()
        
        if pred_count == 0:
            cur.execute("""
                SELECT COUNT(*) FROM player_game_stats WHERE game_id = %s
            """, (old_game_id,))
            stats_count = cur.fetchone()[0]
            if stats_count == 0:
                try:
                    cur.execute("DELETE FROM games WHERE game_id = %s", (old_game_id,))
                    conn.commit()
                    print(f"    Deleted duplicate game {old_game_id} (no predictions or stats)")
                except Exception:
                    conn.rollback()
            continue
        
        cur.execute("""
            UPDATE predictions
            SET game_id = %s
            WHERE game_id = %s
            AND NOT EXISTS (
                SELECT 1 FROM predictions p2
                WHERE p2.player_id = predictions.player_id
                AND p2.game_id = %s
                AND p2.model_version = predictions.model_version
            )
        """, (real_game_id, old_game_id, real_game_id))
        
        updated = cur.rowcount
        total_updated += updated
        
        if updated > 0:
            conf_updated = _exec_with_savepoint(
                cur,
                """
                    UPDATE confidence_components
                    SET game_id = %s
                    WHERE game_id = %s
                """,
                (real_game_id, old_game_id),
                sp_name="sp_confidence_components",
            )
            if conf_updated == 0:
                # Could be "no rows" or a failure; both are non-fatal here.
                print(f"    WARNING: No confidence_components updated for game {old_game_id}")
            else:
                print(f"    Updated {conf_updated} confidence_components from {old_game_id} to {real_game_id}")

            picks_updated = _exec_with_savepoint(
                cur,
                """
                    UPDATE user_picks
                    SET game_id = %s
                    WHERE game_id = %s
                """,
                (real_game_id, old_game_id),
                sp_name="sp_user_picks",
            )
            if picks_updated > 0:
                print(f"    Updated {picks_updated} user_picks from {old_game_id} to {real_game_id}")

            if _relation_exists(cur, "betting_lines"):
                betting_updated = _exec_with_savepoint(
                    cur,
                    """
                        UPDATE betting_lines
                        SET game_id = %s
                        WHERE game_id = %s
                    """,
                    (real_game_id, old_game_id),
                    sp_name="sp_betting_lines",
                )
                if betting_updated > 0:
                    print(f"    Updated {betting_updated} betting_lines from {old_game_id} to {real_game_id}")
            else:
                print("    Skipping betting_lines merge (table does not exist)")

            if _relation_exists(cur, "shot_chart_data"):
                shot_chart_updated = _exec_with_savepoint(
                    cur,
                    """
                        UPDATE shot_chart_data
                        SET game_id = %s
                        WHERE game_id = %s
                    """,
                    (real_game_id, old_game_id),
                    sp_name="sp_shot_chart_data",
                )
                if shot_chart_updated > 0:
                    print(f"    Updated {shot_chart_updated} shot_chart_data from {old_game_id} to {real_game_id}")
            else:
                print("    Skipping shot_chart_data merge (table does not exist)")

            conn.commit()
        
        cur.execute("""
            DELETE FROM predictions
            WHERE game_id = %s
            AND EXISTS (
                SELECT 1 FROM predictions p2
                WHERE p2.player_id = predictions.player_id
                AND p2.game_id = %s
                AND p2.model_version = predictions.model_version
            )
        """, (old_game_id, real_game_id))
        
        cur.execute("""
            DELETE FROM player_game_stats
            WHERE game_id = %s
            AND EXISTS (
                SELECT 1 FROM player_game_stats pgs2
                WHERE pgs2.player_id = player_game_stats.player_id
                AND pgs2.game_id = %s
            )
        """, (old_game_id, real_game_id))
        
        cur.execute("""
            SELECT COUNT(*) FROM predictions WHERE game_id = %s
        """, (old_game_id,))
        remaining_preds = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM player_game_stats WHERE game_id = %s
        """, (old_game_id,))
        remaining_stats = cur.fetchone()[0]
        
        if remaining_preds == 0 and remaining_stats == 0:
            # Deleting the duplicate game can fail if referenced (e.g., user_picks). Keep that failure isolated.
            deleted = _exec_with_savepoint(
                cur,
                "DELETE FROM games WHERE game_id = %s",
                (old_game_id,),
                sp_name="sp_delete_old_game",
            )
            if deleted > 0:
                conn.commit()
                print(f"    Merged predictions from {old_game_id} to {real_game_id} and deleted duplicate game")
            else:
                cur.execute("""
                    SELECT game_status, home_score, away_score
                    FROM games
                    WHERE game_id = %s
                """, (real_game_id,))
                real_game_data = cur.fetchone()
                if real_game_data:
                    real_status, real_home_score, real_away_score = real_game_data
                    cur.execute("""
                        UPDATE games
                        SET game_status = %s,
                            home_score = %s,
                            away_score = %s
                        WHERE game_id = %s
                    """, (real_status, real_home_score, real_away_score, old_game_id))
                    conn.commit()
                    print(f"    Merged {updated} predictions from {old_game_id} to {real_game_id}")
                    print(f"    Updated duplicate game {old_game_id} status to match real game (could not delete - referenced in user_picks)")
                else:
                    print(f"    Merged {updated} predictions from {old_game_id} to {real_game_id}")
                    print(f"    Note: Could not delete duplicate game {old_game_id} (may be referenced in user_picks or other tables)")
        elif updated > 0:
            print(f"    Merged {updated} predictions from {old_game_id} to {real_game_id}")
    
    return total_updated

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
        
        game_row = games_df[games_df['GAME_ID'] == game_id].iloc[0]
        matchup = game_row['MATCHUP']
        team_id = game_row['TEAM_ID']
        
        if 'vs.' in matchup:
            home_team_id = team_id
            away_team_id = games_df[(games_df['GAME_ID'] == game_id) & (games_df['TEAM_ID'] != team_id)].iloc[0]['TEAM_ID']
        else:
            away_team_id = team_id
            home_team_id = games_df[(games_df['GAME_ID'] == game_id) & (games_df['TEAM_ID'] != team_id)].iloc[0]['TEAM_ID']

        # Compute scores by mapping summed player points to the actual home/away team ids.
        points_by_team = (
            player_stats[player_stats["personId"] != 0]
            .groupby("teamId")["points"]
            .sum()
            .to_dict()
        )
        home_score = int(points_by_team.get(int(home_team_id), 0) or 0)
        away_score = int(points_by_team.get(int(away_team_id), 0) or 0)
        
        if not game_exists:
            print(f"  Inserting new game {game_id}...")
        else:
            print(f"  Game {game_id} already exists, updating stats...")

        # Always upsert so re-runs can correct swapped scores or partial boxscores.
        cur.execute("""
            INSERT INTO games (
                game_id, game_date, season, home_team_id, away_team_id,
                home_score, away_score, game_status, game_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE SET
                game_date = EXCLUDED.game_date,
                season = EXCLUDED.season,
                home_team_id = EXCLUDED.home_team_id,
                away_team_id = EXCLUDED.away_team_id,
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                game_status = EXCLUDED.game_status,
                game_type = EXCLUDED.game_type
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
        
        merged_count = merge_predictions_to_real_game_id(cur, conn, game_id, target_date, int(home_team_id), int(away_team_id))
        if merged_count > 0:
            print(f"  Merged {merged_count} predictions to real game ID")
        
        for _, player in player_stats.iterrows():
            player_id = int(player['personId'])
            
            if player_id == 0:
                continue
            
            player_name = player.get('name', player.get('firstName', '') + ' ' + player.get('familyName', f'Player_{player_id}'))
            add_missing_player(cur, player_id, player_name)
            
            minutes_played = parse_minutes(player['minutes'])
            
            game_team_id = int(player['teamId'])
            
            pm_val = player.get('plusMinusPoints')
            if pm_val is None or pm_val == '' or (isinstance(pm_val, float) and math.isnan(pm_val)):
                pm_val = None
            else:
                pm_val = int(pm_val)

            # Upsert player stats so re-runs can correct partial data.
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
                game_id,
                player_id,
                game_team_id,
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
                pm_val
            ))
            
            cur.execute("SELECT team_id FROM players WHERE player_id = %s", (player_id,))
            player_row = cur.fetchone()
            if player_row and player_row[0] != game_team_id:
                cur.execute("UPDATE players SET team_id = %s WHERE player_id = %s", (game_team_id, player_id))
        
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