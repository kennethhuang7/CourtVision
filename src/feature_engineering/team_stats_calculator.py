import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
import pandas as pd
import numpy as np

def calculate_team_ratings_as_of_date(conn, team_id, season, as_of_date):
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                SUM(CASE WHEN g.home_team_id = %s THEN g.home_score ELSE g.away_score END) as points_for,
                SUM(CASE WHEN g.home_team_id = %s THEN g.away_score ELSE g.home_score END) as points_against,
                COUNT(*) as game_count
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
        """, (team_id, team_id, season, team_id, team_id, as_of_date))
        
        result = cur.fetchone()
        if not result or result[2] == 0:
            try:
                season_parts = season.split('-')
                if len(season_parts) == 2:
                    start_year = int(season_parts[0])
                    end_year_short = season_parts[1]
                    prev_start = start_year - 1
                    prev_end = int(end_year_short) - 1
                    prev_season = f"{prev_start}-{str(prev_end).zfill(2)}"
                    
                    cur.execute("""
                        SELECT offensive_rating, defensive_rating, pace
                        FROM team_ratings
                        WHERE team_id = %s AND season = %s
                    """, (team_id, prev_season))
                    
                    prev_result = cur.fetchone()
                    if prev_result and prev_result[0]:
                        print(f"       Fallback: Using previous season ({prev_season}) ratings for team {team_id}")
                        return {
                            'offensive_rating': prev_result[0],
                            'defensive_rating': prev_result[1],
                            'pace': prev_result[2]
                        }
            except:
                pass
            
            cur.execute("""
                SELECT offensive_rating, defensive_rating, pace
                FROM team_ratings
                WHERE team_id = %s AND season = %s
            """, (team_id, season))
            
            season_ratings = cur.fetchone()
            if season_ratings and season_ratings[0]:
                print(f"       Fallback: Using current season ({season}) ratings for team {team_id} (no previous season data)")
                return {
                    'offensive_rating': season_ratings[0],
                    'defensive_rating': season_ratings[1],
                    'pace': season_ratings[2]
                }
            
            print(f"       Fallback: Using default values for team {team_id} (no data available)")
            return {
                'offensive_rating': 105.0,
                'defensive_rating': 105.0,
                'pace': 100.0
            }
        
        points_for = result[0] or 0
        points_against = result[1] or 0
        game_count = result[2] or 0
        
        cur.execute("""
            SELECT 
                SUM(pgs.field_goals_attempted) as fga,
                SUM(pgs.rebounds_offensive) as oreb,
                SUM(pgs.turnovers) as tov,
                SUM(pgs.free_throws_attempted) as fta
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.team_id = %s
                AND g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
        """, (team_id, season, as_of_date))
        
        team_stats = cur.fetchone()
        if not team_stats or not team_stats[0]:
            return None
        
        fga = team_stats[0] or 0
        oreb = team_stats[1] or 0
        tov = team_stats[2] or 0
        fta = team_stats[3] or 0
        
        possessions = fga - oreb + tov + 0.44 * fta
        
        if possessions == 0:
            return None
        
        offensive_rating = round((points_for / possessions) * 100, 1)
        defensive_rating = round((points_against / possessions) * 100, 1)
        pace = round(possessions / game_count, 1)
        
        return {
            'offensive_rating': offensive_rating,
            'defensive_rating': defensive_rating,
            'pace': pace
        }
    finally:
        cur.close()

def calculate_team_defensive_stats_as_of_date(conn, team_id, season, as_of_date):
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                pgs.field_goals_made,
                pgs.field_goals_attempted,
                pgs.three_pointers_made,
                pgs.three_pointers_attempted,
                pgs.turnovers,
                pgs.steals
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
                AND pgs.team_id != %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
        """, (season, as_of_date, team_id, team_id, team_id))
        
        opponent_stats = cur.fetchall()
        
        cur.execute("""
            SELECT COUNT(DISTINCT g.game_id)
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
        """, (season, team_id, team_id, as_of_date))
        
        games_played = cur.fetchone()[0] or 0
        
        if len(opponent_stats) == 0:
            try:
                season_parts = season.split('-')
                if len(season_parts) == 2:
                    start_year = int(season_parts[0])
                    end_year_short = season_parts[1]
                    prev_start = start_year - 1
                    prev_end = int(end_year_short) - 1
                    prev_season = f"{prev_start}-{str(prev_end).zfill(2)}"
                    
                    cur.execute("""
                        SELECT opp_field_goal_pct, opp_three_point_pct
                        FROM team_defensive_stats
                        WHERE team_id = %s AND season = %s
                    """, (team_id, prev_season))
                    
                    prev_result = cur.fetchone()
                    if prev_result and prev_result[0] is not None:
                        print(f"       Fallback: Using previous season ({prev_season}) defensive stats for team {team_id}")
                        return {
                            'opp_field_goal_pct': prev_result[0],
                            'opp_three_point_pct': prev_result[1],
                            'opp_team_turnovers_per_game': 14.0,
                            'opp_team_steals_per_game': 7.0
                        }
            except:
                pass
            
            cur.execute("""
                SELECT opp_field_goal_pct, opp_three_point_pct
                FROM team_defensive_stats
                WHERE team_id = %s AND season = %s
            """, (team_id, season))
            
            season_stats = cur.fetchone()
            if season_stats and season_stats[0] is not None:
                print(f"       Fallback: Using current season ({season}) defensive stats for team {team_id} (no previous season data)")
                return {
                    'opp_field_goal_pct': season_stats[0],
                    'opp_three_point_pct': season_stats[1],
                    'opp_team_turnovers_per_game': 14.0,
                    'opp_team_steals_per_game': 7.0
                }
            
            print(f"       Fallback: Using default defensive stats for team {team_id} (no data available)")
            return {
                'opp_field_goal_pct': 45.0,
                'opp_three_point_pct': 35.0,
                'opp_team_turnovers_per_game': 14.0,
                'opp_team_steals_per_game': 7.0
            }
        
        total_fgm = sum(row[0] or 0 for row in opponent_stats)
        total_fga = sum(row[1] or 0 for row in opponent_stats)
        total_3pm = sum(row[2] or 0 for row in opponent_stats)
        total_3pa = sum(row[3] or 0 for row in opponent_stats)
        total_turnovers = sum(row[4] or 0 for row in opponent_stats)
        total_steals = sum(row[5] or 0 for row in opponent_stats)
        
        opp_fg_pct = round((total_fgm / total_fga) * 100, 1) if total_fga > 0 else 0
        opp_3p_pct = round((total_3pm / total_3pa) * 100, 1) if total_3pa > 0 else 0
        opp_turnovers_per_game = round(total_turnovers / games_played, 1) if games_played > 0 else 0
        opp_steals_per_game = round(total_steals / games_played, 1) if games_played > 0 else 0
        
        return {
            'opp_field_goal_pct': opp_fg_pct,
            'opp_three_point_pct': opp_3p_pct,
            'opp_team_turnovers_per_game': opp_turnovers_per_game,
            'opp_team_steals_per_game': opp_steals_per_game
        }
    finally:
        cur.close()

def calculate_position_defense_stats_as_of_date(conn, team_id, season, position, as_of_date):
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                pgs.points,
                pgs.rebounds_total,
                pgs.assists,
                pgs.steals,
                pgs.blocks,
                pgs.turnovers,
                pgs.three_pointers_made,
                p.position
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            JOIN players p ON pgs.player_id = p.player_id
            WHERE g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
                AND pgs.team_id != %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
        """, (season, as_of_date, team_id, team_id, team_id))
        
        all_opponent_stats = cur.fetchall()
        
        opponent_stats = []
        for row in all_opponent_stats:
            player_pos = row[7]
            if map_position_to_defense_position(player_pos) == position:
                opponent_stats.append(row[:7])
        
        if len(opponent_stats) == 0:
            return None
        
        cur.execute("""
            SELECT COUNT(DISTINCT g.game_id)
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
        """, (season, team_id, team_id, as_of_date))
        
        games_played = cur.fetchone()[0] or 0
        
        if games_played == 0:
            return None
        
        total_points = sum(row[0] or 0 for row in opponent_stats)
        total_rebounds = sum(row[1] or 0 for row in opponent_stats)
        total_assists = sum(row[2] or 0 for row in opponent_stats)
        total_steals = sum(row[3] or 0 for row in opponent_stats)
        total_blocks = sum(row[4] or 0 for row in opponent_stats)
        total_turnovers = sum(row[5] or 0 for row in opponent_stats)
        total_3pm = sum(row[6] or 0 for row in opponent_stats)
        
        return {
            'opp_points_allowed_to_position': round(total_points / games_played, 1),
            'opp_rebounds_allowed_to_position': round(total_rebounds / games_played, 1),
            'opp_assists_allowed_to_position': round(total_assists / games_played, 1),
            'opp_blocks_allowed_to_position': round(total_blocks / games_played, 1),
            'opp_three_pointers_allowed_to_position': round(total_3pm / games_played, 1),
            'opp_position_turnovers_vs_team': round(total_turnovers / games_played, 1),
            'opp_position_steals_vs_team': round(total_steals / games_played, 1)
        }
    finally:
        cur.close()

def calculate_opponent_team_turnover_stats_as_of_date(conn, team_id, season, position, as_of_date):
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                pgs.steals,
                pgs.turnovers,
                p.position
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            JOIN players p ON pgs.player_id = p.player_id
            WHERE pgs.team_id = %s
                AND g.season = %s
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
        """, (team_id, season, as_of_date))
        
        all_stats = cur.fetchall()
        
        position_stats = []
        for row in all_stats:
            player_pos = row[2]
            if map_position_to_defense_position(player_pos) == position:
                position_stats.append((row[0], row[1]))
        
        if len(position_stats) == 0:
            return None
        
        cur.execute("""
            SELECT COUNT(DISTINCT g.game_id)
            FROM games g
            WHERE g.season = %s
                AND (g.home_team_id = %s OR g.away_team_id = %s)
                AND g.game_status = 'completed'
                AND g.game_type = 'regular_season'
                AND g.game_date < %s
        """, (season, team_id, team_id, as_of_date))
        
        games_played = cur.fetchone()[0] or 0
        
        if games_played == 0:
            return None
        
        total_steals = sum(row[0] or 0 for row in position_stats)
        total_turnovers = sum(row[1] or 0 for row in position_stats)
        
        return {
            'opp_position_steals_overall': round(total_steals / games_played, 1),
            'opp_position_turnovers_overall': round(total_turnovers / games_played, 1)
        }
    except Exception as e:
        return None
    finally:
        cur.close()

def map_position_to_defense_position(pos):
    if pd.isna(pos):
        return 'G'
    pos_str = str(pos).upper().strip()
    if ('CENTER' in pos_str or pos_str == 'C') and 'GUARD' not in pos_str and 'FORWARD' not in pos_str:
        return 'C'
    elif 'FORWARD' in pos_str or pos_str == 'F' or pos_str == 'F-C':
        return 'F'
    elif 'GUARD' in pos_str or pos_str == 'G' or pos_str == 'G-F':
        return 'G'
    else:
        return 'G'

