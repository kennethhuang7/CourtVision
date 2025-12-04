import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
import pandas as pd
import joblib
from datetime import datetime, timedelta

def get_injured_stars(target_date, season='2025-26'):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT i.player_id, p.full_name
        FROM injuries i
        JOIN players p ON i.player_id = p.player_id
        WHERE i.injury_status = 'Out'
        AND i.report_date <= %s
        AND (i.return_date IS NULL OR i.return_date > %s)
        AND EXISTS (
            SELECT 1 
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.player_id = i.player_id
            AND g.season = %s
            GROUP BY pgs.player_id
            HAVING AVG(pgs.points) >= 20
        )
    """, (target_date, target_date, season))
    
    injured_stars = cur.fetchall()
    cur.close()
    conn.close()
    
    return injured_stars

def get_teammate_boosts(player_id, season='2025-26'):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            player_id,
            ppg_boost,
            rpg_boost,
            apg_boost
        FROM teammate_dependency
        WHERE teammate_id = %s
        AND season = %s
    """, (player_id, season))
    
    boosts = {}
    for row in cur.fetchall():
        teammate_id, ppg_boost, rpg_boost, apg_boost = row
        boosts[teammate_id] = {
            'ppg_boost': float(ppg_boost),
            'rpg_boost': float(rpg_boost),
            'apg_boost': float(apg_boost)
        }
    
    cur.close()
    conn.close()
    
    return boosts

def apply_teammate_boosts(all_predictions, target_date, season='2025-26'):
    print("\nChecking for injured stars and applying teammate boosts...")
    
    injured_stars = get_injured_stars(target_date, season)
    
    if not injured_stars:
        print("No injured star players found")
        return all_predictions
    
    print(f"Found {len(injured_stars)} injured star players:")
    
    total_adjustments = 0
    
    for star_id, star_name in injured_stars:
        print(f"  {star_name} is out")
        
        boosts = get_teammate_boosts(star_id, season)
        
        if not boosts:
            print(f"    No teammate dependencies found")
            continue
        
        print(f"    Found {len(boosts)} teammates with usage boosts")
        
        for pred in all_predictions:
            player_id = pred['player_id']
            
            if player_id in boosts:
                boost = boosts[player_id]
                
                pred['points'] += boost['ppg_boost']
                pred['rebounds'] += boost['rpg_boost']
                pred['assists'] += boost['apg_boost']
                
                total_adjustments += 1
                
                print(f"      Adjusted player {player_id}: {boost['ppg_boost']:+.1f} PPG, {boost['rpg_boost']:+.1f} RPG, {boost['apg_boost']:+.1f} APG")
    
    print(f"\nApplied {total_adjustments} teammate boost adjustments\n")
    
    return all_predictions

def predict_upcoming_games(target_date=None):
    print("Predicting player performance for upcoming games...\n")
    
    if target_date is None:
        target_date = datetime.now().date()
    else:
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    print(f"Target date: {target_date}\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("Loading upcoming games...")
    games_query = f"""
        SELECT game_id, game_date, game_type, home_team_id, away_team_id, season
        FROM games
        WHERE game_date = '{target_date}'
            AND game_status = 'scheduled'
    """
    
    games_df = pd.read_sql(games_query, conn)
    
    if len(games_df) == 0:
        print(f"No scheduled games found for {target_date}")
        cur.close()
        conn.close()
        return
    
    print(f"Found {len(games_df)} games\n")
    
    print("Loading models...")
    models = {
        'points': joblib.load('../../data/models/xgboost_points.pkl'),
        'rebounds': joblib.load('../../data/models/xgboost_rebounds.pkl'),
        'assists': joblib.load('../../data/models/xgboost_assists.pkl'),
        'steals': joblib.load('../../data/models/xgboost_steals.pkl'),
        'blocks': joblib.load('../../data/models/xgboost_blocks.pkl'),
        'turnovers': joblib.load('../../data/models/xgboost_turnovers.pkl'),
        'three_pointers_made': joblib.load('../../data/models/xgboost_three_pointers_made.pkl')
    }
    
    all_predictions = []
    
    for _, game in games_df.iterrows():
        game_id = game['game_id']
        home_team = game['home_team_id']
        away_team = game['away_team_id']
        season = game['season']
        game_type = game['game_type']
        
        for team_id in [home_team, away_team]:
            is_home = 1 if team_id == home_team else 0
            opponent_id = away_team if is_home else home_team
            
            players_query = f"""
                SELECT DISTINCT player_id
                FROM player_game_stats
                WHERE team_id = {team_id}
                    AND game_id IN (
                        SELECT game_id FROM games 
                        WHERE season = '{season}' 
                        AND game_date < '{target_date}'
                        ORDER BY game_date DESC
                        LIMIT 10
                    )
            """
            
            players = pd.read_sql(players_query, conn)
            
            for player_id in players['player_id']:
                features = build_features_for_player(
                    conn, player_id, team_id, opponent_id, 
                    is_home, season, target_date, game_type
                )
                
                if features is None:
                    continue
                
                predictions = {}
                for stat_name, model in models.items():
                    pred = model.predict(features)[0]
                    predictions[stat_name] = float(round(pred, 1))
                
                all_predictions.append({
                    'game_id': game_id,
                    'player_id': player_id,
                    'team_id': team_id,
                    'is_home': is_home,
                    **predictions
                })
    
    all_predictions = apply_teammate_boosts(all_predictions, target_date, games_df.iloc[0]['season'])
    
    predictions_inserted = 0
    
    for pred in all_predictions:
        try:
            cur.execute("""
                INSERT INTO predictions (
                    game_id, player_id, prediction_date,
                    predicted_points, predicted_rebounds, predicted_assists,
                    predicted_steals, predicted_blocks, predicted_turnovers,
                    predicted_three_pointers_made, confidence_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, player_id) DO UPDATE SET
                    predicted_points = EXCLUDED.predicted_points,
                    predicted_rebounds = EXCLUDED.predicted_rebounds,
                    predicted_assists = EXCLUDED.predicted_assists,
                    predicted_steals = EXCLUDED.predicted_steals,
                    predicted_blocks = EXCLUDED.predicted_blocks,
                    predicted_turnovers = EXCLUDED.predicted_turnovers,
                    predicted_three_pointers_made = EXCLUDED.predicted_three_pointers_made,
                    confidence_score = EXCLUDED.confidence_score
            """, (
                pred['game_id'],
                pred['player_id'],
                target_date,
                pred['points'],
                pred['rebounds'],
                pred['assists'],
                pred['steals'],
                pred['blocks'],
                pred['turnovers'],
                pred['three_pointers_made'],
                0.75
            ))
            
            predictions_inserted += 1
            
        except Exception as e:
            print(f"Error inserting prediction for player {pred['player_id']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    if len(all_predictions) == 0:
        print("No predictions generated")
        return
    
    pred_df = pd.DataFrame(all_predictions)
    
    output_path = f'../../data/predictions/predictions_{target_date}.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pred_df.to_csv(output_path, index=False)
    
    print("="*50)
    print("PREDICTIONS COMPLETE!")
    print("="*50)
    print(f"Generated predictions for {len(pred_df)} players")
    print(f"Saved {predictions_inserted} predictions to database")
    print(f"Saved CSV backup to: {output_path}\n")
    
    print("Sample predictions:")
    print(pred_df.head(10))
    
    return pred_df

def build_features_for_player(conn, player_id, team_id, opponent_id, 
                               is_home, season, target_date, game_type):
    
    query = f"""
        SELECT 
            pgs.points,
            pgs.rebounds_total,
            pgs.assists,
            g.game_date,
            g.game_type
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
            AND g.game_date < '{target_date}'
            AND g.game_status = 'completed'
        ORDER BY g.game_date DESC
        LIMIT 20
    """
    
    recent_games = pd.read_sql(query, conn)
    
    if len(recent_games) < 5:
        return None
    
    features = {}
    
    features['is_playoff'] = 1 if game_type == 'playoff' else 0
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        features[f'points_l{window}'] = window_games['points'].mean()
        features[f'rebounds_total_l{window}'] = window_games['rebounds_total'].mean()
        features[f'assists_l{window}'] = window_games['assists'].mean()
    
    playoff_games_query = f"""
        SELECT COUNT(*) as playoff_games
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
            AND g.game_type = 'playoff'
            AND g.game_status = 'completed'
    """
    
    playoff_count = pd.read_sql(playoff_games_query, conn)
    features['playoff_games_career'] = playoff_count.iloc[0]['playoff_games'] if len(playoff_count) > 0 else 0
    
    playoff_avg_query = f"""
        SELECT AVG(pgs.points) as playoff_avg
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
            AND g.game_type = 'playoff'
            AND g.game_status = 'completed'
    """
    
    regular_avg_query = f"""
        SELECT AVG(pgs.points) as regular_avg
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
            AND g.game_type = 'regular_season'
            AND g.game_status = 'completed'
    """
    
    playoff_avg = pd.read_sql(playoff_avg_query, conn)
    regular_avg = pd.read_sql(regular_avg_query, conn)
    
    if len(playoff_avg) > 0 and len(regular_avg) > 0:
        playoff_ppg = playoff_avg.iloc[0]['playoff_avg'] or 0
        regular_ppg = regular_avg.iloc[0]['regular_avg'] or 0
        features['playoff_performance_boost'] = playoff_ppg - regular_ppg
    else:
        features['playoff_performance_boost'] = 0
    
    features['is_home'] = is_home
    
    recent_games['game_date'] = pd.to_datetime(recent_games['game_date'])
    days_rest = (pd.to_datetime(target_date) - recent_games['game_date'].iloc[0]).days
    features['days_rest'] = days_rest
    features['is_back_to_back'] = 1 if days_rest == 1 else 0
    
    features['games_played_season'] = len(recent_games)
    
    team_ratings = pd.read_sql(f"""
        SELECT offensive_rating, defensive_rating, pace
        FROM team_ratings
        WHERE team_id = {team_id} AND season = '{season}'
    """, conn)
    
    if len(team_ratings) > 0:
        features['offensive_rating_team'] = team_ratings.iloc[0]['offensive_rating']
        features['defensive_rating_team'] = team_ratings.iloc[0]['defensive_rating']
        features['pace_team'] = team_ratings.iloc[0]['pace']
    else:
        features['offensive_rating_team'] = 110
        features['defensive_rating_team'] = 110
        features['pace_team'] = 100
    
    opp_ratings = pd.read_sql(f"""
        SELECT offensive_rating, defensive_rating, pace
        FROM team_ratings
        WHERE team_id = {opponent_id} AND season = '{season}'
    """, conn)
    
    if len(opp_ratings) > 0:
        features['offensive_rating_opp'] = opp_ratings.iloc[0]['offensive_rating']
        features['defensive_rating_opp'] = opp_ratings.iloc[0]['defensive_rating']
        features['pace_opp'] = opp_ratings.iloc[0]['pace']
    else:
        features['offensive_rating_opp'] = 110
        features['defensive_rating_opp'] = 110
        features['pace_opp'] = 100
    
    opp_defense = pd.read_sql(f"""
        SELECT opp_field_goal_pct, opp_three_point_pct
        FROM team_defensive_stats
        WHERE team_id = {opponent_id} AND season = '{season}'
    """, conn)
    
    if len(opp_defense) > 0:
        features['opp_field_goal_pct'] = opp_defense.iloc[0]['opp_field_goal_pct']
        features['opp_three_point_pct'] = opp_defense.iloc[0]['opp_three_point_pct']
    else:
        features['opp_field_goal_pct'] = 45
        features['opp_three_point_pct'] = 35
    
    altitude_query = pd.read_sql(f"""
        SELECT arena_altitude
        FROM teams
        WHERE team_id = {opponent_id}
    """, conn)
    
    if len(altitude_query) > 0:
        altitude = altitude_query.iloc[0]['arena_altitude']
        features['team_id_opp_venue'] = opponent_id
        features['arena_altitude'] = altitude
        features['altitude_away'] = 1 if (is_home == 0 and altitude > 3000) else 0
    else:
        features['team_id_opp_venue'] = opponent_id
        features['arena_altitude'] = 0
        features['altitude_away'] = 0
    
    features_df = pd.DataFrame([features])
    
    column_order = [
        'is_playoff', 'points_l5', 'rebounds_total_l5', 'assists_l5',
        'points_l10', 'rebounds_total_l10', 'assists_l10',
        'points_l20', 'rebounds_total_l20', 'assists_l20',
        'playoff_games_career', 'playoff_performance_boost',
        'is_home', 'days_rest', 'is_back_to_back', 'games_played_season',
        'offensive_rating_team', 'defensive_rating_team', 'pace_team',
        'offensive_rating_opp', 'defensive_rating_opp', 'pace_opp',
        'opp_field_goal_pct', 'opp_three_point_pct',
        'team_id_opp_venue', 'arena_altitude', 'altitude_away'
    ]
    
    features_df = features_df[column_order]
    
    return features_df

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        predict_upcoming_games(target_date)
    else:
        predict_upcoming_games()