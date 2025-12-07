import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

def calculate_confidence(features_df, recent_games_df):
    score = 0
    
    if len(recent_games_df) >= 5:
        points_std = recent_games_df['points'].std()
        points_mean = recent_games_df['points'].mean()
        if points_mean > 0:
            cv = points_std / points_mean
            score += max(0, 30 - (cv * 60))
        else:
            score += 15
    else:
        score += 10
    
    required = ['points_l5', 'points_l10', 'is_home', 'days_rest', 
                'offensive_rating_team', 'defensive_rating_opp']
    available = sum(1 for feat in required if feat in features_df.columns)
    score += (available / len(required)) * 20
    
    games = len(recent_games_df)
    if games >= 20:
        score += 25
    elif games >= 10:
        score += 20
    elif games >= 5:
        score += 15
    else:
        score += 10
    
    context = 25
    if 'is_back_to_back' in features_df.columns and features_df['is_back_to_back'].iloc[0] == 1:
        context -= 5
    if 'star_teammate_out' in features_df.columns and features_df['star_teammate_out'].iloc[0] == 1:
        context -= 5
    if 'altitude_away' in features_df.columns and features_df['altitude_away'].iloc[0] == 1:
        context -= 3
    score += max(0, context)
    
    return int(max(0, min(100, score)))

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
    
    print("Loading models and scalers...")
    models = {
        'points': joblib.load('../../data/models/xgboost_points.pkl'),
        'rebounds': joblib.load('../../data/models/xgboost_rebounds.pkl'),
        'assists': joblib.load('../../data/models/xgboost_assists.pkl'),
        'steals': joblib.load('../../data/models/xgboost_steals.pkl'),
        'blocks': joblib.load('../../data/models/xgboost_blocks.pkl'),
        'turnovers': joblib.load('../../data/models/xgboost_turnovers.pkl'),
        'three_pointers_made': joblib.load('../../data/models/xgboost_three_pointers_made.pkl')
    }
    
    scalers = {}
    for stat_name in models.keys():
        scaler_path = f'../../data/models/scaler_{stat_name}.pkl'
        if os.path.exists(scaler_path):
            scalers[stat_name] = joblib.load(scaler_path)
        else:
            print(f"Warning: Scaler not found for {stat_name}, predictions may be inaccurate")
            scalers[stat_name] = None
    
    all_predictions = []
    predictions_inserted = 0
    
    for _, game in games_df.iterrows():
        game_id = game['game_id']
        home_team = game['home_team_id']
        away_team = game['away_team_id']
        season = game['season']
        game_type = game['game_type']
        
        print(f"\nProcessing game {game_id}...")
        
        for team_id in [home_team, away_team]:
            is_home = 1 if team_id == home_team else 0
            opponent_id = away_team if is_home else home_team
            
            team_name = "home" if is_home else "away"
            print(f"  Processing {team_name} team {team_id}...")
            
            players_query = f"""
                SELECT DISTINCT pgs.player_id
                FROM player_game_stats pgs
                WHERE pgs.team_id = {team_id}
                    AND pgs.game_id IN (
                        SELECT game_id FROM games 
                        WHERE season = '{season}' 
                        AND game_date < '{target_date}'
                        AND (home_team_id = {team_id} OR away_team_id = {team_id})
                        ORDER BY game_date DESC
                        LIMIT 10
                    )
                    AND pgs.player_id NOT IN (
                        SELECT DISTINCT i.player_id
                        FROM injuries i
                        WHERE i.injury_status = 'Out'
                        AND i.report_date <= '{target_date}'
                        AND (i.return_date IS NULL OR i.return_date > '{target_date}')
                    )
            """
            
            players = pd.read_sql(players_query, conn)
            
            print(f"    Found {len(players)} qualifying players (injured players excluded)")
            
            for player_id in players['player_id']:
                features, recent_games = build_features_for_player(
                    conn, player_id, team_id, opponent_id, 
                    is_home, season, target_date, game_type
                )
                
                if features is None:
                    continue
                
                predictions = {}
                for stat_name, model in models.items():
                    features_scaled = features.copy()
                    if scalers[stat_name] is not None:
                        features_scaled = pd.DataFrame(
                            scalers[stat_name].transform(features),
                            columns=features.columns
                        )
                    pred = model.predict(features_scaled)[0]
                    predictions[stat_name] = float(round(pred, 1))
                
                confidence_score = calculate_confidence(features, recent_games)
                
                try:
                    cur.execute("""
                        INSERT INTO predictions (
                            game_id, player_id, prediction_date,
                            predicted_points, predicted_rebounds, predicted_assists,
                            predicted_steals, predicted_blocks, predicted_turnovers,
                            predicted_three_pointers_made, confidence_score, model_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, player_id) DO UPDATE SET
                            predicted_points = EXCLUDED.predicted_points,
                            predicted_rebounds = EXCLUDED.predicted_rebounds,
                            predicted_assists = EXCLUDED.predicted_assists,
                            predicted_steals = EXCLUDED.predicted_steals,
                            predicted_blocks = EXCLUDED.predicted_blocks,
                            predicted_turnovers = EXCLUDED.predicted_turnovers,
                            predicted_three_pointers_made = EXCLUDED.predicted_three_pointers_made,
                            confidence_score = EXCLUDED.confidence_score,
                            model_version = EXCLUDED.model_version
                    """, (
                        game_id,
                        player_id,
                        target_date,
                        predictions['points'],
                        predictions['rebounds'],
                        predictions['assists'],
                        predictions['steals'],
                        predictions['blocks'],
                        predictions['turnovers'],
                        predictions['three_pointers_made'],
                        confidence_score,
                        'xgboost'
                    ))
                    
                    predictions_inserted += 1
                    
                except Exception as e:
                    print(f"Error inserting prediction for player {player_id}: {e}")
                    conn.rollback()
                    continue
                
                all_predictions.append({
                    'game_id': game_id,
                    'player_id': player_id,
                    'team_id': team_id,
                    'is_home': is_home,
                    **predictions
                })
    
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
    
    print("\n" + "="*50)
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
        return None, None
    
    features = {}
    
    features['is_playoff'] = 1 if game_type == 'playoff' else 0
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        features[f'points_l{window}'] = window_games['points'].mean()
        features[f'rebounds_total_l{window}'] = window_games['rebounds_total'].mean()
        features[f'assists_l{window}'] = window_games['assists'].mean()
    
    decay_factor = 0.1
    for window in [5, 10, 20]:
        window_games = recent_games.head(window).copy()
        if len(window_games) > 0:
            weights = np.exp(-decay_factor * np.arange(len(window_games))[::-1])
            weights = weights / weights.sum()
            
            features[f'points_l{window}_weighted'] = np.sum(window_games['points'].values * weights)
            features[f'rebounds_total_l{window}_weighted'] = np.sum(window_games['rebounds_total'].values * weights)
            features[f'assists_l{window}_weighted'] = np.sum(window_games['assists'].values * weights)
        else:
            features[f'points_l{window}_weighted'] = 0
            features[f'rebounds_total_l{window}_weighted'] = 0
            features[f'assists_l{window}_weighted'] = 0
    
    if game_type == 'playoff':
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
    else:
        features['playoff_games_career'] = 0
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
    
    star_query = f"""
        SELECT DISTINCT pgs2.player_id, AVG(pgs2.points) as ppg
        FROM player_game_stats pgs2
        JOIN games g2 ON pgs2.game_id = g2.game_id
        WHERE pgs2.team_id = {team_id}
        AND g2.season = '{season}'
        AND g2.game_date < '{target_date}'
        AND pgs2.player_id != {player_id}
        AND pgs2.minutes_played >= 15
        GROUP BY pgs2.player_id
        HAVING AVG(pgs2.points) >= 20
    """
    
    star_teammates = pd.read_sql(star_query, conn)
    
    features['star_teammate_out'] = 0
    features['star_teammate_ppg'] = 0.0
    features['games_without_star'] = 0
    
    if len(star_teammates) > 0:
        for _, star in star_teammates.iterrows():
            star_id = star['player_id']
            star_ppg = star['ppg']
            
            injury_query = f"""
                SELECT COUNT(*)
                FROM injuries
                WHERE player_id = {star_id}
                AND injury_status = 'Out'
                AND report_date <= '{target_date}'
                AND (return_date IS NULL OR return_date > '{target_date}')
            """
            
            star_out = pd.read_sql(injury_query, conn).iloc[0][0]
            
            if star_out > 0:
                games_without_query = f"""
                    SELECT COUNT(*)
                    FROM player_game_stats pgs
                    JOIN games g ON pgs.game_id = g.game_id
                    WHERE pgs.player_id = {player_id}
                    AND pgs.team_id = {team_id}
                    AND g.season = '{season}'
                    AND g.game_date < '{target_date}'
                    AND NOT EXISTS (
                        SELECT 1 FROM player_game_stats pgs2
                        WHERE pgs2.game_id = pgs.game_id
                        AND pgs2.player_id = {star_id}
                        AND pgs2.minutes_played >= 15
                    )
                """
                
                games_without = pd.read_sql(games_without_query, conn).iloc[0][0]
                
                features['star_teammate_out'] = 1
                features['star_teammate_ppg'] = float(star_ppg)
                features['games_without_star'] = games_without
                break
    
    features_df = pd.DataFrame([features])
    
    column_order = [
        'is_playoff', 
        'points_l5', 'rebounds_total_l5', 'assists_l5',
        'points_l10', 'rebounds_total_l10', 'assists_l10',
        'points_l20', 'rebounds_total_l20', 'assists_l20',
        'points_l5_weighted', 'rebounds_total_l5_weighted', 'assists_l5_weighted',
        'points_l10_weighted', 'rebounds_total_l10_weighted', 'assists_l10_weighted',
        'points_l20_weighted', 'rebounds_total_l20_weighted', 'assists_l20_weighted',
        'star_teammate_out', 'star_teammate_ppg', 'games_without_star',  
        'playoff_games_career', 'playoff_performance_boost',
        'is_home', 'days_rest', 'is_back_to_back', 'games_played_season',
        'offensive_rating_team', 'defensive_rating_team', 'pace_team',
        'offensive_rating_opp', 'defensive_rating_opp', 'pace_opp',
        'opp_field_goal_pct', 'opp_three_point_pct',
        'team_id_opp_venue', 'arena_altitude', 'altitude_away'
    ]
    
    features_df = features_df[column_order]
    
    return features_df, recent_games

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        predict_upcoming_games(target_date)
    else:
        predict_upcoming_games()