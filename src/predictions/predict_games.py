import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection, ensure_connection
from feature_engineering.team_stats_calculator import (
    calculate_team_defensive_stats_as_of_date,
    calculate_position_defense_stats_as_of_date,
    calculate_opponent_team_turnover_stats_as_of_date,
    map_position_to_defense_position
)
from predictions.feature_explanations import get_top_features_with_impact
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta, date
import json

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

class EnsemblePredictor:
    def __init__(self, models_dict, validation_maes=None):
        self.models = models_dict
        self.validation_maes = validation_maes or {}
        
    def predict_simple_average(self, features, selected_models=None):
        if selected_models is None:
            selected_models = list(self.models.keys())
        
        predictions = []
        for model_name in selected_models:
            if model_name in self.models:
                pred = self.models[model_name].predict(features)
                predictions.append(pred)
        
        if len(predictions) == 0:
            raise ValueError("No valid models selected")
        
        weights = {m: 1.0/len(predictions) for m in selected_models}
        
        return np.mean(predictions, axis=0), weights
    
    def predict_weighted_average(self, features, selected_models=None):
        if selected_models is None:
            selected_models = list(self.models.keys())
        
        weights = {}
        for model_name in selected_models:
            if model_name in self.models and model_name in self.validation_maes:
                mae = self.validation_maes[model_name]
                weights[model_name] = 1.0 / mae
            elif model_name in self.models:
                weights[model_name] = 1.0
        
        total_weight = sum(weights.values())
        weights = {k: v / total_weight for k, v in weights.items()}
        
        predictions = []
        for model_name in selected_models:
            if model_name in self.models:
                pred = self.models[model_name].predict(features)
                predictions.append(pred * weights[model_name])
        
        if len(predictions) == 0:
            raise ValueError("No valid models selected")
        
        return np.sum(predictions, axis=0), weights
    
    def predict_custom(self, features, custom_weights):
        total_weight = sum(custom_weights.values())
        normalized_weights = {k: v / total_weight for k, v in custom_weights.items()}
        
        predictions = []
        for model_name, weight in normalized_weights.items():
            if model_name in self.models:
                pred = self.models[model_name].predict(features)
                predictions.append(pred * weight)
        
        if len(predictions) == 0:
            raise ValueError("No valid models in custom_weights")
        
        return np.sum(predictions, axis=0), normalized_weights

def calculate_confidence(features_df, recent_games_df, conn=None, player_id=None, target_date=None, season=None):
    score = 0
    
    season_cv_score = 0
    career_cv_score = 0
    if len(recent_games_df) >= 5:
        points_std = recent_games_df['points'].std()
        points_mean = recent_games_df['points'].mean()
        if points_mean > 0:
            cv = points_std / points_mean
            season_cv_score = max(0, 30 - (cv * 60))
        else:
            season_cv_score = 15
    else:
        season_cv_score = 10
    
    if conn and player_id:
        try:
            career_query = f"""
                SELECT pgs.points
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = {player_id}
                AND g.game_status = 'completed'
                AND g.game_date < '{target_date}'
                ORDER BY g.game_date DESC
                LIMIT 100
            """
            career_games = pd.read_sql(career_query, conn)
            
            if len(career_games) >= 20:
                career_std = career_games['points'].std()
                career_mean = career_games['points'].mean()
                if career_mean > 0:
                    career_cv = career_std / career_mean
                    career_cv_score = max(0, 30 - (career_cv * 60))
                else:
                    career_cv_score = 15
            else:
                career_cv_score = season_cv_score
        except:
            career_cv_score = season_cv_score
    
    score += (season_cv_score * 0.75) + (career_cv_score * 0.25)
    expected_features = [
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
        'opp_team_turnovers_per_game', 'opp_team_steals_per_game',
        'opp_points_allowed_to_position', 'opp_rebounds_allowed_to_position',
        'opp_assists_allowed_to_position', 'opp_blocks_allowed_to_position',
        'opp_three_pointers_allowed_to_position',
        'opp_position_turnovers_vs_team', 'opp_position_steals_vs_team',
        'opp_position_turnovers_overall', 'opp_position_steals_overall',
        'arena_altitude', 'altitude_away'
    ]
    
    available = sum(1 for feat in expected_features 
                   if feat in features_df.columns 
                   and not pd.isna(features_df[feat].iloc[0]))
    score += (available / len(expected_features)) * 20
    
    season_games = len(recent_games_df)
    coming_off_injury = False
    games_missed = 0
    if conn and player_id and target_date:
        try:
            injury_check = pd.read_sql(f"""
                SELECT games_missed, return_date, 
                       report_date as injury_start_date
                FROM injuries
                WHERE player_id = {player_id}
                AND return_date IS NOT NULL
                AND return_date >= %s::date - INTERVAL '60 days'
                AND return_date <= %s::date
                ORDER BY return_date DESC
                LIMIT 1
            """, conn, params=(target_date, target_date))
            
            if len(injury_check) > 0:
                days_since_return = (pd.to_datetime(target_date) - pd.to_datetime(injury_check.iloc[0]['return_date'])).days
                games_missed = injury_check.iloc[0]['games_missed'] or 0
                if days_since_return <= 30 and games_missed >= 5:
                    coming_off_injury = True
        except:
            pass
    
    career_games_count = 0
    if conn and player_id:
        try:
            career_count_query = f"""
                SELECT COUNT(*) as career_games
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = {player_id}
                AND g.game_status = 'completed'
                AND g.game_date < '{target_date}'
            """
            career_count = pd.read_sql(career_count_query, conn)
            career_games_count = career_count.iloc[0]['career_games'] if len(career_count) > 0 else 0
        except:
            career_games_count = season_games
    
    if season_games >= 20:
        season_score = 25
    elif season_games >= 10:
        season_score = 20
    elif season_games >= 5:
        season_score = 15
    else:
        season_score = 10
    
    deductions = 0
    
    if career_games_count < 20:
        deductions += 10
    elif career_games_count < 50:
        deductions += 5
    
    if season_games < 5:
        if coming_off_injury:
            if games_missed >= 20:
                deductions += 8
            elif games_missed >= 10:
                deductions += 5
            else:
                deductions += 3
        else:
            deductions += 2
    elif season_games < 10:
        if coming_off_injury:
            deductions += 3
        else:
            deductions += 1
    
    score += max(0, season_score - deductions)
    
    transaction_score = 25
    
    if conn and player_id and target_date and season:
        try:
            transaction_check = pd.read_sql(f"""
                SELECT transaction_date, to_team_id, transaction_type
                FROM player_transactions
                WHERE player_id = {player_id}
                AND transaction_type IN ('trade', 'signing')
                AND transaction_date >= %s::date - INTERVAL '30 days'
                AND transaction_date <= %s::date
                ORDER BY transaction_date DESC
                LIMIT 1
            """, conn, params=(target_date, target_date))
            
            if len(transaction_check) > 0:
                trans_date = transaction_check.iloc[0]['transaction_date']
                trans_type = transaction_check.iloc[0]['transaction_type']
                days_since_trans = (pd.to_datetime(target_date) - pd.to_datetime(trans_date)).days
                
                if trans_type == 'trade':
                    if days_since_trans <= 7:
                        transaction_score -= 15
                    elif days_since_trans <= 14:
                        transaction_score -= 10
                    elif days_since_trans <= 21:
                        transaction_score -= 5
                elif trans_type == 'signing':
                    if days_since_trans <= 7:
                        transaction_score -= 12
                    elif days_since_trans <= 14:
                        transaction_score -= 8
                    elif days_since_trans <= 21:
                        transaction_score -= 4
        except Exception as e:
            pass
    
    if 'games_played_season' in features_df.columns:
        games_with_team = features_df['games_played_season'].iloc[0] if not pd.isna(features_df['games_played_season'].iloc[0]) else season_games
        if games_with_team < 3 and season_games >= 5:
            transaction_score -= 8
    
    score += max(0, transaction_score)
    
    return int(max(0, min(100, score)))

def predict_upcoming_games(target_date=None, model_type='xgboost'):
    print(f"Predicting player performance for upcoming games using {model_type}...\n")
    
    if target_date is None:
        target_date = datetime.now().date()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    elif isinstance(target_date, date):
        target_date = target_date
    
    print(f"Target date: {target_date}")
    print(f"Model type: {model_type}\n")
    
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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    models_dir = os.path.join(project_root, 'data', 'models')
    features_path = os.path.join(project_root, 'data', 'processed', 'training_features.csv')
    
    league_means = {}
    if os.path.exists(features_path):
        training_df = pd.read_csv(features_path)
        feature_cols = [col for col in training_df.columns if any(x in col for x in 
                   ['_l5', '_l10', '_l20', '_weighted', 'is_', 'days_rest', 'games_played',
                    'offensive_rating', 'defensive_rating', 'net_rating', 'pace', 'opp_', 'altitude', 'playoff',
                    'star_teammate', 'games_without_star', 'usage_rate', 'minutes_played', 'minutes_trend',
                    'per_36', '_pct', '_ratio', 'pts_per', 'ast_to', 'reb_rate', 'position_'])]
        feature_cols = [col for col in feature_cols if 'team_id' not in col and 'player_id' not in col and 'game_id' not in col]
        
        for col in feature_cols:
            if col in training_df.columns:
                if 'team' in col or 'opp' in col or 'pace' in col:
                    league_means[col] = training_df[col].mean()
                elif col.startswith('is_') or col.startswith('position_'):
                    league_means[col] = 0
                elif 'trend' in col:
                    league_means[col] = 0
                else:
                    league_means[col] = training_df[col].mean()
    
    models = {}
    scalers = {}
    
    targets = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    for stat_name in targets.keys():
        model_path = os.path.join(models_dir, f'{model_type}_{stat_name}.pkl')
        scaler_path = os.path.join(models_dir, f'scaler_{model_type}_{stat_name}.pkl')
        
        if os.path.exists(model_path):
            models[stat_name] = joblib.load(model_path)
            if os.path.exists(scaler_path):
                scalers[stat_name] = joblib.load(scaler_path)
            else:
                print(f"Warning: Scaler not found for {model_type}_{stat_name}, predictions may be inaccurate")
                scalers[stat_name] = None
        else:
            print(f"Warning: Model not found: {model_path}")
            models[stat_name] = None
    
    if all(v is None for v in models.values()):
        print(f"No {model_type} models found! Please train models first.")
        cur.close()
        conn.close()
        return
    
    model_version = model_type
    
    all_predictions = []
    predictions_inserted = 0
    
    for _, game in games_df.iterrows():
        conn, cur = ensure_connection(conn, cur)
        
        game_id = game['game_id']
        home_team = game['home_team_id']
        away_team = game['away_team_id']
        season = game['season']
        game_type = game['game_type']
        
        print(f"\nProcessing game {game_id}...")
        
        for team_id in [home_team, away_team]:
            conn, cur = ensure_connection(conn, cur)
            
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
            
            newly_traded_query = f"""
                SELECT DISTINCT p.player_id
                FROM players p
                WHERE p.team_id = {team_id}
                    AND p.is_active = TRUE
                    AND p.player_id NOT IN (
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
                    )
                    AND p.player_id NOT IN (
                        SELECT DISTINCT i.player_id
                        FROM injuries i
                        WHERE i.injury_status = 'Out'
                        AND i.report_date <= '{target_date}'
                        AND (i.return_date IS NULL OR i.return_date > '{target_date}')
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM player_game_stats pgs2
                        JOIN games g2 ON pgs2.game_id = g2.game_id
                        WHERE pgs2.player_id = p.player_id
                        AND g2.season = '{season}'
                        AND g2.game_date < '{target_date}'
                        AND g2.game_status = 'completed'
                        GROUP BY pgs2.player_id
                        HAVING COUNT(*) >= 5
                    )
            """
            
            players = pd.read_sql(players_query, conn)
            newly_traded = pd.read_sql(newly_traded_query, conn)
            
            if len(newly_traded) > 0:
                print(f"    Found {len(newly_traded)} newly traded players (no games with new team yet)")
                players = pd.concat([players, newly_traded]).drop_duplicates(subset=['player_id'])
            
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
                    if model is None:
                        continue
                    
                    try:
                        if hasattr(model, 'get_booster'):
                            model_feature_names = model.get_booster().feature_names
                        elif hasattr(model, 'feature_name_'):
                            model_feature_names = model.feature_name_
                        elif hasattr(model, 'feature_names_in_'):
                            model_feature_names = model.feature_names_in_
                        elif hasattr(model, 'feature_names_'):
                            model_feature_names = model.feature_names_
                        else:
                            model_feature_names = features.columns.tolist()
                        
                        features_ordered = features[[col for col in model_feature_names if col in features.columns]].copy()
                        
                        for col in model_feature_names:
                            if col not in features_ordered.columns:
                                if 'team_id' not in col and 'player_id' not in col and 'game_id' not in col:
                                    if 'team' in col or 'opp' in col or 'pace' in col:
                                        features_ordered[col] = league_means.get(col, np.nan)
                                    elif col.startswith('is_') or col.startswith('position_') or 'trend' in col:
                                        features_ordered[col] = 0
                                    else:
                                        player_avg = league_means.get(col, np.nan)
                                        if recent_games is not None and len(recent_games) > 0:
                                            if col.startswith('points_'):
                                                player_avg = recent_games['points'].mean()
                                            elif col.startswith('rebounds_total_'):
                                                player_avg = recent_games['rebounds_total'].mean()
                                            elif col.startswith('assists_'):
                                                player_avg = recent_games['assists'].mean()
                                            elif col.startswith('steals_'):
                                                player_avg = recent_games['steals'].mean()
                                            elif col.startswith('blocks_'):
                                                player_avg = recent_games['blocks'].mean()
                                            elif col.startswith('turnovers_'):
                                                player_avg = recent_games['turnovers'].mean()
                                            elif col.startswith('three_pointers_made_'):
                                                player_avg = recent_games['three_pointers_made'].mean()
                                            elif 'minutes_played' in col and 'per_36' not in col:
                                                player_avg = recent_games['minutes_played'].mean()
                                            elif 'usage_rate' in col:
                                                player_avg = recent_games['usage_rate'].mean() if 'usage_rate' in recent_games.columns else league_means.get(col, 0)
                                            elif 'offensive_rating' in col and 'team' not in col and 'opp' not in col:
                                                player_avg = recent_games['offensive_rating'].mean() if 'offensive_rating' in recent_games.columns else league_means.get(col, 0)
                                            elif 'defensive_rating' in col and 'team' not in col and 'opp' not in col:
                                                player_avg = recent_games['defensive_rating'].mean() if 'defensive_rating' in recent_games.columns else league_means.get(col, 0)
                                        features_ordered[col] = player_avg
                        
                        features_ordered = features_ordered[[col for col in model_feature_names if col in features_ordered.columns]]
                        
                        for col in features_ordered.columns:
                            if features_ordered[col].isna().any():
                                if 'team' in col or 'opp' in col or 'pace' in col:
                                    features_ordered[col] = features_ordered[col].fillna(league_means.get(col, 0))
                                elif col.startswith('is_') or col.startswith('position_') or 'trend' in col:
                                    features_ordered[col] = features_ordered[col].fillna(0)
                                else:
                                    player_avg = league_means.get(col, 0)
                                    if recent_games is not None and len(recent_games) > 0:
                                        if col.startswith('points_'):
                                            player_avg = recent_games['points'].mean()
                                        elif col.startswith('rebounds_total_'):
                                            player_avg = recent_games['rebounds_total'].mean()
                                        elif col.startswith('assists_'):
                                            player_avg = recent_games['assists'].mean()
                                        elif col.startswith('steals_'):
                                            player_avg = recent_games['steals'].mean()
                                        elif col.startswith('blocks_'):
                                            player_avg = recent_games['blocks'].mean()
                                        elif col.startswith('turnovers_'):
                                            player_avg = recent_games['turnovers'].mean()
                                        elif col.startswith('three_pointers_made_'):
                                            player_avg = recent_games['three_pointers_made'].mean()
                                        elif 'minutes_played' in col and 'per_36' not in col:
                                            player_avg = recent_games['minutes_played'].mean()
                                        elif 'usage_rate' in col:
                                            player_avg = recent_games['usage_rate'].mean() if 'usage_rate' in recent_games.columns else league_means.get(col, 0)
                                        elif 'offensive_rating' in col and 'team' not in col and 'opp' not in col:
                                            player_avg = recent_games['offensive_rating'].mean() if 'offensive_rating' in recent_games.columns else league_means.get(col, 0)
                                        elif 'defensive_rating' in col and 'team' not in col and 'opp' not in col:
                                            player_avg = recent_games['defensive_rating'].mean() if 'defensive_rating' in recent_games.columns else league_means.get(col, 0)
                                    features_ordered[col] = features_ordered[col].fillna(player_avg)
                        
                        if scalers[stat_name] is not None:
                            features_scaled = pd.DataFrame(
                                scalers[stat_name].transform(features_ordered),
                                columns=features_ordered.columns
                            )
                        else:
                            features_scaled = features_ordered
                        
                        pred = model.predict(features_scaled)[0]
                        pred = max(0.0, pred)
                        predictions[stat_name] = float(round(pred, 1))
                        
                    except Exception as e:
                        print(f"Warning: Error predicting {stat_name} with {model_type}: {e}")
                        predictions[stat_name] = 0.0
                
                confidence_score = calculate_confidence(
                    features, recent_games, 
                    conn=conn, player_id=player_id, 
                    target_date=target_date, season=season
                )
                
                feature_explanations = {}
                if isinstance(features, pd.DataFrame):
                    features_dict = features.iloc[0].to_dict()
                else:
                    features_dict = features
                
                for stat_name in predictions.keys():
                    top_features = get_top_features_with_impact(
                        features_dict,
                        model_type,
                        stat_name,
                        league_means,
                        top_n=10
                    )
                    feature_explanations[stat_name] = top_features
                
                try:
                    conn, cur = ensure_connection(conn, cur)
                    
                    cur.execute("""
                        INSERT INTO predictions (
                            game_id, player_id, prediction_date,
                            predicted_points, predicted_rebounds, predicted_assists,
                            predicted_steals, predicted_blocks, predicted_turnovers,
                            predicted_three_pointers_made, confidence_score, model_version,
                            feature_explanations
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_id, game_id, model_version) DO UPDATE SET
                            predicted_points = EXCLUDED.predicted_points,
                            predicted_rebounds = EXCLUDED.predicted_rebounds,
                            predicted_assists = EXCLUDED.predicted_assists,
                            predicted_steals = EXCLUDED.predicted_steals,
                            predicted_blocks = EXCLUDED.predicted_blocks,
                            predicted_turnovers = EXCLUDED.predicted_turnovers,
                            predicted_three_pointers_made = EXCLUDED.predicted_three_pointers_made,
                            confidence_score = EXCLUDED.confidence_score,
                            prediction_date = EXCLUDED.prediction_date,
                            feature_explanations = EXCLUDED.feature_explanations
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
                        model_version,
                        json.dumps(feature_explanations)
                    ))
                    
                    predictions_inserted += 1
                    
                except Exception as e:
                    print(f"Error inserting prediction for player {player_id}: {e}")
                    if "connection" in str(e).lower() or "cursor" in str(e).lower():
                        conn, cur = ensure_connection(conn, cur)
                    else:
                        try:
                            conn.rollback()
                        except:
                            conn, cur = ensure_connection(conn, cur)
                    continue
                
                all_predictions.append({
                    'game_id': game_id,
                    'player_id': player_id,
                    'team_id': team_id,
                    'is_home': is_home,
                    'feature_explanations': json.dumps(feature_explanations),
                    **predictions
                })
    
    try:
        conn.commit()
    except Exception as commit_error:
        print(f"Final commit error, reconnecting: {commit_error}")
        conn, cur = ensure_connection(conn, cur)
        conn.commit()
    
    try:
        cur.close()
        conn.close()
    except:
        pass
    
    if len(all_predictions) == 0:
        print("No predictions generated")
        return
    
    pred_df = pd.DataFrame(all_predictions)
    
    if 'feature_explanations' in pred_df.columns:
        pred_df_csv = pred_df.drop(columns=['feature_explanations'])
    else:
        pred_df_csv = pred_df
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    output_path = os.path.join(project_root, 'data', 'predictions', f'predictions_{target_date}.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pred_df_csv.to_csv(output_path, index=False)
    
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
            pgs.steals,
            pgs.blocks,
            pgs.turnovers,
            pgs.three_pointers_made,
            pgs.minutes_played,
            pgs.field_goals_made,
            pgs.field_goals_attempted,
            pgs.three_pointers_attempted,
            pgs.free_throws_made,
            pgs.free_throws_attempted,
            pgs.usage_rate,
            pgs.true_shooting_pct,
            pgs.offensive_rating,
            pgs.defensive_rating,
            pgs.is_starter,
            g.game_date,
            g.game_type
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
            AND g.game_date < '{target_date}'
            AND g.game_status = 'completed'
            AND g.season = '{season}'
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
        for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
            features[f'{stat}_l{window}'] = window_games[stat].mean() if stat in window_games.columns else np.nan
    
    decay_factor = 0.1
    for window in [5, 10, 20]:
        window_games = recent_games.head(window).copy()
        if len(window_games) > 0:
            weights = np.exp(-decay_factor * np.arange(len(window_games))[::-1])
            weights = weights / weights.sum()
            
            for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
                if stat in window_games.columns:
                    features[f'{stat}_l{window}_weighted'] = np.sum(window_games[stat].values * weights)
                else:
                    features[f'{stat}_l{window}_weighted'] = np.nan
        else:
            for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
                features[f'{stat}_l{window}_weighted'] = np.nan
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        if 'minutes_played' in window_games.columns:
            features[f'minutes_played_l{window}'] = window_games['minutes_played'].mean()
            if len(window_games) > 0:
                weights = np.exp(-decay_factor * np.arange(len(window_games))[::-1])
                weights = weights / weights.sum()
                features[f'minutes_played_l{window}_weighted'] = np.sum(window_games['minutes_played'].values * weights)
            else:
                features[f'minutes_played_l{window}_weighted'] = np.nan
        else:
            features[f'minutes_played_l{window}'] = np.nan
            features[f'minutes_played_l{window}_weighted'] = np.nan
    
    if 'is_starter' in recent_games.columns:
        recent_games['is_starter'] = recent_games['is_starter'].astype(int)
        for window in [5, 10]:
            window_games = recent_games.head(window)
            features[f'is_starter_l{window}'] = window_games['is_starter'].mean() if len(window_games) > 0 else 0
    else:
        for window in [5, 10]:
            features[f'is_starter_l{window}'] = 0
    
    if 'minutes_played' in recent_games.columns and len(recent_games) >= 3:
        recent_minutes = recent_games.head(10)['minutes_played'].values
        if len(recent_minutes) >= 3 and np.std(recent_minutes) > 0:
            x = np.arange(len(recent_minutes))
            slope = np.polyfit(x, recent_minutes, 1)[0]
            features['minutes_trend'] = slope
        else:
            features['minutes_trend'] = 0.0
    else:
        features['minutes_trend'] = 0.0
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        if 'usage_rate' in window_games.columns:
            features[f'usage_rate_l{window}'] = window_games['usage_rate'].mean()
            if len(window_games) > 0:
                weights = np.exp(-decay_factor * np.arange(len(window_games))[::-1])
                weights = weights / weights.sum()
                features[f'usage_rate_l{window}_weighted'] = np.sum(window_games['usage_rate'].values * weights)
            else:
                features[f'usage_rate_l{window}_weighted'] = np.nan
        else:
            features[f'usage_rate_l{window}'] = np.nan
            features[f'usage_rate_l{window}_weighted'] = np.nan
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        for stat in ['offensive_rating', 'defensive_rating']:
            if stat in window_games.columns:
                features[f'{stat}_l{window}'] = window_games[stat].mean()
            else:
                features[f'{stat}_l{window}'] = np.nan
        features[f'net_rating_l{window}'] = features[f'offensive_rating_l{window}'] - features[f'defensive_rating_l{window}']
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        if len(window_games) > 0:
            if 'field_goals_made' in window_games.columns and 'field_goals_attempted' in window_games.columns:
                fgm_sum = window_games['field_goals_made'].sum()
                fga_sum = window_games['field_goals_attempted'].sum()
                features[f'fg_pct_l{window}'] = (fgm_sum / fga_sum) if fga_sum > 0 else 0
            else:
                features[f'fg_pct_l{window}'] = np.nan
            
            if 'three_pointers_made' in window_games.columns and 'three_pointers_attempted' in window_games.columns:
                made_3p_sum = window_games['three_pointers_made'].sum()
                att_3p_sum = window_games['three_pointers_attempted'].sum()
                features[f'three_pct_l{window}'] = (made_3p_sum / att_3p_sum) if att_3p_sum > 0 else 0
            else:
                features[f'three_pct_l{window}'] = np.nan
            
            if 'free_throws_made' in window_games.columns and 'free_throws_attempted' in window_games.columns:
                made_ft_sum = window_games['free_throws_made'].sum()
                att_ft_sum = window_games['free_throws_attempted'].sum()
                features[f'ft_pct_l{window}'] = (made_ft_sum / att_ft_sum) if att_ft_sum > 0 else 0
            else:
                features[f'ft_pct_l{window}'] = np.nan
            
            if 'true_shooting_pct' in window_games.columns:
                features[f'true_shooting_pct_l{window}'] = window_games['true_shooting_pct'].mean()
            else:
                features[f'true_shooting_pct_l{window}'] = np.nan
        else:
            features[f'fg_pct_l{window}'] = np.nan
            features[f'three_pct_l{window}'] = np.nan
            features[f'ft_pct_l{window}'] = np.nan
            features[f'true_shooting_pct_l{window}'] = np.nan
    
    for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
        for window in [5, 10, 20]:
            window_games = recent_games.head(window)
            if len(window_games) > 0 and stat in window_games.columns and 'minutes_played' in window_games.columns:
                stat_sum = window_games[stat].sum()
                min_sum = window_games['minutes_played'].sum()
                features[f'{stat}_per_36_l{window}'] = (stat_sum / min_sum * 36) if min_sum > 0 else 0
            else:
                features[f'{stat}_per_36_l{window}'] = np.nan
    
    for window in [5, 10, 20]:
        window_games = recent_games.head(window)
        if len(window_games) > 0:
            if 'assists' in window_games.columns and 'turnovers' in window_games.columns:
                ast_sum = window_games['assists'].sum()
                tov_sum = window_games['turnovers'].sum()
                features[f'ast_to_ratio_l{window}'] = (ast_sum / tov_sum) if tov_sum > 0 else ast_sum
            else:
                features[f'ast_to_ratio_l{window}'] = np.nan
            
            if 'points' in window_games.columns and 'field_goals_attempted' in window_games.columns:
                pts_sum = window_games['points'].sum()
                fga_sum = window_games['field_goals_attempted'].sum()
                features[f'pts_per_fga_l{window}'] = (pts_sum / fga_sum) if fga_sum > 0 else 0
            else:
                features[f'pts_per_fga_l{window}'] = np.nan
            
            if 'points' in window_games.columns and 'assists' in window_games.columns:
                pts_sum = window_games['points'].sum()
                ast_sum = window_games['assists'].sum()
                features[f'pts_per_ast_l{window}'] = (pts_sum / ast_sum) if ast_sum > 0 else pts_sum
            else:
                features[f'pts_per_ast_l{window}'] = np.nan
            
            if 'rebounds_total' in window_games.columns and 'minutes_played' in window_games.columns:
                reb_sum = window_games['rebounds_total'].sum()
                min_sum = window_games['minutes_played'].sum()
                features[f'reb_rate_l{window}'] = (reb_sum / (min_sum / 36)) if min_sum > 0 else 0
            else:
                features[f'reb_rate_l{window}'] = np.nan
        else:
            features[f'ast_to_ratio_l{window}'] = 0
            features[f'pts_per_fga_l{window}'] = 0
            features[f'pts_per_ast_l{window}'] = 0
            features[f'reb_rate_l{window}'] = 0
    
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
    
    if len(recent_games) > 0:
        recent_games['game_date'] = pd.to_datetime(recent_games['game_date'])
        days_rest = (pd.to_datetime(target_date) - recent_games['game_date'].iloc[0]).days
        features['days_rest'] = days_rest
        features['is_back_to_back'] = 1 if days_rest == 1 else 0
    else:
        features['days_rest'] = 3
        features['is_back_to_back'] = 0
    
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
    
    opp_ratings = pd.read_sql(f"""
        SELECT offensive_rating, defensive_rating, pace
        FROM team_ratings
        WHERE team_id = {opponent_id} AND season = '{season}'
    """, conn)
    
    if len(opp_ratings) > 0:
        features['offensive_rating_opp'] = opp_ratings.iloc[0]['offensive_rating']
        features['defensive_rating_opp'] = opp_ratings.iloc[0]['defensive_rating']
        features['pace_opp'] = opp_ratings.iloc[0]['pace']
    
    opp_defense = pd.read_sql(f"""
        SELECT opp_field_goal_pct, opp_three_point_pct
        FROM team_defensive_stats
        WHERE team_id = {opponent_id} AND season = '{season}'
    """, conn)
    
    if len(opp_defense) > 0:
        features['opp_field_goal_pct'] = opp_defense.iloc[0]['opp_field_goal_pct']
        features['opp_three_point_pct'] = opp_defense.iloc[0]['opp_three_point_pct']
    
    opp_defense_stats = calculate_team_defensive_stats_as_of_date(
        conn, opponent_id, season, target_date
    )
    if opp_defense_stats:
        features['opp_team_turnovers_per_game'] = opp_defense_stats.get('opp_team_turnovers_per_game', 14.0)
        features['opp_team_steals_per_game'] = opp_defense_stats.get('opp_team_steals_per_game', 7.0)
    else:
        features['opp_team_turnovers_per_game'] = 14.0
        features['opp_team_steals_per_game'] = 7.0
    
    player_position_query = pd.read_sql(f"""
        SELECT position
        FROM players
        WHERE player_id = {player_id}
    """, conn)
    
    if len(player_position_query) > 0:
        player_position = str(player_position_query.iloc[0]['position'] or '').upper().strip()
        if ('CENTER' in player_position or player_position == 'C') and 'GUARD' not in player_position and 'FORWARD' not in player_position:
            defense_position = 'C'
            features['position_guard'] = 0
            features['position_forward'] = 0
            features['position_center'] = 1
        elif 'FORWARD' in player_position or player_position == 'F' or player_position == 'F-C':
            defense_position = 'F'
            features['position_guard'] = 0
            features['position_forward'] = 1
            features['position_center'] = 0
        elif 'GUARD' in player_position or player_position == 'G' or player_position == 'G-F':
            defense_position = 'G'
            features['position_guard'] = 1
            features['position_forward'] = 0
            features['position_center'] = 0
        else:
            defense_position = 'G'
            features['position_guard'] = 1
            features['position_forward'] = 0
            features['position_center'] = 0
    else:
        defense_position = 'G'
        features['position_guard'] = 1
        features['position_forward'] = 0
        features['position_center'] = 0
    
    pos_defense = pd.read_sql(f"""
        SELECT points_allowed_per_game,
               rebounds_allowed_per_game,
               assists_allowed_per_game,
               blocks_allowed_per_game,
               turnovers_forced_per_game,
               three_pointers_made_allowed_per_game
        FROM position_defense_stats
        WHERE team_id = {opponent_id} AND season = '{season}' AND position = '{defense_position}'
    """, conn)
    
    if len(pos_defense) > 0:
        features['opp_points_allowed_to_position'] = pos_defense.iloc[0]['points_allowed_per_game']
        features['opp_rebounds_allowed_to_position'] = pos_defense.iloc[0]['rebounds_allowed_per_game']
        features['opp_assists_allowed_to_position'] = pos_defense.iloc[0]['assists_allowed_per_game']
        features['opp_blocks_allowed_to_position'] = pos_defense.iloc[0]['blocks_allowed_per_game']
        features['opp_three_pointers_allowed_to_position'] = pos_defense.iloc[0]['three_pointers_made_allowed_per_game']
    
    pos_defense_stats = calculate_position_defense_stats_as_of_date(
        conn, opponent_id, season, defense_position, target_date
    )
    if pos_defense_stats:
        features['opp_position_turnovers_vs_team'] = pos_defense_stats.get('opp_position_turnovers_vs_team', 0)
        features['opp_position_steals_vs_team'] = pos_defense_stats.get('opp_position_steals_vs_team', 0)
    else:
        features['opp_position_turnovers_vs_team'] = 0
        features['opp_position_steals_vs_team'] = 0
    
    opp_turnover_stats = calculate_opponent_team_turnover_stats_as_of_date(
        conn, opponent_id, season, defense_position, target_date
    )
    if opp_turnover_stats:
        features['opp_position_turnovers_overall'] = opp_turnover_stats.get('opp_position_turnovers_overall', 0)
        features['opp_position_steals_overall'] = opp_turnover_stats.get('opp_position_steals_overall', 0)
    else:
        features['opp_position_turnovers_overall'] = 0
        features['opp_position_steals_overall'] = 0
    
    altitude_query = pd.read_sql(f"""
        SELECT arena_altitude
        FROM teams
        WHERE team_id = {opponent_id}
    """, conn)
    
    if len(altitude_query) > 0:
        altitude = altitude_query.iloc[0]['arena_altitude']
        if altitude:
            features['arena_altitude'] = altitude
            features['altitude_away'] = 1 if (is_home == 0 and altitude > 3000) else 0
    
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
        'points_l5', 'rebounds_total_l5', 'assists_l5', 'steals_l5', 'blocks_l5', 'turnovers_l5', 'three_pointers_made_l5',
        'points_l10', 'rebounds_total_l10', 'assists_l10', 'steals_l10', 'blocks_l10', 'turnovers_l10', 'three_pointers_made_l10',
        'points_l20', 'rebounds_total_l20', 'assists_l20', 'steals_l20', 'blocks_l20', 'turnovers_l20', 'three_pointers_made_l20',
        'points_l5_weighted', 'rebounds_total_l5_weighted', 'assists_l5_weighted', 'steals_l5_weighted', 'blocks_l5_weighted', 'turnovers_l5_weighted', 'three_pointers_made_l5_weighted',
        'points_l10_weighted', 'rebounds_total_l10_weighted', 'assists_l10_weighted', 'steals_l10_weighted', 'blocks_l10_weighted', 'turnovers_l10_weighted', 'three_pointers_made_l10_weighted',
        'points_l20_weighted', 'rebounds_total_l20_weighted', 'assists_l20_weighted', 'steals_l20_weighted', 'blocks_l20_weighted', 'turnovers_l20_weighted', 'three_pointers_made_l20_weighted',
        'minutes_played_l5', 'minutes_played_l10', 'minutes_played_l20',
        'minutes_played_l5_weighted', 'minutes_played_l10_weighted', 'minutes_played_l20_weighted',
        'is_starter_l5', 'is_starter_l10',
        'usage_rate_l5', 'usage_rate_l10', 'usage_rate_l20',
        'usage_rate_l5_weighted', 'usage_rate_l10_weighted', 'usage_rate_l20_weighted',
        'offensive_rating_l5', 'offensive_rating_l10', 'offensive_rating_l20',
        'defensive_rating_l5', 'defensive_rating_l10', 'defensive_rating_l20',
        'net_rating_l5', 'net_rating_l10', 'net_rating_l20',
        'fg_pct_l5', 'fg_pct_l10', 'fg_pct_l20',
        'three_pct_l5', 'three_pct_l10', 'three_pct_l20',
        'ft_pct_l5', 'ft_pct_l10', 'ft_pct_l20',
        'true_shooting_pct_l5', 'true_shooting_pct_l10', 'true_shooting_pct_l20',
        'points_per_36_l5', 'points_per_36_l10', 'points_per_36_l20',
        'rebounds_total_per_36_l5', 'rebounds_total_per_36_l10', 'rebounds_total_per_36_l20',
        'assists_per_36_l5', 'assists_per_36_l10', 'assists_per_36_l20',
        'steals_per_36_l5', 'steals_per_36_l10', 'steals_per_36_l20',
        'blocks_per_36_l5', 'blocks_per_36_l10', 'blocks_per_36_l20',
        'turnovers_per_36_l5', 'turnovers_per_36_l10', 'turnovers_per_36_l20',
        'three_pointers_made_per_36_l5', 'three_pointers_made_per_36_l10', 'three_pointers_made_per_36_l20',
        'ast_to_ratio_l5', 'ast_to_ratio_l10', 'ast_to_ratio_l20',
        'pts_per_fga_l5', 'pts_per_fga_l10', 'pts_per_fga_l20',
        'pts_per_ast_l5', 'pts_per_ast_l10', 'pts_per_ast_l20',
        'reb_rate_l5', 'reb_rate_l10', 'reb_rate_l20',
        'minutes_trend',
        'position_guard', 'position_forward', 'position_center',
        'star_teammate_out', 'star_teammate_ppg', 'games_without_star',  
        'playoff_games_career', 'playoff_performance_boost',
        'is_home', 'days_rest', 'is_back_to_back', 'games_played_season',
        'offensive_rating_team', 'defensive_rating_team', 'pace_team',
        'offensive_rating_opp', 'defensive_rating_opp', 'pace_opp',
        'opp_field_goal_pct', 'opp_three_point_pct',
        'opp_team_turnovers_per_game', 'opp_team_steals_per_game',
        'opp_points_allowed_to_position', 'opp_rebounds_allowed_to_position',
        'opp_assists_allowed_to_position', 'opp_blocks_allowed_to_position',
        'opp_three_pointers_allowed_to_position',
        'opp_position_turnovers_vs_team', 'opp_position_steals_vs_team',
        'opp_position_turnovers_overall', 'opp_position_steals_overall',
        'arena_altitude', 'altitude_away'
    ]
    
    available_cols = [col for col in column_order if col in features_df.columns]
    features_df = features_df[available_cols]
    
    if 'team_id_opp_venue' in features_df.columns:
        features_df = features_df.drop(columns=['team_id_opp_venue'])
    
    return features_df, recent_games

def predict_all_models(target_date=None):
    if target_date is None:
        target_date = datetime.now().date()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    model_types = ['xgboost', 'lightgbm', 'random_forest', 'catboost']
    
    for model_type in model_types:
        try:
            predict_upcoming_games(target_date, model_type)
        except Exception as e:
            print(f"Error predicting with {model_type}: {e}")
            continue

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        if len(sys.argv) > 2 and sys.argv[2] == '--all':
            predict_all_models(target_date)
        else:
            model_type = sys.argv[2] if len(sys.argv) > 2 else 'xgboost'
            predict_upcoming_games(target_date, model_type)
    else:
        predict_all_models()