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
from predictions.confidence_scoring import (
    calculate_confidence_score,
    calculate_confidence_score_per_stat,
    calculate_multi_stat_variance,
    reset_variance_diagnostic,
    enable_variance_diagnostic,
    CONFIDENCE_CONFIG,
    ConfidenceBreakdown
)
from predictions.confidence_helpers import (
    load_feature_importances,
    get_feature_groups,
    collect_player_stats_for_variance,
    get_available_features
)
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
import json
import logging

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

logger = logging.getLogger(__name__)

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


def calculate_confidence_new(
    predictions_by_model: Dict[str, Dict[str, float]],
    selected_models: List[str],
    features_df: pd.DataFrame,
    recent_games: pd.DataFrame,
    conn,
    player_id: int,
    game_id: int,
    target_date: date,
    season: str,
    opponent_def_rating: float,
    project_root: str,
    player_name: Optional[str] = None
) -> Tuple[int, Dict[str, Dict]]:

    try:
        feature_importances = load_feature_importances(project_root)
        feature_groups = get_feature_groups()
        
        player_stats = collect_player_stats_for_variance(
            recent_games, conn, player_id, target_date
        )
        
        available_features = get_available_features(features_df)
        
        season_games = len(recent_games) if recent_games is not None else 0
        career_games_query = f"""
            SELECT COUNT(*) as career_games
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE pgs.player_id = {player_id}
            AND g.game_status = 'completed'
            AND g.game_date < '{target_date}'
        """
        career_games_df = pd.read_sql(career_games_query, conn)
        career_games = career_games_df.iloc[0]['career_games'] if len(career_games_df) > 0 else season_games
        
        days_since_transaction = None
        games_with_team = season_games
        if 'games_played_season' in features_df.columns:
            games_with_team = int(features_df['games_played_season'].iloc[0]) if not pd.isna(features_df['games_played_season'].iloc[0]) else season_games
        
        transaction_query = f"""
            SELECT transaction_date, transaction_type
            FROM player_transactions
            WHERE player_id = {player_id}
            AND transaction_type IN ('trade', 'signing')
            AND transaction_date >= %s::date - INTERVAL '30 days'
            AND transaction_date <= %s::date
            ORDER BY transaction_date DESC
            LIMIT 1
        """
        transaction_df = pd.read_sql(transaction_query, conn, params=(target_date, target_date))
        if len(transaction_df) > 0:
            trans_date = transaction_df.iloc[0]['transaction_date']
            days_since_transaction = (pd.to_datetime(target_date) - pd.to_datetime(trans_date)).days
        
        games_since_injury = None
        injury_query = f"""
            SELECT return_date, games_missed
            FROM injuries
            WHERE player_id = {player_id}
            AND return_date IS NOT NULL
            AND return_date >= %s::date - INTERVAL '60 days'
            AND return_date <= %s::date
            ORDER BY return_date DESC
            LIMIT 1
        """
        injury_df = pd.read_sql(injury_query, conn, params=(target_date, target_date))
        if len(injury_df) > 0:
            return_date = injury_df.iloc[0]['return_date']
            days_since_return = (pd.to_datetime(target_date) - pd.to_datetime(return_date)).days
            games_since_injury = max(0, int(days_since_return / 2.5))
        
        is_playoff = features_df['is_playoff'].iloc[0] if 'is_playoff' in features_df.columns else False
        is_back_to_back = features_df['is_back_to_back'].iloc[0] if 'is_back_to_back' in features_df.columns else False
        
        stat_breakdowns = {}
        stat_confidences = []
        target_stats = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']
        
        for stat_name in target_stats:
            if stat_name not in predictions_by_model:
                continue
            
            try:
                stat_confidence, stat_breakdown = calculate_confidence_score_per_stat(
                    stat_name=stat_name,
                    predictions_by_model=predictions_by_model,
                    selected_models=selected_models,
                    player_stats=player_stats,
                    available_features=available_features,
                    feature_importances=feature_importances,
                    feature_groups=feature_groups,
                    games_this_season=season_games,
                    career_games=career_games,
                    days_since_transaction=days_since_transaction,
                    games_with_team=games_with_team,
                    opponent_def_rating=opponent_def_rating,
                    calibrator=None,
                    config=CONFIDENCE_CONFIG,
                    logger=logger,
                    games_since_injury=games_since_injury,
                    is_playoff=bool(is_playoff),
                    is_back_to_back=bool(is_back_to_back),
                    player_id=player_id,
                    game_id=game_id,
                    player_name=player_name
                )
                stat_breakdowns[stat_name] = stat_breakdown.to_dict()
                stat_confidences.append(stat_confidence)
            except Exception as e:
                logger.warning(f"Error calculating confidence for stat {stat_name}, player {player_id}, game {game_id}: {e}")
                continue
        
        if stat_confidences:
            overall_confidence = sum(stat_confidences) / len(stat_confidences)
        else:
            old_score = calculate_confidence(features_df, recent_games, conn, player_id, target_date, season)
            return old_score, {}
        
        return int(round(overall_confidence)), stat_breakdowns
        
    except Exception as e:
        logger.warning(f"Error calculating new confidence for player {player_id}, game {game_id}: {e}")
        old_score = calculate_confidence(features_df, recent_games, conn, player_id, target_date, season)
        return old_score, {}

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
                
                missing_transactions = []
                for _, row in newly_traded.iterrows():
                    player_id = row['player_id']
                    transaction_check = f"""
                        SELECT transaction_id, transaction_date, transaction_type
                        FROM player_transactions
                        WHERE player_id = {player_id}
                            AND transaction_type IN ('trade', 'signing', 'waiver')
                            AND transaction_date >= %s::date - INTERVAL '7 days'
                            AND transaction_date <= %s::date
                        LIMIT 1
                    """
                    trans_result = pd.read_sql(transaction_check, conn, params=(target_date, target_date))
                    if len(trans_result) == 0:
                        player_name_query = f"SELECT full_name FROM players WHERE player_id = {player_id}"
                        player_name_result = pd.read_sql(player_name_query, conn)
                        player_name = player_name_result.iloc[0]['full_name'] if len(player_name_result) > 0 else f"Player {player_id}"
                        missing_transactions.append((player_id, player_name))
                
                if missing_transactions:
                    print(f"    WARNING: {len(missing_transactions)} newly traded player(s) missing from transactions table:")
                    for pid, pname in missing_transactions:
                        print(f"      - {pname} (ID: {pid}) - No transaction record found in past 7 days")
                    print(f"    Run detect_and_update_trades.py to update transaction records")
                
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
                                        features_ordered[col] = league_means.get(col, 0)
                                    elif col.startswith('is_') or col.startswith('position_') or 'trend' in col or col in ['west_to_east', 'east_to_west', 'post_asb_bounce']:
                                        features_ordered[col] = 0
                                    else:
                                        player_avg = league_means.get(col, 0)
                                        if recent_games is not None and len(recent_games) > 0:
                                            if col.startswith('points_'):
                                                player_avg = recent_games['points'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif col.startswith('rebounds_total_'):
                                                player_avg = recent_games['rebounds_total'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif col.startswith('assists_'):
                                                player_avg = recent_games['assists'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif col.startswith('steals_'):
                                                player_avg = recent_games['steals'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif col.startswith('blocks_'):
                                                player_avg = recent_games['blocks'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif col.startswith('turnovers_'):
                                                player_avg = recent_games['turnovers'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif col.startswith('three_pointers_made_'):
                                                player_avg = recent_games['three_pointers_made'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif 'minutes_played' in col and 'per_36' not in col:
                                                player_avg = recent_games['minutes_played'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                            elif 'usage_rate' in col:
                                                player_avg = recent_games['usage_rate'].mean() if 'usage_rate' in recent_games.columns and len(recent_games) > 0 else league_means.get(col, 0)
                                            elif 'offensive_rating' in col and 'team' not in col and 'opp' not in col:
                                                player_avg = recent_games['offensive_rating'].mean() if 'offensive_rating' in recent_games.columns and len(recent_games) > 0 else league_means.get(col, 0)
                                            elif 'defensive_rating' in col and 'team' not in col and 'opp' not in col:
                                                player_avg = recent_games['defensive_rating'].mean() if 'defensive_rating' in recent_games.columns and len(recent_games) > 0 else league_means.get(col, 0)
                                        if pd.isna(player_avg):
                                            player_avg = league_means.get(col, 0)
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
                        
                        for col in features_ordered.columns:
                            if features_ordered[col].isna().any():
                                if 'team' in col or 'opp' in col or 'pace' in col:
                                    features_ordered[col] = features_ordered[col].fillna(league_means.get(col, 0))
                                elif col.startswith('is_') or col.startswith('position_') or 'trend' in col or col in ['west_to_east', 'east_to_west', 'post_asb_bounce']:
                                    features_ordered[col] = features_ordered[col].fillna(0)
                                else:
                                    player_avg = league_means.get(col, 0)
                                    if recent_games is not None and len(recent_games) > 0:
                                        if col.startswith('points_'):
                                            player_avg = recent_games['points'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif col.startswith('rebounds_total_'):
                                            player_avg = recent_games['rebounds_total'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif col.startswith('assists_'):
                                            player_avg = recent_games['assists'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif col.startswith('steals_'):
                                            player_avg = recent_games['steals'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif col.startswith('blocks_'):
                                            player_avg = recent_games['blocks'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif col.startswith('turnovers_'):
                                            player_avg = recent_games['turnovers'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif col.startswith('three_pointers_made_'):
                                            player_avg = recent_games['three_pointers_made'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif 'minutes_played' in col and 'per_36' not in col:
                                            player_avg = recent_games['minutes_played'].mean() if len(recent_games) > 0 else league_means.get(col, 0)
                                        elif 'usage_rate' in col:
                                            player_avg = recent_games['usage_rate'].mean() if 'usage_rate' in recent_games.columns and len(recent_games) > 0 else league_means.get(col, 0)
                                        elif 'offensive_rating' in col and 'team' not in col and 'opp' not in col:
                                            player_avg = recent_games['offensive_rating'].mean() if 'offensive_rating' in recent_games.columns and len(recent_games) > 0 else league_means.get(col, 0)
                                        elif 'defensive_rating' in col and 'team' not in col and 'opp' not in col:
                                            player_avg = recent_games['defensive_rating'].mean() if 'defensive_rating' in recent_games.columns and len(recent_games) > 0 else league_means.get(col, 0)
                                    if pd.isna(player_avg):
                                        player_avg = league_means.get(col, 0)
                                    features_ordered[col] = features_ordered[col].fillna(player_avg)
                        
                        features_ordered = features_ordered.fillna(0)
                        
                        if scalers[stat_name] is not None:
                            features_scaled = pd.DataFrame(
                                scalers[stat_name].transform(features_ordered),
                                columns=features_ordered.columns
                            )
                            features_scaled = features_scaled.fillna(0)
                        else:
                            features_scaled = features_ordered
                        
                        pred = model.predict(features_scaled)[0]
                        pred = max(0.0, pred)
                        predictions[stat_name] = float(round(pred, 1))
                        
                    except Exception as e:
                        print(f"Warning: Error predicting {stat_name} with {model_type}: {e}")
                        predictions[stat_name] = 0.0
                
                predictions_by_model = {}
                for stat_name in ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
                    if stat_name in predictions:
                        predictions_by_model[stat_name] = {model_type: predictions[stat_name]}
                
                opponent_def_rating = 114.0
                if isinstance(features, pd.DataFrame):
                    if 'defensive_rating_opp' in features.columns:
                        opp_dr = features['defensive_rating_opp'].iloc[0]
                        if not pd.isna(opp_dr):
                            opponent_def_rating = float(opp_dr)
                elif isinstance(features, dict):
                    if 'defensive_rating_opp' in features:
                        opp_dr = features['defensive_rating_opp']
                        if opp_dr is not None and not (isinstance(opp_dr, float) and np.isnan(opp_dr)):
                            opponent_def_rating = float(opp_dr)
                
                script_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.dirname(os.path.dirname(script_dir))
                
                if isinstance(features, dict):
                    features_df = pd.DataFrame([features])
                else:
                    features_df = features
                
                player_name_query = f"SELECT full_name FROM players WHERE player_id = {player_id}"
                player_name_result = pd.read_sql(player_name_query, conn)
                player_name = player_name_result.iloc[0]['full_name'] if len(player_name_result) > 0 else None
                
                stat_breakdowns = {}
                try:
                    confidence_score, stat_breakdowns = calculate_confidence_new(
                        predictions_by_model=predictions_by_model,
                        selected_models=[model_type],
                        features_df=features_df,
                        recent_games=recent_games,
                        conn=conn,
                        player_id=player_id,
                        game_id=game_id,
                        target_date=target_date,
                        season=season,
                        opponent_def_rating=opponent_def_rating,
                        project_root=project_root,
                        player_name=player_name
                    )
                except Exception as e:
                    logger.warning(f"Error with new confidence system, falling back to old: {e}")
                    confidence_score = calculate_confidence(
                        features_df, recent_games, 
                        conn=conn, player_id=player_id, 
                        target_date=target_date, season=season
                    )
                    stat_breakdowns = {}
                
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
                        top_n=15
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
                        RETURNING prediction_id
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
                    
                    result = cur.fetchone()
                    prediction_id = result[0] if result else None
                    predictions_inserted += 1
                    
                    if stat_breakdowns and prediction_id is not None:
                        try:
                            for stat_name, breakdown in stat_breakdowns.items():
                                cur.execute("""
                                    INSERT INTO confidence_components (
                                        prediction_id, player_id, game_id, prediction_date, model_version, stat_name,
                                        ensemble_score, variance_score, feature_score, experience_score,
                                        transaction_score, opponent_adj, injury_adj, playoff_adj,
                                        back_to_back_adj, raw_score, calibrated_score, n_models
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (prediction_id, stat_name) DO UPDATE SET
                                        ensemble_score = EXCLUDED.ensemble_score,
                                        variance_score = EXCLUDED.variance_score,
                                        feature_score = EXCLUDED.feature_score,
                                        experience_score = EXCLUDED.experience_score,
                                        transaction_score = EXCLUDED.transaction_score,
                                        opponent_adj = EXCLUDED.opponent_adj,
                                        injury_adj = EXCLUDED.injury_adj,
                                        playoff_adj = EXCLUDED.playoff_adj,
                                        back_to_back_adj = EXCLUDED.back_to_back_adj,
                                        raw_score = EXCLUDED.raw_score,
                                        calibrated_score = EXCLUDED.calibrated_score,
                                        n_models = EXCLUDED.n_models
                                """, (
                                    prediction_id,
                                    player_id,
                                    game_id,
                                    target_date,
                                    model_version,
                                    stat_name,
                                    breakdown.get('ensemble_score', 0.0),
                                    breakdown.get('variance_score', 0.0),
                                    breakdown.get('feature_score', 0.0),
                                    breakdown.get('experience_score', 0.0),
                                    breakdown.get('transaction_score', 0.0),
                                    breakdown.get('opponent_adj', 0.0),
                                    breakdown.get('injury_adj', 0.0),
                                    breakdown.get('playoff_adj', 0.0),
                                    breakdown.get('back_to_back_adj', 0.0),
                                    breakdown.get('raw_score', 0.0),
                                    breakdown.get('calibrated_score', 0.0),
                                    breakdown.get('n_models', 1)
                                ))
                        except Exception as e:
                            logger.warning(f"Could not save confidence breakdowns for prediction {prediction_id}: {e}")
                    
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
                usage_rates = window_games['usage_rate'].values
                valid_mask = ~pd.isna(usage_rates)
                if valid_mask.sum() > 0:
                    valid_rates = usage_rates[valid_mask]
                    valid_indices = np.where(valid_mask)[0]
                    weights = np.exp(-decay_factor * np.arange(len(valid_indices))[::-1])
                    weights = weights / weights.sum()
                    features[f'usage_rate_l{window}_weighted'] = np.sum(valid_rates * weights)
                else:
                    features[f'usage_rate_l{window}_weighted'] = np.nan
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
    
    if len(recent_games) > 0:
        recent_games['game_date'] = pd.to_datetime(recent_games['game_date'])
        target_dt = pd.to_datetime(target_date)
        days_diff = (target_dt - recent_games['game_date']).dt.days
        features['games_in_last_3_days'] = (days_diff <= 3).sum()
        features['games_in_last_7_days'] = (days_diff <= 7).sum()
    else:
        features['games_in_last_3_days'] = 0
        features['games_in_last_7_days'] = 0
    
    features['is_heavy_schedule'] = 1 if features.get('games_in_last_7_days', 0) >= 4 else 0
    features['is_well_rested'] = 1 if features.get('days_rest', 0) >= 3 else 0
    
    if len(recent_games) > 1:
        recent_games_sorted = recent_games.sort_values('game_date', ascending=False)
        game_dates = pd.to_datetime(recent_games_sorted['game_date'])
        consecutive = 0
        for i in range(len(game_dates) - 1):
            days_diff = (game_dates.iloc[i] - game_dates.iloc[i + 1]).days
            if days_diff <= 2:
                consecutive += 1
            else:
                break
        features['consecutive_games'] = consecutive
    else:
        features['consecutive_games'] = 0
    
    season_start_query = f"""
        SELECT MIN(game_date) as season_start
        FROM games
        WHERE season = '{season}'
        AND game_status = 'completed'
    """
    season_start_df = pd.read_sql(season_start_query, conn)
    if len(season_start_df) > 0 and season_start_df.iloc[0]['season_start']:
        season_start = pd.to_datetime(season_start_df.iloc[0]['season_start'])
        target_dt = pd.to_datetime(target_date)
        days_elapsed = (target_dt - season_start).days
        features['season_progress'] = min(1.0, max(0.0, days_elapsed / 180.0))
    else:
        features['season_progress'] = 0.5
    
    games_played = features.get('games_played_season', 0)
    features['is_early_season'] = 1 if games_played <= 20 else 0
    features['is_mid_season'] = 1 if 20 < games_played <= 60 else 0
    features['is_late_season'] = 1 if games_played > 60 else 0
    
    team_games_query = f"""
        SELECT COUNT(*) as team_games
        FROM games
        WHERE season = '{season}'
        AND game_status = 'completed'
        AND (home_team_id = {team_id} OR away_team_id = {team_id})
        AND game_date < '{target_date}'
    """
    team_games_df = pd.read_sql(team_games_query, conn)
    team_games_played = team_games_df.iloc[0]['team_games'] if len(team_games_df) > 0 else 0
    features['games_remaining'] = max(0, 82 - team_games_played)
    
    teams_tz = pd.read_sql("""
        SELECT team_id, timezone
        FROM teams
        WHERE timezone IS NOT NULL
    """, conn)
    
    tz_to_offset = {
        'America/New_York': -5,
        'America/Chicago': -6,
        'America/Denver': -7,
        'America/Los_Angeles': -8,
        'America/Phoenix': -7,
        'America/Anchorage': -9,
        'Pacific/Honolulu': -10,
        'America/Toronto': -5
    }
    
    teams_tz['tz_offset'] = teams_tz['timezone'].map(tz_to_offset).fillna(-6)
    
    team_tz_row = teams_tz[teams_tz['team_id'] == team_id]
    opp_tz_row = teams_tz[teams_tz['team_id'] == opponent_id]
    
    tz_offset_team = team_tz_row.iloc[0]['tz_offset'] if len(team_tz_row) > 0 else -6
    tz_offset_opp = opp_tz_row.iloc[0]['tz_offset'] if len(opp_tz_row) > 0 else -6
    
    features['tz_difference'] = tz_offset_opp - tz_offset_team
    features['west_to_east'] = 1 if (is_home == 0 and features['tz_difference'] > 0) else 0
    features['east_to_west'] = 1 if (is_home == 0 and features['tz_difference'] < 0) else 0
    
    all_star_breaks = {
        '2020-21': '2021-03-07',
        '2021-22': '2022-02-20',
        '2022-23': '2023-02-19',
        '2023-24': '2024-02-18',
        '2024-25': '2025-02-16',
        '2025-26': '2026-02-15'
    }
    
    asb_date_str = all_star_breaks.get(season)
    if asb_date_str:
        asb_date = pd.to_datetime(asb_date_str, errors='coerce')
        target_dt = pd.to_datetime(target_date)
        if pd.notna(asb_date):
            days_since_asb = (target_dt - asb_date).days
            features['days_since_asb'] = max(-365, min(365, days_since_asb))
            features['post_asb_bounce'] = 1 if (0 < days_since_asb <= 14) else 0
        else:
            features['days_since_asb'] = 0
            features['post_asb_bounce'] = 0
    else:
        features['days_since_asb'] = 0
        features['post_asb_bounce'] = 0
    
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
        if altitude and pd.notna(altitude):
            features['arena_altitude'] = altitude
            features['altitude_away'] = 1 if (is_home == 0 and altitude > 3000) else 0
        else:
            features['arena_altitude'] = None
            features['altitude_away'] = 0
    else:
        features['arena_altitude'] = None
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
        'games_in_last_3_days', 'games_in_last_7_days',
        'is_heavy_schedule', 'is_well_rested', 'consecutive_games',
        'season_progress', 'is_early_season', 'is_mid_season', 'is_late_season', 'games_remaining',
        'tz_difference', 'west_to_east', 'east_to_west',
        'days_since_asb', 'post_asb_bounce',
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

def recalculate_all_confidence_scores(prediction_date):
    if isinstance(prediction_date, str):
        prediction_date = datetime.strptime(prediction_date, '%Y-%m-%d').date()
    elif isinstance(prediction_date, date):
        prediction_date = prediction_date
    
    print("\n" + "="*70)
    print("RECALCULATING CONFIDENCE SCORES WITH ALL MODELS")
    print("="*70)
    print(f"Target date: {prediction_date}\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(script_dir))
        
        date_str = prediction_date.strftime('%Y-%m-%d')
        
        query = f"""
            SELECT 
                p.player_id,
                p.game_id,
                p.model_version,
                p.predicted_points,
                p.predicted_rebounds,
                p.predicted_assists,
                p.predicted_steals,
                p.predicted_blocks,
                p.predicted_turnovers,
                p.predicted_three_pointers_made,
                p.prediction_id,
                g.season,
                g.home_team_id,
                g.away_team_id,
                g.game_type
            FROM predictions p
            JOIN games g ON p.game_id = g.game_id
            WHERE DATE(p.prediction_date) = '{date_str}'
                AND p.model_version IN ('xgboost', 'lightgbm', 'catboost', 'random_forest')
            ORDER BY p.player_id, p.game_id, p.model_version
        """
        
        all_predictions_df = pd.read_sql(query, conn)
        
        if len(all_predictions_df) == 0:
            print("No predictions found for this date.")
            return
        
        print(f"Found {len(all_predictions_df)} predictions from {all_predictions_df['model_version'].nunique()} models")
        
        grouped = all_predictions_df.groupby(['player_id', 'game_id'])
        total_groups = len(grouped)
        print(f"Processing {total_groups} unique player/game combinations...\n")
        
        updated_count = 0
        error_count = 0
        skipped_insufficient_models = 0
        skipped_no_team = 0
        skipped_no_features = 0
        
        for (player_id, game_id), group in grouped:
            try:
                conn, cur = ensure_connection(conn, cur)
                
                player_id = int(player_id)
                game_id = int(game_id)
                
                if len(group) < 2:
                    skipped_insufficient_models += 1
                    continue
                
                predictions_by_model = {}
                stat_mapping = {
                    'predicted_points': 'points',
                    'predicted_rebounds': 'rebounds',
                    'predicted_assists': 'assists',
                    'predicted_steals': 'steals',
                    'predicted_blocks': 'blocks',
                    'predicted_turnovers': 'turnovers',
                    'predicted_three_pointers_made': 'three_pointers_made'
                }
                
                selected_models = []
                for _, row in group.iterrows():
                    model_version = row['model_version']
                    if model_version not in selected_models:
                        selected_models.append(model_version)
                    
                    for db_col, stat_name in stat_mapping.items():
                        if stat_name not in predictions_by_model:
                            predictions_by_model[stat_name] = {}
                        if pd.notna(row[db_col]):
                            predictions_by_model[stat_name][model_version] = float(row[db_col])
                
                if len(selected_models) < 2:
                    skipped_insufficient_models += 1
                    continue
                
                first_row = group.iloc[0]
                season = first_row['season']
                home_team_id = first_row['home_team_id']
                away_team_id = first_row['away_team_id']
                game_type = first_row['game_type']
                
                team_id_query = f"""
                    SELECT team_id
                    FROM player_game_stats
                    WHERE player_id = {player_id}
                        AND game_id IN (
                            SELECT game_id FROM games
                            WHERE season = '{season}'
                            AND game_date < '{date_str}'
                            AND (home_team_id = {home_team_id} OR away_team_id = {away_team_id})
                            ORDER BY game_date DESC
                            LIMIT 1
                        )
                    LIMIT 1
                """
                team_result = pd.read_sql(team_id_query, conn)
                if len(team_result) == 0:
                    fallback_query = f"""
                        SELECT team_id
                        FROM players
                        WHERE player_id = {player_id}
                        AND is_active = TRUE
                        LIMIT 1
                    """
                    fallback_result = pd.read_sql(fallback_query, conn)
                    if len(fallback_result) == 0:
                        skipped_no_team += 1
                        continue
                    team_id_val = fallback_result.iloc[0]['team_id']
                    if pd.isna(team_id_val) or team_id_val is None:
                        skipped_no_team += 1
                        continue
                    team_id = int(team_id_val)
                else:
                    team_id_val = team_result.iloc[0]['team_id']
                    if pd.isna(team_id_val) or team_id_val is None:
                        skipped_no_team += 1
                        continue
                    team_id = int(team_id_val)
                opponent_id = int(away_team_id) if team_id == int(home_team_id) else int(home_team_id)
                is_home = 1 if team_id == int(home_team_id) else 0
                
                features_df, recent_games = build_features_for_player(
                    conn, player_id, team_id, opponent_id,
                    is_home, season, prediction_date, game_type
                )
                
                if features_df is None or len(features_df) == 0:
                    skipped_no_features += 1
                    continue
                
                opponent_def_rating = 114.0
                if 'defensive_rating_opp' in features_df.columns:
                    opp_dr = features_df['defensive_rating_opp'].iloc[0]
                    if pd.notna(opp_dr):
                        opponent_def_rating = float(opp_dr)
                
                player_name_query = f"SELECT full_name FROM players WHERE player_id = {player_id}"
                player_name_result = pd.read_sql(player_name_query, conn)
                player_name = player_name_result.iloc[0]['full_name'] if len(player_name_result) > 0 else None
                
                confidence_score, stat_breakdowns = calculate_confidence_new(
                    predictions_by_model=predictions_by_model,
                    selected_models=selected_models,
                    features_df=features_df,
                    recent_games=recent_games,
                    conn=conn,
                    player_id=player_id,
                    game_id=game_id,
                    target_date=prediction_date,
                    season=season,
                    opponent_def_rating=opponent_def_rating,
                    project_root=project_root,
                    player_name=player_name
                )
                
                for _, row in group.iterrows():
                    prediction_id = row['prediction_id']
                    
                    cur.execute("""
                        UPDATE predictions
                        SET confidence_score = %s
                        WHERE prediction_id = %s
                    """, (float(confidence_score), int(prediction_id)))
                    
                    cur.execute("""
                        DELETE FROM confidence_components
                        WHERE prediction_id = %s
                    """, (int(prediction_id),))
                    
                    if stat_breakdowns:
                        for stat_name, breakdown in stat_breakdowns.items():
                            cur.execute("""
                                INSERT INTO confidence_components (
                                    prediction_id, player_id, game_id, prediction_date, model_version, stat_name,
                                    ensemble_score, variance_score, feature_score, experience_score,
                                    transaction_score, opponent_adj, injury_adj, playoff_adj,
                                    back_to_back_adj, raw_score, calibrated_score, n_models
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                int(prediction_id),
                                int(player_id),
                                int(game_id),
                                prediction_date,
                                row['model_version'],
                                stat_name,
                                float(breakdown.get('ensemble_score', 0)),
                                float(breakdown.get('variance_score', 0)),
                                float(breakdown.get('feature_score', 0)),
                                float(breakdown.get('experience_score', 0)),
                                float(breakdown.get('transaction_score', 0)),
                                float(breakdown.get('opponent_adj', 0)),
                                float(breakdown.get('injury_adj', 0)),
                                float(breakdown.get('playoff_adj', 0)),
                                float(breakdown.get('back_to_back_adj', 0)),
                                float(breakdown.get('raw_score', 0)),
                                float(breakdown.get('calibrated_score', 0)),
                                int(breakdown.get('n_models', len(selected_models)))
                            ))
                
                conn.commit()
                updated_count += 1
                
                if updated_count % 50 == 0:
                    print(f"  Updated {updated_count}/{total_groups} player/game combinations...")
                    
            except Exception as e:
                error_count += 1
                logger.warning(f"Error recalculating confidence for player {player_id}, game {game_id}: {e}")
                conn.rollback()
                continue
        
        if updated_count > 0 and updated_count % 50 != 0:
            print(f"  Updated {updated_count}/{total_groups} player/game combinations...")
        
        print(f"\n{'='*70}")
        print(f"RECALCULATION COMPLETE")
        print(f"{'='*70}")
        print(f"Successfully updated: {updated_count}")
        print(f"Errors: {error_count}")
        print(f"Skipped - insufficient models (<2): {skipped_insufficient_models}")
        print(f"Skipped - no team found: {skipped_no_team}")
        print(f"Skipped - no features: {skipped_no_features}")
        print(f"Total processed: {total_groups}")
        
    except Exception as e:
        logger.error(f"Error in recalculate_all_confidence_scores: {e}")
        raise
    finally:
        cur.close()
        conn.close()


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
    
    print("\n" + "="*70)
    print("All models completed. Recalculating confidence scores with ensemble data...")
    print("="*70)
    
    try:
        recalculate_all_confidence_scores(target_date)
    except Exception as e:
        logger.error(f"Error recalculating confidence scores: {e}")
        print(f"Warning: Confidence recalculation failed: {e}")
        print("Predictions stored but confidence scores may not include ensemble agreement.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        if len(sys.argv) > 2 and sys.argv[2] == '--recalculate-only':
            if len(sys.argv) > 3 and sys.argv[3] == '--diagnostic':
                enable_variance_diagnostic()
                reset_variance_diagnostic()
            recalculate_all_confidence_scores(target_date)
        elif len(sys.argv) > 2 and sys.argv[2] == '--all':
            if len(sys.argv) > 3 and sys.argv[3] == '--diagnostic':
                enable_variance_diagnostic()
                reset_variance_diagnostic()
            predict_all_models(target_date)
        else:
            model_type = sys.argv[2] if len(sys.argv) > 2 else 'xgboost'
            predict_upcoming_games(target_date, model_type)
    else:
        predict_all_models()