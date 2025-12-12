import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Set
from pathlib import Path

def load_feature_importances(project_root: str) -> Dict[str, float]:
    models_dir = os.path.join(project_root, 'data', 'models')
    
    model_types = ['xgboost', 'lightgbm', 'catboost', 'random_forest']
    stats = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']
    
    feature_importances = {}
    total_importance = 0.0
    
    for model_type in model_types:
        for stat in stats:
            importance_file = os.path.join(models_dir, f'feature_importance_{model_type}_{stat}.csv')
            
            if os.path.exists(importance_file):
                try:
                    df = pd.read_csv(importance_file)
                    for _, row in df.iterrows():
                        feature = row['feature']
                        importance = float(row['importance'])
                        
                        if feature not in feature_importances:
                            feature_importances[feature] = 0.0
                        feature_importances[feature] += importance
                        total_importance += importance
                except Exception as e:
                    print(f"Warning: Could not load {importance_file}: {e}")
                    continue
    
    if total_importance > 0:
        feature_importances = {k: v / total_importance for k, v in feature_importances.items()}
    
    return feature_importances


def get_feature_groups() -> Dict[str, List[str]]:
    return {
        'rolling_windows': [
            'points_l5', 'points_l10', 'points_l20',
            'rebounds_total_l5', 'rebounds_total_l10', 'rebounds_total_l20',
            'assists_l5', 'assists_l10', 'assists_l20',
            'points_l5_weighted', 'points_l10_weighted', 'points_l20_weighted',
            'rebounds_total_l5_weighted', 'rebounds_total_l10_weighted', 'rebounds_total_l20_weighted',
            'assists_l5_weighted', 'assists_l10_weighted', 'assists_l20_weighted',
            'usage_rate_l5', 'usage_rate_l10', 'usage_rate_l20',
            'usage_rate_l5_weighted', 'usage_rate_l10_weighted', 'usage_rate_l20_weighted'
        ],
        'player_status': [
            'minutes_played_l5', 'minutes_played_l10', 'minutes_played_l20',
            'minutes_played_l5_weighted', 'minutes_played_l10_weighted', 'minutes_played_l20_weighted',
            'minutes_trend', 'is_starter_l5', 'is_starter_l10',
            'games_played_season'
        ],
        'team_context': [
            'offensive_rating_team', 'defensive_rating_team', 'pace_team'
        ],
        'opponent': [
            'offensive_rating_opp', 'defensive_rating_opp', 'pace_opp',
            'opp_field_goal_pct', 'opp_three_point_pct',
            'opp_team_turnovers_per_game', 'opp_team_steals_per_game'
        ],
        'game_context': [
            'is_home', 'days_rest', 'is_back_to_back',
            'games_in_last_3_days', 'games_in_last_7_days',
            'is_heavy_schedule', 'is_well_rested', 'consecutive_games'
        ]
    }


def collect_player_stats_for_variance(
    recent_games: pd.DataFrame,
    conn,
    player_id: int,
    target_date
) -> Dict[str, Dict[str, float]]:
    stat_mapping = {
        'rebounds_total': 'rebounds'
    }
    
    stats = ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 
             'turnovers', 'three_pointers_made']
    
    player_stats = {}
    
    if recent_games is not None and len(recent_games) >= 5:
        for stat in stats:
            normalized_stat = stat_mapping.get(stat, stat)
            
            if stat in recent_games.columns:
                stat_data = recent_games[stat].dropna()
                if len(stat_data) > 0:
                    player_stats[normalized_stat] = {
                        'mean': float(stat_data.mean()),
                        'std': float(stat_data.std() if len(stat_data) > 1 else 0.0)
                    }
                else:
                    player_stats[normalized_stat] = {'mean': 0.0, 'std': 0.0}
            else:
                player_stats[normalized_stat] = {'mean': 0.0, 'std': 0.0}
    else:
        try:
            career_query = f"""
                SELECT 
                    points, rebounds_total, assists, steals, blocks, 
                    turnovers, three_pointers_made
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = {player_id}
                AND g.game_status = 'completed'
                AND g.game_date < '{target_date}'
                ORDER BY g.game_date DESC
                LIMIT 100
            """
            career_games = pd.read_sql(career_query, conn)
            
            if len(career_games) >= 5:
                for stat in stats:
                    normalized_stat = stat_mapping.get(stat, stat)
                    
                    if stat in career_games.columns:
                        stat_data = career_games[stat].dropna()
                        if len(stat_data) > 0:
                            player_stats[normalized_stat] = {
                                'mean': float(stat_data.mean()),
                                'std': float(stat_data.std() if len(stat_data) > 1 else 0.0)
                            }
                        else:
                            player_stats[normalized_stat] = {'mean': 0.0, 'std': 0.0}
                    else:
                        player_stats[normalized_stat] = {'mean': 0.0, 'std': 0.0}
            else:
                for stat in stats:
                    normalized_stat = stat_mapping.get(stat, stat)
                    player_stats[normalized_stat] = {'mean': 0.0, 'std': 0.0}
        except:
            for stat in stats:
                normalized_stat = stat_mapping.get(stat, stat)
                player_stats[normalized_stat] = {'mean': 0.0, 'std': 0.0}
    
    return player_stats


def get_available_features(features_df: pd.DataFrame) -> Set[str]:
    if isinstance(features_df, pd.DataFrame):
        if len(features_df) == 0:
            return set()
        row = features_df.iloc[0]
        available = set()
        for col in features_df.columns:
            if col not in ['player_id', 'game_id', 'team_id']:
                if not pd.isna(row[col]):
                    available.add(col)
        return available
    elif isinstance(features_df, dict):
        available = set()
        for key, value in features_df.items():
            if key not in ['player_id', 'game_id', 'team_id']:
                if value is not None and not (isinstance(value, float) and np.isnan(value)):
                    available.add(key)
        return available
    else:
        return set()




