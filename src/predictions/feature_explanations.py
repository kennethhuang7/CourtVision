import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from pathlib import Path
import json

pd.options.mode.chained_assignment = None

def load_feature_importance(model_type, stat_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    importance_path = os.path.join(project_root, 'data', 'models', f'feature_importance_{model_type}_{stat_name}.csv')
    
    if not os.path.exists(importance_path):
        return None
    
    try:
        df = pd.read_csv(importance_path)
        if 'feature' in df.columns and 'importance' in df.columns:
            return dict(zip(df['feature'], df['importance']))
        return None
    except:
        return None

def get_feature_description(feature_name):
    descriptions = {
        'points_l5': 'Recent scoring (last 5 games)',
        'points_l10': 'Recent scoring (last 10 games)',
        'points_l20': 'Recent scoring (last 20 games)',
        'points_l5_weighted': 'Recent scoring trend (last 5 games, weighted)',
        'points_l10_weighted': 'Recent scoring trend (last 10 games, weighted)',
        'points_l20_weighted': 'Recent scoring trend (last 20 games, weighted)',
        'rebounds_total_l5': 'Recent rebounding (last 5 games)',
        'rebounds_total_l10': 'Recent rebounding (last 10 games)',
        'rebounds_total_l20': 'Recent rebounding (last 20 games)',
        'rebounds_total_l5_weighted': 'Recent rebounding trend (last 5 games, weighted)',
        'rebounds_total_l10_weighted': 'Recent rebounding trend (last 10 games, weighted)',
        'rebounds_total_l20_weighted': 'Recent rebounding trend (last 20 games, weighted)',
        'assists_l5': 'Recent assists (last 5 games)',
        'assists_l10': 'Recent assists (last 10 games)',
        'assists_l20': 'Recent assists (last 20 games)',
        'assists_l5_weighted': 'Recent assists trend (last 5 games, weighted)',
        'assists_l10_weighted': 'Recent assists trend (last 10 games, weighted)',
        'assists_l20_weighted': 'Recent assists trend (last 20 games, weighted)',
        'steals_l5': 'Recent steals (last 5 games)',
        'steals_l10': 'Recent steals (last 10 games)',
        'steals_l20': 'Recent steals (last 20 games)',
        'blocks_l5': 'Recent blocks (last 5 games)',
        'blocks_l10': 'Recent blocks (last 10 games)',
        'blocks_l20': 'Recent blocks (last 20 games)',
        'turnovers_l5': 'Recent turnovers (last 5 games)',
        'turnovers_l10': 'Recent turnovers (last 10 games)',
        'turnovers_l20': 'Recent turnovers (last 20 games)',
        'three_pointers_made_l5': 'Recent three-pointers (last 5 games)',
        'three_pointers_made_l10': 'Recent three-pointers (last 10 games)',
        'three_pointers_made_l20': 'Recent three-pointers (last 20 games)',
        'minutes_played_l5': 'Recent minutes (last 5 games)',
        'minutes_played_l10': 'Recent minutes (last 10 games)',
        'minutes_played_l20': 'Recent minutes (last 20 games)',
        'minutes_trend': 'Minutes trend (increasing/decreasing)',
        'usage_rate_l5': 'Recent usage rate (last 5 games)',
        'usage_rate_l10': 'Recent usage rate (last 10 games)',
        'usage_rate_l20': 'Recent usage rate (last 20 games)',
        'offensive_rating_l5': 'Recent offensive rating (last 5 games)',
        'offensive_rating_l10': 'Recent offensive rating (last 10 games)',
        'offensive_rating_l20': 'Recent offensive rating (last 20 games)',
        'defensive_rating_l5': 'Recent defensive rating (last 5 games)',
        'defensive_rating_l10': 'Recent defensive rating (last 10 games)',
        'defensive_rating_l20': 'Recent defensive rating (last 20 games)',
        'net_rating_l5': 'Recent net rating (last 5 games)',
        'net_rating_l10': 'Recent net rating (last 10 games)',
        'net_rating_l20': 'Recent net rating (last 20 games)',
        'field_goal_pct_l5': 'Recent field goal percentage (last 5 games)',
        'field_goal_pct_l10': 'Recent field goal percentage (last 10 games)',
        'field_goal_pct_l20': 'Recent field goal percentage (last 20 games)',
        'three_point_pct_l5': 'Recent three-point percentage (last 5 games)',
        'three_point_pct_l10': 'Recent three-point percentage (last 10 games)',
        'three_point_pct_l20': 'Recent three-point percentage (last 20 games)',
        'free_throw_pct_l5': 'Recent free throw percentage (last 5 games)',
        'free_throw_pct_l10': 'Recent free throw percentage (last 10 games)',
        'free_throw_pct_l20': 'Recent free throw percentage (last 20 games)',
        'true_shooting_pct_l5': 'Recent true shooting percentage (last 5 games)',
        'true_shooting_pct_l10': 'Recent true shooting percentage (last 10 games)',
        'true_shooting_pct_l20': 'Recent true shooting percentage (last 20 games)',
        'points_per_36_l5': 'Recent scoring rate per 36 minutes (last 5 games)',
        'points_per_36_l10': 'Recent scoring rate per 36 minutes (last 10 games)',
        'points_per_36_l20': 'Recent scoring rate per 36 minutes (last 20 games)',
        'rebounds_per_36_l5': 'Recent rebounding rate per 36 minutes (last 5 games)',
        'rebounds_per_36_l10': 'Recent rebounding rate per 36 minutes (last 10 games)',
        'rebounds_per_36_l20': 'Recent rebounding rate per 36 minutes (last 20 games)',
        'assists_per_36_l5': 'Recent assists rate per 36 minutes (last 5 games)',
        'assists_per_36_l10': 'Recent assists rate per 36 minutes (last 10 games)',
        'assists_per_36_l20': 'Recent assists rate per 36 minutes (last 20 games)',
        'assist_to_turnover_ratio_l5': 'Recent assist-to-turnover ratio (last 5 games)',
        'assist_to_turnover_ratio_l10': 'Recent assist-to-turnover ratio (last 10 games)',
        'assist_to_turnover_ratio_l20': 'Recent assist-to-turnover ratio (last 20 games)',
        'points_per_assist_l5': 'Recent points per assist (last 5 games)',
        'points_per_assist_l10': 'Recent points per assist (last 10 games)',
        'points_per_assist_l20': 'Recent points per assist (last 20 games)',
        'rebound_rate_l5': 'Recent rebound rate (last 5 games)',
        'rebound_rate_l10': 'Recent rebound rate (last 10 games)',
        'rebound_rate_l20': 'Recent rebound rate (last 20 games)',
        'is_home': 'Home game',
        'is_playoff': 'Playoff game',
        'days_rest': 'Days of rest',
        'is_back_to_back': 'Back-to-back game',
        'games_played_season': 'Games played this season',
        'consecutive_games': 'Consecutive games played',
        'games_in_last_3_days': 'Games in last 3 days',
        'games_in_last_7_days': 'Games in last 7 days',
        'is_heavy_schedule': 'Heavy schedule (4+ games in 7 days)',
        'is_well_rested': 'Well rested (3+ days rest)',
        'season_progress': 'Season progress (0.0 to 1.0)',
        'is_early_season': 'Early season (first 20 games)',
        'is_mid_season': 'Mid season (games 21-60)',
        'is_late_season': 'Late season (games 61+)',
        'games_remaining': 'Games remaining in season',
        'tz_difference': 'Timezone difference from home',
        'west_to_east': 'Traveling west to east',
        'east_to_west': 'Traveling east to west',
        'days_since_asb': 'Days since All-Star break',
        'post_asb_bounce': 'Post All-Star break bounce (within 14 days)',
        'offensive_rating_team': 'Team offensive rating',
        'defensive_rating_team': 'Team defensive rating',
        'pace_team': 'Team pace',
        'offensive_rating_opp': 'Opponent offensive rating',
        'defensive_rating_opp': 'Opponent defensive rating',
        'pace_opp': 'Opponent pace',
        'opp_field_goal_pct': 'Opponent field goal percentage allowed',
        'opp_three_point_pct': 'Opponent three-point percentage allowed',
        'opp_team_turnovers_per_game': 'Opponent team turnovers per game',
        'opp_team_steals_per_game': 'Opponent team steals per game',
        'opp_points_allowed_to_position': 'Opponent points allowed to position',
        'opp_rebounds_allowed_to_position': 'Opponent rebounds allowed to position',
        'opp_assists_allowed_to_position': 'Opponent assists allowed to position',
        'opp_blocks_allowed_to_position': 'Opponent blocks allowed to position',
        'opp_three_pointers_allowed_to_position': 'Opponent three-pointers allowed to position',
        'opp_position_turnovers_vs_team': 'Opponent position turnovers vs this team',
        'opp_position_steals_vs_team': 'Opponent position steals vs this team',
        'opp_position_turnovers_overall': 'Opponent position turnovers (season average)',
        'opp_position_steals_overall': 'Opponent position steals (season average)',
        'star_teammate_out': 'Star teammate out',
        'star_teammate_ppg': 'Star teammate points per game',
        'games_without_star': 'Games without star teammate',
        'playoff_games_career': 'Career playoff games',
        'playoff_performance_boost': 'Playoff performance boost',
        'position_guard': 'Position: Guard',
        'position_forward': 'Position: Forward',
        'position_center': 'Position: Center',
        'arena_altitude': 'Arena altitude',
        'altitude_away': 'Away game altitude effect',
    }
    
    if feature_name in descriptions:
        return descriptions[feature_name]
    
    if '_l5' in feature_name:
        base = feature_name.replace('_l5', '').replace('_', ' ').title()
        return f'Recent {base} (last 5 games)'
    if '_l10' in feature_name:
        base = feature_name.replace('_l10', '').replace('_', ' ').title()
        return f'Recent {base} (last 10 games)'
    if '_l20' in feature_name:
        base = feature_name.replace('_l20', '').replace('_', ' ').title()
        return f'Recent {base} (last 20 games)'
    
    return feature_name.replace('_', ' ').title()

def calculate_impact_tier(importance, deviation_std, importance_rank, total_features):
    if importance is None or importance == 0:
        return 'neutral', '='
    
    importance_weight = (total_features - importance_rank + 1) / total_features
    
    if abs(deviation_std) >= 2.0:
        magnitude = 3
    elif abs(deviation_std) >= 1.0:
        magnitude = 2
    elif abs(deviation_std) >= 0.5:
        magnitude = 1
    else:
        magnitude = 0
    
    if deviation_std > 0:
        if magnitude == 3:
            return 'strong_positive', '+++'
        elif magnitude == 2:
            return 'moderate_positive', '++'
        elif magnitude == 1:
            return 'slight_positive', '+'
        else:
            return 'neutral', '='
    elif deviation_std < 0:
        if magnitude == 3:
            return 'strong_negative', '---'
        elif magnitude == 2:
            return 'moderate_negative', '--'
        elif magnitude == 1:
            return 'slight_negative', '-'
        else:
            return 'neutral', '='
    else:
        return 'neutral', '='

def get_top_features_with_impact(
    features_dict, 
    model_type, 
    stat_name, 
    league_means, 
    top_n=10
):
    importance_dict = load_feature_importance(model_type, stat_name)
    
    if importance_dict is None or len(importance_dict) == 0:
        return []
    
    feature_values = {}
    feature_importances = {}
    feature_league_means = {}
    feature_stds = {}
    
    for feat_name, importance in importance_dict.items():
        if feat_name in features_dict:
            feat_value = features_dict[feat_name]
            if isinstance(feat_value, (list, np.ndarray)):
                feat_value = feat_value[0] if len(feat_value) > 0 else 0
            if pd.isna(feat_value):
                feat_value = 0
            feature_values[feat_name] = feat_value
            feature_importances[feat_name] = importance
            feature_league_means[feat_name] = league_means.get(feat_name, 0)
            
            if feat_name in league_means:
                league_mean = feature_league_means[feat_name]
                
                if feat_name.startswith('is_') or feat_name.startswith('position_'):
                    std_estimate = 0.5
                elif '_pct' in feat_name or 'pct' in feat_name or feat_name.endswith('_ratio'):
                    std_estimate = 0.08
                elif 'per_36' in feat_name or 'per_' in feat_name:
                    std_estimate = abs(league_mean) * 0.4
                elif feat_name in ['points_l5', 'points_l10', 'points_l20', 'points_l5_weighted', 'points_l10_weighted', 'points_l20_weighted',
                                   'rebounds_total_l5', 'rebounds_total_l10', 'rebounds_total_l20', 'rebounds_total_l5_weighted', 'rebounds_total_l10_weighted', 'rebounds_total_l20_weighted',
                                   'assists_l5', 'assists_l10', 'assists_l20', 'assists_l5_weighted', 'assists_l10_weighted', 'assists_l20_weighted',
                                   'steals_l5', 'steals_l10', 'steals_l20', 'blocks_l5', 'blocks_l10', 'blocks_l20',
                                   'turnovers_l5', 'turnovers_l10', 'turnovers_l20', 'three_pointers_made_l5', 'three_pointers_made_l10', 'three_pointers_made_l20']:
                    std_estimate = abs(league_mean) * 0.4
                elif 'opp_' in feat_name or 'opponent' in feat_name.lower():
                    std_estimate = abs(league_mean) * 0.25
                else:
                    std_estimate = abs(league_mean) * 0.3
                
                if std_estimate == 0:
                    std_estimate = 1.0
                feature_stds[feat_name] = std_estimate
            else:
                feature_stds[feat_name] = 1.0
    
    if len(feature_importances) == 0:
        return []
    
    sorted_features = sorted(
        feature_importances.items(), 
        key=lambda x: x[1], 
        reverse=True
    )
    
    top_features = []
    total_features = len(sorted_features)
    
    rank = 0
    for feat_name, importance in sorted_features:
        if feat_name not in feature_values:
            continue
        
        rank += 1
        feat_value = feature_values[feat_name]
        league_mean = feature_league_means.get(feat_name, 0)
        feat_std = feature_stds.get(feat_name, 1.0)
        
        if feat_std == 0:
            deviation_std = 0
        else:
            deviation_std = (feat_value - league_mean) / feat_std
        
        tier_type, tier_symbol = calculate_impact_tier(
            importance, 
            deviation_std, 
            rank, 
            total_features
        )
        
        description = get_feature_description(feat_name)
        
        context = ""
        if tier_type == 'strong_positive':
            context = " - well above average"
        elif tier_type == 'moderate_positive':
            context = " - above average"
        elif tier_type == 'slight_positive':
            context = " - slightly above average"
        elif tier_type == 'strong_negative':
            context = " - well below average"
        elif tier_type == 'moderate_negative':
            context = " - below average"
        elif tier_type == 'slight_negative':
            context = " - slightly below average"
        else:
            context = " - near average"
        
        if 'opp_' in feat_name or 'opponent' in description.lower():
            if tier_type.startswith('strong_positive') or tier_type.startswith('moderate_positive'):
                context = " - favorable matchup"
            elif tier_type.startswith('strong_negative') or tier_type.startswith('moderate_negative'):
                context = " - difficult matchup"
        
        if feat_name == 'is_home':
            if feat_value > 0:
                context = " - home court advantage"
            else:
                context = " - away game"
        
        if feat_name == 'is_back_to_back':
            if feat_value > 0:
                context = " - fatigue factor"
            else:
                context = " - well rested"
        
        if feat_name == 'is_well_rested':
            if feat_value > 0:
                context = " - extra rest"
            else:
                context = " - normal rest"
        
        top_features.append({
            'feature_name': feat_name,
            'description': description,
            'value': float(feat_value) if not (isinstance(feat_value, float) and np.isnan(feat_value)) else 0.0,
            'importance_rank': rank,
            'impact_tier': tier_type,
            'impact_symbol': tier_symbol,
            'context': context
        })
        
        if len(top_features) >= top_n:
            break
    
    return top_features

