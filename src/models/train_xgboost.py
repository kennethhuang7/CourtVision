import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import joblib
from collections import defaultdict

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)
import json
from pathlib import Path
from selective_tuning_config import should_use_tuned_params

def load_tuned_params(model_type, target_name, use_selective=True):
    if not should_use_tuned_params(model_type, target_name, use_selective):
        return None
    
    params_path = Path(f'data/models/best_params/{model_type}_{target_name}.json')
    if params_path.exists():
        with open(params_path, 'r') as f:
            tuned_params = json.load(f)
        return tuned_params
    return None

def train_xgboost_models(use_tuned_params=False, use_selective=True):
    print("Training XGBoost models for NBA player predictions...\n")
    
    print("Loading features...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    features_path = os.path.join(project_root, 'data', 'processed', 'training_features.csv')
    df = pd.read_csv(features_path)
    print(f"Loaded {len(df)} records\n")
    
    df = df.dropna(subset=['points_l5', 'points_l10'])
    print(f"After removing NaN: {len(df)} records\n")
    
    feature_cols = [col for col in df.columns if any(x in col for x in 
               ['_l5', '_l10', '_l20', '_weighted', 'is_', 'days_rest', 'games_played',
                'offensive_rating', 'defensive_rating', 'net_rating', 'pace', 'opp_', 'altitude', 'playoff',
                'star_teammate', 'games_without_star', 'usage_rate', 'minutes_played', 'minutes_trend',
                'per_36', '_pct', '_ratio', 'pts_per', 'ast_to', 'reb_rate', 'position_',
                'games_in_last', 'is_heavy', 'is_well', 'consecutive_games', 'season_progress',
                'is_early', 'is_mid', 'is_late', 'games_remaining', 'tz_difference', 'west_to_east',
                'east_to_west', 'days_since_asb', 'post_asb'])]
    
    feature_cols = [col for col in feature_cols if 'team_id' not in col and 'player_id' not in col and 'game_id' not in col]
    
    raw_leakage_cols = ['offensive_rating', 'defensive_rating', 'usage_rate', 'true_shooting_pct', 
                        'points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 
                        'three_pointers_made', 'minutes_played', 'is_starter', 'field_goals_made',
                        'field_goals_attempted', 'three_pointers_attempted', 'free_throws_made',
                        'free_throws_attempted']
    feature_cols = [col for col in feature_cols if col not in raw_leakage_cols]
    
    print("Calculating imputation values for NaN handling...")
    league_means = {}
    for col in feature_cols:
        if col in df.columns:
            if 'team' in col or 'opp' in col or 'pace' in col:
                league_means[col] = df[col].mean()
            elif col.startswith('is_') or col.startswith('position_'):
                league_means[col] = 0
            elif 'trend' in col or col in ['west_to_east', 'east_to_west', 'post_asb_bounce']:
                league_means[col] = 0
            else:
                league_means[col] = df[col].mean()
    
    X = df[feature_cols].copy()
    player_means = {}
    for col in feature_cols:
        if col in X.columns:
            if 'team' in col or 'opp' in col or 'pace' in col:
                X[col] = X[col].fillna(league_means.get(col, 0))
            elif col.startswith('is_') or col.startswith('position_') or 'trend' in col or col in ['west_to_east', 'east_to_west', 'post_asb_bounce']:
                X[col] = X[col].fillna(league_means.get(col, 0))
            else:
                if col not in player_means:
                    player_means[col] = df.groupby('player_id')[col].transform(
                        lambda x: x.expanding().mean().shift(1)
                    ).fillna(league_means.get(col, 0))
                X[col] = X[col].fillna(player_means[col])
    
    X = X.fillna(0)
    
    targets = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    models_dir = os.path.join(project_root, 'data', 'models')
    os.makedirs(models_dir, exist_ok=True)
    
    results = {}
    
    for target_name, target_col in targets.items():
        print("="*50)
        print(f"TRAINING: {target_name.upper()} PREDICTION")
        print("="*50)
        
        y = df[target_col]
        
        print("Fitting StandardScaler...")
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        X_scaled = pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
        
        seasons = df['season'].values
        unique_seasons = sorted(df['season'].unique())
        n_seasons = len(unique_seasons)
        
        if n_seasons > 2:
            split_indices = []
            for i, test_season in enumerate(unique_seasons[2:], start=2):
                train_seasons = unique_seasons[:i]
                train_mask = df['season'].isin(train_seasons)
                test_mask = df['season'] == test_season
                train_idx = np.where(train_mask)[0]
                val_idx = np.where(test_mask)[0]
                if len(train_idx) > 0 and len(val_idx) > 0:
                    split_indices.append((train_idx, val_idx))
        else:
            tscv = TimeSeriesSplit(n_splits=3)
            split_indices = list(tscv.split(X_scaled))
        
        best_model = None
        best_score = float('inf')
        feature_importance_scores = defaultdict(float)
        fold_maes = []
        
        tuned_params = None
        if use_tuned_params:
            tuned_params = load_tuned_params('xgboost', target_name, use_selective)
            if tuned_params:
                print(f"  Using TUNED params for xgboost-{target_name}")
            else:
                print(f"  Using DEFAULT params for xgboost-{target_name}")
        
        for fold, (train_idx, val_idx) in enumerate(split_indices, 1):
            X_train, X_val = X_scaled.iloc[train_idx], X_scaled.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
            
            objective = 'count:poisson' if target_name in ['blocks', 'steals'] else 'reg:squarederror'
            
            if tuned_params:
                params = tuned_params.copy()
                params['random_state'] = 42
                params['n_jobs'] = -1
                params['objective'] = objective
                model = xgb.XGBRegressor(**params)
            else:
                model = xgb.XGBRegressor(
                    n_estimators=100,
                    max_depth=6,
                    learning_rate=0.1,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    random_state=42,
                    n_jobs=-1,
                    objective=objective
                )
            
            model.fit(X_train, y_train, verbose=False)
            
            y_pred = model.predict(X_val)
            
            mae = mean_absolute_error(y_val, y_pred)
            rmse = np.sqrt(mean_squared_error(y_val, y_pred))
            
            print(f"Fold {fold}: MAE={mae:.2f}, RMSE={rmse:.2f}")
            
            fold_maes.append(mae)
            importance = model.feature_importances_
            for idx, feat_name in enumerate(X.columns):
                feature_importance_scores[feat_name] += importance[idx]
            
            if mae < best_score:
                best_score = mae
                best_model = model
        
        avg_mae = np.mean(fold_maes)
        print(f"\nAverage MAE: {avg_mae:.2f} (Best: {best_score:.2f})\n")
        results[target_name] = avg_mae
        
        avg_importance = {k: v / len(split_indices) for k, v in feature_importance_scores.items()}
        sorted_features = sorted(avg_importance.items(), key=lambda x: x[1], reverse=True)
        top_features = [f[0] for f in sorted_features[:20]]
        print(f"Top 20 features: {', '.join(top_features[:5])}...")
        
        print("Training final model on all data...")
        objective = 'count:poisson' if target_name in ['blocks', 'steals'] else 'reg:squarederror'
        
        tuned_params = None
        if use_tuned_params:
            tuned_params = load_tuned_params('xgboost', target_name, use_selective)
            if tuned_params:
                print(f"  Using TUNED params for final xgboost-{target_name} model")
            else:
                print(f"  Using DEFAULT params for final xgboost-{target_name} model")
        
        if tuned_params:
            params = tuned_params.copy()
            params['random_state'] = 42
            params['n_jobs'] = -1
            params['objective'] = objective
            final_model = xgb.XGBRegressor(**params)
        else:
            final_model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1,
                objective=objective
            )
        
        final_model.fit(X_scaled, y, verbose=False)
        
        final_importance = final_model.feature_importances_
        importance_df = pd.DataFrame({
            'feature': X.columns,
            'importance': final_importance
        }).sort_values('importance', ascending=False)
        
        importance_path = os.path.join(models_dir, f'feature_importance_xgboost_{target_name}.csv')
        importance_df.to_csv(importance_path, index=False)
        
        model_path = os.path.join(models_dir, f'xgboost_{target_name}.pkl')
        scaler_path = os.path.join(models_dir, f'scaler_xgboost_{target_name}.pkl')
        mae_path = os.path.join(models_dir, f'xgboost_{target_name}_mae.txt')
        
        joblib.dump(final_model, model_path)
        joblib.dump(scaler, scaler_path)
        with open(mae_path, 'w') as f:
            f.write(str(avg_mae))
        
        print(f"Saved: {model_path}")
        print(f"Saved: {scaler_path}")
        print(f"Saved: {importance_path}")
        print(f"Saved: {mae_path}\n")
        
        import shutil
        config_source = os.path.join(script_dir, 'selective_tuning_config.py')
        config_dest = os.path.join(models_dir, 'selective_tuning_config_used.py')
        if os.path.exists(config_source):
            shutil.copy(config_source, config_dest)
    
    print("="*50)
    print("ALL MODELS TRAINED!")
    print("="*50)
    for target, mae in results.items():
        print(f"{target.capitalize()}: MAE = {mae:.2f}")
    
    return results

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-tuned-params', action='store_true',
                       help='Use hyperparameters from tune_hyperparameters.py')
    parser.add_argument('--use-all-tuned', action='store_true',
                       help='Use all tuned params (ignore selective config)')
    args = parser.parse_args()
    train_xgboost_models(use_tuned_params=args.use_tuned_params, use_selective=not args.use_all_tuned)