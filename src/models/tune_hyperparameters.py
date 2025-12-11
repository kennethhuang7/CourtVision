import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import optuna
import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
import joblib
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def create_cv_splits(df):
    unique_seasons = sorted(df['season'].unique())
    
    if len(unique_seasons) < 3:
        split_point = int(len(df) * 0.8)
        return [(np.arange(split_point), np.arange(split_point, len(df)))]
    
    split_indices = []
    for i, test_season in enumerate(unique_seasons[2:], start=2):
        train_seasons = unique_seasons[:i]
        
        train_mask = df['season'].isin(train_seasons)
        test_mask = df['season'] == test_season
        
        train_idx = np.where(train_mask)[0]
        val_idx = np.where(test_mask)[0]
        
        if len(train_idx) > 0 and len(val_idx) > 0:
            split_indices.append((train_idx, val_idx))
    
    return split_indices

def create_objective(model_type, X, y, split_indices, target_name):
    def objective(trial):
        if model_type == 'xgboost':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 250),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
                'gamma': trial.suggest_float('gamma', 0, 0.5),
                'reg_alpha': trial.suggest_float('reg_alpha', 0, 1.0),
                'reg_lambda': trial.suggest_float('reg_lambda', 0, 1.0),
                'random_state': 42,
                'n_jobs': 1
            }
            
            if target_name in ['blocks', 'steals']:
                params['objective'] = 'count:poisson'
            else:
                params['objective'] = 'reg:squarederror'
            
            model = XGBRegressor(**params)
            
        elif model_type == 'lightgbm':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 250),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 50),
                'reg_alpha': trial.suggest_float('reg_alpha', 0, 1.0),
                'reg_lambda': trial.suggest_float('reg_lambda', 0, 1.0),
                'random_state': 42,
                'n_jobs': 1,
                'verbose': -1
            }
            
            if target_name in ['blocks', 'steals']:
                params['objective'] = 'poisson'
            else:
                params['objective'] = 'regression'
            
            model = LGBMRegressor(**params)
            
        elif model_type == 'catboost':
            params = {
                'iterations': trial.suggest_int('iterations', 50, 250),
                'depth': trial.suggest_int('depth', 3, 10),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bylevel': trial.suggest_float('colsample_bylevel', 0.6, 1.0),
                'reg_lambda': trial.suggest_float('reg_lambda', 0, 10.0),
                'random_state': 42,
                'verbose': False
            }
            
            if target_name in ['blocks', 'steals']:
                params['loss_function'] = 'Poisson'
            else:
                params['loss_function'] = 'RMSE'
            
            model = CatBoostRegressor(**params)
            
        elif model_type == 'random_forest':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 250),
                'max_depth': trial.suggest_int('max_depth', 5, 15),
                'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
                'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
                'max_features': trial.suggest_float('max_features', 0.5, 1.0),
                'random_state': 42,
                'n_jobs': 1
            }
            
            model = RandomForestRegressor(**params)
        
        fold_maes = []
        for train_idx, val_idx in split_indices:
            X_fold_train = X.iloc[train_idx]
            X_fold_val = X.iloc[val_idx]
            y_fold_train = y.iloc[train_idx]
            y_fold_val = y.iloc[val_idx]
            
            model.fit(X_fold_train, y_fold_train)
            y_pred = model.predict(X_fold_val)
            mae = mean_absolute_error(y_fold_val, y_pred)
            fold_maes.append(mae)
        
        return np.mean(fold_maes)
    
    return objective

def tune_model(model_type, target_name, X, y, split_indices, n_trials=30):
    print(f"\n{'='*70}")
    print(f"Tuning {model_type} for {target_name}")
    print(f"{'='*70}")
    print(f"Running {n_trials} trials with {len(split_indices)}-fold season-aware CV")
    
    objective = create_objective(model_type, X, y, split_indices, target_name)
    
    study = optuna.create_study(
        direction='minimize',
        study_name=f"{model_type}_{target_name}"
    )
    
    study.optimize(
        objective,
        n_trials=n_trials,
        n_jobs=8,
        show_progress_bar=True
    )
    
    print(f"\nBest MAE: {study.best_value:.4f}")
    print(f"Best parameters:")
    for param, value in study.best_params.items():
        print(f"  {param}: {value}")
    
    params_dir = Path('data/models/best_params')
    params_dir.mkdir(parents=True, exist_ok=True)
    
    params_path = params_dir / f'{model_type}_{target_name}.json'
    with open(params_path, 'w') as f:
        json.dump(study.best_params, f, indent=2)
    
    print(f"Parameters saved to {params_path}")
    
    return study.best_params, study.best_value

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Tune hyperparameters for NBA prediction models')
    parser.add_argument('--n-trials', type=int, default=30, 
                       help='Number of Optuna trials per model/stat')
    parser.add_argument('--models', nargs='+', 
                       default=['xgboost', 'lightgbm', 'catboost', 'random_forest'],
                       help='Models to tune')
    parser.add_argument('--stats', nargs='+',
                       default=['points', 'rebounds_total', 'assists', 'steals', 
                               'blocks', 'turnovers', 'three_pointers_made'],
                       help='Stats to tune for')
    
    args = parser.parse_args()
    
    print("Loading training features...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    features_path = os.path.join(project_root, 'data', 'processed', 'training_features.csv')
    df = pd.read_csv(features_path)
    
    df = df.dropna(subset=['points_l5', 'points_l10'])
    print(f"Loaded {len(df)} records\n")
    
    feature_cols = [col for col in df.columns if any(x in col for x in 
        ['_l5', '_l10', '_l20', '_weighted', 'is_', 'days_rest', 'games_played',
         'offensive_rating', 'defensive_rating', 'net_rating', 'pace', 'opp_',
         'altitude', 'playoff', 'star_teammate', 'games_without_star',
         'usage_rate', 'minutes_played', 'minutes_trend', 'per_36',
         '_pct', '_ratio', 'pts_per', 'ast_to', 'reb_rate', 'position_'])]
    
    feature_cols = [col for col in feature_cols 
                    if 'team_id' not in col and 'player_id' not in col and 'game_id' not in col]
    
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
            elif 'trend' in col:
                league_means[col] = 0
            else:
                league_means[col] = df[col].mean()
    
    X = df[feature_cols].copy()
    player_means = {}
    for col in feature_cols:
        if col in X.columns:
            if 'team' in col or 'opp' in col or 'pace' in col:
                X[col] = X[col].fillna(league_means.get(col, 0))
            elif col.startswith('is_') or col.startswith('position_') or 'trend' in col:
                X[col] = X[col].fillna(league_means.get(col, 0))
            else:
                if col not in player_means:
                    player_means[col] = df.groupby('player_id')[col].transform(
                        lambda x: x.expanding().mean().shift(1)
                    ).fillna(league_means.get(col, 0))
                X[col] = X[col].fillna(player_means[col])
    
    X = X.fillna(0)
    
    print("Creating season-aware CV splits...")
    split_indices = create_cv_splits(df)
    print(f"Created {len(split_indices)} CV folds\n")
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
    
    results = {}
    
    for model_type in args.models:
        results[model_type] = {}
        
        for target_name in args.stats:
            y = df[target_name]
            
            best_params, best_value = tune_model(
                model_type, target_name, X_scaled, y, split_indices, args.n_trials
            )
            
            results[model_type][target_name] = {
                'best_params': best_params,
                'best_mae': best_value
            }
    
    summary_path = Path('data/models/best_params/tuning_summary.json')
    with open(summary_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*70}")
    print("TUNING COMPLETE")
    print(f"{'='*70}")
    print(f"Summary saved to {summary_path}")
    print("\nTo train models with tuned parameters:")
    print("  python train_all_models.py --use-tuned-params")

if __name__ == "__main__":
    main()

