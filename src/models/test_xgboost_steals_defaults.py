import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/models/test_xgboost_steals_defaults.py

import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

def test_xgboost_steals_defaults():
    print("Testing XGBoost steals with DEFAULT parameters...\n")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    features_path = os.path.join(project_root, 'data', 'processed', 'training_features.csv')
    
    print("Loading features...")
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
    
    print("Calculating imputation values...")
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
    
    y = df['steals']
    
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
    
    print(f"Using {len(split_indices)}-fold season-aware CV\n")
    print("="*70)
    print("TRAINING XGBOOST STEALS WITH DEFAULT PARAMETERS")
    print("="*70)
    print("\nDefault parameters:")
    print("  n_estimators: 100")
    print("  max_depth: 6")
    print("  learning_rate: 0.1")
    print("  subsample: 0.8")
    print("  colsample_bytree: 0.8")
    print("  objective: count:poisson")
    print()
    
    fold_maes = []
    
    for fold, (train_idx, val_idx) in enumerate(split_indices, 1):
        X_train, X_val = X_scaled.iloc[train_idx], X_scaled.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
            objective='count:poisson'
        )
        
        model.fit(X_train, y_train, verbose=False)
        y_pred = model.predict(X_val)
        mae = mean_absolute_error(y_val, y_pred)
        fold_maes.append(mae)
        
        print(f"Fold {fold}: MAE = {mae:.4f}")
    
    avg_mae = np.mean(fold_maes)
    std_mae = np.std(fold_maes)
    
    print("\n" + "="*70)
    print("RESULTS")
    print("="*70)
    print(f"Average MAE: {avg_mae:.4f} ± {std_mae:.4f}")
    print()
    print("COMPARISON:")
    print(f"  Default params:     {avg_mae:.4f} MAE")
    print(f"  New tuned params:   0.5068 MAE")
    print(f"  Previous tuned:     0.4634 MAE")
    print()
    
    if avg_mae < 0.5068:
        improvement = ((0.5068 - avg_mae) / 0.5068) * 100
        print(f"Defaults are BETTER by {improvement:.1f}%")
        print("Recommendation: Use default parameters")
    elif avg_mae > 0.5068:
        degradation = ((avg_mae - 0.5068) / 0.5068) * 100
        print(f"Defaults are WORSE by {degradation:.1f}%")
        print("Recommendation: Keep new tuned params (or find old ones)")
    else:
        print("Defaults are similar to new tuned params")
    
    print("="*70)

if __name__ == "__main__":
    test_xgboost_steals_defaults()

