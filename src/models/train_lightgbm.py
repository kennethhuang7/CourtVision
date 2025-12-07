import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler
import lightgbm as lgb
import joblib

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

def train_lightgbm_models():
    print("Training LightGBM models for NBA player predictions...\n")
    
    print("Loading features...")
    df = pd.read_csv('../../data/processed/training_features.csv')
    print(f"Loaded {len(df)} records\n")
    
    df = df.dropna(subset=['points_l5', 'points_l10'])
    print(f"After removing NaN: {len(df)} records\n")
    
    feature_cols = [col for col in df.columns if any(x in col for x in 
               ['_l5', '_l10', '_l20', '_weighted', 'is_', 'days_rest', 'games_played',
                'offensive_rating', 'defensive_rating', 'pace', 'opp_', 'altitude', 'playoff',
                'star_teammate', 'games_without_star'])]
    
    feature_cols = [col for col in feature_cols if 'team_id' not in col and 'player_id' not in col and 'game_id' not in col]
    
    X = df[feature_cols].fillna(0)
    
    targets = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    os.makedirs('../../data/models', exist_ok=True)
    
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
        
        tscv = TimeSeriesSplit(n_splits=3)
        
        best_model = None
        best_score = float('inf')
        
        for fold, (train_idx, val_idx) in enumerate(tscv.split(X_scaled), 1):
            X_train, X_val = X_scaled.iloc[train_idx], X_scaled.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
            
            model = lgb.LGBMRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1,
                verbose=-1
            )
            
            model.fit(X_train, y_train)
            
            y_pred = model.predict(X_val)
            
            mae = mean_absolute_error(y_val, y_pred)
            rmse = np.sqrt(mean_squared_error(y_val, y_pred))
            
            print(f"Fold {fold}: MAE={mae:.2f}, RMSE={rmse:.2f}")
            
            if mae < best_score:
                best_score = mae
                best_model = model
        
        print(f"\nBEST MAE: {best_score:.2f}\n")
        results[target_name] = best_score
        
        print("Training final model on all data...")
        final_model = lgb.LGBMRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
            verbose=-1
        )
        
        final_model.fit(X_scaled, y)
        
        model_path = f'../../data/models/lightgbm_{target_name}.pkl'
        scaler_path = f'../../data/models/scaler_lightgbm_{target_name}.pkl'
        
        joblib.dump(final_model, model_path)
        joblib.dump(scaler, scaler_path)
        print(f"Saved: {model_path}")
        print(f"Saved: {scaler_path}\n")
    
    print("="*50)
    print("ALL MODELS TRAINED!")
    print("="*50)
    for target, mae in results.items():
        print(f"{target.capitalize()}: MAE = {mae:.2f}")
    
    return results

if __name__ == "__main__":
    train_lightgbm_models()

