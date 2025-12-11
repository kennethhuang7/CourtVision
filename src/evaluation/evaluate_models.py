import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/evaluation/evaluate_models.py
# To evaluate current models and save metrics:
# python src/evaluation/evaluate_models.py --output data/evaluation/current_metrics.json
# To compare two metric files:
# python src/evaluation/evaluate_models.py --compare data/evaluation/baseline_metrics.json data/evaluation/tuned_metrics.json

import pandas as pd
import numpy as np
import json
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

def evaluate_all_models(models_dir='data/models', features_path='data/processed/training_features.csv', 
                       output_path='data/evaluation/metrics.json'):
    print("Loading training data...")
    df = pd.read_csv(features_path)
    
    feature_cols = [col for col in df.columns if any(x in col for x in 
        ['_l5', '_l10', '_l20', '_weighted', 'is_', 'days_rest', 'games_played',
         'offensive_rating', 'defensive_rating', 'net_rating', 'pace', 'opp_',
         'altitude', 'playoff', 'star_teammate', 'games_without_star',
         'usage_rate', 'minutes_played', 'minutes_trend', 'per_36',
         '_pct', '_ratio', 'pts_per', 'ast_to', 'reb_rate', 'position_',
         'games_in_last', 'is_heavy', 'is_well', 'consecutive_games', 'season_progress',
         'is_early', 'is_mid', 'is_late', 'games_remaining', 'tz_difference', 'west_to_east',
         'east_to_west', 'days_since_asb', 'post_asb'])]
    
    feature_cols = [col for col in feature_cols 
                    if 'team_id' not in col and 'player_id' not in col and 'game_id' not in col]
    
    raw_leakage_cols = ['offensive_rating', 'defensive_rating', 'usage_rate', 'true_shooting_pct',
                        'points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers',
                        'three_pointers_made', 'minutes_played', 'is_starter', 'field_goals_made',
                        'field_goals_attempted', 'three_pointers_attempted', 'free_throws_made',
                        'free_throws_attempted']
    feature_cols = [col for col in feature_cols if col not in raw_leakage_cols]
    
    X = df[feature_cols].fillna(0)
    
    seasons = sorted(df['season'].unique())
    val_season = seasons[-1]
    val_mask = df['season'] == val_season
    
    X_val = X[val_mask]
    
    stat_mapping = {
        'points': 'points',
        'rebounds_total': 'rebounds',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    stats = ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']
    models = ['xgboost', 'lightgbm', 'catboost', 'random_forest']
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'validation_season': val_season,
        'models': {}
    }
    
    for model_type in models:
        results['models'][model_type] = {}
        
        for stat in stats:
            try:
                model_file_name = stat_mapping.get(stat, stat)
                model_path = Path(models_dir) / f'{model_type}_{model_file_name}.pkl'
                scaler_path = Path(models_dir) / f'scaler_{model_type}_{model_file_name}.pkl'
                
                if not model_path.exists():
                    print(f"  Model not found: {model_path}")
                    continue
                
                model = joblib.load(model_path)
                scaler = joblib.load(scaler_path)
                
                y_val = df[val_mask][stat]
                
                X_val_scaled = scaler.transform(X_val)
                
                y_pred = model.predict(X_val_scaled)
                
                mae = mean_absolute_error(y_val, y_pred)
                rmse = np.sqrt(mean_squared_error(y_val, y_pred))
                r2 = r2_score(y_val, y_pred)
                
                results['models'][model_type][stat] = {
                    'mae': float(mae),
                    'rmse': float(rmse),
                    'r2': float(r2)
                }
                
                print(f"  {model_type} - {stat}: MAE = {mae:.4f}, RMSE = {rmse:.4f}, R² = {r2:.4f}")
                
            except Exception as e:
                print(f"  Error evaluating {model_type} - {stat}: {str(e)}")
                results['models'][model_type][stat] = {'error': str(e)}
    
    print("\nEvaluating ensemble (simple average)...")
    results['ensemble'] = {}
    
    for stat in stats:
        predictions = []
        
        for model_type in models:
            try:
                model_file_name = stat_mapping.get(stat, stat)
                model_path = Path(models_dir) / f'{model_type}_{model_file_name}.pkl'
                scaler_path = Path(models_dir) / f'scaler_{model_type}_{model_file_name}.pkl'
                
                if not model_path.exists():
                    continue
                
                model = joblib.load(model_path)
                scaler = joblib.load(scaler_path)
                
                X_val_scaled = scaler.transform(X_val)
                y_pred = model.predict(X_val_scaled)
                predictions.append(y_pred)
                
            except:
                continue
        
        if len(predictions) > 0:
            ensemble_pred = np.mean(predictions, axis=0)
            y_val = df[val_mask][stat]
            
            mae = mean_absolute_error(y_val, ensemble_pred)
            rmse = np.sqrt(mean_squared_error(y_val, ensemble_pred))
            r2 = r2_score(y_val, ensemble_pred)
            
            results['ensemble'][stat] = {
                'mae': float(mae),
                'rmse': float(rmse),
                'r2': float(r2),
                'num_models': len(predictions)
            }
            
            print(f"  Ensemble - {stat}: MAE = {mae:.4f}, RMSE = {rmse:.4f}, R² = {r2:.4f}")
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nMetrics saved to {output_path}")
    
    return results


def compare_metrics(baseline_path, improved_path):
    with open(baseline_path, 'r') as f:
        baseline = json.load(f)
    
    with open(improved_path, 'r') as f:
        improved = json.load(f)
    
    print("\n" + "="*70)
    print("PERFORMANCE COMPARISON")
    print("="*70)
    print(f"\nBaseline: {baseline['timestamp']}")
    print(f"Improved: {improved['timestamp']}")
    print()
    
    if 'ensemble' in baseline and 'ensemble' in improved:
        print("ENSEMBLE PERFORMANCE:")
        print("-" * 70)
        
        total_improvement = []
        
        for stat in baseline['ensemble'].keys():
            if stat in improved['ensemble']:
                baseline_mae = baseline['ensemble'][stat]['mae']
                improved_mae = improved['ensemble'][stat]['mae']
                improvement = ((baseline_mae - improved_mae) / baseline_mae) * 100
                total_improvement.append(improvement)
                
                print(f"{stat:20s}: {baseline_mae:.4f} → {improved_mae:.4f} ({improvement:+.1f}%)")
        
        if total_improvement:
            avg_improvement = np.mean(total_improvement)
            print(f"\n{'Average improvement':20s}: {avg_improvement:+.1f}%")
    
    print("\n\nINDIVIDUAL MODEL PERFORMANCE:")
    print("-" * 70)
    
    for model_type in ['xgboost', 'lightgbm', 'catboost', 'random_forest']:
        if model_type not in baseline['models'] or model_type not in improved['models']:
            continue
        
        print(f"\n{model_type.upper()}:")
        
        model_improvements = []
        
        for stat in baseline['models'][model_type].keys():
            if stat in improved['models'][model_type]:
                baseline_mae = baseline['models'][model_type][stat]['mae']
                improved_mae = improved['models'][model_type][stat]['mae']
                improvement = ((baseline_mae - improved_mae) / baseline_mae) * 100
                model_improvements.append(improvement)
                
                print(f"  {stat:20s}: {baseline_mae:.4f} → {improved_mae:.4f} ({improvement:+.1f}%)")
        
        if model_improvements:
            avg = np.mean(model_improvements)
            print(f"  {'Average':20s}: {avg:+.1f}%")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Evaluate NBA prediction models')
    parser.add_argument('--output', default='data/evaluation/metrics.json', 
                       help='Output path for metrics')
    parser.add_argument('--compare', nargs=2, metavar=('BASELINE', 'IMPROVED'),
                       help='Compare two metric files')
    
    args = parser.parse_args()
    
    if args.compare:
        compare_metrics(args.compare[0], args.compare[1])
    else:
        evaluate_all_models(output_path=args.output)
