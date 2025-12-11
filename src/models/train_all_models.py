import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/models/train_all_models.py
# or (if you already built features):
# python src/models/train_all_models.py --skip-features
# To use tuned hyperparameters:
# python src/models/train_all_models.py --use-tuned-params
# or (if you already built features):
# python src/models/train_all_models.py --skip-features --use-tuned-params

import subprocess
from datetime import datetime

def train_all_models(build_features_first=True, use_tuned_params=False):
    print("="*70)
    print("TRAINING ALL MODELS")
    print("="*70)
    print(f"Started at: {datetime.now()}\n")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    
    if build_features_first:
        print("="*70)
        print("STEP 1: Building training features...")
        print("="*70)
        print("Starting feature building process...\n")
        features_script = os.path.join(project_root, 'src', 'feature_engineering', 'build_features.py')
        
        result = subprocess.run(
            [sys.executable, '-u', features_script],
            text=True
        )
        
        if result.returncode != 0:
            print("\n" + "="*70)
            print("ERROR: Feature building failed!")
            print("="*70)
            return False
        
        print("\n" + "="*70)
        print("Features built successfully!")
        print("="*70 + "\n")
    
    models = [
        ('XGBoost', 'train_xgboost.py'),
        ('LightGBM', 'train_lightgbm.py'),
        ('CatBoost', 'train_catboost.py'),
        ('Random Forest', 'train_random_forest.py')
    ]
    
    results = {}
    
    for model_name, script_name in models:
        print("\n" + "="*70)
        print(f"Training {model_name}...")
        print("="*70)
        
        model_script = os.path.join(script_dir, script_name)
        
        cmd = [sys.executable, model_script]
        if use_tuned_params:
            cmd.append('--use-tuned-params')
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        if result.stderr:
            print("WARNINGS:", result.stderr)
        
        if result.returncode != 0:
            print(f"\nERROR: {model_name} training failed!")
            results[model_name] = 'FAILED'
        else:
            print(f"\n{model_name} training completed successfully!")
            results[model_name] = 'SUCCESS'
    
    print("\n" + "="*70)
    print("TRAINING SUMMARY")
    print("="*70)
    print(f"Completed at: {datetime.now()}\n")
    
    for model_name, status in results.items():
        print(f"{model_name}: {status}")
    
    all_success = all(status == 'SUCCESS' for status in results.values())
    
    if all_success:
        print("\n" + "="*70)
        print("ALL MODELS TRAINED SUCCESSFULLY!")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("SOME MODELS FAILED - CHECK ERRORS ABOVE")
        print("="*70)
    
    return all_success

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Train all 4 ML models')
    parser.add_argument(
        '--skip-features',
        action='store_true',
        help='Skip building features (assumes features already exist)'
    )
    parser.add_argument(
        '--use-tuned-params',
        action='store_true',
        help='Use hyperparameters from tune_hyperparameters.py'
    )
    
    args = parser.parse_args()
    
    build_features = not args.skip_features
    train_all_models(build_features_first=build_features, use_tuned_params=args.use_tuned_params)

