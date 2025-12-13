import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
import subprocess

# RUN THIS:
# python src/automation/manual_data_collection.py

def run_manual_collection():
    print("="*50)
    print("MANUAL NBA DATA COLLECTION")
    print(f"Running at: {datetime.now()}")
    print("="*50)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    
    yesterday = (datetime.now() - timedelta(days=1)).date()
    today = datetime.now().date()
    
    print(f"\nYesterday: {yesterday}")
    print(f"Today: {today}\n")
    
    steps = [
        ("Collect yesterday's games", os.path.join(project_root, "src", "data_collection", "update_yesterday_games.py"), yesterday),
        ("Update team ratings", os.path.join(project_root, "src", "data_collection", "update_team_ratings_incremental.py"), yesterday),
        ("Update team defensive stats", os.path.join(project_root, "src", "data_collection", "update_team_defensive_stats_incremental.py"), yesterday),
        ("Update position defense", os.path.join(project_root, "src", "data_collection", "update_position_defense_stats_incremental.py"), yesterday),
        ("Update injury log (detect injuries and recoveries)", os.path.join(project_root, "src", "data_collection", "update_injury_log.py"), None),
        ("Detect and update player transactions", os.path.join(project_root, "src", "data_collection", "detect_and_update_trades.py"), today),
        ("Update player teams from recent games (backup)", os.path.join(project_root, "src", "data_collection", "detect_and_update_trades.py"), (today, "--from-games")),
        ("Collect today's schedule", os.path.join(project_root, "src", "data_collection", "collect_todays_schedule.py"), today),
        ("Generate predictions for today", os.path.join(project_root, "src", "predictions", "predict_games.py"), (today, "--all")),
        ("Evaluate yesterday's predictions", os.path.join(project_root, "src", "predictions", "evaluate_predictions.py"), yesterday)
    ]
    
    for step_name, script_path, date_arg in steps:
        print(f"\n{'='*50}")
        print(f"STEP: {step_name}")
        print("="*50)
        
        if date_arg is None:
            result = subprocess.run([
                sys.executable,
                script_path
            ], capture_output=True, text=True, encoding='utf-8', errors='replace')
        elif isinstance(date_arg, tuple):
            cmd = [sys.executable, script_path]
            for arg in date_arg:
                cmd.append(str(arg))
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
        else:
            result = subprocess.run([
                sys.executable,
                script_path,
                str(date_arg)
            ], capture_output=True, text=True, encoding='utf-8', errors='replace')
        
        print(result.stdout)
        
        if result.stderr:
            print("ERRORS:", result.stderr)
    
    print("\n" + "="*50)
    print("COLLECTION COMPLETE!")
    print("="*50)

if __name__ == "__main__":
    run_manual_collection()