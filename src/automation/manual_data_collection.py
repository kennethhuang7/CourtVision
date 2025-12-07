import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
import subprocess

def run_manual_collection():
    print("="*50)
    print("MANUAL NBA DATA COLLECTION")
    print(f"Running at: {datetime.now()}")
    print("="*50)
    
    yesterday = (datetime.now() - timedelta(days=1)).date()
    today = datetime.now().date()
    
    print(f"\nYesterday: {yesterday}")
    print(f"Today: {today}\n")
    
    steps = [
        ("Collect yesterday's games", "../data_collection/update_yesterday_games.py", yesterday),
        ("Update career stats", "../data_collection/update_career_stats_incremental.py", yesterday),
        ("Update team ratings", "../data_collection/update_team_ratings_incremental.py", yesterday),
        ("Update team defensive stats", "../data_collection/update_team_defensive_stats_incremental.py", yesterday),
        ("Update position defense", "../data_collection/update_position_defense_stats_incremental.py", yesterday),
        ("Scrape injuries", "../data_collection/scrape_injuries.py", None),
        ("Mark recovered players", "../data_collection/mark_recovered_players.py", yesterday),
        ("Collect today's schedule", "../data_collection/collect_todays_schedule.py", today),
        ("Generate predictions for today", "../predictions/predict_games.py", (today, "--all")),
        ("Evaluate yesterday's predictions", "../predictions/evaluate_predictions.py", yesterday)
    ]
    
    for step_name, script_path, date_arg in steps:
        print(f"\n{'='*50}")
        print(f"STEP: {step_name}")
        print("="*50)
        
        if date_arg is None:
            result = subprocess.run([
                sys.executable,
                script_path
            ], capture_output=True, text=True)
        elif isinstance(date_arg, tuple):
            result = subprocess.run([
                sys.executable,
                script_path,
                str(date_arg[0]),
                date_arg[1]
            ], capture_output=True, text=True)
        else:
            result = subprocess.run([
                sys.executable,
                script_path,
                str(date_arg)
            ], capture_output=True, text=True)
        
        print(result.stdout)
        
        if result.stderr:
            print("ERRORS:", result.stderr)
    
    print("\n" + "="*50)
    print("COLLECTION COMPLETE!")
    print("="*50)

if __name__ == "__main__":
    run_manual_collection()