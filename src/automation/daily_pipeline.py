import sys
import os
import subprocess
from datetime import datetime, timedelta

def run_daily_pipeline():
    print("="*50)
    print("NBA PREDICTION DAILY PIPELINE")
    print(f"Running at: {datetime.now()}")
    print("="*50)
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    print(f"\nYesterday: {yesterday}")
    print(f"Today: {today}\n")
    
    print("\nSTEP 1: Collect yesterday's games")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_yesterday_games.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    if result.returncode != 0:
        print("\n" + "="*50)
        print("STEP 1 FAILED - NBA API TIMEOUT")
        print("="*50)
        print("Manual collection required!")
        print("Run: python src/automation/manual_data_collection.py")
        sys.exit(1)
    
    print("\nSTEP 2: Update team ratings (teams that played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_team_ratings_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 3: Update team defensive stats (teams that played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_team_defensive_stats_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 4: Update position defense stats (teams that played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_position_defense_stats_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 5: Update injury log (detect injuries and recoveries)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_injury_log.py'
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 6: Detect and update player transactions (trades, signings, waivers)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/detect_and_update_trades.py',
        str(today)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 7: Update player teams from recent game data (backup)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/detect_and_update_trades.py',
        str(today),
        '--from-games'
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 8: Collect today's schedule")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/collect_todays_schedule.py',
        str(today)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 9: Generate predictions for today (all models)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../predictions/predict_games.py',
        str(today),
        '--all'
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 10: Evaluate yesterday's predictions")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../predictions/evaluate_predictions.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\n" + "="*50)
    print("DAILY PIPELINE COMPLETE!")
    print("="*50)

if __name__ == "__main__":
    run_daily_pipeline()