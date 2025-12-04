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
    
    print("\nSTEP 2: Update career stats (players who played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_career_stats_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 3: Update team ratings (teams that played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_team_ratings_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 4: Update team defensive stats (teams that played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_team_defensive_stats_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 5: Update position defense stats (teams that played yesterday)")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/update_position_defense_stats_incremental.py',
        str(yesterday)
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 6: Scrape current injuries")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/scrape_injuries.py'
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)
    
    print("\nSTEP 7: Mark recovered players")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../data_collection/mark_recovered_players.py',
        str(yesterday)
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
    
    print("\nSTEP 9: Generate predictions for today")
    print("-"*50)
    result = subprocess.run([
        sys.executable,
        '../predictions/predict_games.py',
        str(today)
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