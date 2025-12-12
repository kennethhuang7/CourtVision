import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# Compare baseline (oldest) with most recent:
# python src/evaluation/compare_metrics_by_date.py --baseline-vs-newest
# Compare second most recent with newest:
# python src/evaluation/compare_metrics_by_date.py --recent-vs-newest

import json
import argparse
from pathlib import Path
from datetime import datetime
from evaluate_models import compare_metrics

def get_metrics_files_sorted():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    eval_dir = Path(project_root) / 'data' / 'evaluation'
    
    if not eval_dir.exists():
        print(f"Directory {eval_dir} does not exist")
        return []
    
    json_files = []
    for file_path in eval_dir.glob('*.json'):
        stat = file_path.stat()
        json_files.append({
            'name': file_path.name,
            'path': str(file_path),
            'modified': datetime.fromtimestamp(stat.st_mtime)
        })
    
    json_files.sort(key=lambda x: x['modified'])
    return json_files

def compare_baseline_vs_newest():
    files = get_metrics_files_sorted()
    
    if len(files) < 2:
        print("Error: Need at least 2 metrics files to compare")
        return
    
    baseline_file = None
    for f in files:
        if f['name'] == 'baseline_metrics.json':
            baseline_file = f
            break
    
    if not baseline_file:
        baseline_file = files[0]
    
    newest_file = files[-1]
    
    print(f"Comparing: {baseline_file['name']} (oldest) vs {newest_file['name']} (newest)")
    print()
    
    compare_metrics(baseline_file['path'], newest_file['path'])

def compare_recent_vs_newest():
    files = get_metrics_files_sorted()
    
    if len(files) < 2:
        print("Error: Need at least 2 metrics files to compare")
        return
    
    if len(files) == 2:
        second_most_recent = files[0]
    else:
        second_most_recent = files[-2]
    
    newest_file = files[-1]
    
    print(f"Comparing: {second_most_recent['name']} (second most recent) vs {newest_file['name']} (newest)")
    print()
    
    compare_metrics(second_most_recent['path'], newest_file['path'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare metrics files by date')
    parser.add_argument('--baseline-vs-newest', action='store_true',
                       help='Compare baseline (oldest) with most recent')
    parser.add_argument('--recent-vs-newest', action='store_true',
                       help='Compare second most recent with newest')
    
    args = parser.parse_args()
    
    if args.baseline_vs_newest:
        compare_baseline_vs_newest()
    elif args.recent_vs_newest:
        compare_recent_vs_newest()
    else:
        print("Error: Must specify --baseline-vs-newest or --recent-vs-newest")
        parser.print_help()

