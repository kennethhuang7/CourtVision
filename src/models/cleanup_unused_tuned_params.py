import os
import sys
from pathlib import Path

# RUN THIS:
# python src/models/cleanup_unused_tuned_params.py

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from selective_tuning_config import SELECTIVE_TUNING_CONFIG

def cleanup_unused_tuned_params():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    params_dir = Path(project_root) / 'data' / 'models' / 'best_params'
    
    if not params_dir.exists():
        print(f"Directory {params_dir} does not exist")
        return
    
    files_to_keep = {'tuning_summary.json'}
    
    stat_name_mapping = {
        'rebounds': 'rebounds_total',
        'points': 'points',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    for model_type, stats_config in SELECTIVE_TUNING_CONFIG.items():
        for stat_name, should_use in stats_config.items():
            if should_use:
                file_stat_name = stat_name_mapping.get(stat_name, stat_name)
                file_name = f"{model_type}_{file_stat_name}.json"
                files_to_keep.add(file_name)
                print(f"KEEPING: {file_name} (marked True in config)")
    
    deleted_count = 0
    kept_count = 0
    
    print(f"\n{'='*70}")
    print("CLEANING UP UNUSED TUNED PARAMETER FILES")
    print(f"{'='*70}\n")
    
    for file_path in params_dir.glob('*.json'):
        file_name = file_path.name
        
        if file_name in files_to_keep:
            kept_count += 1
        else:
            print(f"DELETING: {file_name} (marked False in config or not in config)")
            file_path.unlink()
            deleted_count += 1
    
    print(f"\n{'='*70}")
    print("CLEANUP COMPLETE!")
    print(f"{'='*70}")
    print(f"Kept: {kept_count} files")
    print(f"Deleted: {deleted_count} files")
    print(f"\nFiles kept based on selective_tuning_config.py:")
    for file_name in sorted(files_to_keep):
        print(f"  - {file_name}")

if __name__ == "__main__":
    cleanup_unused_tuned_params()

