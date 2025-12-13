import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/evaluation/list_metrics_files.py

from pathlib import Path
from datetime import datetime

def list_metrics_files():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    eval_dir = Path(project_root) / 'data' / 'evaluation'
    
    if not eval_dir.exists():
        print(f"Directory {eval_dir} does not exist")
        return
    
    json_files = []
    for file_path in eval_dir.glob('*.json'):
        stat = file_path.stat()
        json_files.append({
            'name': file_path.name,
            'path': file_path,
            'modified': datetime.fromtimestamp(stat.st_mtime)
        })
    
    json_files.sort(key=lambda x: x['modified'], reverse=True)
    
    print("="*70)
    print("METRICS FILES (Most Recent to Oldest)")
    print("="*70)
    print()
    
    for i, file_info in enumerate(json_files, 1):
        print(f"{i}. {file_info['name']}")
        print(f"   Modified: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        print()

if __name__ == "__main__":
    list_metrics_files()

