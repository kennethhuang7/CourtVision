import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/evaluation/read_comparison_csv.py data/evaluation/comparison.csv

import pandas as pd
import numpy as np
import argparse
from pathlib import Path

def read_comparison_csv(csv_path):
    csv_file = Path(csv_path)
    
    if not csv_file.exists():
        print(f"Error: File {csv_path} does not exist")
        return
    
    df = pd.read_csv(csv_file)
    
    print("\n" + "="*70)
    print("PERFORMANCE COMPARISON")
    print("="*70)
    print()
    
    if 'stat' in df.columns or 'Stat' in df.columns:
        stat_col = 'stat' if 'stat' in df.columns else 'Stat'
        
        if 'baseline' in df.columns and 'full_tuned' in df.columns:
            print("ENSEMBLE PERFORMANCE:")
            print("-" * 70)
            
            improvements = []
            
            for _, row in df.iterrows():
                stat = row[stat_col]
                baseline_mae = row['baseline']
                improved_mae = row['selective'] if 'selective' in row else row['full_tuned']
                improvement = row['selective_improve'] if 'selective_improve' in row else row['full_improve']
                
                improvements.append(improvement)
                print(f"{stat:20s}: {baseline_mae:.4f} → {improved_mae:.4f} ({improvement:+.1f}%)")
            
            if improvements:
                avg_improvement = np.mean(improvements)
                print(f"\n{'Average improvement':20s}: {avg_improvement:+.1f}%")
        
        elif 'Baseline MAE' in df.columns and 'New MAE' in df.columns:
            print("ENSEMBLE PERFORMANCE:")
            print("-" * 70)
            
            improvements = []
            
            for _, row in df.iterrows():
                stat = row[stat_col]
                baseline_mae = row['Baseline MAE']
                
                if pd.isna(baseline_mae) or baseline_mae == 'N/A':
                    continue
                
                improved_mae = row['New MAE']
                improvement_str = str(row['MAE Improvement %'])
                
                if improvement_str.startswith('+') or improvement_str.startswith('-'):
                    improvement = float(improvement_str.replace('%', '').replace('+', ''))
                else:
                    improvement = 0.0
                
                improvements.append(improvement)
                baseline_mae = float(baseline_mae)
                improved_mae = float(improved_mae)
                print(f"{stat:20s}: {baseline_mae:.4f} → {improved_mae:.4f} ({improvement:+.1f}%)")
            
            if improvements:
                avg_improvement = np.mean(improvements)
                print(f"\n{'Average improvement':20s}: {avg_improvement:+.1f}%")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read and display comparison CSV')
    parser.add_argument('csv_path', help='Path to comparison CSV file')
    
    args = parser.parse_args()
    read_comparison_csv(args.csv_path)

