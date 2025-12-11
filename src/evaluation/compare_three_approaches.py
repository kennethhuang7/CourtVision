import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import pandas as pd
import numpy as np

baseline_path = 'data/evaluation/baseline_metrics.json'
full_tuned_path = 'data/evaluation/tuned_metrics.json'
selective_path = 'data/evaluation/selective_metrics.json'

if not os.path.exists(baseline_path):
    print(f"Error: {baseline_path} not found. Run baseline evaluation first.")
    sys.exit(1)

if not os.path.exists(full_tuned_path):
    print(f"Error: {full_tuned_path} not found. Run full tuning evaluation first.")
    sys.exit(1)

if not os.path.exists(selective_path):
    print(f"Error: {selective_path} not found. Run selective tuning evaluation first.")
    print("Run: python src/evaluation/evaluate_models.py --output data/evaluation/selective_metrics.json")
    sys.exit(1)

with open(baseline_path) as f:
    baseline = json.load(f)

with open(full_tuned_path) as f:
    full_tuned = json.load(f)

with open(selective_path) as f:
    selective = json.load(f)

stats = ['points', 'assists', 'steals', 'blocks', 'rebounds_total', 'turnovers', 'three_pointers_made']

print("\n" + "="*120)
print("COMPARISON: BASELINE vs FULL TUNING vs SELECTIVE TUNING")
print("="*120)
print("\nEnsemble Performance:\n")

results = []
for stat in stats:
    baseline_mae = baseline['ensemble'].get(stat, {}).get('mae', None)
    full_mae = full_tuned['ensemble'].get(stat, {}).get('mae', None)
    selective_mae = selective['ensemble'].get(stat, {}).get('mae', None)
    
    if baseline_mae and full_mae and selective_mae:
        full_improvement = ((baseline_mae - full_mae) / baseline_mae) * 100
        selective_improvement = ((baseline_mae - selective_mae) / baseline_mae) * 100
        selective_vs_full = ((full_mae - selective_mae) / full_mae) * 100 if full_mae > 0 else 0
        
        results.append({
            'stat': stat,
            'baseline': baseline_mae,
            'full_tuned': full_mae,
            'full_improve': full_improvement,
            'selective': selective_mae,
            'selective_improve': selective_improvement,
            'selective_vs_full': selective_vs_full
        })

df = pd.DataFrame(results)

print(f"{'Stat':<20} {'Baseline':<12} {'Full Tuned':<12} {'Full %':<10} "
      f"{'Selective':<12} {'Select %':<10} {'Select vs Full':<15}")
print("-" * 120)

for _, row in df.iterrows():
    print(f"{row['stat']:<20} {row['baseline']:<12.4f} {row['full_tuned']:<12.4f} "
          f"{row['full_improve']:>+9.1f}% {row['selective']:<12.4f} "
          f"{row['selective_improve']:>+9.1f}% {row['selective_vs_full']:>+13.1f}%")

print("-" * 120)
print(f"{'AVERAGE':<20} {'':<12} {'':<12} {df['full_improve'].mean():>+9.1f}% "
      f"{'':<12} {df['selective_improve'].mean():>+9.1f}% "
      f"{df['selective_vs_full'].mean():>+13.1f}%")

print("\n" + "="*120)
print("\nKey Observations:")
print("  1. Full Tuning: Average improvement = {:.1f}%".format(df['full_improve'].mean()))
print("  2. Selective Tuning: Average improvement = {:.1f}%".format(df['selective_improve'].mean()))
print("  3. Selective vs Full: {:.1f}% better".format(df['selective_vs_full'].mean()))
print("\nExpected selective improvement: 4-6%")
print("Actual selective improvement: {:.1f}%".format(df['selective_improve'].mean()))

if df['selective_improve'].mean() >= 4.0:
    print("\nRESULT: Selective tuning achieved expected improvement!")
elif df['selective_improve'].mean() >= 3.0:
    print("\nRESULT: Selective tuning improved, but slightly below expectation")
else:
    print("\nRESULT: Selective tuning did not achieve expected improvement")

print("="*120)

output_dir = os.path.dirname(selective_path)
comparison_path = os.path.join(output_dir, 'tuning_comparison.csv')
df.to_csv(comparison_path, index=False)
print(f"\nComparison saved to: {comparison_path}\n")

