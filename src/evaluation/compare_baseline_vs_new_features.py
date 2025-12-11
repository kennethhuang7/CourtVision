import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import pandas as pd
import numpy as np

baseline_path = 'data/evaluation/baseline_metrics.json'
new_features_path = 'data/evaluation/new_features_metrics.json'

if not os.path.exists(baseline_path):
    print(f"Error: {baseline_path} not found. Run baseline evaluation first.")
    sys.exit(1)

if not os.path.exists(new_features_path):
    print(f"Error: {new_features_path} not found. Run new features evaluation first.")
    print("Run: python src/evaluation/evaluate_models.py --output data/evaluation/new_features_metrics.json")
    sys.exit(1)

with open(baseline_path) as f:
    baseline = json.load(f)

with open(new_features_path) as f:
    new_features = json.load(f)

stats = ['points', 'assists', 'steals', 'blocks', 'rebounds_total', 'turnovers', 'three_pointers_made']

print("\n" + "="*100)
print("COMPARISON: BASELINE vs NEW FEATURES (Temporal + Opponent Turnover/Steal)")
print("="*100)
print("\nEnsemble Performance:\n")

results = []
for stat in stats:
    baseline_mae = baseline['ensemble'].get(stat, {}).get('mae', None)
    new_mae = new_features['ensemble'].get(stat, {}).get('mae', None)
    
    baseline_r2 = baseline['ensemble'].get(stat, {}).get('r2', None)
    new_r2 = new_features['ensemble'].get(stat, {}).get('r2', None)
    
    if baseline_mae and new_mae:
        mae_improvement = ((baseline_mae - new_mae) / baseline_mae) * 100
        r2_improvement = new_r2 - baseline_r2 if baseline_r2 and new_r2 else None
        
        results.append({
            'stat': stat,
            'baseline_mae': baseline_mae,
            'new_mae': new_mae,
            'mae_improvement': mae_improvement,
            'baseline_r2': baseline_r2,
            'new_r2': new_r2,
            'r2_improvement': r2_improvement
        })
    elif new_mae:
        results.append({
            'stat': stat,
            'baseline_mae': None,
            'new_mae': new_mae,
            'mae_improvement': None,
            'baseline_r2': None,
            'new_r2': new_features['ensemble'].get(stat, {}).get('r2', None),
            'r2_improvement': None
        })

df = pd.DataFrame(results)

print(f"{'Stat':<20} {'Baseline MAE':<14} {'New MAE':<14} {'MAE %':<10} "
      f"{'Baseline R²':<14} {'New R²':<14} {'R² Change':<12}")
print("-" * 100)

for _, row in df.iterrows():
    if row['baseline_mae'] is not None and not pd.isna(row['baseline_mae']):
        print(f"{row['stat']:<20} {row['baseline_mae']:<14.4f} {row['new_mae']:<14.4f} "
              f"{row['mae_improvement']:>+9.1f}% {row['baseline_r2']:<14.4f} "
              f"{row['new_r2']:<14.4f} {row['r2_improvement']:>+11.4f}")
    else:
        print(f"{row['stat']:<20} {'N/A':<14} {row['new_mae']:<14.4f} "
              f"{'N/A':<10} {'N/A':<14} {row['new_r2']:<14.4f} {'N/A':<12}")

print("-" * 100)

valid_improvements = df['mae_improvement'].dropna()
if len(valid_improvements) > 0:
    avg_mae_improvement = valid_improvements.mean()
    avg_r2_improvement = df['r2_improvement'].dropna().mean()
    
    print(f"{'AVERAGE':<20} {'':<14} {'':<14} {avg_mae_improvement:>+9.1f}% "
          f"{'':<14} {'':<14} {avg_r2_improvement:>+11.4f}")

print("\n" + "="*100)
print("\nKey Observations:")

if len(valid_improvements) > 0:
    print("  1. Average MAE improvement: {:.2f}%".format(avg_mae_improvement))
    print("  2. Average R² improvement: {:.4f}".format(avg_r2_improvement))
    
    improved_stats = len(valid_improvements[valid_improvements > 0])
    total_stats = len(valid_improvements)
    print("  3. Stats improved: {}/{}".format(improved_stats, total_stats))
    
    best_improvement = valid_improvements.max()
    best_stat = df.loc[df['mae_improvement'].idxmax(), 'stat']
    print("  4. Best improvement: {} ({:.2f}%)".format(best_stat, best_improvement))
    
    print("\nNew Features Added:")
    print("  - Temporal features: schedule density, season periods, timezone travel, All-Star break")
    print("  - Opponent turnover/steal features: team-level and position-specific")
    print("  - Total: 21 new features")
    
    if avg_mae_improvement >= 3.0:
        print("\nRESULT: New features achieved significant improvement!")
    elif avg_mae_improvement >= 1.0:
        print("\nRESULT: New features improved performance")
    else:
        print("\nRESULT: New features had minimal impact")

print("="*100)

output_dir = os.path.dirname(new_features_path)
comparison_path = os.path.join(output_dir, 'baseline_vs_new_features_comparison.csv')

df_output = df.copy()
df_output['baseline_mae'] = df_output['baseline_mae'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
df_output['new_mae'] = df_output['new_mae'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
df_output['mae_improvement'] = df_output['mae_improvement'].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A")
df_output['baseline_r2'] = df_output['baseline_r2'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
df_output['new_r2'] = df_output['new_r2'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
df_output['r2_improvement'] = df_output['r2_improvement'].apply(lambda x: f"{x:+.4f}" if pd.notna(x) else "N/A")

df_output.columns = ['Stat', 'Baseline MAE', 'New MAE', 'MAE Improvement %', 'Baseline R²', 'New R²', 'R² Change']

df_output.to_csv(comparison_path, index=False)
print(f"\nComparison saved to: {comparison_path}\n")

