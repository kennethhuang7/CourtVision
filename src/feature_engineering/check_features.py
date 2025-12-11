import pandas as pd
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
features_path = os.path.join(project_root, 'data', 'processed', 'training_features.csv')

print("="*60)
print("CHECKING TRAINING FEATURES")
print("="*60)

df = pd.read_csv(features_path)

print(f"\nTotal records: {len(df):,}")
print(f"Total columns: {len(df.columns)}")

print("\n" + "="*60)
print("KEY FEATURE COLUMNS CHECK")
print("="*60)

key_features = {
    'Team Ratings (Player Team)': [
        'offensive_rating_team',
        'defensive_rating_team', 
        'pace_team'
    ],
    'Team Ratings (Opponent)': [
        'offensive_rating_opp',
        'defensive_rating_opp',
        'pace_opp'
    ],
    'Position Defense Stats': [
        'opp_points_allowed_to_position',
        'opp_rebounds_allowed_to_position',
        'opp_assists_allowed_to_position',
        'opp_steals_allowed_to_position',
        'opp_blocks_allowed_to_position',
        'opp_turnovers_forced_to_position',
        'opp_three_pointers_allowed_to_position'
    ],
    'Other Key Features': [
        'points_l5', 'points_l10', 'points_l20',
        'is_home', 'days_rest', 'games_played_season',
        'star_teammate_out', 'is_playoff'
    ]
}

all_good = True
for category, cols in key_features.items():
    print(f"\n{category}:")
    for col in cols:
        if col in df.columns:
            null_count = df[col].isna().sum()
            null_pct = 100 * null_count / len(df)
            status = "[OK]" if null_pct < 50 else "[WARN]"
            print(f"  {status} {col}: {null_pct:.1f}% null ({len(df) - null_count:,} non-null)")
            if null_pct >= 50:
                all_good = False
        else:
            print(f"  [MISSING] {col}: MISSING!")
            all_good = False

print("\n" + "="*60)
print("CHECKING FOR OLD COLUMN NAMES (should not exist)")
print("="*60)

old_cols = ['offensive_rating', 'defensive_rating', 'pace']
found_old = False
for col in old_cols:
    if col in df.columns:
        print(f"  [FOUND] Found old column: {col}")
        found_old = True
        all_good = False

if not found_old:
    print("  [OK] No old column names found")

print("\n" + "="*60)
print("SAMPLE VALUES (first row)")
print("="*60)
if len(df) > 0:
    sample = df.iloc[0]
    print(f"offensive_rating_team: {sample.get('offensive_rating_team', 'N/A')}")
    print(f"offensive_rating_opp: {sample.get('offensive_rating_opp', 'N/A')}")
    print(f"opp_points_allowed_to_position: {sample.get('opp_points_allowed_to_position', 'N/A')}")
    print(f"points_l5: {sample.get('points_l5', 'N/A')}")

print("\n" + "="*60)
if all_good:
    print("ALL CHECKS PASSED! Features look good for training.")
else:
    print("SOME ISSUES FOUND. Review the output above.")
print("="*60)

