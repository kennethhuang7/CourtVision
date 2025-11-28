from nba_api.stats.endpoints import boxscoretraditionalv3
import pandas as pd

game_id = '0022200001'

print(f"Testing boxscore for game {game_id}...")

boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
player_stats = boxscore.get_data_frames()[0]

print(f"\nFound {len(player_stats)} players")
print(f"\nColumn names:")
print(player_stats.columns.tolist())

print(f"\nFirst player data:")
print(player_stats.iloc[0])

print(f"\nSample values:")
first_row = player_stats.iloc[0]
print(f"personId: {first_row.get('personId')}")
print(f"teamId: {first_row.get('teamId')}")
print(f"points: {first_row.get('points')}")
print(f"minutes: {first_row.get('minutes')}")