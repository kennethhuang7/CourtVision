from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
import pandas as pd
import time

def test_nba_api():
    print("Testing NBA API connection...\n")
    
    all_players = players.get_players()
    lebron = [p for p in all_players if p['full_name'] == 'LeBron James'][0]
    
    print(f"Found player: {lebron['full_name']} (ID: {lebron['id']})")
    
    print("\nFetching game logs...")
    time.sleep(1) 
    
    game_log = playergamelog.PlayerGameLog(
        player_id=lebron['id'],
        season='2024-25'
    )
    
    df = game_log.get_data_frames()[0]
    
    print(f"Fetched {len(df)} games\n")
    print("Last 5 games:")
    print(df[['GAME_DATE', 'MATCHUP', 'PTS', 'REB', 'AST', 'MIN']].head())
    
    print("\nNBA API is working!")
    return df

if __name__ == "__main__":
    test_nba_api()