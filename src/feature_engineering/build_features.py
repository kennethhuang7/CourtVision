import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

def build_features_for_training():
    print("Building features for model training...\n")
    
    conn = get_db_connection()
    
    print("Loading player game stats...")
    query = """
        SELECT 
            pgs.player_id,
            pgs.team_id,
            pgs.game_id,
            pgs.points,
            pgs.rebounds_total,
            pgs.assists,
            pgs.steals,
            pgs.blocks,
            pgs.turnovers,
            pgs.three_pointers_made,
            pgs.minutes_played,
            pgs.field_goals_attempted,
            pgs.three_pointers_attempted,
            pgs.free_throws_attempted,
            g.game_date,
            g.season,
            g.game_type,
            g.home_team_id,
            g.away_team_id,
            p.position
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        JOIN players p ON pgs.player_id = p.player_id
        WHERE g.game_status = 'completed'
        ORDER BY pgs.player_id, g.game_date
    """
    
    df = pd.read_sql(query, conn)
    print(f"Loaded {len(df)} records\n")
    
    print("Calculating features...")
    
    print("  - Playoff indicator")
    df['is_playoff'] = (df['game_type'] == 'playoff').astype(int)
    
    print("  - Recent form (L5, L10, L20) - unweighted")
    for window in [5, 10, 20]:
        for stat in ['points', 'rebounds_total', 'assists']:
            df[f'{stat}_l{window}'] = df.groupby('player_id')[stat].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
            )
    
    print("  - Recent form (L5, L10, L20) - exponentially weighted")
    decay_factor = 0.1
    for window in [5, 10, 20]:
        for stat in ['points', 'rebounds_total', 'assists']:
            def exp_weighted_mean(series):
                if len(series) == 0:
                    return np.nan
                weights = np.exp(-decay_factor * np.arange(len(series))[::-1])
                weights = weights / weights.sum()
                return np.sum(series * weights)
            
            df[f'{stat}_l{window}_weighted'] = df.groupby('player_id')[stat].transform(
                lambda x: x.rolling(window=window, min_periods=1).apply(exp_weighted_mean, raw=True).shift(1)
            )
    
    print("  - Teammate dependency features")
    df['star_teammate_out'] = 0
    df['star_teammate_ppg'] = 0.0
    df['games_without_star'] = 0
    
    team_season_stars = df[df['minutes_played'] >= 15].groupby(['player_id', 'team_id', 'season'])['points'].mean()
    team_season_stars = team_season_stars[team_season_stars >= 20].reset_index()
    team_season_stars.columns = ['star_id', 'team_id', 'season', 'star_ppg']
    
    for idx, row in team_season_stars.iterrows():
        star_id = row['star_id']
        team_id = row['team_id']
        season = row['season']
        star_ppg = row['star_ppg']
        
        star_games = set(df[(df['player_id'] == star_id) & 
                            (df['team_id'] == team_id) & 
                            (df['season'] == season) & 
                            (df['minutes_played'] >= 15)]['game_id'])
        
        teammate_mask = ((df['team_id'] == team_id) & 
                        (df['season'] == season) & 
                        (df['player_id'] != star_id) & 
                        (~df['game_id'].isin(star_games)))
        
        df.loc[teammate_mask, 'star_teammate_out'] = 1
        df.loc[teammate_mask, 'star_teammate_ppg'] = star_ppg
    
    for player_id in df[df['star_teammate_out'] == 1]['player_id'].unique():
        player_data = df[df['player_id'] == player_id].copy()
        cumsum = player_data['star_teammate_out'].cumsum()
        df.loc[df['player_id'] == player_id, 'games_without_star'] = cumsum
    
    print("  - Playoff experience")
    df['playoff_games_career'] = df.groupby('player_id')['is_playoff'].cumsum()
    
    print("  - Playoff performance boost")
    playoff_stats = df[df['is_playoff'] == 1].groupby('player_id')['points'].mean()
    regular_stats = df[df['is_playoff'] == 0].groupby('player_id')['points'].mean()
    playoff_boost = (playoff_stats - regular_stats).fillna(0)
    df['playoff_performance_boost'] = df['player_id'].map(playoff_boost).fillna(0)
    
    print("  - Home/away")
    df['is_home'] = (df['team_id'] == df['home_team_id']).astype(int)
    
    print("  - Days rest")
    df['game_date'] = pd.to_datetime(df['game_date'])
    df['days_rest'] = df.groupby('player_id')['game_date'].diff().dt.days
    df['days_rest'] = df['days_rest'].fillna(3)
    df['is_back_to_back'] = (df['days_rest'] == 1).astype(int)
    
    print("  - Games played")
    df['games_played_season'] = df.groupby(['player_id', 'season']).cumcount() + 1
    
    print("  - Team ratings")
    team_ratings = pd.read_sql("""
        SELECT team_id, season, offensive_rating, defensive_rating, pace
        FROM team_ratings
    """, conn)
    
    df = df.merge(
        team_ratings,
        left_on=['team_id', 'season'],
        right_on=['team_id', 'season'],
        how='left'
    )
    
    print("  - Opponent ratings")
    df['opponent_id'] = df.apply(
        lambda row: row['away_team_id'] if row['is_home'] == 1 else row['home_team_id'],
        axis=1
    )
    
    df = df.merge(
        team_ratings,
        left_on=['opponent_id', 'season'],
        right_on=['team_id', 'season'],
        how='left',
        suffixes=('_team', '_opp')
    )
    
    print("  - Opponent defense")
    opp_defense = pd.read_sql("""
        SELECT team_id, season, 
               opp_field_goal_pct,
               opp_three_point_pct
        FROM team_defensive_stats
    """, conn)
    
    df = df.merge(
        opp_defense,
        left_on=['opponent_id', 'season'],
        right_on=['team_id', 'season'],
        how='left',
        suffixes=('', '_oppdef')
    )
    
    print("  - Altitude")
    teams_altitude = pd.read_sql("""
        SELECT team_id, arena_altitude
        FROM teams
    """, conn)
    
    df = df.merge(
        teams_altitude,
        left_on='opponent_id',
        right_on='team_id',
        how='left',
        suffixes=('', '_opp_venue')
    )
    
    df['altitude_away'] = ((df['is_home'] == 0) & (df['arena_altitude'] > 3000)).astype(int)
    
    conn.close()
    
    print("\n" + "="*50)
    print("FEATURES COMPLETE!")
    print("="*50)
    print(f"Total columns: {len(df.columns)}")
    print(f"Total records: {len(df)}")
    print(f"Records with star teammate out: {df['star_teammate_out'].sum()}")
    
    output_path = '../../data/processed/training_features.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")
    
    return df

if __name__ == "__main__":
    build_features_for_training()