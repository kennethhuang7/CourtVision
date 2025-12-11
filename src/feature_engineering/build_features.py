import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
from feature_engineering.team_stats_calculator import (
    calculate_team_ratings_as_of_date,
    calculate_team_defensive_stats_as_of_date,
    calculate_position_defense_stats_as_of_date,
    calculate_opponent_team_turnover_stats_as_of_date,
    map_position_to_defense_position
)
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
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
            pgs.field_goals_made,
            pgs.field_goals_attempted,
            pgs.three_pointers_attempted,
            pgs.free_throws_made,
            pgs.free_throws_attempted,
            pgs.usage_rate,
            pgs.true_shooting_pct,
            pgs.offensive_rating,
            pgs.defensive_rating,
            pgs.is_starter,
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
    
    decay_factor = 0.1
    def exp_weighted_mean(series):
        if len(series) == 0:
            return np.nan
        weights = np.exp(-decay_factor * np.arange(len(series))[::-1])
        weights = weights / weights.sum()
        return np.sum(series * weights)
    
    print("  - Playoff indicator")
    df['is_playoff'] = (df['game_type'] == 'playoff').astype(int)
    
    print("  - Recent form (L5, L10, L20) - unweighted")
    for window in [5, 10, 20]:
        for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
            df[f'{stat}_l{window}'] = df.groupby('player_id')[stat].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
            )
    
    print("  - Recent form (L5, L10, L20) - exponentially weighted")
    for window in [5, 10, 20]:
        for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
            df[f'{stat}_l{window}_weighted'] = df.groupby('player_id')[stat].transform(
                lambda x: x.rolling(window=window, min_periods=1).apply(exp_weighted_mean, raw=True).shift(1)
            )
    
    print("  - Minutes played features")
    for window in [5, 10, 20]:
        df[f'minutes_played_l{window}'] = df.groupby('player_id')['minutes_played'].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
        )
        df[f'minutes_played_l{window}_weighted'] = df.groupby('player_id')['minutes_played'].transform(
            lambda x: x.rolling(window=window, min_periods=1).apply(exp_weighted_mean, raw=True).shift(1)
        )
    
    df['is_starter'] = df['is_starter'].astype(int)
    for window in [5, 10]:
        df[f'is_starter_l{window}'] = df.groupby('player_id')['is_starter'].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
        )
    
    print("  - Minutes trend")
    def calc_minutes_trend(group):
        shifted = group.shift(1)
        recent = shifted.tail(10)
        if len(recent) >= 3:
            x = np.arange(len(recent))
            y = recent.values
            if np.std(y) > 0:
                slope = np.polyfit(x, y, 1)[0]
                return slope
        return 0.0
    
    df['minutes_trend'] = df.groupby('player_id')['minutes_played'].transform(calc_minutes_trend)
    
    print("  - Position encoding")
    df['position'] = df['position'].fillna('G')
    position_map = {
        'G': [1, 0, 0],
        'F': [0, 1, 0],
        'C': [0, 0, 1]
    }
    
    def map_position_to_one_hot(pos):
        pos_str = str(pos).upper().strip()
        if ('CENTER' in pos_str or pos_str == 'C') and 'GUARD' not in pos_str and 'FORWARD' not in pos_str:
            return position_map['C']
        elif 'FORWARD' in pos_str or pos_str == 'F' or pos_str == 'F-C':
            return position_map['F']
        elif 'GUARD' in pos_str or pos_str == 'G' or pos_str == 'G-F':
            return position_map['G']
        else:
            return position_map['G']
    
    position_encoded = df['position'].apply(map_position_to_one_hot)
    df['position_guard'] = position_encoded.apply(lambda x: x[0])
    df['position_forward'] = position_encoded.apply(lambda x: x[1])
    df['position_center'] = position_encoded.apply(lambda x: x[2])
    
    print("  - Usage rate features")
    for window in [5, 10, 20]:
        df[f'usage_rate_l{window}'] = df.groupby('player_id')['usage_rate'].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
        )
        df[f'usage_rate_l{window}_weighted'] = df.groupby('player_id')['usage_rate'].transform(
            lambda x: x.rolling(window=window, min_periods=1).apply(exp_weighted_mean, raw=True).shift(1)
        )
    
    print("  - Player-level advanced stats")
    for window in [5, 10, 20]:
        for stat in ['offensive_rating', 'defensive_rating']:
            df[f'{stat}_l{window}'] = df.groupby('player_id')[stat].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
            )
    
    for window in [5, 10, 20]:
        df[f'net_rating_l{window}'] = df[f'offensive_rating_l{window}'] - df[f'defensive_rating_l{window}']
    
    print("  - Shooting percentage features")
    for window in [5, 10, 20]:
        fgm_sum = df.groupby('player_id')['field_goals_made'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        fga_sum = df.groupby('player_id')['field_goals_attempted'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'fg_pct_l{window}'] = np.where(fga_sum > 0, fgm_sum / fga_sum, 0)
        
        made_3p_sum = df.groupby('player_id')['three_pointers_made'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        att_3p_sum = df.groupby('player_id')['three_pointers_attempted'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'three_pct_l{window}'] = np.where(att_3p_sum > 0, made_3p_sum / att_3p_sum, 0)
        
        made_ft_sum = df.groupby('player_id')['free_throws_made'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        att_ft_sum = df.groupby('player_id')['free_throws_attempted'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'ft_pct_l{window}'] = np.where(att_ft_sum > 0, made_ft_sum / att_ft_sum, 0)
        
        df[f'true_shooting_pct_l{window}'] = df.groupby('player_id')['true_shooting_pct'].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean().shift(1)
        )
    
    print("  - Per-minute rate features (per 36 minutes)")
    for stat in ['points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']:
        for window in [5, 10, 20]:
            stat_sum = df.groupby('player_id')[stat].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
            min_sum = df.groupby('player_id')['minutes_played'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
            df[f'{stat}_per_36_l{window}'] = np.where(min_sum > 0, (stat_sum / min_sum) * 36, 0)
    
    print("  - Cross-stat ratio features")
    for window in [5, 10, 20]:
        ast_sum = df.groupby('player_id')['assists'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        tov_sum = df.groupby('player_id')['turnovers'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'ast_to_ratio_l{window}'] = np.where(tov_sum > 0, ast_sum / tov_sum, ast_sum)
        
        pts_sum = df.groupby('player_id')['points'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        fga_sum = df.groupby('player_id')['field_goals_attempted'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'pts_per_fga_l{window}'] = np.where(fga_sum > 0, pts_sum / fga_sum, 0)
        
        ast_sum_pts = df.groupby('player_id')['assists'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        pts_sum_ast = df.groupby('player_id')['points'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'pts_per_ast_l{window}'] = np.where(ast_sum_pts > 0, pts_sum_ast / ast_sum_pts, pts_sum_ast)
        
        reb_sum = df.groupby('player_id')['rebounds_total'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        min_sum_reb = df.groupby('player_id')['minutes_played'].transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).sum())
        df[f'reb_rate_l{window}'] = np.where(min_sum_reb > 0, reb_sum / (min_sum_reb / 36), 0)
    
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
    
    print("  - Opponent ID")
    df['opponent_id'] = df.apply(
        lambda row: row['away_team_id'] if row['is_home'] == 1 else row['home_team_id'],
        axis=1
    )
    
    print("  - Schedule density features")
    df = df.sort_values(['player_id', 'game_date']).reset_index(drop=True)
    
    games_3d = []
    games_7d = []
    for player_id in df['player_id'].unique():
        player_mask = df['player_id'] == player_id
        player_dates = df.loc[player_mask, 'game_date'].values
        player_indices = df.index[player_mask].values
        
        for i, idx in enumerate(player_indices):
            if i == 0:
                games_3d.append(0)
                games_7d.append(0)
            else:
                prev_dates = player_dates[:i]
                days_diff = (player_dates[i] - prev_dates) / np.timedelta64(1, 'D')
                games_3d.append((days_diff <= 3).sum())
                games_7d.append((days_diff <= 7).sum())
    
    df['games_in_last_3_days'] = games_3d
    df['games_in_last_7_days'] = games_7d
    
    df['is_heavy_schedule'] = (df['games_in_last_7_days'] >= 4).astype(int)
    df['is_well_rested'] = (df['days_rest'] >= 3).astype(int)
    
    df['consecutive_games'] = df.groupby('player_id')['game_date'].transform(
        lambda x: (x.diff().dt.days <= 2).groupby((x.diff().dt.days > 2).cumsum()).cumsum()
    )
    df['consecutive_games'] = df['consecutive_games'].fillna(1)
    df['consecutive_games'] = df.groupby('player_id')['consecutive_games'].shift(1).fillna(0)
    
    print("  - Season period features")
    season_starts = df.groupby('season')['game_date'].transform('min')
    df['season_progress'] = (df['game_date'] - season_starts).dt.days / 180.0
    df['season_progress'] = df['season_progress'].clip(upper=1.0, lower=0.0)
    
    df['games_played_season'] = df.groupby(['player_id', 'season']).cumcount() + 1
    df['games_played_season'] = df.groupby(['player_id', 'season'])['games_played_season'].shift(1).fillna(0)
    df['is_early_season'] = (df['games_played_season'] <= 20).astype(int)
    df['is_mid_season'] = ((df['games_played_season'] > 20) & (df['games_played_season'] <= 60)).astype(int)
    df['is_late_season'] = (df['games_played_season'] > 60).astype(int)
    
    df['team_games_played'] = df.groupby(['team_id', 'season'])['game_date'].transform(
        lambda x: x.rank(method='dense')
    )
    df['team_games_played'] = df.groupby(['team_id', 'season'])['team_games_played'].shift(1).fillna(0)
    df['games_remaining'] = 82 - df['team_games_played']
    df['games_remaining'] = df['games_remaining'].clip(lower=0)
    df = df.drop(columns=['team_games_played'], errors='ignore')
    
    print("  - Timezone travel features")
    teams_tz = pd.read_sql("""
        SELECT team_id, timezone
        FROM teams
        WHERE timezone IS NOT NULL
    """, conn)
    
    tz_to_offset = {
        'America/New_York': -5,
        'America/Chicago': -6,
        'America/Denver': -7,
        'America/Los_Angeles': -8,
        'America/Phoenix': -7,
        'America/Anchorage': -9,
        'Pacific/Honolulu': -10,
        'America/Toronto': -5
    }
    
    teams_tz['tz_offset'] = teams_tz['timezone'].map(tz_to_offset).fillna(-6)
    
    df = df.merge(teams_tz[['team_id', 'tz_offset']], on='team_id', how='left', suffixes=('', '_team'))
    df = df.rename(columns={'tz_offset': 'tz_offset_team'})
    df = df.merge(teams_tz[['team_id', 'tz_offset']], left_on='opponent_id', right_on='team_id', how='left', suffixes=('', '_opp'))
    df = df.rename(columns={'tz_offset': 'tz_offset_opp'})
    df['tz_offset_team'] = df['tz_offset_team'].fillna(-6)
    df['tz_offset_opp'] = df['tz_offset_opp'].fillna(-6)
    df['tz_difference'] = df['tz_offset_opp'] - df['tz_offset_team']
    df['west_to_east'] = ((df['is_home'] == 0) & (df['tz_difference'] > 0)).astype(int)
    df['east_to_west'] = ((df['is_home'] == 0) & (df['tz_difference'] < 0)).astype(int)
    
    print("  - All-Star break features")
    all_star_breaks = {
        '2020-21': '2021-03-07',
        '2021-22': '2022-02-20',
        '2022-23': '2023-02-19',
        '2023-24': '2024-02-18',
        '2024-25': '2025-02-16',
        '2025-26': '2026-02-15'
    }
    
    df['asb_date'] = df['season'].map(all_star_breaks)
    df['asb_date'] = pd.to_datetime(df['asb_date'], errors='coerce')
    df['days_since_asb'] = (df['game_date'] - df['asb_date']).dt.days
    df['days_since_asb'] = df['days_since_asb'].clip(lower=-365, upper=365)
    df['days_since_asb'] = df['days_since_asb'].fillna(0)
    df['post_asb_bounce'] = ((df['days_since_asb'] > 0) & (df['days_since_asb'] <= 14)).astype(int)
    df = df.drop(columns=['asb_date'], errors='ignore')
    
    print("  - Defense position mapping")
    df['defense_position'] = df['position'].apply(map_position_to_defense_position)
    
    print("  - Team ratings")
    print("     This may take a few minutes...")
    
    team_date_combos = df[['team_id', 'season', 'game_date']].drop_duplicates()
    team_ratings_list = []
    total_combos = len(team_date_combos)
    
    for i, (idx, row) in enumerate(team_date_combos.iterrows(), 1):
        if i % 100 == 0 or i == total_combos:
            print(f"     Processing team ratings: {i}/{total_combos} ({i/total_combos*100:.1f}%)")
        ratings = calculate_team_ratings_as_of_date(
            conn, row['team_id'], row['season'], row['game_date']
        )
        if ratings:
            ratings['team_id'] = row['team_id']
            ratings['season'] = row['season']
            ratings['game_date'] = row['game_date']
            team_ratings_list.append(ratings)
    
    if team_ratings_list:
        team_ratings_df = pd.DataFrame(team_ratings_list)
        team_ratings_df = team_ratings_df.rename(columns={
            'offensive_rating': 'offensive_rating_team',
            'defensive_rating': 'defensive_rating_team',
            'pace': 'pace_team'
        })
        df = df.merge(
            team_ratings_df,
            on=['team_id', 'season', 'game_date'],
            how='left'
        )
    else:
        df['offensive_rating_team'] = None
        df['defensive_rating_team'] = None
        df['pace_team'] = None
    
    print("  - Opponent ratings (calculating as-of each game date)...")
    opp_date_combos = df[['opponent_id', 'season', 'game_date']].drop_duplicates()
    opp_ratings_list = []
    total_opp = len(opp_date_combos)
    
    for i, (idx, row) in enumerate(opp_date_combos.iterrows(), 1):
        if i % 100 == 0 or i == total_opp:
            print(f"     Processing opponent ratings: {i}/{total_opp} ({i/total_opp*100:.1f}%)")
        ratings = calculate_team_ratings_as_of_date(
            conn, row['opponent_id'], row['season'], row['game_date']
        )
        if ratings:
            ratings['opponent_id'] = row['opponent_id']
            ratings['season'] = row['season']
            ratings['game_date'] = row['game_date']
            opp_ratings_list.append(ratings)
    
    if opp_ratings_list:
        opp_ratings_df = pd.DataFrame(opp_ratings_list)
        opp_ratings_df = opp_ratings_df.rename(columns={
            'offensive_rating': 'offensive_rating_opp',
            'defensive_rating': 'defensive_rating_opp',
            'pace': 'pace_opp'
        })
        df = df.merge(
            opp_ratings_df,
            on=['opponent_id', 'season', 'game_date'],
            how='left'
        )
    else:
        df['offensive_rating_opp'] = None
        df['defensive_rating_opp'] = None
        df['pace_opp'] = None
    
    print("  - Opponent defense stats (calculating as-of each game date)...")
    opp_def_date_combos = df[['opponent_id', 'season', 'game_date']].drop_duplicates()
    opp_def_list = []
    total_def = len(opp_def_date_combos)
    
    for i, (idx, row) in enumerate(opp_def_date_combos.iterrows(), 1):
        if i % 100 == 0 or i == total_def:
            print(f"     Processing opponent defense: {i}/{total_def} ({i/total_def*100:.1f}%)")
        def_stats = calculate_team_defensive_stats_as_of_date(
            conn, row['opponent_id'], row['season'], row['game_date']
        )
        if def_stats:
            def_stats['opponent_id'] = row['opponent_id']
            def_stats['season'] = row['season']
            def_stats['game_date'] = row['game_date']
            opp_def_list.append(def_stats)
    
    if opp_def_list:
        opp_def_df = pd.DataFrame(opp_def_list)
        df = df.merge(
            opp_def_df,
            on=['opponent_id', 'season', 'game_date'],
            how='left'
        )
    else:
        df['opp_field_goal_pct'] = None
        df['opp_three_point_pct'] = None
        df['opp_team_turnovers_per_game'] = None
        df['opp_team_steals_per_game'] = None
    
    print("  - Position-specific opponent defense (calculating as-of each game date)...")
    pos_def_combos = df[['opponent_id', 'season', 'defense_position', 'game_date']].drop_duplicates()
    pos_def_list = []
    total_pos = len(pos_def_combos)
    
    for i, (idx, row) in enumerate(pos_def_combos.iterrows(), 1):
        if i % 100 == 0 or i == total_pos:
            print(f"     Processing position defense: {i}/{total_pos} ({i/total_pos*100:.1f}%)")
        pos_stats = calculate_position_defense_stats_as_of_date(
            conn, row['opponent_id'], row['season'], row['defense_position'], row['game_date']
        )
        if pos_stats:
            pos_stats['opponent_id'] = row['opponent_id']
            pos_stats['season'] = row['season']
            pos_stats['defense_position'] = row['defense_position']
            pos_stats['game_date'] = row['game_date']
            pos_def_list.append(pos_stats)
    
    if pos_def_list:
        pos_def_df = pd.DataFrame(pos_def_list)
        df = df.merge(
            pos_def_df,
            on=['opponent_id', 'season', 'defense_position', 'game_date'],
            how='left'
        )
    else:
        df['opp_points_allowed_to_position'] = None
        df['opp_rebounds_allowed_to_position'] = None
        df['opp_assists_allowed_to_position'] = None
        df['opp_blocks_allowed_to_position'] = None
        df['opp_three_pointers_allowed_to_position'] = None
        df['opp_position_turnovers_vs_team'] = None
        df['opp_position_steals_vs_team'] = None
    
    print("  - Opponent team turnover stats by position (calculating as-of each game date)...")
    opp_turnover_combos = df[['opponent_id', 'season', 'defense_position', 'game_date']].drop_duplicates()
    opp_turnover_list = []
    total_turnover = len(opp_turnover_combos)
    
    for i, (idx, row) in enumerate(opp_turnover_combos.iterrows(), 1):
        if i % 100 == 0 or i == total_turnover:
            print(f"     Processing opponent turnovers: {i}/{total_turnover} ({i/total_turnover*100:.1f}%)")
        turnover_stats = calculate_opponent_team_turnover_stats_as_of_date(
            conn, row['opponent_id'], row['season'], row['defense_position'], row['game_date']
        )
        if turnover_stats:
            turnover_stats['opponent_id'] = row['opponent_id']
            turnover_stats['season'] = row['season']
            turnover_stats['defense_position'] = row['defense_position']
            turnover_stats['game_date'] = row['game_date']
            opp_turnover_list.append(turnover_stats)
    
    if opp_turnover_list:
        opp_turnover_df = pd.DataFrame(opp_turnover_list)
        df = df.merge(
            opp_turnover_df,
            on=['opponent_id', 'season', 'defense_position', 'game_date'],
            how='left'
        )
    else:
        df['opp_position_steals_overall'] = None
        df['opp_position_turnovers_overall'] = None
    
    df = df.drop(columns=['defense_position'], errors='ignore')
    
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
    
    df['altitude_away'] = ((df['is_home'] == 0) & (df['arena_altitude'].notna()) & (df['arena_altitude'] > 3000)).astype(int)
    
    conn.close()
    
    print("\n" + "="*50)
    print("FEATURES COMPLETE!")
    print("="*50)
    print(f"Total columns: {len(df.columns)}")
    print(f"Total records: {len(df)}")
    print(f"Records with star teammate out: {df['star_teammate_out'].sum()}")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    output_path = os.path.join(project_root, 'data', 'processed', 'training_features.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")
    
    return df

if __name__ == "__main__":
    build_features_for_training()