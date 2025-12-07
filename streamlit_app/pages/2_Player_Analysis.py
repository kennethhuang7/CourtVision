import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
import plotly.express as px
import re
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_ensemble_selection

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

def normalize_name(name):
    char_map = {
        'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ã': 'a', 'å': 'a', 'ā': 'a', 'ă': 'a', 'ą': 'a', 'ǎ': 'a',
        'Á': 'a', 'À': 'a', 'Ä': 'a', 'Â': 'a', 'Ã': 'a', 'Å': 'a', 'Ā': 'a', 'Ă': 'a', 'Ą': 'a', 'Ǎ': 'a',
        'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e', 'ē': 'e', 'ė': 'e', 'ę': 'e', 'ě': 'e',
        'É': 'e', 'È': 'e', 'Ë': 'e', 'Ê': 'e', 'Ē': 'e', 'Ė': 'e', 'Ę': 'e', 'Ě': 'e',
        'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i', 'ī': 'i', 'į': 'i', 'ı': 'i',
        'Í': 'i', 'Ì': 'i', 'Ï': 'i', 'Î': 'i', 'Ī': 'i', 'Į': 'i', 'İ': 'i',
        'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'õ': 'o', 'ø': 'o', 'ō': 'o', 'ő': 'o', 'ǫ': 'o',
        'Ó': 'o', 'Ò': 'o', 'Ö': 'o', 'Ô': 'o', 'Õ': 'o', 'Ø': 'o', 'Ō': 'o', 'Ő': 'o', 'Ǫ': 'o',
        'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u', 'ū': 'u', 'ů': 'u', 'ű': 'u', 'ų': 'u',
        'Ú': 'u', 'Ù': 'u', 'Ü': 'u', 'Û': 'u', 'Ū': 'u', 'Ů': 'u', 'Ű': 'u', 'Ų': 'u',
        'ý': 'y', 'ÿ': 'y', 'ŷ': 'y', 'Ý': 'y', 'Ÿ': 'y', 'Ŷ': 'y',
        'ç': 'c', 'ć': 'c', 'č': 'c', 'ĉ': 'c', 'ċ': 'c',
        'Ç': 'c', 'Ć': 'c', 'Č': 'c', 'Ĉ': 'c', 'Ċ': 'c',
        'đ': 'd', 'Đ': 'd',
        'ñ': 'n', 'ń': 'n', 'ň': 'n', 'ņ': 'n',
        'Ñ': 'n', 'Ń': 'n', 'Ň': 'n', 'Ņ': 'n',
        'š': 's', 'ś': 's', 'ş': 's',
        'Š': 's', 'Ś': 's', 'Ş': 's',
        'ž': 'z', 'ź': 'z', 'ż': 'z',
        'Ž': 'z', 'Ź': 'z', 'Ż': 'z',
        'ģ': 'g', 'Ģ': 'g',
        'ķ': 'k', 'Ķ': 'k',
        'ļ': 'l', 'Ļ': 'l',
    }
    normalized = ''.join(char_map.get(c, c) for c in str(name))
    normalized = normalized.lower().strip()
    return normalized

load_dotenv()

st.set_page_config(
    page_title="Player Analysis - NBA Predictions",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_db_connection():
    try:
        if os.getenv('DATABASE_URL'):
            return psycopg2.connect(os.getenv('DATABASE_URL'))
        else:
            return psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        raise

def load_custom_css():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        
        * {
            font-family: 'Inter', sans-serif;
        }
        
        .main {
            background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%);
            padding: 1.5rem 2rem 2rem 2rem;
            margin-top: 1.5rem;
        }
        
        h1 {
            color: #d4af37 !important;
            font-weight: 700;
            font-size: 2.5rem;
            margin: 0 0 0 0 !important;
            letter-spacing: -0.02em;
            padding-top: 0 !important;
            padding-bottom: 0 !important;
        }

        [data-testid="stAppViewContainer"] {
            padding-top: 0rem !important;
            margin-top: 0rem !important;
        }

        .block-container {
            padding-top: 0.25rem !important;
            margin-top: 0 !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            margin-bottom: 0.5rem;
            margin-top: 0 !important;
            padding-top: 0 !important;
        }
        
        .player-info-card {
            background: #1e1e1e;
            border: 2px solid #2d2d2d;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 2.5rem;
        }
        
        .player-info-card h3 {
            color: #d4af37;
            font-size: 1.2rem;
            margin-bottom: 1rem;
            font-weight: 700;
        }
        
        .player-header {
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .player-photo-large {
            width: 100px;
            height: 100px;
            border-radius: 50%;
            border: 3px solid #d4af37;
            object-fit: cover;
            background: #1e1e1e;
            box-shadow: 0 2px 8px rgba(212, 175, 55, 0.3);
        }
        
        .player-name-large {
            color: #ffffff;
            font-size: 1.5rem;
            font-weight: 700;
            margin: 0;
        }
        
        .player-meta {
            color: #cccccc;
            font-size: 0.95rem;
            margin-top: 0.25rem;
        }
        
        .stats-section {
            margin-top: 1rem;
        }
        
        .stats-section-title {
            color: #d4af37;
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 0.75rem;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 0.75rem;
            font-size: 0.85rem;
        }
        
        .stat-item {
            color: #cccccc;
        }
        
        .stat-item strong {
            color: #ffffff;
            font-weight: 600;
        }
        
        div[data-testid="column"]:nth-child(2),
        div[data-testid="column"]:nth-child(3) {
            padding-left: 2rem !important;
        }
        
        .stSelectbox label, .stSlider label, .stRadio label {
            color: #ffffff !important;
        }
        
        .stCheckbox label {
            color: #cccccc !important;
        }
        
        div[data-testid="stMetricContainer"]:not(:has(div[data-testid="stMetricDelta"])) div[data-testid="stMetricValue"] {
            color: #d4af37 !important;
        }
        
        div[data-testid="column"]:nth-child(2) div[data-testid="stMetricContainer"]:not(:has(div[data-testid="stMetricDelta"])) div[data-testid="stMetricValue"] {
            color: #d4af37 !important;
        }
        
        div[data-testid="stDataFrame"] table tbody td:nth-child(3),
        div[data-testid="stDataFrame"] table thead th:nth-child(3),
        div[data-testid="stDataFrame"] table tbody tr td:last-child,
        div[data-testid="stDataFrame"] table thead tr th:last-child,
        table[data-testid="stDataFrame"] tbody td:nth-child(3),
        table[data-testid="stDataFrame"] thead th:nth-child(3),
        .stDataFrame table tbody td:nth-child(3),
        .stDataFrame table thead th:nth-child(3) {
            text-align: left !important;
            padding-left: 0.5rem !important;
        }
        
        div[data-testid="stDataFrame"] table tbody td[style*="text-align: right"],
        div[data-testid="stDataFrame"] table tbody td[style*="text-align:right"] {
            text-align: left !important;
        }
        
        .context-info-card-wrapper {
            display: flex;
            flex-direction: column;
            height: 100%;
            min-height: 130px;
            margin: 0;
            padding: 0;
        }
        
        div[data-testid="stPlotlyChart"] {
            margin-bottom: 0 !important;
        }
        
        .element-container:has(.info-cards-grid) {
            margin-top: -2rem !important;
            padding-top: 0 !important;
        }
        
        .element-container:has(div[data-testid="stPlotlyChart"]) {
            margin-bottom: 0 !important;
            padding-bottom: 0 !important;
        }
        
        .info-cards-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 0.5rem;
            margin-top: 0 !important;
            width: 100%;
        }
        
        .context-info-card-wrapper {
            width: 100%;
            margin: 0;
            padding: 0;
        }
        
        .context-info-card {
            background: #1e1e1e;
            border: 2px solid #2d2d2d;
            border-radius: 12px;
            padding: 1.25rem;
            margin-bottom: 0;
            height: 100%;
            display: flex;
            flex-direction: column;
            flex-grow: 1;
        }
        
        .context-info-card h4 {
            color: #d4af37;
            font-size: 0.95rem;
            font-weight: 700;
            margin: 0 0 0.08rem 0;
        }
        
        .context-info-card p {
            color: #cccccc;
            font-size: 0.85rem;
            margin: 0.5rem 0;
            line-height: 1.5;
            flex-grow: 1;
        }
        
        .context-info-card .highlight {
            color: #ffffff;
            font-weight: 600;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <script>
    (function() {
        function colorPredictionMetric() {
            const metrics = document.querySelectorAll('div[data-testid="stMetricContainer"]');
            metrics.forEach(function(metric) {
                const hasDelta = metric.querySelector('div[data-testid="stMetricDelta"]');
                if (!hasDelta) {
                    const value = metric.querySelector('div[data-testid="stMetricValue"]');
                    if (value) {
                        value.style.color = '#d4af37';
                    }
                }
            });
        }
        colorPredictionMetric();
        setTimeout(colorPredictionMetric, 100);
        setTimeout(colorPredictionMetric, 500);
    })();
    </script>
    """, unsafe_allow_html=True)

def get_player_photo_url(player_id):
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"

def get_player_info(player_id):
    conn = get_db_connection()
    player_id_int = int(player_id)
    
    query = """
        SELECT 
            p.full_name,
            p.position,
            t.full_name as team_name,
            t.abbreviation as team_abbr,
            pcs.career_games,
            pcs.career_points,
            pcs.career_rebounds,
            pcs.career_assists,
            pcs.career_steals,
            pcs.career_blocks
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN player_career_stats pcs ON p.player_id = pcs.player_id
        WHERE p.player_id = %s
        ORDER BY pcs.updated_date DESC NULLS LAST
        LIMIT 1
    """
    
    result = pd.read_sql(query, conn, params=(player_id_int,))
    conn.close()
    
    if len(result) > 0:
        return result.iloc[0].to_dict()
    return None

def get_season_averages(player_id, season='2025-26'):
    conn = get_db_connection()
    player_id_int = int(player_id)
    
    query = """
        SELECT 
            AVG(points) as avg_points,
            AVG(rebounds_total) as avg_rebounds,
            AVG(assists) as avg_assists,
            AVG(steals) as avg_steals,
            AVG(blocks) as avg_blocks,
            AVG(turnovers) as avg_turnovers,
            AVG(three_pointers_made) as avg_three_pointers,
            COUNT(*) as games_played
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = %s
        AND g.season = %s
        AND g.game_status = 'completed'
    """
    
    result = pd.read_sql(query, conn, params=(player_id_int, season))
    conn.close()
    
    if len(result) > 0 and result.iloc[0]['games_played'] > 0:
        return result.iloc[0].to_dict()
    return None

def get_todays_games():
    conn = get_db_connection()
    today = datetime.now().date()
    
    query = """
        SELECT DISTINCT
            g.game_id,
            g.game_date,
            t1.full_name as home_team,
            t2.full_name as away_team,
            t1.team_id as home_team_id,
            t2.team_id as away_team_id
        FROM games g
        JOIN teams t1 ON g.home_team_id = t1.team_id
        JOIN teams t2 ON g.away_team_id = t2.team_id
        WHERE g.game_date = %s
        AND g.game_status = 'scheduled'
        ORDER BY g.game_id
    """
    
    df = pd.read_sql(query, conn, params=(today,))
    conn.close()
    return df

def get_players_for_game(game_id):
    conn = get_db_connection()
    
    query = """
        SELECT DISTINCT
            p.player_id,
            p.full_name,
            p.position,
            t.abbreviation as team_abbr
        FROM predictions pred
        JOIN players p ON pred.player_id = p.player_id
        JOIN teams t ON p.team_id = t.team_id
        WHERE pred.game_id = %s
        ORDER BY p.full_name
    """
    
    df = pd.read_sql(query, conn, params=(game_id,))
    conn.close()
    return df

def get_player_historical_stats(player_id, opponent_id, is_home, window_size, filter_h2h, filter_home_away, current_team_only, player_team_id, exclude_dnp=True, min_minutes=0):
    conn = get_db_connection()
    
    player_id_int = int(player_id)
    
    filter_conditions = []
    query_params = []
    
    if filter_h2h and opponent_id:
        opponent_id_int = int(opponent_id)
        filter_conditions.append("""
            ((g.home_team_id = %s AND pgs.team_id = g.away_team_id)
             OR (g.away_team_id = %s AND pgs.team_id = g.home_team_id))
        """)
        query_params.extend([opponent_id_int, opponent_id_int])
    
    if filter_home_away:
        if is_home:
            filter_conditions.append("pgs.team_id = g.home_team_id")
        else:
            filter_conditions.append("pgs.team_id = g.away_team_id")
    
    if current_team_only and player_team_id:
        filter_conditions.append("pgs.team_id = %s")
        query_params.append(player_team_id)
    
    if exclude_dnp:
        filter_conditions.append("pgs.minutes_played > 0")
        filter_conditions.append("pgs.minutes_played IS NOT NULL")
    
    if min_minutes > 0:
        filter_conditions.append("pgs.minutes_played >= %s")
        query_params.append(min_minutes)
    
    filter_condition = ""
    if filter_conditions:
        filter_condition = "AND " + " AND ".join(filter_conditions)
    
    limit_size = window_size * 3 if exclude_dnp else window_size
    
    query = f"""
        SELECT 
            g.game_date,
            pgs.points,
            pgs.rebounds_total,
            pgs.assists,
            pgs.steals,
            pgs.blocks,
            pgs.turnovers,
            pgs.three_pointers_made,
            pgs.team_id as player_team_id,
            g.home_team_id,
            g.away_team_id,
            t_home.abbreviation as home_team_abbr,
            t_away.abbreviation as away_team_abbr,
            pgs.minutes_played
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        JOIN teams t_home ON g.home_team_id = t_home.team_id
        JOIN teams t_away ON g.away_team_id = t_away.team_id
        WHERE pgs.player_id = %s
        AND g.game_status = 'completed'
        {filter_condition}
        ORDER BY g.game_date DESC
        LIMIT %s
    """
    
    all_params = (player_id_int,) + tuple(query_params) + (limit_size,)
    df = pd.read_sql(query, conn, params=all_params)
    conn.close()
    
    if exclude_dnp:
        df = df[(df['minutes_played'] > 0) & (df['minutes_played'].notna())].copy()
    
    if min_minutes > 0:
        df = df[df['minutes_played'] >= min_minutes].copy()
    
    df = df.head(window_size).copy()
    
    return df

def get_player_prediction(player_id, game_id, stat_name):
    conn = get_db_connection()
    
    stat_column_map = {
        'points': 'predicted_points',
        'rebounds': 'predicted_rebounds',
        'assists': 'predicted_assists',
        'steals': 'predicted_steals',
        'blocks': 'predicted_blocks',
        'turnovers': 'predicted_turnovers',
        'three_pointers_made': 'predicted_three_pointers_made'
    }
    
    column = stat_column_map.get(stat_name)
    if not column:
        return None
    
    ensemble_models = get_ensemble_selection()
    
    if len(ensemble_models) == 0:
        ensemble_models = ['xgboost']
    
    placeholders = ','.join(['%s'] * len(ensemble_models))
    
    query = f"""
        SELECT AVG({column}) as prediction
        FROM predictions
        WHERE player_id = %s AND game_id = %s AND model_version IN ({placeholders})
    """
    
    player_id_int = int(player_id)
    game_id_str = str(game_id)
    
    params = [player_id_int, game_id_str] + ensemble_models
    
    result = pd.read_sql(query, conn, params=params)
    conn.close()
    
    if len(result) > 0 and pd.notna(result.iloc[0]['prediction']):
        return float(result.iloc[0]['prediction'])
    return None

def get_position_defense_ranking(opponent_id, season, position, stat_name):
    conn = get_db_connection()
    
    stat_column_map = {
        'points': 'points_allowed_per_game',
        'rebounds': 'rebounds_allowed_per_game',
        'assists': 'assists_allowed_per_game',
        'steals': 'steals_allowed_per_game',
        'blocks': 'blocks_allowed_per_game',
        'turnovers': 'turnovers_forced_per_game',
        'three_pointers_made': 'three_pointers_made_allowed_per_game'
    }
    
    stat_column = stat_column_map.get(stat_name)
    if not stat_column:
        conn.close()
        return None
    
    def map_position_to_defense_position(pos):
        if pd.isna(pos):
            return 'G'
        pos_str = str(pos).upper()
        if 'C' in pos_str and 'G' not in pos_str and 'F' not in pos_str:
            return 'C'
        elif 'F' in pos_str and 'G' not in pos_str:
            return 'F'
        else:
            return 'G'
    
    defense_position = map_position_to_defense_position(position)
    
    query = f"""
        SELECT pds.{stat_column}, pds.team_id, t.abbreviation as team_abbr
        FROM position_defense_stats pds
        JOIN teams t ON pds.team_id = t.team_id
        WHERE pds.season = %s AND pds.position = %s
        ORDER BY pds.{stat_column} {'DESC' if stat_name == 'turnovers' else 'ASC'}
    """
    
    df = pd.read_sql(query, conn, params=(season, defense_position))
    conn.close()
    
    if len(df) == 0:
        return None
    
    opponent_row = df[df['team_id'] == opponent_id]
    if len(opponent_row) == 0:
        return None
    
    rank = (df[stat_column] <= opponent_row.iloc[0][stat_column]).sum() if stat_name != 'turnovers' else (df[stat_column] >= opponent_row.iloc[0][stat_column]).sum()
    value = opponent_row.iloc[0][stat_column]
    team_abbr = opponent_row.iloc[0]['team_abbr']
    total_teams = len(df)
    
    is_best = rank == 1 if stat_name != 'turnovers' else rank == total_teams
    is_worst = rank == total_teams if stat_name != 'turnovers' else rank == 1
    
    if is_best:
        rank_direction = "best" if stat_name != 'turnovers' else "worst"
    elif is_worst:
        rank_direction = "worst" if stat_name != 'turnovers' else "best"
    else:
        rank_direction = "highest" if rank <= total_teams / 2 else "lowest"
    
    return {'rank': rank, 'value': value, 'total_teams': total_teams, 'team_abbr': team_abbr, 'rank_direction': rank_direction}

def get_star_players_out_info(player_id, team_id, season, target_date, stat_name):
    conn = get_db_connection()
    
    stat_column_map = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    stat_column = stat_column_map.get(stat_name)
    if not stat_column:
        conn.close()
        return None
    
    star_query = f"""
        SELECT DISTINCT pgs2.player_id, AVG(pgs2.points) as ppg
        FROM player_game_stats pgs2
        JOIN games g2 ON pgs2.game_id = g2.game_id
        WHERE pgs2.team_id = {team_id}
        AND g2.season = '{season}'
        AND g2.game_date < '{target_date}'
        AND pgs2.player_id != {player_id}
        AND pgs2.minutes_played >= 15
        GROUP BY pgs2.player_id
        HAVING AVG(pgs2.points) >= 20
    """
    
    star_teammates = pd.read_sql(star_query, conn)
    
    if len(star_teammates) == 0:
        conn.close()
        return None
    
    stars_out = []
    stars_out_names = []
    for _, star in star_teammates.iterrows():
        star_id = star['player_id']
        
        player_name_query = f"""
            SELECT full_name
            FROM players
            WHERE player_id = {star_id}
        """
        player_name_result = pd.read_sql(player_name_query, conn)
        star_name = player_name_result.iloc[0]['full_name'] if len(player_name_result) > 0 else f"Player {star_id}"
        
        injury_query = f"""
            SELECT COUNT(*)
            FROM injuries
            WHERE player_id = {star_id}
            AND injury_status = 'Out'
            AND report_date <= '{target_date}'
            AND (return_date IS NULL OR return_date > '{target_date}')
        """
        
        star_out = pd.read_sql(injury_query, conn).iloc[0][0]
        
        if star_out > 0:
            stars_out_names.append(star_name)
            
            with_star_query = f"""
                SELECT AVG(pgs.{stat_column}) as avg_stat, COUNT(*) as games
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = {player_id}
                AND pgs.team_id = {team_id}
                AND g.season = '{season}'
                AND g.game_date < '{target_date}'
                AND EXISTS (
                    SELECT 1 FROM player_game_stats pgs2
                    WHERE pgs2.game_id = pgs.game_id
                    AND pgs2.player_id = {star_id}
                    AND pgs2.minutes_played >= 15
                )
            """
            
            without_star_query = f"""
                SELECT AVG(pgs.{stat_column}) as avg_stat, COUNT(*) as games
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = {player_id}
                AND pgs.team_id = {team_id}
                AND g.season = '{season}'
                AND g.game_date < '{target_date}'
                AND NOT EXISTS (
                    SELECT 1 FROM player_game_stats pgs2
                    WHERE pgs2.game_id = pgs.game_id
                    AND pgs2.player_id = {star_id}
                    AND pgs2.minutes_played >= 15
                )
            """
            
            with_star = pd.read_sql(with_star_query, conn)
            without_star = pd.read_sql(without_star_query, conn)
            
            if len(with_star) > 0 and len(without_star) > 0:
                with_stat = with_star.iloc[0]['avg_stat']
                without_stat = without_star.iloc[0]['avg_stat']
                games_without = without_star.iloc[0]['games'] or 0
                
                if with_stat is not None and without_stat is not None and games_without > 0:
                    change = without_stat - with_stat
                    stars_out.append({
                        'player_id': star_id,
                        'player_name': star_name,
                        'change': change,
                        'games_without': games_without
                    })
    
    conn.close()
    
    if len(stars_out_names) == 0:
        return None
    
    if len(stars_out) == 0:
        return {'stars_out_names': stars_out_names, 'insufficient_data': True}
    
    total_change = sum(s['change'] for s in stars_out)
    total_games = sum(s['games_without'] for s in stars_out)
    
    return {'total_change': total_change, 'total_games': total_games, 'stars': stars_out, 'stars_out_names': stars_out_names}

def get_rest_days(team_id, opponent_id, target_date, season):
    conn = get_db_connection()
    
    team_query = f"""
        SELECT MAX(g.game_date) as last_game_date
        FROM games g
        WHERE (g.home_team_id = {team_id} OR g.away_team_id = {team_id})
        AND g.season = '{season}'
        AND g.game_status = 'completed'
        AND g.game_date < '{target_date}'
    """
    
    opp_query = f"""
        SELECT MAX(g.game_date) as last_game_date
        FROM games g
        WHERE (g.home_team_id = {opponent_id} OR g.away_team_id = {opponent_id})
        AND g.season = '{season}'
        AND g.game_status = 'completed'
        AND g.game_date < '{target_date}'
    """
    
    team_result = pd.read_sql(team_query, conn)
    opp_result = pd.read_sql(opp_query, conn)
    
    conn.close()
    
    team_rest = None
    opp_rest = None
    
    if len(team_result) > 0 and team_result.iloc[0]['last_game_date'] is not None:
        team_last = pd.to_datetime(team_result.iloc[0]['last_game_date']).date()
        target = pd.to_datetime(target_date).date()
        team_rest = (target - team_last).days
    
    if len(opp_result) > 0 and opp_result.iloc[0]['last_game_date'] is not None:
        opp_last = pd.to_datetime(opp_result.iloc[0]['last_game_date']).date()
        target = pd.to_datetime(target_date).date()
        opp_rest = (target - opp_last).days
    
    return {'team_rest': team_rest, 'opponent_rest': opp_rest}

def get_playoff_experience_impact(player_id, stat_name, game_type):
    if game_type != 'playoff':
        return {'is_playoff': False}
    
    conn = get_db_connection()
    
    stat_column_map = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    stat_column = stat_column_map.get(stat_name)
    if not stat_column:
        conn.close()
        return {'is_playoff': True, 'insufficient_data': True}
    
    playoff_query = f"""
        SELECT AVG(pgs.{stat_column}) as avg_stat, COUNT(*) as games
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
        AND g.game_type = 'playoff'
        AND g.game_status = 'completed'
    """
    
    regular_query = f"""
        SELECT AVG(pgs.{stat_column}) as avg_stat, COUNT(*) as games
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.player_id = {player_id}
        AND g.game_type = 'regular_season'
        AND g.game_status = 'completed'
    """
    
    playoff_result = pd.read_sql(playoff_query, conn)
    regular_result = pd.read_sql(regular_query, conn)
    
    conn.close()
    
    if len(playoff_result) == 0 or playoff_result.iloc[0]['games'] < 3:
        return {'is_playoff': True, 'insufficient_data': True}
    
    if len(regular_result) == 0:
        return {'is_playoff': True, 'insufficient_data': True}
    
    playoff_avg = playoff_result.iloc[0]['avg_stat'] or 0
    regular_avg = regular_result.iloc[0]['avg_stat'] or 0
    change = playoff_avg - regular_avg
    
    return {
        'is_playoff': True,
        'change': change,
        'playoff_avg': playoff_avg,
        'regular_avg': regular_avg,
        'playoff_games': playoff_result.iloc[0]['games']
    }

def get_pace_comparison(team_id, opponent_id, season):
    conn = get_db_connection()
    
    team_query = f"""
        SELECT pace, t.full_name as team_name
        FROM team_ratings tr
        JOIN teams t ON tr.team_id = t.team_id
        WHERE tr.team_id = {team_id} AND tr.season = '{season}'
    """
    
    opp_query = f"""
        SELECT pace, t.full_name as team_name
        FROM team_ratings tr
        JOIN teams t ON tr.team_id = t.team_id
        WHERE tr.team_id = {opponent_id} AND tr.season = '{season}'
    """
    
    league_query = f"""
        SELECT AVG(pace) as avg_pace
        FROM team_ratings
        WHERE season = '{season}'
    """
    
    team_result = pd.read_sql(team_query, conn)
    opp_result = pd.read_sql(opp_query, conn)
    league_result = pd.read_sql(league_query, conn)
    
    conn.close()
    
    if len(team_result) == 0 or len(opp_result) == 0 or len(league_result) == 0:
        return None
    
    team_pace_raw = team_result.iloc[0]['pace']
    opp_pace_raw = opp_result.iloc[0]['pace']
    league_avg_raw = league_result.iloc[0]['avg_pace']
    
    if pd.isna(team_pace_raw) or pd.isna(opp_pace_raw) or pd.isna(league_avg_raw) or team_pace_raw is None or opp_pace_raw is None or league_avg_raw is None:
        return None
    
    team_pace = float(team_pace_raw)
    opp_pace = float(opp_pace_raw)
    league_avg = float(league_avg_raw)
    team_name = team_result.iloc[0]['team_name']
    opp_name = opp_result.iloc[0]['team_name']
    
    if team_pace == 0 or opp_pace == 0 or league_avg == 0:
        return None
    
    avg_pace = (team_pace + opp_pace) / 2
    pct_diff = ((avg_pace - league_avg) / league_avg) * 100
    
    return {
        'team_pace': team_pace,
        'opp_pace': opp_pace,
        'team_name': team_name,
        'opp_name': opp_name,
        'avg_pace': avg_pace,
        'league_avg': league_avg,
        'pct_diff': pct_diff
    }

def calculate_over_under_percentage(stats_df, stat_name, line_value, over_under):
    if len(stats_df) == 0:
        return 0.0
    
    stat_column_map = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    column = stat_column_map.get(stat_name)
    if column not in stats_df.columns:
        return 0.0
    
    if over_under == "Over":
        hits = (stats_df[column] >= line_value).sum()
    else:
        hits = (stats_df[column] < line_value).sum()
    
    return (hits / len(stats_df)) * 100

def main():
    load_custom_css()
    
    st.markdown("""
    <h1>Player Analysis</h1>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <p class="subtitle">Analyze player performance trends and compare against betting lines</p>
    """, unsafe_allow_html=True)
    
    today = datetime.now().date()
    games_df = get_todays_games()
    
    if len(games_df) == 0:
        st.warning(f"No scheduled games found for today ({today})")
        return
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        game_options = [f"{row['away_team']} @ {row['home_team']}" for _, row in games_df.iterrows()]
        selected_game_idx = st.selectbox("Select Game", range(len(game_options)), format_func=lambda x: game_options[x])
        selected_game = games_df.iloc[selected_game_idx]
        game_id = selected_game['game_id']
        home_team_id = selected_game['home_team_id']
        away_team_id = selected_game['away_team_id']
    
    players_df = get_players_for_game(game_id)
    
    if len(players_df) == 0:
        st.warning("No players found for this game")
        return
    
    with col2:
        player_options = [f"{row['full_name']} ({row['team_abbr']})" for _, row in players_df.iterrows()]
        selected_player_idx = st.selectbox("Select Player", range(len(player_options)), format_func=lambda x: player_options[x])
        selected_player = players_df.iloc[selected_player_idx]
        player_id = selected_player['player_id']
        player_name = selected_player['full_name']
        player_team_abbr = selected_player['team_abbr']
    
    with col3:
        stat_options = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers_made']
        stat_display = {
            'points': 'Points',
            'rebounds': 'Rebounds',
            'assists': 'Assists',
            'steals': 'Steals',
            'blocks': 'Blocks',
            'turnovers': 'Turnovers',
            'three_pointers_made': '3-Pointers Made'
        }
        selected_stat = st.selectbox("Select Stat", stat_options, format_func=lambda x: stat_display[x])
    
    conn = get_db_connection()
    player_id_int = int(player_id)
    player_team_query = "SELECT team_id FROM players WHERE player_id = %s"
    player_team_df = pd.read_sql(player_team_query, conn, params=(player_id_int,))
    conn.close()
    
    if len(player_team_df) > 0:
        player_team_id = int(player_team_df.iloc[0]['team_id'])
        is_home = (player_team_id == home_team_id)
        opponent_id = away_team_id if is_home else home_team_id
    else:
        is_home = False
        opponent_id = away_team_id
        player_team_id = None
    
    st.markdown("---")
    
    player_info = get_player_info(player_id)
    season_avgs = get_season_averages(player_id)
    
    col_info, col_options, col_filters = st.columns([1.2, 1, 1])
    
    with col_info:
        if player_info:
            photo_url = get_player_photo_url(player_id)
            
            season_html = ""
            if season_avgs and season_avgs.get('games_played', 0) > 0:
                season_html = f"""<div class="stats-section">
<div class="stats-section-title">Current Season Averages</div>
<div class="stats-grid">
<div class="stat-item">PTS: <strong>{season_avgs.get('avg_points', 0):.1f}</strong></div>
<div class="stat-item">REB: <strong>{season_avgs.get('avg_rebounds', 0):.1f}</strong></div>
<div class="stat-item">AST: <strong>{season_avgs.get('avg_assists', 0):.1f}</strong></div>
<div class="stat-item">STL: <strong>{season_avgs.get('avg_steals', 0):.1f}</strong></div>
<div class="stat-item">BLK: <strong>{season_avgs.get('avg_blocks', 0):.1f}</strong></div>
<div class="stat-item">TO: <strong>{season_avgs.get('avg_turnovers', 0):.1f}</strong></div>
<div class="stat-item">3PM: <strong>{season_avgs.get('avg_three_pointers', 0):.1f}</strong></div>
<div class="stat-item">GP: <strong>{int(season_avgs.get('games_played', 0))}</strong></div>
</div>
</div>"""
            
            career_html = ""
            if player_info.get('career_games') and player_info.get('career_games', 0) > 0:
                career_ppg = player_info.get('career_points', 0) / player_info.get('career_games', 1)
                career_rpg = player_info.get('career_rebounds', 0) / player_info.get('career_games', 1)
                career_apg = player_info.get('career_assists', 0) / player_info.get('career_games', 1)
                career_spg = player_info.get('career_steals', 0) / player_info.get('career_games', 1)
                career_bpg = player_info.get('career_blocks', 0) / player_info.get('career_games', 1)
                
                career_html = f"""<div class="stats-section">
<div class="stats-section-title">Career Averages</div>
<div class="stats-grid">
<div class="stat-item">PTS: <strong>{career_ppg:.1f}</strong></div>
<div class="stat-item">REB: <strong>{career_rpg:.1f}</strong></div>
<div class="stat-item">AST: <strong>{career_apg:.1f}</strong></div>
<div class="stat-item">STL: <strong>{career_spg:.1f}</strong></div>
<div class="stat-item">BLK: <strong>{career_bpg:.1f}</strong></div>
<div class="stat-item">GP: <strong>{int(player_info.get('career_games', 0))}</strong></div>
</div>
</div>"""
            
            full_html = f"""<div class="player-info-card">
<div class="player-header">
<img src="{photo_url}" class="player-photo-large" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
<div>
<div class="player-name-large">{player_info.get('full_name', player_name)}</div>
<div class="player-meta">{player_info.get('position', 'N/A')} | {player_info.get('team_name', player_team_abbr)}</div>
</div>
</div>
{season_html}
{career_html}
</div>"""
            st.markdown(full_html, unsafe_allow_html=True)
    
    with col_options:
        st.markdown("<div style='color: #d4af37; font-weight: 600; margin-bottom: 0.5rem;'>Time Window</div>", unsafe_allow_html=True)
        window_options = ['L5', 'L10', 'L20', 'L50', 'All']
        selected_window = st.radio("Time Window", window_options, horizontal=False, key="time_window", label_visibility="collapsed")
        window_size = {'L5': 5, 'L10': 10, 'L20': 20, 'L50': 50, 'All': 100}[selected_window]
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        st.markdown("<div style='color: #d4af37; font-weight: 600; margin-bottom: 0.5rem;'>Over/Under</div>", unsafe_allow_html=True)
        over_under = st.radio("Over/Under", ["Over", "Under"], horizontal=False, key="over_under", label_visibility="collapsed")
    
    with col_filters:
        st.markdown("<div style='color: #d4af37; font-weight: 600; margin-bottom: 0.5rem;'>Filters</div>", unsafe_allow_html=True)
        
        filter_h2h = st.checkbox("H2H (Head-to-Head)", value=False)
        home_away_label = "Home" if is_home else "Away"
        filter_home_away = st.checkbox(f"{home_away_label} Games", value=False)
        current_team_only = st.checkbox("Current Team Only", value=False)
        exclude_dnp = st.checkbox("Exclude DNP", value=True)
        
        st.markdown("<div style='color: #d4af37; font-weight: 600; margin-bottom: 0.5rem; margin-top: 0.5rem;'>Minimum Minutes</div>", unsafe_allow_html=True)
        min_minutes_options = list(range(0, 65, 5))
        min_minutes = st.selectbox("Minimum Minutes", min_minutes_options, index=0, key="min_minutes", label_visibility="collapsed")
        
        filter_labels = []
        if filter_h2h:
            filter_labels.append("H2H")
        if filter_home_away:
            filter_labels.append(home_away_label)
        if current_team_only:
            filter_labels.append("Current Team")
        filter_str = ", ".join(filter_labels) if filter_labels else "All"
    
    historical_stats = get_player_historical_stats(
        player_id, opponent_id, is_home, window_size, filter_h2h, filter_home_away, current_team_only, player_team_id, exclude_dnp, min_minutes
    )
    
    if len(historical_stats) == 0:
        st.warning(f"No historical data found for {player_name} with the selected filters")
        return
    
    prediction = get_player_prediction(player_id, game_id, selected_stat)
    
    stat_column_map = {
        'points': 'points',
        'rebounds': 'rebounds_total',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three_pointers_made'
    }
    
    stat_column = stat_column_map[selected_stat]
    
    historical_stats = historical_stats.sort_values('game_date')
    historical_stats['game_number'] = range(1, len(historical_stats) + 1)
    
    historical_stats['is_home_game'] = historical_stats['player_team_id'] == historical_stats['home_team_id']
    historical_stats['opponent_abbr'] = historical_stats.apply(
        lambda row: row['away_team_abbr'] if row['is_home_game'] else row['home_team_abbr'], axis=1
    )
    historical_stats['game_label'] = historical_stats.apply(
        lambda row: f"{row['game_date'].strftime('%m/%d').lstrip('0').replace('/0', '/')} vs {row['opponent_abbr']}", axis=1
    )
    historical_stats['game_name'] = historical_stats.apply(
        lambda row: f"{row['away_team_abbr']} vs {row['home_team_abbr']}", axis=1
    )
    
    if prediction is not None:
        prediction_row = pd.DataFrame({
            'game_date': [today],
            stat_column: [prediction],
            'game_number': [len(historical_stats) + 1],
            'is_prediction': [True],
            'game_label': ['Prediction']
        })
        historical_stats['is_prediction'] = False
        chart_df = pd.concat([historical_stats, prediction_row], ignore_index=True)
    else:
        chart_df = historical_stats.copy()
        chart_df['is_prediction'] = False
    
    st.markdown("---")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.markdown("### Betting Line")
        
        line_key = f"line_{player_id}_{game_id}_{selected_stat}"
        if line_key not in st.session_state:
            mean_val = float(historical_stats[stat_column].mean())
            st.session_state[line_key] = round(mean_val * 2) / 2
        
        line_value = st.number_input("Line Value", min_value=0.0, value=st.session_state[line_key], step=0.5, format="%.1f", key=f"line_input_{line_key}")
        st.session_state[line_key] = round(line_value * 2) / 2
        
        hit_rate = calculate_over_under_percentage(historical_stats, selected_stat, st.session_state[line_key], over_under)
        
        st.markdown("### Hit Rate")
        st.metric("Hit Rate", f"{hit_rate:.1f}%", delta=f"{over_under} the line", label_visibility="hidden")
        
        if prediction is not None:
            st.markdown("### Our Prediction")
            st.metric("Our Prediction", f"{prediction:.1f}", label_visibility="hidden")
    
    with col1:
        fig = go.Figure()
        
        line_value = st.session_state[line_key]
        
        actual_games = chart_df[chart_df['is_prediction'] == False].copy()
        if len(actual_games) > 0:
            actual_games['bar_color'] = actual_games[stat_column].apply(
                lambda x: '#4a9b5f' if x >= line_value else '#c85a5a'
            )
            
            for idx, row in actual_games.iterrows():
                fig.add_trace(go.Bar(
                    x=[row['game_number']],
                    y=[row[stat_column]],
                    name='',
                    marker_color=row['bar_color'],
                    text=[f"{row[stat_column]:.1f}"],
                    textposition='outside',
                    hovertemplate=f"<b>{row['game_label']}</b><br>" +
                                f"Date: {row['game_date'].strftime('%Y-%m-%d')}<br>" +
                                f"{stat_display[selected_stat]}: {row[stat_column]:.1f}<extra></extra>",
                    showlegend=False
                ))
        
        if prediction is not None:
            pred_row = chart_df[chart_df['is_prediction'] == True]
            if len(pred_row) > 0:
                pred_val = pred_row.iloc[0][stat_column]
                fig.add_trace(go.Bar(
                    x=pred_row['game_number'],
                    y=pred_row[stat_column],
                    name='',
                    marker_color='#d4af37',
                    text=[f"{pred_val:.1f}"],
                    textposition='outside',
                    hovertemplate=f"<b>Prediction</b><br>" +
                                f"Date: {today.strftime('%Y-%m-%d')}<br>" +
                                f"{stat_display[selected_stat]}: {pred_val:.1f}<extra></extra>",
                    showlegend=False
                ))
        
        xaxis_labels = chart_df['game_label'].tolist()
        
        fig.add_hline(
            y=line_value,
            line_dash="dash",
            line_color="white",
            annotation_text=f"Line: {line_value:.1f}",
            annotation_position="right"
        )
        
        fig.update_layout(
            title=f"{player_name} - {stat_display[selected_stat]} ({selected_window}, {filter_str})",
            xaxis=dict(
                title="Game (Most Recent →)",
                tickmode='array',
                tickvals=chart_df['game_number'].tolist(),
                ticktext=xaxis_labels,
                tickangle=0
            ),
            yaxis_title=stat_display[selected_stat],
            height=500,
            showlegend=False,
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white')
        )
        
        st.plotly_chart(
            fig, 
            use_container_width=True,
            config={
                'displayModeBar': True,
                'modeBarButtonsToAdd': ['resetScale2d'],
                'displaylogo': False
            }
        )
    
    conn = get_db_connection()
    game_info_query = f"SELECT game_date, season, game_type FROM games WHERE game_id = '{game_id}'"
    game_info_df = pd.read_sql(game_info_query, conn)
    conn.close()
    
    if len(game_info_df) > 0:
        game_date = game_info_df.iloc[0]['game_date']
        season = game_info_df.iloc[0]['season'] or '2025-26'
        game_type = game_info_df.iloc[0]['game_type'] or 'regular_season'
    else:
        game_date = today
        season = '2025-26'
        game_type = 'regular_season'
    
    player_position = player_info.get('position', 'G') if player_info else 'G'
    
    stat_display_names = {
        'points': 'points',
        'rebounds': 'rebounds',
        'assists': 'assists',
        'steals': 'steals',
        'blocks': 'blocks',
        'turnovers': 'turnovers',
        'three_pointers_made': 'three-pointers made'
    }
    
    stat_display_name = stat_display_names.get(selected_stat, selected_stat)
    
    cards_html = []
    
    pos_def_rank = get_position_defense_ranking(opponent_id, season, player_position, selected_stat)
    if pos_def_rank:
        team_abbr = pos_def_rank['team_abbr']
        rank = pos_def_rank['rank']
        rank_suffix = 'th' if 11 <= rank % 100 <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(rank % 10, 'th')
        
        if selected_stat == 'turnovers':
            if rank == 1:
                verb = "forces the most"
            elif rank == pos_def_rank['total_teams']:
                verb = "forces the least"
            else:
                verb = f"forces the {rank}{rank_suffix} most"
            stat_label = "turnovers"
        else:
            if rank == 1:
                verb = "allows the least"
            elif rank == pos_def_rank['total_teams']:
                verb = "allows the most"
            else:
                verb = f"allows the {rank}{rank_suffix} most"
            stat_label = stat_display_name
        
        cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Opponent Defense</h4><p>{team_abbr} {verb} {stat_label} in the league to {player_position}s ({pos_def_rank["value"]:.1f} per game).</p></div></div>')
    else:
        cards_html.append('<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Opponent Defense</h4><p>Insufficient data to determine opponent\'s position defense ranking.</p></div></div>')
    
    star_info = get_star_players_out_info(player_id, player_team_id, season, game_date, selected_stat)
    if star_info:
        if star_info.get('insufficient_data'):
            stars_list = ', '.join(star_info['stars_out_names'])
            cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Star Players Out</h4><p>Star teammates out: <span class="highlight">{stars_list}</span>. Insufficient data to determine impact (need at least 3 games without star).</p></div></div>')
        elif star_info.get('total_games', 0) >= 3:
            abs_change = abs(star_info['total_change'])
            more_less = "more" if star_info['total_change'] >= 0 else "less"
            change_str = f"{abs_change:.1f}"
            games_count = int(star_info['total_games'])
            stars_list = ', '.join(star_info['stars_out_names'])
            cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Star Players Out</h4><p>Star teammates out: <span class="highlight">{stars_list}</span>. When out, {player_name} averages <span class="highlight">{change_str}</span> {more_less} {stat_display_name} ({games_count} games).</p></div></div>')
        else:
            stars_list = ', '.join(star_info.get('stars_out_names', []))
            cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Star Players Out</h4><p>Star teammates out: <span class="highlight">{stars_list}</span>. Insufficient data to determine impact (need at least 3 games without star).</p></div></div>')
    else:
        cards_html.append('<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Star Players Out</h4><p>No star teammates currently out.</p></div></div>')
    
    rest_info = get_rest_days(player_team_id, opponent_id, game_date, season)
    if rest_info['team_rest'] is not None and rest_info['opponent_rest'] is not None:
        cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Rest Days</h4><p>Player\'s team: <span class="highlight">{rest_info["team_rest"]}</span> days rest<br>Opponent: <span class="highlight">{rest_info["opponent_rest"]}</span> days rest</p></div></div>')
    else:
        cards_html.append('<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Rest Days</h4><p>Insufficient data to determine rest days.</p></div></div>')
    
    playoff_info = get_playoff_experience_impact(player_id, selected_stat, game_type)
    if not playoff_info['is_playoff']:
        cards_html.append('<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Playoff Experience</h4><p>The current game is not a playoff game, so playoff experience has negligible impact.</p></div></div>')
    elif playoff_info.get('insufficient_data'):
        cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Playoff Experience</h4><p>{player_name} does not have enough playoff experience to determine post-season influence on {stat_display_name}.</p></div></div>')
    else:
        change_str = f"+{playoff_info['change']:.1f}" if playoff_info['change'] >= 0 else f"{playoff_info['change']:.1f}"
        cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Playoff Experience</h4><p>In playoff games, {player_name} averages <span class="highlight">{change_str}</span> {stat_display_name} compared to regular season ({playoff_info["playoff_games"]} playoff games).</p></div></div>')
    
    pace_info = get_pace_comparison(player_team_id, opponent_id, season)
    if pace_info:
        pct_diff_abs = abs(pace_info['pct_diff'])
        pct_diff_rounded = round(pct_diff_abs, 1)
        if pct_diff_abs < 0.01 or pct_diff_rounded == 0.0:
            cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Pace Comparison</h4><p>{pace_info["team_name"]} plays at a pace of <span class="highlight">{pace_info["team_pace"]:.1f}</span> and {pace_info["opp_name"]} plays at a pace of <span class="highlight">{pace_info["opp_pace"]:.1f}</span>. This is exactly the average league pace, meaning expect normal amounts of possessions.</p></div></div>')
        else:
            direction = 'higher' if pace_info['pct_diff'] > 0 else 'lower'
            more_less = 'more' if pace_info['pct_diff'] > 0 else 'less'
            cards_html.append(f'<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Pace Comparison</h4><p>{pace_info["team_name"]} plays at a pace of <span class="highlight">{pace_info["team_pace"]:.1f}</span> and {pace_info["opp_name"]} plays at a pace of <span class="highlight">{pace_info["opp_pace"]:.1f}</span>. This is <span class="highlight">{pct_diff_rounded:.1f}%</span> {direction} pace than the league average, meaning there may be {more_less} possessions.</p></div></div>')
    else:
        cards_html.append('<div class="context-info-card-wrapper"><div class="context-info-card"><h4>Pace Comparison</h4><p>Insufficient data to determine pace comparison.</p></div></div>')
    
    all_cards_html = ''.join(cards_html)
    grid_html = f'<div class="info-cards-grid">{all_cards_html}</div>'
    st.markdown(grid_html, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("### Historical Data")
    display_df = historical_stats[['game_date', 'game_name', stat_column]].copy()
    display_df.columns = ['Game Date', 'Game', stat_display[selected_stat]]
    display_df = display_df.sort_values('Game Date', ascending=False)
    
    stat_col_name = stat_display[selected_stat]
    if stat_col_name in display_df.columns:
        display_df[stat_col_name] = display_df[stat_col_name].apply(
            lambda x: f" {x:.1f}" if isinstance(x, (int, float)) else f" {x}"
        )
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.markdown("""
    <script>
    (function() {
        function alignStatColumn() {
            const tables = document.querySelectorAll('div[data-testid="stDataFrame"] table');
            tables.forEach(function(table) {
                const rows = table.querySelectorAll('tbody tr, thead tr');
                rows.forEach(function(row) {
                    const cells = row.querySelectorAll('td, th');
                    if (cells.length >= 3) {
                        cells[2].style.textAlign = 'left';
                        cells[2].style.paddingLeft = '0.5rem';
                    }
                });
            });
        }
        alignStatColumn();
        setTimeout(alignStatColumn, 100);
        setTimeout(alignStatColumn, 500);
    })();
    </script>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

