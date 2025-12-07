import streamlit as st
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data_collection.utils import get_db_connection
import pandas as pd
import numpy as np

def get_ensemble_selection():
    if 'ensemble_models' not in st.session_state:
        all_models = ['xgboost', 'lightgbm', 'random_forest', 'catboost']
        conn = get_db_connection()
        try:
            query = """
                SELECT DISTINCT model_version
                FROM predictions
                WHERE model_version IS NOT NULL
            """
            df = pd.read_sql(query, conn)
            available_models = df['model_version'].tolist() if not df.empty else []
            st.session_state.ensemble_models = [m for m in all_models if m in available_models] or ['xgboost']
        except:
            st.session_state.ensemble_models = all_models
        finally:
            conn.close()
    return st.session_state.ensemble_models

def load_predictions_with_ensemble(target_date, conn=None):
    ensemble_models = get_ensemble_selection()
    
    if len(ensemble_models) == 0:
        all_models = ['xgboost', 'lightgbm', 'random_forest', 'catboost']
        ensemble_models = all_models
    
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    else:
        close_conn = False
    
    try:
        placeholders = ','.join(['%s'] * len(ensemble_models))
        
        query = f"""
            SELECT 
                p.prediction_id,
                p.player_id,
                p.game_id,
                p.prediction_date,
                p.predicted_points,
                p.predicted_rebounds,
                p.predicted_assists,
                p.predicted_steals,
                p.predicted_blocks,
                p.predicted_turnovers,
                p.predicted_three_pointers_made,
                p.confidence_score,
                p.model_version,
                pl.full_name as player_name,
                pl.position,
                pl.team_id,
                t1.full_name as team_name,
                t1.abbreviation as team_abbr,
                t2.full_name as opponent_name,
                t2.abbreviation as opponent_abbr,
                CASE WHEN g.home_team_id = pl.team_id THEN 'HOME' ELSE 'AWAY' END as location,
                g.game_date,
                p.actual_points,
                p.actual_rebounds,
                p.actual_assists,
                p.actual_steals,
                p.actual_blocks,
                p.actual_turnovers,
                p.actual_three_pointers_made,
                p.prediction_error
            FROM predictions p
            JOIN players pl ON p.player_id = pl.player_id
            JOIN games g ON p.game_id = g.game_id
            JOIN teams t1 ON pl.team_id = t1.team_id
            JOIN teams t2 ON (CASE WHEN g.home_team_id = pl.team_id THEN g.away_team_id ELSE g.home_team_id END) = t2.team_id
            WHERE p.prediction_date = %s
                AND p.model_version IN ({placeholders})
            ORDER BY p.player_id, p.game_id, p.model_version
        """
        
        params = [target_date] + ensemble_models
        df = pd.read_sql(query, conn, params=params)
        
        if df.empty:
            return pd.DataFrame()
        
        available_models_for_date = df['model_version'].unique().tolist()
        missing_models = [m for m in ensemble_models if m not in available_models_for_date]
        
        if missing_models and len(available_models_for_date) > 0:
            st.session_state[f'ensemble_warning_{target_date}'] = {
                'selected': ensemble_models,
                'available': available_models_for_date,
                'missing': missing_models
            }
        
        if len(ensemble_models) == 1:
            return df
        
        if len(available_models_for_date) == 1:
            return df[df['model_version'] == available_models_for_date[0]].copy()
        
        ensemble_df = df.groupby(['player_id', 'game_id']).agg({
            'predicted_points': 'mean',
            'predicted_rebounds': 'mean',
            'predicted_assists': 'mean',
            'predicted_steals': 'mean',
            'predicted_blocks': 'mean',
            'predicted_turnovers': 'mean',
            'predicted_three_pointers_made': 'mean',
            'confidence_score': 'mean',
            'prediction_id': 'first',
            'player_name': 'first',
            'position': 'first',
            'team_id': 'first',
            'team_name': 'first',
            'team_abbr': 'first',
            'opponent_name': 'first',
            'opponent_abbr': 'first',
            'location': 'first',
            'game_date': 'first',
            'prediction_date': 'first',
            'actual_points': 'first',
            'actual_rebounds': 'first',
            'actual_assists': 'first',
            'actual_steals': 'first',
            'actual_blocks': 'first',
            'actual_turnovers': 'first',
            'actual_three_pointers_made': 'first',
            'prediction_error': 'first'
        }).reset_index()
        
        ensemble_df['prediction_id'] = ensemble_df['player_id'].astype(str) + '_' + ensemble_df['game_id'].astype(str)
        
        ensemble_df['predicted_points'] = ensemble_df['predicted_points'].round(1)
        ensemble_df['predicted_rebounds'] = ensemble_df['predicted_rebounds'].round(1)
        ensemble_df['predicted_assists'] = ensemble_df['predicted_assists'].round(1)
        ensemble_df['predicted_steals'] = ensemble_df['predicted_steals'].round(1)
        ensemble_df['predicted_blocks'] = ensemble_df['predicted_blocks'].round(1)
        ensemble_df['predicted_turnovers'] = ensemble_df['predicted_turnovers'].round(1)
        ensemble_df['predicted_three_pointers_made'] = ensemble_df['predicted_three_pointers_made'].round(1)
        ensemble_df['confidence_score'] = ensemble_df['confidence_score'].round(0).astype(int)
        
        return ensemble_df
        
    finally:
        if close_conn:
            conn.close()

def get_ensemble_warning(target_date):
    warning_key = f'ensemble_warning_{target_date}'
    if warning_key in st.session_state:
        warning = st.session_state[warning_key]
        if warning['missing']:
            return warning
    return None

