import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

def get_ensemble_predictions(target_date, model_types, conn=None):
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    else:
        close_conn = False
    
    try:
        if not model_types or len(model_types) == 0:
            model_types = ['xgboost']
        
        placeholders = ','.join(['%s'] * len(model_types))
        
        query = f"""
            SELECT 
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
                p.model_version
            FROM predictions p
            JOIN games g ON p.game_id = g.game_id
            WHERE p.prediction_date = %s
                AND g.game_status = 'scheduled'
                AND p.model_version IN ({placeholders})
        """
        
        params = [target_date] + model_types
        df = pd.read_sql(query, conn, params=params)
        
        if len(df) == 0:
            return pd.DataFrame()
        
        ensemble_df = df.groupby(['player_id', 'game_id', 'prediction_date']).agg({
            'predicted_points': 'mean',
            'predicted_rebounds': 'mean',
            'predicted_assists': 'mean',
            'predicted_steals': 'mean',
            'predicted_blocks': 'mean',
            'predicted_turnovers': 'mean',
            'predicted_three_pointers_made': 'mean',
            'confidence_score': 'mean'
        }).reset_index()
        
        ensemble_df['model_version'] = '+'.join(sorted(model_types))
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

def get_available_model_types():
    conn = get_db_connection()
    try:
        query = """
            SELECT DISTINCT model_version
            FROM predictions
            WHERE model_version IS NOT NULL
            ORDER BY model_version
        """
        df = pd.read_sql(query, conn)
        return df['model_version'].tolist()
    finally:
        conn.close()

