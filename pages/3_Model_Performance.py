import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import sys
import os
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
streamlit_app_dir = os.path.join(root_dir, 'streamlit_app')
if streamlit_app_dir not in sys.path:
    sys.path.insert(0, streamlit_app_dir)
from utils import get_ensemble_selection

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

load_dotenv()

st.set_page_config(
    page_title="Model Performance - NBA Predictions",
    page_icon="📈",
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
            max-width: 100% !important;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            margin-bottom: 0.5rem;
            margin-top: 0 !important;
            padding-top: 0 !important;
        }
        
        .metric-card {
            background: #1e1e1e;
            border: 2px solid #2d2d2d;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1rem;
        }
        
        .metric-label {
            color: #888;
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }
        
        .metric-value {
            color: #d4af37;
            font-size: 2rem;
            font-weight: 700;
        }
        
        [data-testid*="stMultiSelect"] div[data-baseweb="select"] > div {
            background-color: #1e1e1e !important;
            border-color: #2d2d2d !important;
        }
        
        [data-testid*="stMultiSelect"] div[data-baseweb="select"] > div:hover {
            border-color: #d4af37 !important;
        }
        
        [data-testid*="stMultiSelect"] div[data-baseweb="select"] > div:focus-within {
            border-color: #d4af37 !important;
            box-shadow: 0 0 0 1px #d4af37 !important;
        }
        
        [data-testid*="stMultiSelect"] input,
        [data-testid*="stMultiSelect"] input:focus {
            background-color: #1e1e1e !important;
            color: #fff !important;
            border: none !important;
            outline: none !important;
        }
        
        [data-testid*="stMultiSelect"] div[data-baseweb="tag"],
        [data-testid*="stMultiSelect"] span[data-baseweb="tag"],
        [data-testid*="stMultiSelect"] button[data-baseweb="tag"] {
            background-color: #d4af37 !important;
            color: #000 !important;
            border-color: #d4af37 !important;
        }
        
        [data-testid*="stMultiSelect"] div[data-baseweb="tag"] span,
        [data-testid*="stMultiSelect"] span[data-baseweb="tag"] span {
            color: #000 !important;
        }
        
        [data-testid*="stMultiSelect"] div[data-baseweb="tag"] svg,
        [data-testid*="stMultiSelect"] span[data-baseweb="tag"] svg {
            fill: #000 !important;
            color: #000 !important;
        }
        
        [data-testid*="stMultiSelect"] ul[role="listbox"] {
            background-color: #1e1e1e !important;
            border-color: #2d2d2d !important;
        }
        
        [data-testid*="stMultiSelect"] li[role="option"]:hover {
            background-color: #2d2d2d !important;
        }
        
        [data-testid*="stMultiSelect"] li[role="option"][aria-selected="true"] {
            background-color: #d4af37 !important;
            color: #000 !important;
        }
        
        [data-testid*="stSelectbox"] div[data-baseweb="select"] > div {
            background-color: #1e1e1e !important;
            border-color: #2d2d2d !important;
        }
        
        [data-testid*="stSelectbox"] div[data-baseweb="select"] > div:hover {
            border-color: #d4af37 !important;
        }
        
        [data-testid*="stSelectbox"] div[data-baseweb="select"] > div:focus-within {
            border-color: #d4af37 !important;
            box-shadow: 0 0 0 1px #d4af37 !important;
        }
        
        .ensemble-status-card {
            background: #1e1e1e;
            border: 2px solid #d4af37;
            border-radius: 8px;
            padding: 1rem;
            margin-top: 0.5rem;
            color: #d4af37;
        }
        
        .ensemble-status-card.info {
            border-color: #2d2d2d;
            color: #888;
        }
        </style>
        <script>
        function styleMultiselectTags() {
            const multiselectContainer = document.querySelector('[data-testid*="stMultiSelect"]');
            if (!multiselectContainer) return;
            
            const tags = multiselectContainer.querySelectorAll('div[data-baseweb="tag"], span[data-baseweb="tag"], button[data-baseweb="tag"]');
            tags.forEach(tag => {
                tag.style.backgroundColor = '#d4af37';
                tag.style.borderColor = '#d4af37';
                tag.style.color = '#000';
                
                const spans = tag.querySelectorAll('span');
                spans.forEach(span => {
                    span.style.color = '#000';
                });
                
                const svgs = tag.querySelectorAll('svg');
                svgs.forEach(svg => {
                    svg.style.fill = '#000';
                    svg.style.color = '#000';
                });
            });
            
            const inputs = multiselectContainer.querySelectorAll('input');
            inputs.forEach(input => {
                input.style.backgroundColor = '#1e1e1e';
                input.style.color = '#fff';
                input.style.border = 'none';
            });
        }
        
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', styleMultiselectTags);
        } else {
            styleMultiselectTags();
        }
        
        const observer = new MutationObserver(() => {
            styleMultiselectTags();
        });
        observer.observe(document.body, { childList: true, subtree: true });
        </script>
    """, unsafe_allow_html=True)

def get_model_versions():
    conn = get_db_connection()
    try:
        query = """
            SELECT DISTINCT model_version 
            FROM predictions 
            WHERE model_version IS NOT NULL
            ORDER BY model_version DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df['model_version'].tolist() if not df.empty else ['xgboost']
    except Exception as e:
        st.error(f"Error fetching model versions: {str(e)}")
        return ['xgboost']

def get_predictions_with_actuals(start_date=None, end_date=None, model_version=None, stat_filter=None, ensemble_models=None):
    conn = get_db_connection()
    
    where_conditions = ["p.actual_points IS NOT NULL"]
    params = []
    
    if start_date:
        where_conditions.append("DATE(g.game_date) >= %s")
        params.append(start_date)
    
    if end_date:
        where_conditions.append("DATE(g.game_date) <= %s")
        params.append(end_date)
    
    if model_version == 'Ensemble' and ensemble_models:
        if len(ensemble_models) > 0:
            placeholders = ','.join(['%s'] * len(ensemble_models))
            where_conditions.append(f"p.model_version IN ({placeholders})")
            params.extend(ensemble_models)
    elif model_version and model_version != 'Ensemble':
        where_conditions.append("p.model_version = %s")
        params.append(model_version)
    
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            p.prediction_id,
            p.player_id,
            p.game_id,
            DATE(g.game_date) as game_date,
            p.model_version,
            p.predicted_points,
            p.predicted_rebounds,
            p.predicted_assists,
            p.predicted_steals,
            p.predicted_blocks,
            p.predicted_turnovers,
            p.predicted_three_pointers_made,
            p.actual_points,
            p.actual_rebounds,
            p.actual_assists,
            p.actual_steals,
            p.actual_blocks,
            p.actual_turnovers,
            p.actual_three_pointers_made,
            p.prediction_error,
            p.confidence_score
        FROM predictions p
        JOIN games g ON p.game_id = g.game_id
        WHERE {where_clause}
        ORDER BY g.game_date DESC
    """
    
    try:
        df = pd.read_sql(query, conn, params=tuple(params))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching predictions: {str(e)}")
        conn.close()
        return pd.DataFrame()

def calculate_mae_metrics(df):
    if df.empty:
        return {}
    
    metrics = {
        'points': np.mean(np.abs(df['predicted_points'] - df['actual_points'])),
        'rebounds': np.mean(np.abs(df['predicted_rebounds'] - df['actual_rebounds'])),
        'assists': np.mean(np.abs(df['predicted_assists'] - df['actual_assists'])),
        'steals': np.mean(np.abs(df['predicted_steals'] - df['actual_steals'])),
        'blocks': np.mean(np.abs(df['predicted_blocks'] - df['actual_blocks'])),
        'turnovers': np.mean(np.abs(df['predicted_turnovers'] - df['actual_turnovers'])),
        'three_pointers': np.mean(np.abs(df['predicted_three_pointers_made'] - df['actual_three_pointers_made']))
    }
    
    metrics['overall'] = np.mean(df['prediction_error']) if 'prediction_error' in df.columns else np.mean(list(metrics.values()))
    
    return metrics

def calculate_accuracy_percentage(df):
    if df.empty:
        return {}
    
    stats = {
        'points': ('predicted_points', 'actual_points'),
        'rebounds': ('predicted_rebounds', 'actual_rebounds'),
        'assists': ('predicted_assists', 'actual_assists'),
        'steals': ('predicted_steals', 'actual_steals'),
        'blocks': ('predicted_blocks', 'actual_blocks'),
        'turnovers': ('predicted_turnovers', 'actual_turnovers'),
        'three_pointers': ('predicted_three_pointers_made', 'actual_three_pointers_made')
    }
    
    accuracy = {}
    for stat_name, (pred_col, actual_col) in stats.items():
        stat_accuracies = []
        for idx, row in df.iterrows():
            pred_val = row[pred_col] if pd.notna(row[pred_col]) else 0
            actual_val = row[actual_col] if pd.notna(row[actual_col]) else 0
            
            if actual_val > 0:
                error_pct = abs(pred_val - actual_val) / actual_val * 100
                stat_accuracy = max(0, 100 - error_pct)
                stat_accuracies.append(stat_accuracy)
            elif actual_val == 0 and pred_val == 0:
                stat_accuracies.append(100)
        
        accuracy[stat_name] = np.mean(stat_accuracies) if stat_accuracies else 0
    
    return accuracy

def get_performance_over_time(df, period='daily'):
    if df.empty:
        return pd.DataFrame()
    
    df['game_date'] = pd.to_datetime(df['game_date'])
    
    if period == 'daily':
        df['period'] = df['game_date'].dt.date
    elif period == 'weekly':
        df['period'] = df['game_date'].dt.to_period('W').dt.start_time.dt.date
    elif period == 'monthly':
        df['period'] = df['game_date'].dt.to_period('M').dt.start_time.dt.date
    
    grouped = df.groupby('period').agg({
        'prediction_error': 'mean',
        'predicted_points': 'count'
    }).reset_index()
    
    grouped.columns = ['period', 'avg_error', 'num_predictions']
    
    return grouped.sort_values('period')

def create_scatter_plot(df, stat_name):
    stat_map = {
        'points': ('predicted_points', 'actual_points', 'Points'),
        'rebounds': ('predicted_rebounds', 'actual_rebounds', 'Rebounds'),
        'assists': ('predicted_assists', 'actual_assists', 'Assists'),
        'steals': ('predicted_steals', 'actual_steals', 'Steals'),
        'blocks': ('predicted_blocks', 'actual_blocks', 'Blocks'),
        'turnovers': ('predicted_turnovers', 'actual_turnovers', 'Turnovers'),
        'three_pointers': ('predicted_three_pointers_made', 'actual_three_pointers_made', '3-Pointers Made')
    }
    
    if stat_name not in stat_map:
        return None
    
    pred_col, actual_col, display_name = stat_map[stat_name]
    
    if df.empty or pred_col not in df.columns or actual_col not in df.columns:
        return None
    
    plot_df = df[[pred_col, actual_col]].dropna()
    
    if plot_df.empty:
        return None
    
    max_val = max(plot_df[pred_col].max(), plot_df[actual_col].max())
    min_val = min(plot_df[pred_col].min(), plot_df[actual_col].min())
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=plot_df[actual_col],
        y=plot_df[pred_col],
        mode='markers',
        marker=dict(
            color='#d4af37',
            size=5,
            opacity=0.6,
            line=dict(width=0.5, color='#ffffff')
        ),
        name='Predictions',
        hovertemplate='<b>Actual:</b> %{x:.1f}<br><b>Predicted:</b> %{y:.1f}<extra></extra>'
    ))
    
    fig.add_trace(go.Scatter(
        x=[min_val, max_val],
        y=[min_val, max_val],
        mode='lines',
        line=dict(color='#ffffff', dash='dash', width=2),
        name='Perfect Prediction',
        showlegend=True
    ))
    
    fig.update_layout(
        title=f"{display_name}: Predicted vs Actual",
        xaxis_title=f"Actual {display_name}",
        yaxis_title=f"Predicted {display_name}",
        height=450,
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        showlegend=True
    )
    
    return fig

def create_error_distribution(df, stat_name):
    stat_map = {
        'points': ('predicted_points', 'actual_points', 'Points'),
        'rebounds': ('predicted_rebounds', 'actual_rebounds', 'Rebounds'),
        'assists': ('predicted_assists', 'actual_assists', 'Assists'),
        'steals': ('predicted_steals', 'actual_steals', 'Steals'),
        'blocks': ('predicted_blocks', 'actual_blocks', 'Blocks'),
        'turnovers': ('predicted_turnovers', 'actual_turnovers', 'Turnovers'),
        'three_pointers': ('predicted_three_pointers_made', 'actual_three_pointers_made', '3-Pointers Made')
    }
    
    if stat_name not in stat_map:
        return None
    
    pred_col, actual_col, display_name = stat_map[stat_name]
    
    if df.empty or pred_col not in df.columns or actual_col not in df.columns:
        return None
    
    plot_df = df[[pred_col, actual_col]].dropna()
    
    if plot_df.empty:
        return None
    
    errors = plot_df[pred_col] - plot_df[actual_col]
    
    fig = go.Figure()
    
    fig.add_trace(go.Histogram(
        x=errors,
        nbinsx=50,
        marker_color='#d4af37',
        opacity=0.7,
        name='Error Distribution'
    ))
    
    fig.add_vline(
        x=0,
        line_dash="dash",
        line_color="white",
        annotation_text="Perfect Prediction"
    )
    
    fig.update_layout(
        title=f"{display_name}: Prediction Error Distribution",
        xaxis_title="Error (Predicted - Actual)",
        yaxis_title="Frequency",
        height=450,
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        showlegend=False
    )
    
    return fig

def create_performance_trend(df):
    if df.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['period'],
        y=df['avg_error'],
        mode='lines+markers',
        name='Average Error',
        line=dict(color='#d4af37', width=3),
        marker=dict(size=6, color='#d4af37'),
        hovertemplate='<b>Date:</b> %{x}<br><b>Avg Error:</b> %{y:.2f}<br><b>Predictions:</b> %{customdata}<extra></extra>',
        customdata=df['num_predictions']
    ))
    
    fig.update_layout(
        title="Model Performance Over Time",
        xaxis_title="Date",
        yaxis_title="Average Prediction Error",
        height=400,
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        showlegend=False
    )
    
    return fig

def compare_model_versions(df, version1, version2):
    v1_df = df[df['model_version'] == version1].copy()
    v2_df = df[df['model_version'] == version2].copy()
    
    if v1_df.empty or v2_df.empty:
        return None
    
    v1_metrics = calculate_mae_metrics(v1_df)
    v2_metrics = calculate_mae_metrics(v2_df)
    
    comparison = {}
    for stat in ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers', 'overall']:
        v1_val = v1_metrics.get(stat, 0)
        v2_val = v2_metrics.get(stat, 0)
        diff = v1_val - v2_val
        improvement = (diff / v2_val * 100) if v2_val > 0 else 0
        comparison[stat] = {
            'v1': v1_val,
            'v2': v2_val,
            'diff': diff,
            'improvement': improvement
        }
    
    return comparison

def main():
    load_custom_css()
    
    st.markdown("""
    <h1>Model Performance</h1>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <p class="subtitle">Track accuracy metrics and model evolution over time</p>
    """, unsafe_allow_html=True)
        
    st.markdown("### Ensemble Model Configuration")
    st.markdown("Select which models to include in your ensemble. This selection will be used across all pages.")
    
    model_versions = get_model_versions()
    available_models = ['xgboost', 'lightgbm', 'random_forest', 'catboost']
    available_models = [m for m in available_models if m in model_versions]
    
    if 'ensemble_models' not in st.session_state:
        st.session_state.ensemble_models = available_models if len(available_models) > 0 else ['xgboost']
    
    selected_models = st.multiselect(
        "Select Models for Ensemble",
        available_models,
        default=st.session_state.ensemble_models,
        help="Select one or more models. Predictions will be averaged from selected models."
    )
    
    if len(selected_models) == 0:
        st.warning("Please select at least one model for the ensemble.")
        selected_models = st.session_state.ensemble_models
    
    st.session_state.ensemble_models = selected_models
    
    if len(selected_models) > 1:
        st.markdown(f"""
        <div class="ensemble-status-card">
            <strong>Ensemble active:</strong> {', '.join(selected_models)} (averaged predictions)
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="ensemble-status-card info">
            <strong>Using single model:</strong> {selected_models[0] if selected_models else 'None'}
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    model_versions = get_model_versions()
    
    default_version = 'xgboost' if 'xgboost' in model_versions else (model_versions[0] if model_versions else 'xgboost')
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        time_period = st.selectbox(
            "Time Period",
            ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 180 Days", "All Time"],
            index=0
        )
    
    with col2:
        period_grouping = st.selectbox(
            "Group By",
            ["Daily", "Weekly", "Monthly"],
            index=0
        )
    
    with col3:
        ensemble_models = get_ensemble_selection() if 'ensemble_models' in st.session_state else available_models
        ensemble_label = f"Ensemble ({'+'.join(sorted(ensemble_models))})" if len(ensemble_models) > 1 else ensemble_models[0] if ensemble_models else 'xgboost'
        
        version_options = ['Ensemble'] + model_versions
        default_idx = 0 if len(ensemble_models) > 1 else (model_versions.index(default_version) + 1 if default_version in model_versions else 1)
        
        selected_version = st.selectbox(
            "Model Version",
            version_options,
            index=default_idx
        )
    
    end_date = datetime.now().date()
    if time_period == "Last 7 Days":
        start_date = end_date - timedelta(days=7)
    elif time_period == "Last 30 Days":
        start_date = end_date - timedelta(days=30)
    elif time_period == "Last 90 Days":
        start_date = end_date - timedelta(days=90)
    elif time_period == "Last 180 Days":
        start_date = end_date - timedelta(days=180)
    else:
        start_date = None
    
    all_models = ['xgboost', 'lightgbm', 'random_forest', 'catboost']
    available_for_default = [m for m in all_models if m in model_versions]
    ensemble_models_for_filter = st.session_state.get('ensemble_models', available_for_default if len(available_for_default) > 0 else ['xgboost'])
    
    if selected_version == 'Ensemble':
        df = get_predictions_with_actuals(start_date=start_date, end_date=end_date, model_version='Ensemble', ensemble_models=ensemble_models_for_filter)
        
        if not df.empty and len(ensemble_models_for_filter) > 1:
            df = df.groupby(['player_id', 'game_id', 'game_date']).agg({
                'predicted_points': 'mean',
                'predicted_rebounds': 'mean',
                'predicted_assists': 'mean',
                'predicted_steals': 'mean',
                'predicted_blocks': 'mean',
                'predicted_turnovers': 'mean',
                'predicted_three_pointers_made': 'mean',
                'actual_points': 'first',
                'actual_rebounds': 'first',
                'actual_assists': 'first',
                'actual_steals': 'first',
                'actual_blocks': 'first',
                'actual_turnovers': 'first',
                'actual_three_pointers_made': 'first',
                'prediction_error': 'first',
                'confidence_score': 'mean',
                'model_version': 'first'
            }).reset_index()
            
            df['predicted_points'] = df['predicted_points'].round(1)
            df['predicted_rebounds'] = df['predicted_rebounds'].round(1)
            df['predicted_assists'] = df['predicted_assists'].round(1)
            df['predicted_steals'] = df['predicted_steals'].round(1)
            df['predicted_blocks'] = df['predicted_blocks'].round(1)
            df['predicted_turnovers'] = df['predicted_turnovers'].round(1)
            df['predicted_three_pointers_made'] = df['predicted_three_pointers_made'].round(1)
            df['confidence_score'] = df['confidence_score'].round(0).astype(int)
    else:
        df = get_predictions_with_actuals(start_date=start_date, end_date=end_date, model_version=selected_version)
    
    if df.empty:
        st.warning("No prediction data available for the selected filters.")
        return
    
    st.markdown("---")
    
    st.markdown("### Overall Accuracy Metrics")
    
    mae_metrics = calculate_mae_metrics(df)
    accuracy_pct = calculate_accuracy_percentage(df)
    
    overall_accuracy = np.mean(list(accuracy_pct.values())) if accuracy_pct else 0
    
    metric_cols = st.columns(8)
    stat_names = ['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'Turnovers', '3PM', 'Overall']
    stat_keys = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'three_pointers', 'overall']
    
    for col, stat_name, stat_key in zip(metric_cols, stat_names, stat_keys):
        with col:
            mae = mae_metrics.get(stat_key, 0)
            if stat_key == 'overall':
                acc = overall_accuracy
            else:
                acc = accuracy_pct.get(stat_key, 0)
            st.markdown(f"""
                <div style='text-align: center; width: 100%;'>
                    <div style='font-size: 0.85rem; color: #888; margin-bottom: 0.5rem; text-align: center;'>{stat_name}</div>
                    <div style='font-size: 1.5rem; color: #d4af37; font-weight: 700; text-align: center;'>{mae:.2f}</div>
                    <div style='font-size: 0.7rem; color: #666; margin-top: 0.25rem; margin-bottom: 0.5rem; text-align: center;'>MAE</div>
                    <div style='font-size: 1.5rem; color: #d4af37; font-weight: 700; text-align: center; display: block; width: 100%; padding-left: 0.5rem;'>{acc:.1f}%</div>
                    <div style='font-size: 0.7rem; color: #666; margin-top: 0.25rem; text-align: center;'>accuracy</div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("### Performance Over Time")
    
    period_map = {'Daily': 'daily', 'Weekly': 'weekly', 'Monthly': 'monthly'}
    performance_df = get_performance_over_time(df, period=period_map[period_grouping])
    
    if not performance_df.empty:
        trend_fig = create_performance_trend(performance_df)
        if trend_fig:
            st.plotly_chart(trend_fig, use_container_width=True)
    
    st.markdown("---")
    
    st.markdown("### Prediction vs Actual Analysis")
    
    selected_stat = st.selectbox(
        "Select Statistic",
        ["Points", "Rebounds", "Assists", "Steals", "Blocks", "Turnovers", "Three Pointers"],
        index=0,
        key="stat_selector"
    )
    
    stat_map = {
        'Points': 'points',
        'Rebounds': 'rebounds',
        'Assists': 'assists',
        'Steals': 'steals',
        'Blocks': 'blocks',
        'Turnovers': 'turnovers',
        'Three Pointers': 'three_pointers'
    }
    
    stat_key = stat_map[selected_stat]
    
    col_chart1, col_chart2 = st.columns([1, 1])
    
    with col_chart1:
        scatter_fig = create_scatter_plot(df, stat_key)
        if scatter_fig:
            st.plotly_chart(scatter_fig, use_container_width=True)
            st.markdown("""
            <div style='background: #1e1e1e; border: 2px solid #2d2d2d; border-radius: 8px; padding: 1rem; margin-top: 1rem; min-height: 180px; display: flex; flex-direction: column;'>
                <div style='color: #d4af37; font-weight: 600; margin-bottom: 0.5rem;'>Scatter Plot Explanation</div>
                <div style='color: #ccc; font-size: 0.9rem; line-height: 1.6; flex: 1;'>
                    <strong>Gold points:</strong> Each point represents one prediction. The x-axis shows the actual stat value, and the y-axis shows the predicted value.<br>
                    <strong>White dashed line:</strong> The "perfect prediction" line. Points on this line mean the prediction exactly matched the actual value.<br>
                    <strong>Above the line:</strong> Prediction was higher than actual (overestimated).<br>
                    <strong>Below the line:</strong> Prediction was lower than actual (underestimated).<br>
                    <strong>Hover:</strong> See exact values for each prediction.
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with col_chart2:
        error_fig = create_error_distribution(df, stat_key)
        if error_fig:
            st.plotly_chart(error_fig, use_container_width=True)
            st.markdown("""
            <div style='background: #1e1e1e; border: 2px solid #2d2d2d; border-radius: 8px; padding: 1rem; margin-top: 1rem; min-height: 180px; display: flex; flex-direction: column;'>
                <div style='color: #d4af37; font-weight: 600; margin-bottom: 0.5rem;'>Error Distribution Explanation</div>
                <div style='color: #ccc; font-size: 0.9rem; line-height: 1.6; flex: 1;'>
                    <strong>X-axis:</strong> Prediction error (Predicted - Actual). Positive values mean overestimation, negative means underestimation.<br>
                    <strong>Y-axis:</strong> Frequency - how many predictions had that error amount.<br>
                    <strong>White dashed line at 0:</strong> Perfect predictions (no error).<br>
                    <strong>Gold bars:</strong> The height shows how many predictions had errors in that range. A taller bar means more predictions had similar errors.<br>
                    <strong>Ideal distribution:</strong> Centered at 0 with most predictions close to 0 (accurate predictions).
                </div>
            </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

