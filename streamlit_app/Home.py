import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import numpy as np
from utils import load_predictions_with_ensemble, get_ensemble_selection, get_ensemble_warning

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

load_dotenv()

st.set_page_config(
    page_title="NBA Predictions",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded"
)

def show_navigation():
    st.sidebar.markdown("""
    <script>
    (function() {
        function replaceStreamlitAppTitle() {
            const sidebar = document.querySelector('[data-testid="stSidebar"]');
            if (sidebar) {
                const firstChild = sidebar.firstElementChild;
                if (firstChild) {
                    const firstMarkdown = firstChild.querySelector('[data-testid="stMarkdownContainer"]');
                    if (firstMarkdown) {
                        const text = (firstMarkdown.textContent || firstMarkdown.innerText || '').trim().toLowerCase();
                        if (text === 'streamlit app') {
                            firstMarkdown.innerHTML = '<h2 style="color: #d4af37; font-weight: 700; margin: 0; padding: 0;">Predictions</h2>';
                        }
                    }
                    const allText = firstChild.querySelectorAll('p, span, div');
                    allText.forEach(el => {
                        const text = (el.textContent || el.innerText || '').trim().toLowerCase();
                        if (text === 'streamlit app' && el.parentElement === firstChild) {
                            el.innerHTML = '<h2 style="color: #d4af37; font-weight: 700; margin: 0; padding: 0;">Predictions</h2>';
                        }
                    });
                }
            }
        }
        replaceStreamlitAppTitle();
        setTimeout(replaceStreamlitAppTitle, 50);
        setTimeout(replaceStreamlitAppTitle, 100);
        setTimeout(replaceStreamlitAppTitle, 300);
        setTimeout(replaceStreamlitAppTitle, 500);
        setTimeout(replaceStreamlitAppTitle, 1000);
        
        const observer = new MutationObserver(function(mutations) {
            replaceStreamlitAppTitle();
        });
        const sidebar = document.querySelector('[data-testid="stSidebar"]');
        if (sidebar) {
            observer.observe(sidebar, { 
                childList: true, 
                subtree: true,
                characterData: true
            });
        }
    })();
    </script>
    """, unsafe_allow_html=True)
    st.sidebar.markdown("---")
    st.sidebar.markdown("Use the pages menu above to navigate between pages")

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

def get_player_photo_url(player_id):
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"

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
        
        [data-testid="stAppViewContainer"] {
            padding-left: 0 !important;
            padding-right: 0 !important;
        }
        
        div[data-testid="stVerticalBlock"] {
            width: 100% !important;
            overflow-x: hidden !important;
        }
                
        .stApp {
            background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%);
            padding-top: 0 !important;
            margin: 0 !important;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            margin-bottom: 0.5rem;
            margin-top: 0 !important;
            padding-top: 0 !important;
        }
        
        div[data-testid="column"]:has(p.subtitle) {
            display: inline-block !important;
            vertical-align: middle !important;
        }
        
        div[data-testid="column"]:has(button[key="info_button"]) {
            display: inline-block !important;
            vertical-align: middle !important;
            padding-left: 0.5rem !important;
        }
        
        button[key="info_button"],
        button[data-testid="baseButton-secondary"][key="info_button"],
        div[data-testid="stButton"]:has(button[key="info_button"]) button,
        div[data-testid="stButton"] button[key="info_button"] {
            background: #2d2d2d !important;
            border: 1px solid #d4af37 !important;
            color: #d4af37 !important;
            border-radius: 6px !important;
            padding: 0.15rem 0.3rem !important;
            font-size: 0.5rem !important;
            font-weight: 500 !important;
            margin-top: 0 !important;
            margin-bottom: 1.5rem !important;
            height: auto !important;
            min-height: auto !important;
            line-height: 1.2 !important;
            width: auto !important;
        }
        
        button[key="info_button"] p,
        button[key="info_button"] div[data-testid="stMarkdownContainer"],
        button[key="info_button"] div[data-testid="stMarkdownContainer"] p,
        button[data-testid="baseButton-secondary"][key="info_button"] p,
        button[data-testid="baseButton-secondary"][key="info_button"] div[data-testid="stMarkdownContainer"],
        button[data-testid="baseButton-secondary"][key="info_button"] div[data-testid="stMarkdownContainer"] p {
            font-size: 0.5rem !important;
            margin: 0 !important;
            padding: 0 !important;
            line-height: 1.2 !important;
        }
        
        button[key="info_button"]:hover {
            background: #d4af37 !important;
            color: #000 !important;
        }
        
        .info-modal {
            background: #1e1e1e;
            border: 2px solid #d4af37;
            border-radius: 12px;
            padding: 0.75rem;
            padding-top: 0.5rem;
            color: #ffffff;
            max-width: 800px;
            font-size: 0.65rem !important;
            margin-top: 0.1rem;
        }
        
        .info-modal * {
            font-size: inherit !important;
        }
        
        .info-modal h2 {
            color: #d4af37;
            margin-top: 0;
            margin-bottom: 0;
            padding-bottom: 0;
            font-size: 0.65rem !important;
            font-weight: 700 !important;
            cursor: default !important;
            pointer-events: none !important;
        }
        
        .info-modal h2 a,
        .info-modal h3 a,
        .info-modal h4 a {
            pointer-events: none !important;
            cursor: default !important;
            text-decoration: none !important;
        }
        
        .info-modal h3 {
            color: #d4af37;
            margin-top: 0;
            margin-bottom: 0.5rem;
            padding-bottom: 0;
            padding-top: 0;
            font-size: 0.65rem !important;
            font-weight: 600 !important;
            cursor: default !important;
            pointer-events: none !important;
        }
        
        .info-modal h3:first-child {
            margin-top: 0 !important;
            margin-bottom: 0.4rem !important;
            padding-top: 0 !important;
        }
        
        .info-modal h3:not(:first-child) {
            margin-top: 0.65rem !important;
            margin-bottom: 0.4rem !important;
        }
        
        .info-modal h4 {
            color: #d4af37;
            margin-top: 0.1rem;
            margin-bottom: 0.1rem;
            padding-bottom: 0;
            font-size: 0.65rem !important;
            font-weight: 600 !important;
            cursor: default !important;
            pointer-events: none !important;
        }
        
        .info-modal p {
            color: #ccc;
            line-height: 1.3;
            font-size: 0.65rem !important;
            margin-top: 0;
            margin-bottom: 0.05rem;
            padding-top: 0;
        }
        
        .info-modal li {
            color: #ccc;
            line-height: 1.3;
            font-size: 0.65rem !important;
            margin-top: 0;
            margin-bottom: 0;
            padding-top: 0;
            padding-bottom: 0;
        }
        
        .info-modal ul {
            margin-left: 0.8rem;
            margin-top: 0;
            margin-bottom: 0.05rem;
            padding-top: 0;
            padding-bottom: 0;
        }
        
        .info-modal * {
            margin-top: 0 !important;
            margin-bottom: 0 !important;
        }
        
        .info-modal h2 {
            margin-bottom: 0 !important;
        }
        
        .info-modal h3 {
            margin-bottom: 0 !important;
        }
        
        .info-modal h3:not(:first-child) {
            margin-top: 0.65rem !important;
            margin-bottom: 0.4rem !important;
        }
        
        .info-modal h4 {
            margin-top: 0.1rem !important;
            margin-bottom: 0 !important;
        }
        
        .info-modal p {
            margin-top: 0 !important;
            margin-bottom: 0.05rem !important;
        }
        
        .info-modal h3:first-child + p {
            margin-top: 0.25rem !important;
        }
        
        .info-modal p + h3,
        .info-modal ul + h3 {
            margin-top: 0.65rem !important;
        }
        
        .info-modal ul {
            margin-top: 0 !important;
            margin-bottom: 0.05rem !important;
        }
        
        .info-modal li {
            margin-top: 0 !important;
            margin-bottom: 0 !important;
        }
        
        .info-modal strong {
            font-weight: 600 !important;
            color: #d4af37;
        }
        
        .info-modal code {
            background: #2d2d2d;
            padding: 0.1rem 0.2rem;
            border-radius: 4px;
            color: #d4af37;
            font-size: 0.65rem !important;
        }
        
        .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        [data-testid="stAppViewContainer"] {
            padding-left: 0 !important;
            padding-right: 0 !important;
        }
        
        div[data-testid="stVerticalBlock"] {
            width: 100% !important;
            overflow-x: hidden !important;
        }
        
        div:has(.info-modal),
        div[data-testid="element-container"]:has(.info-modal),
        div[data-testid="stMarkdownContainer"]:has(.info-modal),
        div[data-testid="stVerticalBlock"]:has(.info-modal),
        div[data-testid="column"]:has(.info-modal) {
            background: transparent !important;
            border: none !important;
            padding: 0 !important;
            margin: 0 !important;
            box-shadow: none !important;
        }
        
        div[style*="border"]:has(.info-modal),
        div[class*="container"]:has(.info-modal),
        div[class*="block"]:has(.info-modal) {
            border: none !important;
            background: transparent !important;
            outline: none !important;
        }
        
        div[data-testid="stMarkdownContainer"] {
            background: transparent !important;
            border: none !important;
        }
        
        .info-modal div[data-testid="stMarkdownContainer"] p,
        .info-modal div[data-testid="stMarkdownContainer"] li,
        .info-modal div[data-testid="stMarkdownContainer"] {
            font-size: 0.5rem !important;
            line-height: 1.3 !important;
        }
        
        .info-modal div[data-testid="stMarkdownContainer"] h3 {
            font-size: 0.6rem !important;
        }
        
        .info-modal div[data-testid="stMarkdownContainer"] h4 {
            font-size: 0.55rem !important;
        }
        
        .element-container:empty {
            display: none !important;
            height: 0 !important;
        }
        
        div[data-testid="stVerticalBlock"] > div[data-testid="element-container"]:empty {
            display: none !important;
            height: 0 !important;
        }
        
        div[data-testid="stVerticalBlock"] > div:empty {
            display: none !important;
            height: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
        }
        
        .stMarkdown + div:empty {
            display: none !important;
        }
        
        div[data-testid="element-container"] + div[data-testid="element-container"]:empty {
            display: none !important;
        }
        
        div:empty[style*="background"] {
            display: none !important;
        }
        
        .filter-section {
            background: #1e1e1e;
            border: 1px solid #2d2d2d;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            margin-top: 0rem;
        }
        
        [data-testid="stVerticalBlock"] > div:has(> [data-testid="stHorizontalBlock"]) {
            background: #1e1e1e !important;
            border: 1px solid #2d2d2d !important;
            border-radius: 12px !important;
            padding: 1.5rem !important;
            margin-bottom: 2rem !important;
        }
        
        .stSelectbox label, .stDateInput label, .stTextInput label {
            color: #d4af37 !important;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .summary-stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1.5rem;
            margin: 1.5rem 0;
        }
        
        .summary-card {
            background: linear-gradient(135deg, #2d2d2d 0%, #252525 100%);
            border: 1px solid #3d3d3d;
            border-radius: 10px;
            padding: 1.25rem;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
        }
        
        .summary-label {
            color: #888;
            font-size: 0.85rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .summary-value {
            color: #d4af37;
            font-size: 2rem;
            font-weight: 700;
            margin-top: 0.5rem;
        }
        
        .matchup-header {
            background: linear-gradient(135deg, #2d2d2d 0%, #252525 100%);
            border: 2px solid #d4af37;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            color: #d4af37;
            font-size: 1.8rem;
            font-weight: 700;
            text-align: center;
            margin-bottom: 1.5rem;
            box-shadow: 0 2px 8px rgba(212, 175, 55, 0.2);
        }
        
        .team-divider {
            border-top: 2px solid #d4af37;
            margin: 2rem 0;
            position: relative;
        }
        
        .team-divider-label {
            position: absolute;
            top: -12px;
            left: 50%;
            transform: translateX(-50%);
            background: #1e1e1e;
            padding: 0 1rem;
            color: #d4af37;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .player-row {
            background: linear-gradient(135deg, #2d2d2d 0%, #252525 100%);
            border: 1px solid #3d3d3d;
            border-radius: 10px;
            padding: 1rem 1.25rem;
            margin-bottom: 0.5rem;
            margin-top: 0;
            display: flex;
            align-items: center;
            gap: 1.5rem;
            transition: all 0.2s ease;
            height: auto;
            min-height: 70px;
            max-height: none;
            overflow: hidden;
            box-sizing: border-box;
            width: 100%;
            max-width: 100%;
        }
        
        .player-row:hover {
            border-color: #d4af37;
            box-shadow: 0 4px 12px rgba(212, 175, 55, 0.2);
        }
        
        .player-info {
            display: flex;
            align-items: center;
            gap: 1rem;
            min-width: 200px;
            overflow: hidden;
            flex-shrink: 0;
        }
        
        .player-photo {
            width: 70px;
            height: 70px;
            min-width: 70px;
            min-height: 70px;
            max-width: 70px;
            max-height: 70px;
            border-radius: 50%;
            border: 3px solid #d4af37;
            object-fit: cover;
            background: #1e1e1e;
            box-shadow: 0 2px 8px rgba(212, 175, 55, 0.3);
            flex-shrink: 0;
        }
        
        .player-details {
            display: flex;
            flex-direction: column;
            gap: 0.25rem;
        }
        
        .player-name {
            color: #ffffff;
            font-size: 1.1rem;
            font-weight: 700;
        }
        
        .confidence-badge {
            display: inline-block;
            padding: 0.3rem 0.6rem;
            border-radius: 5px;
            font-size: 0.8rem;
            font-weight: 600;
        }
        
        .confidence-high {
            background: linear-gradient(135deg, #4ade80 0%, #22c55e 100%);
            color: #000;
        }
        
        .confidence-medium {
            background: linear-gradient(135deg, #f4a460 0%, #d4af37 100%);
            color: #000;
        }
        
        .confidence-low {
            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
            color: #fff;
        }
        
        .location-badge {
            background: #2d2d2d;
            color: #d4af37;
            padding: 0.3rem 0.6rem;
            border-radius: 5px;
            font-size: 0.75rem;
            font-weight: 600;
            border: 1px solid #3d3d3d;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 1rem;
            flex: 1;
            align-items: center;
            min-width: 0;
            max-width: 100%;
            overflow: hidden;
            box-sizing: border-box;
        }
        
        .stat-card {
            text-align: center;
            min-width: 0;
            max-width: 100%;
            overflow: hidden;
            box-sizing: border-box;
        }
        
        .stats-grid-with-button {
            display: grid;
            grid-template-columns: repeat(7, 1fr) auto;
            gap: 1rem;
            flex: 1;
            align-items: center;
        }
        
        .stat-label {
            color: #888;
            font-size: 0.75rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.25rem;
        }
        
        .stat-value {
            color: #d4af37;
            font-size: 1.4rem;
            font-weight: 700;
        }
        
        .actuals-row {
            background: linear-gradient(135deg, #1a1a1a 0%, #151515 100%);
            border: 1px solid #d4af37;
            border-radius: 10px;
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
            margin-top: 0.5rem;
            display: flex;
            align-items: center;
            gap: 1.5rem;
        }
        
        .actuals-row .player-info {
            min-width: 200px;
            flex-shrink: 0;
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        
        .error-badge {
            display: inline-block;
            padding: 0.3rem 0.6rem;
            border-radius: 5px;
            font-size: 0.8rem;
            font-weight: 600;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #fff;
            margin-right: 0.75rem;
        }
        
        .accuracy-badge {
            display: inline-block;
            padding: 0.3rem 0.6rem;
            border-radius: 5px;
            font-size: 0.8rem;
            font-weight: 600;
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: #fff;
        }
        
        .stat-actual {
            color: #4ade80;
            font-size: 1.4rem;
            font-weight: 700;
        }
        
        .stat-diff {
            color: #888;
            font-size: 0.7rem;
            margin-top: 0.25rem;
        }
        
        .stat-diff.positive {
            color: #4ade80;
        }
        
        .stat-diff.negative {
            color: #ef4444;
        }
        
        .view-actuals-btn {
            min-width: 100px;
            margin-left: 1rem;
        }
        
        div[data-testid="column"]:has(button[key^="actuals_btn_"]) {
            margin-top: 0.5rem;
        }
        
        button[key^="actuals_btn_"] {
            width: auto !important;
            min-width: 120px;
        }
        
        div[data-testid="stHorizontalBlock"]:has(.player-row) {
            display: flex !important;
            align-items: stretch !important;
            width: 100% !important;
            max-width: 100% !important;
            overflow: hidden !important;
        }
        
        div[data-testid="column"]:has(.player-row),
        div[data-testid="column"]:has(button[key^="actuals_btn_"]) {
            display: flex !important;
            flex-direction: column !important;
            align-items: stretch !important;
            width: 100% !important;
            max-width: 100% !important;
            overflow: visible !important;
        }
        
        div[data-testid="column"]:has(.player-row) > div {
            width: 100% !important;
            max-width: 100% !important;
            overflow: visible !important;
        }
        
        div[data-testid="stVerticalBlock"]:has(.player-row) {
            overflow: visible !important;
            min-height: auto !important;
        }
        
        div[data-testid="element-container"]:has(.player-row) {
            overflow: visible !important;
            padding: 0 !important;
            min-height: auto !important;
            max-height: none !important;
        }
        
        div[data-testid="stMarkdownContainer"]:has(.player-row) {
            overflow: visible !important;
            padding: 0 !important;
            margin: 0 !important;
        }
        
        div[data-testid="column"]:has(button[key^="actuals_btn_"]) > div {
            display: flex !important;
            flex-direction: column !important;
            width: 100% !important;
            flex: 1 1 auto !important;
            align-items: stretch !important;
        }
        
        button[key^="actuals_btn_"] {
            min-width: 60px !important;
            width: 60px !important;
            min-height: 70px !important;
            font-size: 0.75rem !important;
            white-space: normal !important;
            line-height: 1.2 !important;
            padding: 0.5rem 0.25rem !important;
            word-break: break-word !important;
            align-self: stretch !important;
            box-sizing: border-box !important;
        }
        
        div[data-testid="column"] {
            padding: 0 !important;
        }
        
        .stExpander {
            background: transparent !important;
            border: none !important;
        }
        
        div[data-testid="stExpander"] > summary,
        div[data-testid="stExpander"] > summary:hover,
        div[data-testid="stExpander"] > summary:focus,
        div[data-testid="stExpander"] > summary:active,
        .stExpander > summary,
        .stExpander > summary:hover,
        .stExpander > summary:focus,
        .stExpander > summary:active {
            color: #d4af37 !important;
            background-color: transparent !important;
        }
        
        div[data-testid="stExpander"] > summary p,
        div[data-testid="stExpander"] > summary p:hover,
        div[data-testid="stExpander"] > summary label,
        div[data-testid="stExpander"] > summary label:hover,
        .stExpander label,
        .stExpander label:hover,
        .stExpander label:focus,
        .stExpander label:active,
        .stExpander p,
        .stExpander p:hover,
        .stExpander div[data-testid="stMarkdownContainer"],
        .stExpander div[data-testid="stMarkdownContainer"] p,
        .stExpander div[data-testid="stMarkdownContainer"] p:hover {
            color: #d4af37 !important;
        }
        
        
        button[kind="primary"] {
            background: linear-gradient(135deg, #d4af37 0%, #b8941f 100%) !important;
            color: #000000 !important;
            border: 2px solid #d4af37 !important;
            font-weight: 600 !important;
        }
        
        button[kind="primary"]:hover {
            background: linear-gradient(135deg, #f4a460 0%, #d4af37 100%) !important;
            border-color: #f4a460 !important;
        }
        
        button[kind="secondary"] {
            background: #2d2d2d !important;
            color: #ffffff !important;
            border: 1px solid #3d3d3d !important;
        }
        
        button[kind="secondary"]:hover {
            background: #3d3d3d !important;
            border-color: #d4af37 !important;
        }
        
        button[key^="all_"], button[key^="team1_"], button[key^="team2_"] {
            transition: all 0.2s ease !important;
        }
        </style>
        <script>
        function matchButtonHeights() {
            document.querySelectorAll('.player-row[data-prediction-id]').forEach(function(playerRow) {
                const predictionId = playerRow.getAttribute('data-prediction-id');
                const rowParent = playerRow.closest('[data-testid="stHorizontalBlock"]');
                if (rowParent) {
                    const button = rowParent.querySelector('button[key^="actuals_btn_"]');
                    if (button) {
                        const playerCardColumn = playerRow.closest('[data-testid="column"]');
                        let totalHeight = 0;
                        
                        if (playerCardColumn) {
                            const actualsRow = document.querySelector('.actuals-row[data-prediction-id="' + predictionId + '"]');
                            
                            if (actualsRow) {
                                const actualsRect = actualsRow.getBoundingClientRect();
                                const actualsStyle = window.getComputedStyle(actualsRow);
                                if (actualsRect.height > 0 && actualsStyle.display !== 'none' && actualsStyle.visibility !== 'hidden') {
                                    let container = playerCardColumn.parentElement;
                                    while (container && !container.contains(actualsRow)) {
                                        container = container.parentElement;
                                    }
                                    
                                    if (container) {
                                        const containerRect = container.getBoundingClientRect();
                                        totalHeight = containerRect.height;
                                    } else {
                                        const playerRowRect = playerRow.getBoundingClientRect();
                                        totalHeight = (actualsRect.bottom - playerRowRect.top);
                                    }
                                } else {
                                    totalHeight = playerRow.offsetHeight;
                                }
                            } else {
                                totalHeight = playerRow.offsetHeight;
                            }
                            
                            if (totalHeight > 0) {
                                button.style.height = totalHeight + 'px';
                                button.style.minHeight = totalHeight + 'px';
                                button.style.maxHeight = totalHeight + 'px';
                            }
                        }
                    }
                }
            });
        }
        
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', matchButtonHeights);
        } else {
            matchButtonHeights();
        }
        
        setTimeout(matchButtonHeights, 100);
        setTimeout(matchButtonHeights, 500);
        setTimeout(matchButtonHeights, 1000);
        
        const observer = new MutationObserver(function(mutations) {
            clearTimeout(window.matchButtonTimeout);
            window.matchButtonTimeout = setTimeout(matchButtonHeights, 50);
        });
        
        observer.observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ['style', 'class']
        });
        </script>
    """, unsafe_allow_html=True)

def get_confidence_class(score):
    if score >= 80:
        return "confidence-high"
    elif score >= 60:
        return "confidence-medium"
    else:
        return "confidence-low"

def load_games_for_date(target_date):
    try:
        conn = get_db_connection()
        query = """
            SELECT DISTINCT
                g.game_id,
                t1.abbreviation as home_team,
                t2.abbreviation as away_team
            FROM games g
            JOIN teams t1 ON g.home_team_id = t1.team_id
            JOIN teams t2 ON g.away_team_id = t2.team_id
            JOIN predictions p ON g.game_id = p.game_id
            WHERE p.prediction_date = %s
            ORDER BY g.game_id
        """
        df = pd.read_sql(query, conn, params=(target_date,))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading games: {str(e)}")
        return pd.DataFrame()

def get_all_players():
    try:
        conn = get_db_connection()
        query = """
            SELECT DISTINCT
                pl.player_id,
                pl.full_name,
                t.abbreviation as team_abbr
            FROM predictions p
            JOIN players pl ON p.player_id = pl.player_id
            JOIN teams t ON pl.team_id = t.team_id
            ORDER BY pl.full_name
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

def get_all_teams():
    try:
        conn = get_db_connection()
        query = """
            SELECT DISTINCT
                t.team_id,
                t.abbreviation,
                t.full_name,
                t.city
            FROM teams t
            JOIN players p ON t.team_id = p.team_id
            JOIN predictions pred ON p.player_id = pred.player_id
            ORDER BY t.full_name
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

def normalize_team_search(search_term):
    if not search_term:
        return ""
    
    search_lower = search_term.lower().strip()
    
    team_mappings = {
        'lakers': 'LAL',
        'celtics': 'BOS',
        'warriors': 'GSW',
        'bulls': 'CHI',
        'heat': 'MIA',
        'knicks': 'NYK',
        'nets': 'BKN',
        '76ers': 'PHI',
        'sixers': 'PHI',
        'raptors': 'TOR',
        'wizards': 'WAS',
        'hawks': 'ATL',
        'hornets': 'CHA',
        'cavaliers': 'CLE',
        'cavs': 'CLE',
        'pistons': 'DET',
        'pacers': 'IND',
        'bucks': 'MIL',
        'magic': 'ORL',
        'suns': 'PHX',
        'blazers': 'POR',
        'trail blazers': 'POR',
        'kings': 'SAC',
        'spurs': 'SAS',
        'thunder': 'OKC',
        'jazz': 'UTA',
        'nuggets': 'DEN',
        'timberwolves': 'MIN',
        'twolves': 'MIN',
        'pelicans': 'NOP',
        'mavericks': 'DAL',
        'mavs': 'DAL',
        'rockets': 'HOU',
        'grizzlies': 'MEM',
        'clippers': 'LAC',
        'lakers': 'LAL'
    }
    
    if search_lower in team_mappings:
        return team_mappings[search_lower]
    
    if len(search_term) == 3 and search_term.isupper():
        return search_term
    
    return search_term

def load_todays_predictions(target_date, search_query="", team_search="", position_filter="All Positions", game_filter="All Games"):
    try:
        df = load_predictions_with_ensemble(target_date)
        
        if df.empty:
            return df
        
        if search_query:
            df = df[df['player_name'].str.contains(search_query, case=False, na=False)]
        
        if team_search:
            normalized_team = normalize_team_search(team_search)
            if normalized_team and len(normalized_team) == 3:
                df = df[df['team_abbr'].str.upper() == normalized_team.upper()]
            else:
                df = df[
                    df['team_abbr'].str.contains(team_search, case=False, na=False) |
                    df['team_name'].str.contains(team_search, case=False, na=False)
                ]
        
        if position_filter != "All Positions":
            if position_filter == "Guard":
                df = df[df['position'].isin(['Guard', 'Guard-Forward'])]
            elif position_filter == "Forward":
                df = df[df['position'].isin(['Forward', 'Forward-Center', 'Guard-Forward'])]
            elif position_filter == "Center":
                df = df[df['position'].isin(['Center', 'Center-Forward', 'Forward-Center'])]
        
        if game_filter != "All Games":
            df = df[df['game_id'] == game_filter]
        
        df = df.sort_values(['game_id', 'confidence_score'], ascending=[True, False])
        
        return df
    except Exception as e:
        st.error(f"Error loading predictions: {str(e)}")
        return pd.DataFrame()

def display_players(players_df, target_date):
    today = datetime.now().date()
    
    for idx, (_, player) in enumerate(players_df.iterrows()):
        photo_url = get_player_photo_url(player['player_id'])
        confidence_class = get_confidence_class(player['confidence_score'])
        prediction_id = player['prediction_id']
        
        game_date_val = player.get('game_date')
        if pd.notna(game_date_val):
            if isinstance(game_date_val, str):
                game_date = pd.to_datetime(game_date_val).date()
            elif hasattr(game_date_val, 'date'):
                game_date = game_date_val.date() if hasattr(game_date_val, 'date') else game_date_val
            else:
                game_date = pd.to_datetime(game_date_val).date()
        else:
            game_date = None
        
        has_actuals = pd.notna(player.get('actual_points'))
        is_past_game = game_date is not None and game_date < today
        
        should_show_button = has_actuals and is_past_game
        show_actuals = st.session_state.get(f"show_actuals_{prediction_id}", False)
        
        player_container = st.container()
        with player_container:
            col1 = st.container()
        
        with col1:
            player_html = f"""
            <div class="player-row" data-prediction-id="{prediction_id}">
                <div class="player-info">
                    <img src="{photo_url}" class="player-photo" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
                    <div class="player-details">
                        <div class="player-name">{player['player_name']}</div>
                        <div>
                            <span class="confidence-badge {confidence_class}">{player['confidence_score']}% Confidence</span>
                            <span class="location-badge">{player['location']}</span>
                        </div>
                    </div>
                </div>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">PTS</div>
                        <div class="stat-value">{player['predicted_points']:.1f}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">REB</div>
                        <div class="stat-value">{player['predicted_rebounds']:.1f}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">AST</div>
                        <div class="stat-value">{player['predicted_assists']:.1f}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">STL</div>
                        <div class="stat-value">{player['predicted_steals']:.1f}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">BLK</div>
                        <div class="stat-value">{player['predicted_blocks']:.1f}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">TO</div>
                        <div class="stat-value">{player['predicted_turnovers']:.1f}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">3PM</div>
                        <div class="stat-value">{player['predicted_three_pointers_made']:.1f}</div>
                    </div>
                </div>
            </div>
            """
            st.markdown(player_html, unsafe_allow_html=True)
        
        if should_show_button:
            with col1:
                button_text = "View Actuals" if not show_actuals else "Hide Actuals"
                if st.button(button_text, 
                            key=f"actuals_btn_{prediction_id}", 
                            type="secondary",
                            use_container_width=True):
                    st.session_state[f"show_actuals_{prediction_id}"] = not show_actuals
                    st.rerun()
        
        if should_show_button and show_actuals:
            with player_container:
                try:
                    error = float(player.get('prediction_error', 0)) if pd.notna(player.get('prediction_error')) else 0.0
                    
                    stats_to_compare = [
                        ('PTS', 'predicted_points', 'actual_points'),
                        ('REB', 'predicted_rebounds', 'actual_rebounds'),
                        ('AST', 'predicted_assists', 'actual_assists'),
                        ('STL', 'predicted_steals', 'actual_steals'),
                        ('BLK', 'predicted_blocks', 'actual_blocks'),
                        ('TO', 'predicted_turnovers', 'actual_turnovers'),
                        ('3PM', 'predicted_three_pointers_made', 'actual_three_pointers_made')
                    ]
                    
                    stat_accuracies = []
                    for _, pred_key, actual_key in stats_to_compare:
                        pred_val = float(player[pred_key]) if pd.notna(player[pred_key]) else 0.0
                        actual_val = float(player.get(actual_key, 0)) if pd.notna(player.get(actual_key)) else 0.0
                        
                        if actual_val > 0:
                            error_pct = abs(pred_val - actual_val) / actual_val * 100
                            stat_accuracy = max(0, 100 - error_pct)
                            stat_accuracies.append(stat_accuracy)
                        elif actual_val == 0 and pred_val == 0:
                            stat_accuracies.append(100)
                    
                    accuracy = np.mean(stat_accuracies) if stat_accuracies else 0
                    
                    shift_map = {
                        'PTS': 1.75,
                        'REB': 1.5,
                        'AST': 1.25,
                        'STL': 1.0,
                        'BLK': 0,
                        'TO': 0,
                        '3PM': 0
                    }
                    
                    stat_cards_parts = []
                    for label, pred_key, actual_key in stats_to_compare:
                        predicted = float(player[pred_key]) if pd.notna(player[pred_key]) else 0.0
                        actual = float(player.get(actual_key, 0)) if pd.notna(player.get(actual_key)) else 0.0
                        diff = actual - predicted
                        diff_class = "positive" if diff > 0 else ("negative" if diff < 0 else "")
                        diff_sign = "+" if diff > 0 else ""
                        
                        shift = shift_map.get(label, 0)
                        margin_left = f"margin-left: -{shift}rem;" if shift > 0 else ""
                        
                        stat_cards_parts.append(
                            f'<div class="stat-card" style="{margin_left}">'
                            f'<div class="stat-label">{label}</div>'
                            f'<div class="stat-actual">{actual:.1f}</div>'
                            f'<div class="stat-diff {diff_class}">{diff_sign}{diff:.1f}</div>'
                            f'</div>'
                        )
                    
                    stat_cards_html = ''.join(stat_cards_parts)
                    
                    actuals_html = f"""
                    <div class="actuals-row" data-prediction-id="{prediction_id}">
                        <div class="player-info">
                            <div style="width: 70px; height: 70px; min-width: 70px; min-height: 70px; max-width: 70px; max-height: 70px; flex-shrink: 0; visibility: hidden;"></div>
                            <div class="player-details" style="justify-content: center; align-items: center; display: flex; flex-direction: column; gap: 0.5rem;">
                                <div style="color: #d4af37; font-size: 1.1rem; font-weight: 700; position: relative; left: -1.75rem;">Actual Performance</div>
                                <div style="position: relative; left: -1.75rem;">
                                    <span class="error-badge">Error: {error:.2f}</span>
                                    <span class="accuracy-badge">{accuracy:.1f}% Accurate</span>
                                </div>
                            </div>
                        </div>
                        <div class="stats-grid">
                            {stat_cards_html}
                        </div>
                    </div>
                    """
                    
                    st.markdown(actuals_html, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Error displaying actuals: {str(e)}")

def main():
    load_custom_css()
    show_navigation()
    
    st.markdown("""
    <h1>NBA Predictions</h1>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <p class="subtitle">AI-powered player performance predictions</p>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        target_date = st.date_input(
            "Game Date",
            value=datetime.now().date(),
            max_value=datetime.now().date() + timedelta(days=7)
        )

    games_df = load_games_for_date(target_date)
    game_options = ["All Games"] + [f"{row['away_team']} @ {row['home_team']}" for _, row in games_df.iterrows()]
    game_ids = {f"{row['away_team']} @ {row['home_team']}": row['game_id'] for _, row in games_df.iterrows()}

    with col2:
        game_filter_display = st.selectbox(
            "Filter by Game",
            game_options
        )
        game_filter = game_ids.get(game_filter_display, "All Games") if game_filter_display != "All Games" else "All Games"

    with col3:
        confidence_filter = st.selectbox(
            "Filter by Confidence",
            ["All Predictions", "High (80+)", "Medium (60-79)", "Low (<60)"]
        )

    col4, col5, col6 = st.columns(3)
    with col4:
        all_players_df = get_all_players()
        player_options = ["All Players"] + [f"{row['full_name']} ({row['team_abbr']})" for _, row in all_players_df.iterrows()]
        selected_player_display = st.selectbox("Search Player", player_options, key="player_search")
        search_query = "" if selected_player_display == "All Players" else selected_player_display.split(" (")[0]

    with col5:
        all_teams_df = get_all_teams()
        team_options = ["All Teams"] + [f"{row['abbreviation']} - {row['full_name']}" for _, row in all_teams_df.iterrows()]
        selected_team_display = st.selectbox("Search Team", team_options, key="team_search")
        if selected_team_display == "All Teams":
            team_search = ""
        else:
            team_search = selected_team_display.split(" - ")[0] if " - " in selected_team_display else selected_team_display

    with col6:
        position_filter = st.selectbox(
            "Filter by Position",
            ["All Positions", "Guard", "Forward", "Center"]
        )

    predictions_df = load_todays_predictions(target_date, search_query, team_search, position_filter, game_filter)

    ensemble_warning = get_ensemble_warning(target_date)
    if ensemble_warning:
        missing = ', '.join(ensemble_warning['missing'])
        available = ', '.join(ensemble_warning['available'])
        st.info(f"ℹ️ **Ensemble Note:** Not all selected models have predictions for {target_date}. Using available models: **{available}**. Missing: {missing}.")

    if predictions_df.empty:
        st.warning(f"No predictions found for {target_date}")
        return

    if confidence_filter == "High (80+)":
        predictions_df = predictions_df[predictions_df['confidence_score'] >= 80]
    elif confidence_filter == "Medium (60-79)":
        predictions_df = predictions_df[(predictions_df['confidence_score'] >= 60) & (predictions_df['confidence_score'] < 80)]
    elif confidence_filter == "Low (<60)":
        predictions_df = predictions_df[predictions_df['confidence_score'] < 60]

    total_games = predictions_df['game_id'].nunique()
    high_conf = len(predictions_df[predictions_df['confidence_score'] >= 80])
    avg_conf = predictions_df['confidence_score'].mean()

    stats_html = f"""
    <div class="summary-stats">
        <div class="summary-card">
            <div class="summary-label">Total Predictions</div>
            <div class="summary-value">{len(predictions_df)}</div>
        </div>
        <div class="summary-card">
            <div class="summary-label">Total Games</div>
            <div class="summary-value">{total_games}</div>
        </div>
        <div class="summary-card">
            <div class="summary-label">Avg Confidence</div>
            <div class="summary-value">{avg_conf:.0f}%</div>
        </div>
        <div class="summary-card">
            <div class="summary-label">High Confidence</div>
            <div class="summary-value">{high_conf}</div>
        </div>
    </div>
    """
    st.markdown(stats_html, unsafe_allow_html=True)

    games_grouped = predictions_df.groupby('game_id')

    for game_id, game_group in games_grouped:
        game_info = games_df[games_df['game_id'] == game_id]
        if len(game_info) > 0:
            home_team = game_info.iloc[0]['home_team']
            away_team = game_info.iloc[0]['away_team']
            matchup_title = f"{away_team} @ {home_team}"
        else:
            teams = game_group['team_abbr'].unique()
            team1_abbr = teams[0]
            team2_abbr = game_group[game_group['team_abbr'] != team1_abbr]['team_abbr'].iloc[0] if len(teams) > 1 else teams[0]
            matchup_title = f"{team1_abbr} vs {team2_abbr}"
        
        if len(game_info) > 0:
            team1_abbr = away_team
            team2_abbr = home_team
        else:
            teams = game_group['team_abbr'].unique()
            team1_abbr = teams[0]
            team2_abbr = game_group[game_group['team_abbr'] != team1_abbr]['team_abbr'].iloc[0] if len(teams) > 1 else teams[0]

        with st.expander(matchup_title, expanded=True):
            st.markdown(f'<div class="matchup-header">{matchup_title}</div>', unsafe_allow_html=True)

            team_filter_key = f"team_filter_{game_id}"
            if team_filter_key not in st.session_state:
                st.session_state[team_filter_key] = "All"

            current_filter = st.session_state[team_filter_key]

            col1, col2, col3 = st.columns([1, 1, 1])

            with col1:
                if st.button("All", key=f"all_{game_id}", use_container_width=True, 
                           type="primary" if current_filter == "All" else "secondary"):
                    st.session_state[team_filter_key] = "All"
                    st.rerun()

            with col2:
                if st.button(team1_abbr, key=f"team1_{game_id}", use_container_width=True,
                           type="primary" if current_filter == team1_abbr else "secondary"):
                    st.session_state[team_filter_key] = team1_abbr
                    st.rerun()

            with col3:
                if st.button(team2_abbr, key=f"team2_{game_id}", use_container_width=True,
                           type="primary" if current_filter == team2_abbr else "secondary"):
                    st.session_state[team_filter_key] = team2_abbr
                    st.rerun()

            if current_filter == "All":
                team1_players = game_group[game_group['team_abbr'] == team1_abbr]
                team2_players = game_group[game_group['team_abbr'] == team2_abbr]

                if len(team1_players) > 0:
                    display_players(team1_players, target_date)
                else:
                    st.markdown(f'<div style="text-align: center; color: #888; padding: 2rem;"><em>No players matched your search criteria for {team1_abbr}</em></div>', unsafe_allow_html=True)

                if len(team2_players) > 0:
                    st.markdown(f'<div class="team-divider"><span class="team-divider-label">{team2_abbr}</span></div>', unsafe_allow_html=True)
                    display_players(team2_players, target_date)
                elif len(team1_players) > 0:
                    st.markdown(f'<div class="team-divider"><span class="team-divider-label">{team2_abbr}</span></div>', unsafe_allow_html=True)
                    st.markdown(f'<div style="text-align: center; color: #888; padding: 2rem;"><em>No players matched your search criteria for {team2_abbr}</em></div>', unsafe_allow_html=True)
            else:
                filtered_players = game_group[game_group['team_abbr'] == current_filter]
                display_players(filtered_players, target_date)

if __name__ == "__main__":
    main()