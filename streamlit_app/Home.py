import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

load_dotenv()

st.set_page_config(
    page_title="NBA Predictions",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="collapsed"
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
            margin: 0 0 0.5rem 0 !important;
            letter-spacing: -0.02em;
            padding-top: 0 !important;
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
        
        /* Prevent layout shift */
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
            margin-top: 0.25rem;
        }
        
        /* Make subtitle and button appear on same line */
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
        
        /* Remove all default spacing from info modal elements */
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
        
        /* Prevent layout shift when info modal appears/disappears */
        .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        [data-testid="stAppViewContainer"] {
            padding-left: 0 !important;
            padding-right: 0 !important;
        }
        
        /* Prevent horizontal scrolling and layout shift */
        div[data-testid="stVerticalBlock"] {
            width: 100% !important;
            overflow-x: hidden !important;
        }
        
        /* Remove any container boxes around info modal - be very aggressive */
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
        
        /* Remove yellow border from any containers - target all possible Streamlit containers */
        div[style*="border"]:has(.info-modal),
        div[class*="container"]:has(.info-modal),
        div[class*="block"]:has(.info-modal) {
            border: none !important;
            background: transparent !important;
            outline: none !important;
        }
        
        /* Target Streamlit markdown containers specifically */
        div[data-testid="stMarkdownContainer"] {
            background: transparent !important;
            border: none !important;
        }
        
        /* Force smaller font sizes on Streamlit markdown content inside info-modal */
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
        
        /* Hide empty containers and reduce gaps */
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
        
        /* Target specific empty divs between elements */
        div[data-testid="element-container"] + div[data-testid="element-container"]:empty {
            display: none !important;
        }
        
        /* Hide all empty divs with background */
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
        
        /* Style the main container that holds the filters */
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
            padding: 1.25rem;
            margin-bottom: 0.5rem;
            display: flex;
            align-items: center;
            gap: 1.5rem;
            transition: all 0.2s ease;
            min-height: 70px;
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
        }
        
        .player-photo {
            width: 70px;
            height: 70px;
            border-radius: 50%;
            border: 3px solid #d4af37;
            object-fit: cover;
            background: #1e1e1e;
            box-shadow: 0 2px 8px rgba(212, 175, 55, 0.3);
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
        }
        
        .stats-grid-with-button {
            display: grid;
            grid-template-columns: repeat(7, 1fr) auto;
            gap: 1rem;
            flex: 1;
            align-items: center;
        }
        
        .stat-card {
            text-align: center;
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
            padding: 1.25rem;
            margin-bottom: 1rem;
            margin-top: 0.5rem;
            display: flex;
            align-items: center;
            gap: 1.5rem;
        }
        
        .actuals-header {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
            min-width: 200px;
            align-items: center;
        }
        
        .actuals-title {
            color: #d4af37;
            font-size: 1.1rem;
            font-weight: 700;
            text-align: center;
            width: 100%;
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
        
        .stat-comparison {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 1rem;
            flex: 1;
        }
        
        .stat-compare-card {
            text-align: center;
        }
        
        .stat-actual {
            color: #4ade80;
            font-size: 1.4rem;
            font-weight: 700;
        }
        
        .stat-diff {
            color: #888;
            font-size: 0.75rem;
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
        
        /* Make the horizontal block (columns container) use flexbox with stretch */
        div[data-testid="stHorizontalBlock"]:has(.player-row) {
            display: flex !important;
            align-items: stretch !important;
        }
        
        /* Make both columns stretch to match the tallest content */
        div[data-testid="column"]:has(.player-row),
        div[data-testid="column"]:has(button[key^="actuals_btn_"]) {
            display: flex !important;
            flex-direction: column !important;
            align-items: stretch !important;
        }
        
        /* Make the button column's inner div stretch */
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
            height: 100% !important;
            font-size: 0.75rem !important;
            white-space: normal !important;
            line-height: 1.2 !important;
            padding: 0.5rem 0.25rem !important;
            word-break: break-word !important;
            flex: 1 1 100% !important;
            align-self: stretch !important;
        }
        
        div[data-testid="column"] {
            padding: 0 !important;
        }
        
        .stExpander {
            background: transparent !important;
            border: none !important;
        }
        
        .stExpander > summary {
            color: #d4af37 !important;
        }
        
        .stExpander > summary:hover {
            color: #f4a460 !important;
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
        // Match button height to player-row height + actuals-row if visible
        function matchButtonHeights() {
            document.querySelectorAll('.player-row[data-prediction-id]').forEach(function(playerRow) {
                const predictionId = playerRow.getAttribute('data-prediction-id');
                const rowParent = playerRow.closest('[data-testid="stHorizontalBlock"]');
                if (rowParent) {
                    const button = rowParent.querySelector('button[key^="actuals_btn_"]');
                    if (button) {
                        // Start with player-row height
                        let totalHeight = playerRow.offsetHeight;
                        
                        // Find the actuals-row with matching prediction-id
                        const actualsRow = document.querySelector('.actuals-row[data-prediction-id="' + predictionId + '"]');
                        if (actualsRow) {
                            // Check if it's visible
                            const rect = actualsRow.getBoundingClientRect();
                            const style = window.getComputedStyle(actualsRow);
                            if (rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden') {
                                // Get positions
                                const rowParentRect = rowParent.getBoundingClientRect();
                                const actualsRect = actualsRow.getBoundingClientRect();
                                
                                // Calculate total height from top of rowParent to bottom of actualsRow
                                // Add any margin between them
                                const margin = parseInt(style.marginTop) || 0;
                                totalHeight = (actualsRect.bottom - rowParentRect.top) + margin;
                            }
                        }
                        
                        const buttonColumn = button.closest('[data-testid="column"]');
                        if (buttonColumn) {
                            // Set height on both column and button
                            buttonColumn.style.height = totalHeight + 'px';
                            button.style.height = totalHeight + 'px';
                            button.style.minHeight = totalHeight + 'px';
                        }
                    }
                }
            });
        }
        
        // Run on page load and after any updates
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', matchButtonHeights);
        } else {
            matchButtonHeights();
        }
        
        // Also run after a short delay to catch dynamically loaded content
        setTimeout(matchButtonHeights, 100);
        setTimeout(matchButtonHeights, 500);
        setTimeout(matchButtonHeights, 1000);
        
        // Use MutationObserver to watch for DOM changes
        const observer = new MutationObserver(function(mutations) {
            // Debounce the function to avoid too many calls
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

def load_todays_predictions(target_date, search_query="", team_search="", position_filter="All Positions", game_filter="All Games"):
    try:
        conn = get_db_connection()
        params = [target_date]
        search_filters = []
        
        if search_query:
            search_filters.append("LOWER(pl.full_name) LIKE LOWER(%s)")
            params.append(f"%{search_query}%")
        
        if team_search:
            search_filters.append("(LOWER(t1.abbreviation) LIKE LOWER(%s) OR LOWER(t1.full_name) LIKE LOWER(%s))")
            params.append(f"%{team_search}%")
            params.append(f"%{team_search}%")
        
        if position_filter != "All Positions":
            if position_filter == "Guard":
                search_filters.append("pl.position IN ('Guard', 'Guard-Forward')")
            elif position_filter == "Forward":
                search_filters.append("pl.position IN ('Forward', 'Forward-Center', 'Guard-Forward')")
            elif position_filter == "Center":
                search_filters.append("pl.position IN ('Center', 'Center-Forward', 'Forward-Center')")
        
        if game_filter != "All Games":
            search_filters.append("p.game_id = %s")
            params.append(game_filter)
        
        where_clause = " AND " + " AND ".join(search_filters) if search_filters else ""
        
        query = f"""
            SELECT 
                p.prediction_id,
                p.player_id,
                p.game_id,
                g.game_date,
                pl.full_name as player_name,
                pl.position,
                pl.team_id,
                t1.full_name as team_name,
                t1.abbreviation as team_abbr,
                t2.full_name as opponent_name,
                t2.abbreviation as opponent_abbr,
                CASE WHEN g.home_team_id = pl.team_id THEN 'HOME' ELSE 'AWAY' END as location,
                p.predicted_points,
                p.predicted_rebounds,
                p.predicted_assists,
                p.predicted_steals,
                p.predicted_blocks,
                p.predicted_turnovers,
                p.predicted_three_pointers_made,
                p.confidence_score,
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
            {where_clause}
            ORDER BY g.game_id, p.confidence_score DESC
        """
        df = pd.read_sql(query, conn, params=tuple(params))
        conn.close()
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
        
        # Check if game is in the past and player has actual stats
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
        
        # Only show button if game is in past AND player has actual stats
        should_show_button = has_actuals and is_past_game
        show_actuals = st.session_state.get(f"show_actuals_{prediction_id}", False)
        
        # Create a wrapper container for the player and button
        if should_show_button:
            player_container = st.container()
            with player_container:
                col1, col2 = st.columns([11, 1])
        else:
            col1 = st.container()
            col2 = None
            player_container = None
        
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
        
        if col2 and should_show_button:
            with col2:
                button_text = "View\nActuals" if not show_actuals else "Hide\nActuals"
                if st.button(button_text, 
                            key=f"actuals_btn_{prediction_id}", 
                            type="secondary",
                            use_container_width=True):
                    st.session_state[f"show_actuals_{prediction_id}"] = not show_actuals
                    st.rerun()
        
        if should_show_button and show_actuals:
            # Render actuals in the same container if it exists
            container_to_use = player_container if player_container else st.container()
            with container_to_use:
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
                    
                    total_pred = sum([float(player[pred]) if pd.notna(player[pred]) else 0.0 for _, pred, _ in stats_to_compare])
                    total_actual = sum([float(player.get(act, 0)) if pd.notna(player.get(act)) else 0.0 for _, _, act in stats_to_compare])
                    accuracy = 100 - (abs(total_pred - total_actual) / max(total_actual, 1) * 100) if total_actual > 0 else 0
                    
                    # Build stat cards HTML with proper formatting
                    stat_cards_parts = []
                    for label, pred_key, actual_key in stats_to_compare:
                        predicted = float(player[pred_key]) if pd.notna(player[pred_key]) else 0.0
                        actual = float(player.get(actual_key, 0)) if pd.notna(player.get(actual_key)) else 0.0
                        diff = actual - predicted
                        diff_class = "positive" if diff > 0 else ("negative" if diff < 0 else "")
                        diff_sign = "+" if diff > 0 else ""
                        
                        stat_cards_parts.append(
                            f'<div class="stat-compare-card">'
                            f'<div class="stat-label">{label}</div>'
                            f'<div class="stat-actual">{actual:.1f}</div>'
                            f'<div class="stat-diff {diff_class}">{diff_sign}{diff:.1f}</div>'
                            f'</div>'
                        )
                    
                    stat_cards_html = ''.join(stat_cards_parts)
                    
                    # Construct complete HTML as a single clean string with data attribute
                    actuals_html = '<div class="actuals-row" data-prediction-id="{}"><div class="actuals-header"><div class="actuals-title">Actual Performance</div><div><span class="error-badge">Error: {:.2f}</span><span class="accuracy-badge">{:.1f}% Accurate</span></div></div><div class="stat-comparison">{}</div></div>'.format(
                        prediction_id, error, accuracy, stat_cards_html
                    )
                    
                    # Render HTML - ensure it's treated as HTML, not markdown code
                    st.markdown(actuals_html, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Error displaying actuals: {str(e)}")

def main():
    load_custom_css()
    
    # Header with title and subtitle
    st.markdown("""
    <h1>NBA PREDICTIONS</h1>
    """, unsafe_allow_html=True)
    
    # Subtitle
    st.markdown("""
    <p class="subtitle">AI-powered player performance predictions powered by XGBoost</p>
    """, unsafe_allow_html=True)
    
    # Info button below subtitle
    if 'show_info' not in st.session_state:
        st.session_state.show_info = False
    
    button_text = "Hide Info" if st.session_state.show_info else "Show Info"
    if st.button(button_text, key="info_button", help="Click for information about how predictions work"):
        st.session_state.show_info = not st.session_state.show_info
        st.rerun()
    
    # Info modal - use pure HTML to avoid Streamlit container styling
    if st.session_state.show_info:
        info_html = """<div class="info-modal"><h3>Model Overview</h3><p>Our predictions use <strong>XGBoost</strong>, a gradient boosting machine learning algorithm, trained on historical NBA player performance data. We maintain separate models for each statistic (Points, Rebounds, Assists, Steals, Blocks, Turnovers, and 3-Pointers Made). All features are standardized (z-score normalization) to ensure fair comparison across different scales.</p><br><h3>Features Used</h3><ul><li><strong>Recent Form:</strong> Rolling averages from last 5, 10, and 20 games for points, rebounds, and assists. We use both unweighted averages and exponentially weighted averages where more recent games are weighted more heavily (exponential decay factor of 0.1). This captures both overall recent performance and recency trends.</li><li><strong>Game Context:</strong> Home/away status (home court advantage), days of rest between games, and whether it's a back-to-back game. These factors significantly impact player performance and fatigue levels.</li><li><strong>Team Ratings:</strong> Offensive rating (points per 100 possessions), defensive rating (points allowed per 100 possessions), and pace (possessions per game) for both the player's team and opponent. These metrics capture team strength and playing style.</li><li><strong>Opponent Defense:</strong> Field goal percentage and 3-point percentage allowed by the opposing team. Stronger defensive teams typically limit individual player production.</li><li><strong>Teammate Impact:</strong> Whether star teammates (20+ PPG) are injured or out. When star players are unavailable, other players often see increased usage and production opportunities.</li><li><strong>Playoff Experience:</strong> Career playoff games played and playoff performance boost (difference between playoff and regular season scoring averages). These features are only applied when predicting playoff games, as playoff basketball has different intensity and defensive schemes.</li><li><strong>Altitude:</strong> Arena altitude effects for away games. High-altitude venues (above 3000 feet) can impact player performance due to reduced oxygen levels, particularly affecting endurance and shooting accuracy.</li></ul><br><h3>Confidence Score</h3><p>The confidence score (0-100%) indicates prediction reliability based on:</p><ul><li>Player's recent game history and consistency (lower variance = higher confidence)</li><li>Availability of required features (missing data reduces confidence)</li><li>Number of games played in the season (more games = more reliable patterns)</li><li>Contextual factors (back-to-back games, injuries, altitude reduce confidence)</li></ul><br><h3>Accuracy Metrics</h3><p>When actual game results are available, we calculate:</p><ul><li><strong>Error:</strong> Average absolute error across all 7 statistics</li><li><strong>% Accurate:</strong> Calculated as 100 - (|Total Predicted - Total Actual| / Total Actual) × 100, where totals are the sum of all 7 statistics (Points, Rebounds, Assists, Steals, Blocks, Turnovers, 3-Pointers). This measures how close the combined predicted stats are to the combined actual stats.</li></ul><br><h3>View Actuals</h3><p>The "View Actuals" button appears for past games where actual statistics are available. It shows a comparison between predicted and actual performance, including the difference for each statistic.</p></div>"""
        st.markdown(info_html, unsafe_allow_html=True)
    
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
        search_query = st.text_input("Search Player", placeholder="e.g. LeBron James")

    with col5:
        team_search = st.text_input("Search Team", placeholder="e.g. LAL")

    with col6:
        position_filter = st.selectbox(
            "Filter by Position",
            ["All Positions", "Guard", "Forward", "Center"]
        )

    predictions_df = load_todays_predictions(target_date, search_query, team_search, position_filter, game_filter)

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
        teams = game_group['team_abbr'].unique()
        team1_abbr = teams[0]
        team2_abbr = game_group[game_group['team_abbr'] != team1_abbr]['team_abbr'].iloc[0] if len(teams) > 1 else teams[0]

        matchup_title = f"{team1_abbr} vs {team2_abbr}"

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

                display_players(team1_players, target_date)

                if len(team2_players) > 0:
                    st.markdown(f'<div class="team-divider"><span class="team-divider-label">{team2_abbr}</span></div>', unsafe_allow_html=True)
                    display_players(team2_players, target_date)
            else:
                filtered_players = game_group[game_group['team_abbr'] == current_filter]
                display_players(filtered_players, target_date)

if __name__ == "__main__":
    main()