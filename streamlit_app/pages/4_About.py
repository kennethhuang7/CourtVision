import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

st.set_page_config(
    page_title="About - NBA Predictions",
    page_icon="ℹ️",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
        
        h2 {
            color: #d4af37 !important;
            font-weight: 600;
            font-size: 1.8rem;
            margin-top: -0.5rem;
            margin-bottom: 0.6rem;
            border-bottom: 2px solid #2d2d2d;
            padding-bottom: 0.3rem;
        }
        
        h3 {
            color: #d4af37 !important;
            font-weight: 600;
            font-size: 1.4rem;
            margin-top: 1rem;
            margin-bottom: 0.5rem;
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
        
        .info-card {
            background: #1e1e1e;
            border: 2px solid #2d2d2d;
            border-radius: 12px;
            padding: 1.2rem;
            margin: 0.6rem 0;
        }
        
        .info-card.gold {
            border-color: #d4af37;
        }
        </style>
    """, unsafe_allow_html=True)

def create_mae_example():
    example_data = {
        'Player': ['Player A', 'Player B', 'Player C', 'Player D', 'Player E'],
        'Predicted': [25.0, 18.5, 12.0, 8.5, 15.2],
        'Actual': [27.0, 17.0, 11.5, 9.0, 14.8]
    }
    df = pd.DataFrame(example_data)
    df['Error'] = abs(df['Predicted'] - df['Actual'])
    df['MAE'] = df['Error'].mean()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Predicted',
        x=df['Player'],
        y=df['Predicted'],
        marker_color='#d4af37',
        opacity=0.8,
        hovertemplate='<b>%{x}</b><br>Predicted: <b>%{y:.1f}</b> points<extra></extra>',
        text=df['Predicted'].round(1),
        textposition='outside',
        textfont=dict(color='#d4af37', size=11)
    ))
    
    fig.add_trace(go.Bar(
        name='Actual',
        x=df['Player'],
        y=df['Actual'],
        marker_color='#666',
        opacity=0.8,
        hovertemplate='<b>%{x}</b><br>Actual: <b>%{y:.1f}</b> points<extra></extra>',
        text=df['Actual'].round(1),
        textposition='outside',
        textfont=dict(color='#ccc', size=11)
    ))
    
    for i, row in df.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Player'], row['Player']],
            y=[min(row['Predicted'], row['Actual']), max(row['Predicted'], row['Actual'])],
            mode='lines+markers',
            line=dict(color='#ff4444', width=3),
            marker=dict(size=8, color='#ff4444', symbol='diamond'),
            showlegend=False,
            hoverinfo='skip'
        ))
        
        mid_y = (row['Predicted'] + row['Actual']) / 2
        fig.add_annotation(
            x=row['Player'],
            y=max(row['Predicted'], row['Actual']) + 1,
            text=f"Error: {row['Error']:.1f}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor='#ff4444',
            ax=0,
            ay=-20,
            bgcolor='rgba(255, 68, 68, 0.9)',
            bordercolor='#ff4444',
            borderwidth=1,
            font=dict(color='white', size=10, family='Inter')
        )
    
    fig.update_layout(
        title=dict(
            text="MAE Example: Predicted vs Actual Points",
            font=dict(size=18, color='#d4af37', family='Inter'),
            x=0.5,
            xanchor='center',
            pad=dict(b=2)
        ),
        xaxis=dict(
            title="Player",
            titlefont=dict(color='#ccc', size=12),
            tickfont=dict(color='#888', size=11),
            gridcolor='#2d2d2d',
            linecolor='#2d2d2d'
        ),
        yaxis=dict(
            title="Points",
            titlefont=dict(color='#ccc', size=12),
            tickfont=dict(color='#888', size=11),
            gridcolor='#2d2d2d',
            linecolor='#2d2d2d',
            range=[0, max(df['Actual'].max(), df['Predicted'].max()) + 5]
        ),
        height=500,
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white', family='Inter'),
        barmode='group',
        bargap=0.3,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.18,
            xanchor="center",
            x=0.5,
            bgcolor='rgba(0,0,0,0)',
            bordercolor='#2d2d2d',
            borderwidth=1,
            font=dict(color='#ccc', size=11)
        ),
        hovermode='closest',
        hoverlabel=dict(
            bgcolor='#1e1e1e',
            bordercolor='#d4af37',
            font_size=12,
            font_family='Inter',
            font_color='#fff'
        ),
        margin=dict(t=100, b=80, l=50, r=50)
    )
    
    fig.add_annotation(
        x=0.5,
        y=1.1,
        xref='paper',
        yref='paper',
        text=f"<b>Mean Absolute Error (MAE) = {df['MAE'].mean():.2f} points</b><br><i>Average of all individual errors</i>",
        showarrow=False,
        bgcolor='rgba(212, 175, 55, 0.2)',
        bordercolor='#d4af37',
        borderwidth=2,
        font=dict(color='#d4af37', size=11, family='Inter'),
        align='center'
    )
    
    return fig, df

def main():
    load_custom_css()
    
    st.markdown("""
    <h1>About This Dashboard</h1>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <p style="color: #888; font-size: 1.1rem; margin-bottom: -0.3rem;">
        Learn how our NBA player performance predictions work, what the metrics mean, and how to interpret the results.
    </p>
    """, unsafe_allow_html=True)
    
    st.markdown("### Model Overview")
    st.markdown("""
    <div class="info-card">
        <p>Our predictions use a <strong>voting ensemble of machine learning models</strong> trained on historical NBA player performance data. 
        We maintain separate models for each statistic (Points, Rebounds, Assists, Steals, Blocks, Turnovers, and 3-Pointers Made).</p>
        <p><strong>What is a voting ensemble?</strong> A voting ensemble (also called averaging ensemble) combines predictions from multiple models by averaging them. 
        Each model "votes" with its prediction, and we take the average of all votes as the final prediction. 
        This typically improves accuracy because different models may make different types of errors, and averaging helps cancel out those errors.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Available Models")
    st.markdown("""
    <div class="info-card">
        <p>We train multiple machine learning algorithms to capture different patterns in player performance:</p>
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">XGBoost:</strong> Gradient boosting algorithm optimized for structured data. 
            Think of it as a series of decision trees that learn from their mistakes and get progressively better.</li>
            <li><strong style="color: #d4af37;">LightGBM:</strong> Fast gradient boosting framework, often more accurate than XGBoost. 
            Uses a different tree-building strategy that can find better patterns in the data.</li>
            <li><strong style="color: #d4af37;">Random Forest:</strong> Ensemble of decision trees using bagging for robust predictions. 
            Creates many trees and averages their predictions, making it less prone to overfitting.</li>
            <li><strong style="color: #d4af37;">CatBoost:</strong> Gradient boosting optimized for categorical features. 
            Handles team names, positions, and other categories more effectively.</li>
        </ul>
        <p><strong>By default, all available models are included in the ensemble.</strong> You can configure which models to include 
        in your ensemble on the <strong>Model Performance</strong> page. Predictions from selected models are averaged to create the 
        final ensemble prediction.</p>
        <p><strong>How to choose which models to use:</strong></p>
        <ul style="color: #ccc; line-height: 1.8;">
            <li>Navigate to the <strong style="color: #d4af37;">Model Performance</strong> page to view accuracy metrics (MAE) for each individual model</li>
            <li>Compare model performance across different statistics (Points, Rebounds, Assists, etc.) using the "Model Version" dropdown</li>
            <li>Select models that perform best for the statistics you care about most</li>
            <li>Use the "Ensemble Model Configuration" section at the top of the Model Performance page to select which models to include</li>
            <li>You can also view the "Ensemble" option in the Model Version dropdown to see how your selected ensemble performs compared to individual models</li>
        </ul>
        <p style="color: #888; font-style: italic; margin-top: 0.5rem;">Note: Your ensemble configuration is saved during your current session but resets 
        to the default (all models) when you restart Streamlit.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Features Used")
    st.markdown("""
    <div class="info-card">
        <p>Our models consider many factors that influence player performance:</p>
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">Recent Form:</strong> Rolling averages from last 5, 10, and 20 games. 
            We also use exponentially weighted averages where more recent games matter more. This captures both overall recent 
            performance and recency trends.</li>
            <li><strong style="color: #d4af37;">Game Context:</strong> Home/away status (home court advantage), days of rest 
            between games, and whether it's a back-to-back game. These factors significantly impact player performance and fatigue levels.</li>
            <li><strong style="color: #d4af37;">Team Ratings:</strong> Offensive rating (points per 100 possessions), defensive rating 
            (points allowed per 100 possessions), and pace (possessions per game) for both the player's team and opponent. 
            These metrics capture team strength and playing style.</li>
            <li><strong style="color: #d4af37;">Opponent Defense:</strong> Field goal percentage and 3-point percentage allowed by the 
            opposing team. Stronger defensive teams typically limit individual player production.</li>
            <li><strong style="color: #d4af37;">Position-Specific Defense:</strong> How well the opposing team defends against the 
            player's position (Guard, Forward, or Center). For example, a guard facing a team that struggles to defend guards will 
            likely see increased production opportunities.</li>
            <li><strong style="color: #d4af37;">Teammate Impact:</strong> Whether star teammates (20+ PPG) are injured or out. 
            When star players are unavailable, other players often see increased usage and production opportunities.</li>
            <li><strong style="color: #d4af37;">Playoff Experience:</strong> Career playoff games played and playoff performance boost. 
            These features are only applied when predicting playoff games, as playoff basketball has different intensity.</li>
            <li><strong style="color: #d4af37;">Altitude:</strong> Arena altitude effects for away games. High-altitude venues 
            (above 3000 feet) can impact player performance due to reduced oxygen levels.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Data Preprocessing")
    st.markdown("""
    <div class="info-card">
        <p><strong>Standardization (Z-score normalization):</strong> All features are standardized to ensure fair comparison across 
        different scales. For example, points (typically 0-50) and field goal percentage (0-100%) are on very different scales. 
        Standardization converts everything to a common scale so the model can learn patterns effectively.</p>
        <p>Each model uses its own scaler trained on the same feature set, ensuring consistent preprocessing across all models.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Understanding Accuracy Metrics")
    
    st.markdown("""
    <div class="info-card gold">
        <h3 style="margin-top: 0;">Mean Absolute Error (MAE)</h3>
        <p><strong>What it means:</strong> MAE tells you, on average, how far off our predictions are from the actual values. 
        Lower is better!</p>
        <p><strong>How it's calculated:</strong></p>
        <p>For each prediction, we calculate: <strong>|Predicted - Actual|</strong></p>
        <p>Then we take the average of all these errors.</p>
        <p><strong>Example:</strong></p>
        <p>If we predict 25 points and the player scores 27, the error is |25 - 27| = 2 points.</p>
        <p>If we predict 18 points and the player scores 17, the error is |18 - 17| = 1 point.</p>
        <p>MAE = (2 + 1) / 2 = 1.5 points</p>
        <p><strong>What's a good MAE?</strong></p>
        <ul style="color: #ccc;">
            <li><strong>Points:</strong> MAE of 3-5 is excellent, 5-7 is good, 7+ needs improvement</li>
            <li><strong>Rebounds:</strong> MAE of 1-2 is excellent, 2-3 is good</li>
            <li><strong>Assists:</strong> MAE of 1-1.5 is excellent, 1.5-2 is good</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    mae_fig, mae_df = create_mae_example()
    st.plotly_chart(mae_fig, use_container_width=True)
    
    st.markdown(f"""
    <div class="info-card">
        <p><strong>In this example:</strong></p>
        <p>• The red lines with error labels show the absolute difference between predicted and actual points for each player</p>
        <p>• MAE = {mae_df['Error'].mean():.2f} points (average of all errors)</p>
        <p>• This means, on average, our predictions are off by {mae_df['Error'].mean():.2f} points</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-card">
        <h3>% Accurate</h3>
        <p><strong>What it means:</strong> This measures how accurate our predictions are across all statistics. We calculate accuracy for each individual stat, then average them together.</p>
        <p><strong>How it's calculated:</strong></p>
        <p>1. For each statistic (Points, Rebounds, Assists, Steals, Blocks, Turnovers, 3-Pointers), calculate: <strong>100 - (|Predicted - Actual| / Actual) × 100</strong></p>
        <p>2. Average all 7 individual accuracies together</p>
        <p><strong>Example:</strong></p>
        <p>Predicted: 25 pts, 8 reb, 5 ast, 1 stl, 0 blk, 2 to, 3 3pm</p>
        <p>Actual: 27 pts, 7 reb, 6 ast, 1 stl, 1 blk, 2 to, 2 3pm</p>
        <p>• Points: 100 - (|25-27|/27) × 100 = 100 - 7.41 = <strong>92.59%</strong></p>
        <p>• Rebounds: 100 - (|8-7|/7) × 100 = 100 - 14.29 = <strong>85.71%</strong></p>
        <p>• Assists: 100 - (|5-6|/6) × 100 = 100 - 16.67 = <strong>83.33%</strong></p>
        <p>• Steals: 100 - (|1-1|/1) × 100 = <strong>100%</strong></p>
        <p>• Blocks: 100 - (|0-1|/1) × 100 = <strong>0%</strong></p>
        <p>• Turnovers: 100 - (|2-2|/2) × 100 = <strong>100%</strong></p>
        <p>• 3-Pointers: 100 - (|3-2|/2) × 100 = 100 - 50 = <strong>50%</strong></p>
        <p>Average: (92.59 + 85.71 + 83.33 + 100 + 0 + 100 + 50) / 7 = <strong>73.09%</strong></p>
        <p><strong>What to expect with accuracy percentages:</strong></p>
        <p>You'll typically see overall accuracy percentages in the 50-60% range. Here's why:</p>
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">Small stats have higher relative error:</strong> For statistics like steals, blocks, and 3-pointers, values are typically 0-3. A small absolute error (like 0.5) becomes a large percentage error. For example, predicting 1.5 steals when actual is 1.0 results in 50% accuracy, even though the absolute error is small.</li>
            <li><strong style="color: #d4af37;">Even larger stats can show lower percentages:</strong> While points, rebounds, and assists have larger values, they also have more variance. A player might score 20 points one game and 30 the next. When we predict 25 points and the player scores 20, that's a 25% error. When averaged across many predictions with varying errors, accuracy percentages for these stats typically fall in the 50-60% range.</li>
            <li><strong style="color: #d4af37;">Basketball is inherently variable:</strong> Player performance varies significantly game-to-game due to matchups, game flow, foul trouble, and other factors. This natural variability means percentage-based accuracy will be lower than you might expect.</li>
            <li><strong style="color: #d4af37;">MAE provides absolute context:</strong> The Mean Absolute Error (MAE) metric shows the actual error in units (e.g., "off by 4.82 points on average"), which helps put the percentage accuracy in perspective. A 54.5% accuracy on points might seem low, but an MAE of 4.82 points means we're typically within 5 points of the actual value.</li>
        </ul>
        <p><strong>Typical accuracy ranges:</strong></p>
        <ul style="color: #ccc;">
            <li><strong>Points/Rebounds/Assists:</strong> 50-60% is typical (larger values but high variance)</li>
            <li><strong>Steals/Turnovers/3PM:</strong> 50-60% is typical (small values make percentage errors larger)</li>
            <li><strong>Blocks:</strong> 40-50% is typical (very small values, highest variance)</li>
            <li><strong>Overall:</strong> 50-60% is typical when averaging across all stats</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Confidence Score")
    st.markdown("""
    <div class="info-card">
        <p>The confidence score (0-100%) tells you how reliable a prediction is. Think of it as our "certainty level" - 
        higher scores mean we're more confident the prediction will be accurate. The score is calculated from four main factors:</p>
        
        <h4 style="color: #d4af37; margin-top: 1rem; margin-bottom: 0.5rem;">1. Player Consistency (How Predictable They Are)</h4>
        <p>We look at how consistent a player's performance has been. A player who scores 20-22 points every game is much easier 
        to predict than someone who scores anywhere from 10 to 30 points. We consider both:</p>
        <ul style="color: #ccc; line-height: 1.8; margin-left: 1.5rem;">
            <li><strong>Current season performance (75% weight):</strong> How consistent they've been this year</li>
            <li><strong>Career performance (25% weight):</strong> Their overall career consistency</li>
        </ul>
        <p style="margin-top: 0.5rem;"><strong>Example:</strong> A player averaging 20 points with very little variation gets a high consistency score. 
        A player averaging 20 points but sometimes scoring 5 and sometimes 35 gets a lower score.</p>
        
        <h4 style="color: #d4af37; margin-top: 1rem; margin-bottom: 0.5rem;">2. Data Completeness (Do We Have All The Information?)</h4>
        <p>Our models use about 40 different pieces of information to make predictions - things like recent form, opponent strength, 
        team ratings, and more. If we're missing important data (like recent games, opponent stats, or team information), our confidence 
        goes down because we're making predictions with incomplete information.</p>
        <p style="margin-top: 0.5rem;"><strong>Example:</strong> If a player just returned from injury and we only have 2 recent games, 
        we have less data to work with, so confidence is lower.</p>
        
        <h4 style="color: #d4af37; margin-top: 1rem; margin-bottom: 0.5rem;">3. Experience & Sample Size (How Much History Do We Have?)</h4>
        <p>More games played means more reliable patterns. We look at both:</p>
        <ul style="color: #ccc; line-height: 1.8; margin-left: 1.5rem;">
            <li><strong>Season games:</strong> How many games they've played this season (20+ is ideal, 10+ is good, 5+ is okay)</li>
            <li><strong>Career games:</strong> Their total career experience (50+ is ideal, 20+ is good)</li>
        </ul>
        <p style="margin-top: 0.5rem;">We also check if a player is coming back from injury. If they recently returned after missing many games, 
        we're less confident because their performance might still be adjusting.</p>
        <p style="margin-top: 0.5rem;"><strong>Example:</strong> A veteran with 500 career games and 50 games this season gets a high score. 
        A rookie with 10 career games gets a lower score. A player who just returned from a 20-game injury absence gets an additional deduction.</p>
        
        <h4 style="color: #d4af37; margin-top: 1rem; margin-bottom: 0.5rem;">4. Recent Transactions (Team Changes Create Uncertainty)</h4>
        <p>When a player is traded or signed to a new team, there's uncertainty about how they'll perform in a new system, with new teammates, 
        and potentially a different role. We automatically detect trades, signings, and waivers from ESPN and reduce confidence based on how recently the transaction happened:</p>
        <ul style="color: #ccc; line-height: 1.8; margin-left: 1.5rem;">
            <li><strong>Traded within last 7 days:</strong> Major confidence reduction (15 points) - very uncertain</li>
            <li><strong>Traded 8-14 days ago:</strong> Moderate reduction (10 points) - still adjusting</li>
            <li><strong>Traded 15-21 days ago:</strong> Small reduction (5 points) - some uncertainty remains</li>
            <li><strong>Signed within last 7 days:</strong> Significant reduction (12 points) - adjusting to new team</li>
            <li><strong>Signed 8-14 days ago:</strong> Moderate reduction (8 points) - still settling in</li>
            <li><strong>Signed 15-21 days ago:</strong> Small reduction (4 points) - some uncertainty remains</li>
            <li><strong>Transaction 22+ days ago:</strong> Minimal or no reduction - player has had time to adjust</li>
        </ul>
        <p style="margin-top: 0.5rem;">We also check if a player has played very few games with their current team (even if we don't have a transaction record). 
        If they have less than 3 games with their current team but have played 5+ games overall this season, it suggests they might be new to the team, 
        and we apply an additional confidence reduction.</p>
        <p style="margin-top: 0.5rem;"><strong>Example:</strong> A player traded 3 days ago gets a significant confidence reduction. 
        A player signed 5 days ago also gets a reduction, though slightly less than a trade. A player traded 25 days ago with 8 games under their belt gets little to no penalty.</p>
        
        <h4 style="color: #d4af37; margin-top: 1.5rem; margin-bottom: 0.5rem;">How to Interpret Confidence Scores:</h4>
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #4ade80;">80-100%:</strong> High confidence - very reliable prediction. Player is consistent, 
            we have complete data, and there are no major uncertainties.</li>
            <li><strong style="color: #fbbf24;">60-79%:</strong> Medium confidence - generally reliable. Prediction should be reasonably accurate, 
            but there may be some factors creating uncertainty.</li>
            <li><strong style="color: #f87171;">Below 60%:</strong> Low confidence - use with caution. The player may be inconsistent, 
            we're missing data, they're inexperienced, recently traded, or coming off injury. Predictions are less reliable.</li>
        </ul>
        
        <p style="margin-top: 1rem; color: #888; font-style: italic;"><strong>Remember:</strong> Confidence scores don't tell you if a prediction 
        will be high or low - they tell you how reliable we think the prediction is. A low confidence score on a 25-point prediction means we're 
        less certain it will be accurate, not that we think they'll score less.</p>
    </div>
    """, unsafe_allow_html=True)
    
    
    st.markdown("### Using the Dashboard")
    st.markdown("""
    <div class="info-card">
        <h3>Home Page</h3>
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">Date Selection:</strong> View predictions for any upcoming game date</li>
            <li><strong style="color: #d4af37;">Filters:</strong> Filter by game, team, player, position, or confidence level</li>
            <li><strong style="color: #d4af37;">View Actuals:</strong> For past games, click "View Actuals" to see how accurate our predictions were</li>
        </ul>
        <br>
        <h3>Player Analysis Page</h3>
        <ul style="color: #ccc; line-height: 1.8;">
            <li>Search for any player to see their detailed prediction history</li>
            <li>View trends, recent performance, and prediction accuracy over time</li>
            <li>Compare predicted vs actual performance with visualizations</li>
        </ul>
        <br>
        <h3>Model Performance Page</h3>
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">Ensemble Configuration:</strong> Select which models to include in your ensemble</li>
            <li><strong style="color: #d4af37;">Compare Models:</strong> See which models perform best for different statistics</li>
            <li><strong style="color: #d4af37;">Performance Over Time:</strong> Track how model accuracy changes over the season</li>
            <li><strong style="color: #d4af37;">Prediction vs Actual Analysis:</strong> Visualize prediction accuracy with scatter plots and error distributions</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Tips for Best Results")
    st.markdown("""
    <div class="info-card gold">
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">Check confidence scores:</strong> Higher confidence predictions are more reliable</li>
            <li><strong style="color: #d4af37;">Consider context:</strong> Look at recent form, opponent strength, and game situation</li>
            <li><strong style="color: #d4af37;">Use ensemble predictions:</strong> Ensemble models typically outperform individual models</li>
            <li><strong style="color: #d4af37;">Review actuals:</strong> Check past predictions to understand model strengths and weaknesses</li>
            <li><strong style="color: #d4af37;">Account for injuries:</strong> Our models consider teammate injuries, but last-minute lineup changes may not be reflected</li>
            <li><strong style="color: #d4af37;">Understand variance:</strong> Some players are inherently more predictable than others</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Limitations & Considerations")
    st.markdown("""
    <div class="info-card">
        <ul style="color: #ccc; line-height: 1.8;">
            <li><strong style="color: #d4af37;">Historical data only:</strong> Models are trained on past performance and may not account for sudden changes 
            (coaching changes, role changes, etc.). However, we do account for trades by reducing confidence scores for recently traded players.</li>
            <li><strong style="color: #d4af37;">Injury updates:</strong> While we track injuries and update our database daily, last-minute lineup changes 
            (announced hours before game time) may not be reflected in same-day predictions. Players returning from injury will have lower confidence scores 
            until they've played several games.</li>
            <li><strong style="color: #d4af37;">Minutes played:</strong> Our models predict stats assuming normal playing time. Unexpected benchings, 
            foul trouble, or blowout games (where starters sit in the 4th quarter) aren't predicted and can affect accuracy.</li>
            <li><strong style="color: #d4af37;">Playoff vs Regular Season:</strong> Playoff predictions use additional features (playoff experience, 
            playoff performance history), but playoff basketball can be more unpredictable due to increased intensity and strategic adjustments.</li>
            <li><strong style="color: #d4af37;">Rookies and new players:</strong> Limited historical data means lower confidence scores and potentially 
            less accurate predictions. The system accounts for this by reducing confidence for players with fewer career games.</li>
            <li><strong style="color: #d4af37;">Recently traded or signed players:</strong> While we automatically detect trades, signings, and waivers from ESPN 
            and adjust confidence scores accordingly, it takes time for players to adjust to new teams. Predictions for players traded or signed within the last 2-3 weeks 
            may be less accurate as they adapt to new systems and roles.</li>
            <li><strong style="color: #d4af37;">Statistical variance:</strong> Basketball has inherent randomness. Even perfect models can't predict 
            every game perfectly. A player might have an off night, get in foul trouble early, or have an unexpectedly great game.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    

if __name__ == "__main__":
    main()

