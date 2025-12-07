import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection, ensure_connection
import pandas as pd
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=FutureWarning)

def evaluate_predictions(target_date=None):
    print("Evaluating prediction accuracy...\n")
    
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()
    else:
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    print(f"Evaluating predictions for: {target_date}\n")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT model_version
        FROM predictions
        WHERE prediction_date = %s
        AND model_version IS NOT NULL
    """, (target_date,))
    
    model_versions = [row[0] for row in cur.fetchall()]
    
    if len(model_versions) == 0:
        print(f"No predictions found for {target_date}")
        cur.close()
        conn.close()
        return
    
    print(f"Found models to evaluate: {', '.join(model_versions)}\n")
    
    all_metrics = {}
    
    for model_version in model_versions:
        print(f"{'='*50}")
        print(f"Evaluating: {model_version.upper()}")
        print(f"{'='*50}\n")
        
        conn, cur = ensure_connection(conn, cur)
        
        cur.execute("""
            SELECT 
                p.prediction_id,
                p.game_id,
                p.player_id,
                p.predicted_points,
                p.predicted_rebounds,
                p.predicted_assists,
                p.predicted_steals,
                p.predicted_blocks,
                p.predicted_turnovers,
                p.predicted_three_pointers_made
            FROM predictions p
            JOIN games g ON p.game_id = g.game_id
            WHERE p.prediction_date = %s
            AND p.model_version = %s
            AND g.game_status = 'completed'
        """, (target_date, model_version))
        
        predictions = cur.fetchall()
        
        if len(predictions) == 0:
            print(f"No completed games found with {model_version} predictions for {target_date}\n")
            continue
        
        print(f"Found {len(predictions)} predictions to evaluate\n")
        
        updated = 0
        errors = 0
        
        for pred in predictions:
            (prediction_id, game_id, player_id, pred_points, pred_rebounds, pred_assists, 
             pred_steals, pred_blocks, pred_turnovers, pred_threes) = pred
            
            try:
                conn, cur = ensure_connection(conn, cur)
                
                cur.execute("""
                    SELECT points, rebounds_total, assists, steals, blocks, turnovers, three_pointers_made
                    FROM player_game_stats
                    WHERE game_id = %s AND player_id = %s
                """, (game_id, player_id))
                
                actual = cur.fetchone()
                
                if not actual:
                    print(f"  No stats found for player {player_id} in game {game_id}")
                    continue
                
                (actual_points, actual_rebounds, actual_assists, actual_steals, 
                 actual_blocks, actual_turnovers, actual_threes) = actual
                
                point_error = abs(pred_points - actual_points)
                rebound_error = abs(pred_rebounds - actual_rebounds)
                assist_error = abs(pred_assists - actual_assists)
                steal_error = abs(pred_steals - actual_steals)
                block_error = abs(pred_blocks - actual_blocks)
                turnover_error = abs(pred_turnovers - actual_turnovers)
                three_error = abs(pred_threes - actual_threes)
                
                avg_error = (point_error + rebound_error + assist_error + 
                            steal_error + block_error + turnover_error + three_error) / 7
                
                cur.execute("""
                    UPDATE predictions SET
                        actual_points = %s,
                        actual_rebounds = %s,
                        actual_assists = %s,
                        actual_steals = %s,
                        actual_blocks = %s,
                        actual_turnovers = %s,
                        actual_three_pointers_made = %s,
                        prediction_error = %s
                    WHERE prediction_id = %s
                """, (
                    actual_points, actual_rebounds, actual_assists,
                    actual_steals, actual_blocks, actual_turnovers, actual_threes,
                    round(avg_error, 2),
                    prediction_id
                ))
                
                updated += 1
                
                if updated % 10 == 0:
                    print(f"Progress: {updated}/{len(predictions)} evaluated")
                    try:
                        conn.commit()
                    except Exception as commit_error:
                        print(f"  Commit error, reconnecting: {commit_error}")
                        conn, cur = ensure_connection(conn, cur)
                        conn.commit()
                
            except Exception as e:
                print(f"  Error evaluating prediction {prediction_id}: {e}")
                errors += 1
                if "connection" in str(e).lower() or "cursor" in str(e).lower():
                    conn, cur = ensure_connection(conn, cur)
                continue
        
        try:
            conn.commit()
        except Exception as commit_error:
            print(f"  Final commit error: {commit_error}")
            conn, cur = ensure_connection(conn, cur)
            conn.commit()
        
        conn, cur = ensure_connection(conn, cur)
        
        cur.execute("""
            SELECT 
                AVG(ABS(predicted_points - actual_points)) as points_mae,
                AVG(ABS(predicted_rebounds - actual_rebounds)) as rebounds_mae,
                AVG(ABS(predicted_assists - actual_assists)) as assists_mae,
                AVG(ABS(predicted_steals - actual_steals)) as steals_mae,
                AVG(ABS(predicted_blocks - actual_blocks)) as blocks_mae,
                AVG(ABS(predicted_turnovers - actual_turnovers)) as turnovers_mae,
                AVG(ABS(predicted_three_pointers_made - actual_three_pointers_made)) as threes_mae,
                AVG(prediction_error) as overall_mae
            FROM predictions
            WHERE prediction_date = %s
            AND model_version = %s
            AND actual_points IS NOT NULL
        """, (target_date, model_version))
        
        metrics = cur.fetchone()
        
        print(f"\nUpdated: {updated}")
        print(f"Errors: {errors}")
        
        if metrics and metrics[0]:
            all_metrics[model_version] = {
                'points': metrics[0],
                'rebounds': metrics[1],
                'assists': metrics[2],
                'steals': metrics[3],
                'blocks': metrics[4],
                'turnovers': metrics[5],
                'three_pointers': metrics[6],
                'overall': metrics[7]
            }
            print(f"\n{model_version.upper()} Metrics (MAE):")
            print(f"  Points:       {metrics[0]:.2f}")
            print(f"  Rebounds:     {metrics[1]:.2f}")
            print(f"  Assists:      {metrics[2]:.2f}")
            print(f"  Steals:       {metrics[3]:.2f}")
            print(f"  Blocks:       {metrics[4]:.2f}")
            print(f"  Turnovers:    {metrics[5]:.2f}")
            print(f"  3-Pointers:   {metrics[6]:.2f}")
            print(f"  Overall:      {metrics[7]:.2f}")
        print()
    
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
    except:
        pass
    
    if all_metrics:
        print(f"\n{'='*50}")
        print("EVALUATION COMPLETE - SUMMARY")
        print(f"{'='*50}")
        print(f"\n{'Model':<20} {'Points':<10} {'Rebounds':<10} {'Assists':<10} {'Overall':<10}")
        print("-" * 60)
        for model, metrics in sorted(all_metrics.items(), key=lambda x: x[1]['overall']):
            print(f"{model:<20} {metrics['points']:<10.2f} {metrics['rebounds']:<10.2f} {metrics['assists']:<10.2f} {metrics['overall']:<10.2f}")
        print("\n(Lower is better - sorted by Overall MAE)")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        evaluate_predictions(target_date)
    else:
        evaluate_predictions()