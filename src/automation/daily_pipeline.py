import sys
import os

if sys.platform == 'win32':
    import io
    try:
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'buffer'):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import subprocess
from datetime import datetime, timedelta

def run_step(step_name, script_path, args=None, critical=True, timeout=600):
    print(f"\n{'='*60}")
    print(f"STEP: {step_name}")
    print("="*60)

    cmd = [sys.executable, script_path]
    if args:
        if isinstance(args, list):
            cmd.extend([str(a) for a in args])
        else:
            cmd.append(str(args))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=timeout
        )

        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode != 0:
            print(f"\n[FAILED] {step_name} exited with code {result.returncode}")
            return False

        if "ERROR:" in result.stdout or "FAILED" in result.stdout.upper():
            if "Errors: 0" not in result.stdout:
                print(f"\n[WARNING] {step_name} completed but reported errors")
                if critical:
                    return False

        print(f"[OK] {step_name} completed successfully")
        return True

    except subprocess.TimeoutExpired:
        print(f"\n[FAILED] {step_name} timed out after {timeout // 60} minutes")
        return False
    except Exception as e:
        print(f"\n[FAILED] {step_name} exception: {e}")
        return False

def run_daily_pipeline():
    print("="*60)
    print("NBA PREDICTION DAILY PIPELINE (AUTOMATED)")
    print(f"Running at: {datetime.now()}")
    print("="*60)

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    print(f"\nYesterday: {yesterday}")
    print(f"Today: {today}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    data_collection_dir = os.path.join(project_root, "src", "data_collection")
    predictions_dir = os.path.join(project_root, "src", "predictions")

    failed_steps = []

    step1_success = run_step(
        "Collect yesterday's games (ESPN)",
        os.path.join(data_collection_dir, "update_yesterday_games_espn.py"),
        str(yesterday),
        critical=True
    )
    if not step1_success:
        failed_steps.append("Collect yesterday's games")

    step2_success = run_step(
        "Update team ratings",
        os.path.join(data_collection_dir, "update_team_ratings_incremental.py"),
        str(yesterday),
        critical=False
    )
    if not step2_success:
        failed_steps.append("Update team ratings")

    step3_success = run_step(
        "Update team defensive stats",
        os.path.join(data_collection_dir, "update_team_defensive_stats_incremental.py"),
        str(yesterday),
        critical=False
    )
    if not step3_success:
        failed_steps.append("Update team defensive stats")

    step4_success = run_step(
        "Update position defense stats",
        os.path.join(data_collection_dir, "update_position_defense_stats_incremental.py"),
        str(yesterday),
        critical=False
    )
    if not step4_success:
        failed_steps.append("Update position defense stats")

    step5_success = run_step(
        "Update injury log",
        os.path.join(data_collection_dir, "update_injury_log.py"),
        None,
        critical=False
    )
    if not step5_success:
        failed_steps.append("Update injury log")

    step6_success = run_step(
        "Detect player transactions",
        os.path.join(data_collection_dir, "detect_and_update_trades.py"),
        str(today),
        critical=False,
        timeout=1200
    )
    if not step6_success:
        failed_steps.append("Detect player transactions")

    step7_success = run_step(
        "Update player teams from games",
        os.path.join(data_collection_dir, "detect_and_update_trades.py"),
        [str(today), "--from-games"],
        critical=False
    )
    if not step7_success:
        failed_steps.append("Update player teams from games")

    step8_success = run_step(
        "Collect today's schedule (ESPN HTML)",
        os.path.join(data_collection_dir, "collect_schedule_html.py"),
        str(today),
        critical=True
    )
    if not step8_success:
        failed_steps.append("Collect today's schedule")

    step9_success = run_step(
        "Generate predictions for today",
        os.path.join(predictions_dir, "predict_games.py"),
        [str(today), "--all"],
        critical=True,
        timeout=2700
    )
    if not step9_success:
        failed_steps.append("Generate predictions")

    step10_success = run_step(
        "Evaluate yesterday's predictions",
        os.path.join(predictions_dir, "evaluate_predictions.py"),
        str(yesterday),
        critical=False
    )
    if not step10_success:
        failed_steps.append("Evaluate predictions")

    print("\n" + "="*60)

    if failed_steps:
        print("PIPELINE COMPLETED WITH ERRORS")
        print("="*60)
        print(f"\nFailed steps ({len(failed_steps)}):")
        for step in failed_steps:
            print(f"  - {step}")
        print("\n" + "="*60)

        critical_failures = [
            "Collect yesterday's games",
            "Collect today's schedule",
            "Generate predictions"
        ]

        has_critical_failure = any(step in critical_failures for step in failed_steps)

        if has_critical_failure:
            print("\nCRITICAL FAILURE: One or more essential steps failed.")
            print("Manual intervention may be required.")
            return False
        else:
            print("\nNon-critical steps failed. Core pipeline completed.")
            return True
    else:
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        return True

if __name__ == "__main__":
    success = run_daily_pipeline()
    sys.exit(0 if success else 1)
