import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
from datetime import datetime, timedelta

# RUN THIS:
# python src/database/cleanup_orphaned_games.py                         (dry run - show what would be deleted)
# python src/database/cleanup_orphaned_games.py --delete                (delete orphaned games from last 90 days)
# python src/database/cleanup_orphaned_games.py --delete --days 30      (delete orphaned games from last 30 days)

def find_orphaned_games(cur, days_back=90):
    print(f"\nSearching for orphaned games from last {days_back} days...")
    print("="*80)

    # Find duplicate games where one has predictions/stats and the other doesn't
    cur.execute("""
        WITH game_pairs AS (
            SELECT
                g1.game_id as game_id_1,
                g2.game_id as game_id_2,
                g1.game_date,
                g1.home_team_id,
                g1.away_team_id,
                g1.game_status as status_1,
                g2.game_status as status_2
            FROM games g1
            INNER JOIN games g2 ON
                g1.game_date = g2.game_date
                AND g1.home_team_id = g2.home_team_id
                AND g1.away_team_id = g2.away_team_id
                AND g1.game_id < g2.game_id  -- Avoid duplicates in results
            WHERE g1.game_date >= CURRENT_DATE - INTERVAL '%s days'
        ),
        game_stats AS (
            SELECT
                gp.*,
                (SELECT COUNT(*) FROM predictions WHERE game_id = gp.game_id_1) as pred_count_1,
                (SELECT COUNT(*) FROM predictions WHERE game_id = gp.game_id_2) as pred_count_2,
                (SELECT COUNT(*) FROM player_game_stats WHERE game_id = gp.game_id_1) as stats_count_1,
                (SELECT COUNT(*) FROM player_game_stats WHERE game_id = gp.game_id_2) as stats_count_2
            FROM game_pairs gp
        )
        SELECT
            game_id_1 as orphaned_game_id,
            game_id_2 as real_game_id,
            game_date,
            home_team_id,
            away_team_id,
            pred_count_1,
            pred_count_2,
            stats_count_1,
            stats_count_2
        FROM game_stats
        WHERE
            -- Game 1 is orphaned (no predictions/stats), Game 2 is real
            pred_count_1 = 0 AND stats_count_1 = 0
            AND (pred_count_2 > 0 OR stats_count_2 > 0)
            AND status_1 = 'completed' AND status_2 = 'completed'

        UNION ALL

        SELECT
            game_id_2 as orphaned_game_id,
            game_id_1 as real_game_id,
            game_date,
            home_team_id,
            away_team_id,
            pred_count_2 as pred_count_1,
            pred_count_1 as pred_count_2,
            stats_count_2 as stats_count_1,
            stats_count_1 as stats_count_2
        FROM game_stats
        WHERE
            -- Game 2 is orphaned (no predictions/stats), Game 1 is real
            pred_count_2 = 0 AND stats_count_2 = 0
            AND (pred_count_1 > 0 OR stats_count_1 > 0)
            AND status_1 = 'completed' AND status_2 = 'completed'

        ORDER BY game_date DESC
    """ % days_back)

    return cur.fetchall()

def cleanup_orphaned_games(dry_run=True, days_back=90):
    conn = get_db_connection()
    cur = conn.cursor()

    print("="*80)
    print("ORPHANED GAME CLEANUP SCRIPT")
    print("="*80)
    print(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (will delete games)'}")
    print(f"Search window: Last {days_back} days")
    print("="*80)

    orphaned = find_orphaned_games(cur, days_back)

    if not orphaned:
        print("\nNo orphaned games found!")
        print("Your database is clean.")
        cur.close()
        conn.close()
        return

    print(f"\nFound {len(orphaned)} orphaned game(s):\n")
    print(f"{'Orphaned ID':<15} {'Real ID':<15} {'Date':<12} {'Predictions':<12} {'Stats':<12}")
    print("-"*80)

    for orph_id, real_id, date, home, away, pred1, pred2, stats1, stats2 in orphaned:
        print(f"{orph_id:<15} {real_id:<15} {str(date):<12} {pred1}/{pred2:<11} {stats1}/{stats2:<11}")

    if dry_run:
        print("\n" + "="*80)
        print("DRY RUN MODE - No changes made")
        print("="*80)
        print("\nTo actually delete these games, run:")
        print("  python src/database/cleanup_orphaned_games.py --delete")
        print("\nOr to delete games older than 30 days:")
        print("  python src/database/cleanup_orphaned_games.py --delete --days 30")
    else:
        print("\n" + "="*80)
        print("DELETING ORPHANED GAMES")
        print("="*80)

        deleted_count = 0
        failed_count = 0

        for orph_id, real_id, date, home, away, pred1, pred2, stats1, stats2 in orphaned:
            try:
                # Double-check before deleting
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM predictions WHERE game_id = %s) as pred_count,
                        (SELECT COUNT(*) FROM player_game_stats WHERE game_id = %s) as stats_count
                """, (orph_id, orph_id))

                pred_count, stats_count = cur.fetchone()

                if pred_count == 0 and stats_count == 0:
                    cur.execute("DELETE FROM games WHERE game_id = %s", (orph_id,))
                    deleted_count += 1
                    print(f"  Deleted: {orph_id}")
                else:
                    print(f"  Skipped {orph_id}: has {pred_count} predictions, {stats_count} stats")
                    failed_count += 1

            except Exception as e:
                print(f"  Failed to delete {orph_id}: {e}")
                failed_count += 1
                conn.rollback()

        if deleted_count > 0:
            conn.commit()
            print(f"\nSuccessfully deleted {deleted_count} orphaned game(s)")

        if failed_count > 0:
            print(f"Failed to delete {failed_count} game(s) (likely foreign key constraints)")

    cur.close()
    conn.close()

    print("\n" + "="*80)
    print("CLEANUP COMPLETE")
    print("="*80)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Clean up orphaned game records')
    parser.add_argument('--delete', action='store_true',
                       help='Actually delete games (default is dry run)')
    parser.add_argument('--days', type=int, default=90,
                       help='How many days back to search (default: 90)')

    args = parser.parse_args()

    cleanup_orphaned_games(dry_run=not args.delete, days_back=args.days)
