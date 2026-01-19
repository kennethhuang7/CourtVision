import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection

# RUN THIS:
# python src/database/fix_confidence_gameids.py
# python src/database/fix_confidence_gameids.py --fix

def fix_confidence_gameids(dry_run=True):
    conn = get_db_connection()
    cur = conn.cursor()

    print("="*80)
    print("FIX CONFIDENCE_COMPONENTS GAME_IDS")
    print("="*80)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will update database)'}")
    print("="*80)

    cur.execute("""
        SELECT COUNT(*)
        FROM confidence_components cc
        INNER JOIN predictions p ON cc.prediction_id = p.prediction_id
        WHERE cc.game_id != p.game_id
    """)

    mismatch_count = cur.fetchone()[0]

    if mismatch_count == 0:
        print("\nNo mismatches found - all game_ids are correct!")
        cur.close()
        conn.close()
        return

    print(f"\nFound {mismatch_count} confidence_components with incorrect game_ids")

    cur.execute("""
        SELECT
            cc.game_id as wrong_id,
            p.game_id as correct_id,
            COUNT(*) as count
        FROM confidence_components cc
        INNER JOIN predictions p ON cc.prediction_id = p.prediction_id
        WHERE cc.game_id != p.game_id
        GROUP BY cc.game_id, p.game_id
        ORDER BY count DESC
        LIMIT 20
    """)

    examples = cur.fetchall()
    print(f"\n{'Wrong ID':<15} {'Correct ID':<15} {'Count':<10}")
    print("-"*50)
    for wrong, correct, count in examples:
        print(f"{wrong:<15} {correct:<15} {count:<10}")

    if dry_run:
        print("\n" + "="*80)
        print("DRY RUN - No changes made")
        print("="*80)
        print("\nTo fix these game_ids, run:")
        print("  python src/database/fix_confidence_gameids.py --fix")
    else:
        print("\n" + "="*80)
        print("FIXING GAME_IDS")
        print("="*80)

        cur.execute("""
            UPDATE confidence_components cc
            SET game_id = p.game_id
            FROM predictions p
            WHERE cc.prediction_id = p.prediction_id
            AND cc.game_id != p.game_id
        """)

        updated = cur.rowcount
        conn.commit()

        print(f"\nUpdated {updated} confidence_components records")

        cur.execute("""
            SELECT COUNT(*)
            FROM confidence_components cc
            INNER JOIN predictions p ON cc.prediction_id = p.prediction_id
            WHERE cc.game_id != p.game_id
        """)

        remaining = cur.fetchone()[0]
        if remaining == 0:
            print("All game_ids fixed successfully!")
        else:
            print(f"WARNING: {remaining} mismatches still remain")

    cur.close()
    conn.close()

    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Fix confidence_components game_ids')
    parser.add_argument('--fix', action='store_true', help='Actually fix game_ids (default is dry run)')
    args = parser.parse_args()

    fix_confidence_gameids(dry_run=not args.fix)
