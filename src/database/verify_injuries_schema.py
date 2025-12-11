import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection
import pandas as pd

def verify_injuries_schema():
    print("="*60)
    print("VERIFYING INJURIES TABLE SCHEMA")
    print("="*60)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'injuries'
        ORDER BY ordinal_position
    """)
    
    columns = cur.fetchall()
    
    print("\nCurrent columns in injuries table:")
    print("-" * 60)
    for col_name, data_type, is_nullable, default in columns:
        nullable_str = "NULL" if is_nullable == "YES" else "NOT NULL"
        default_str = f" DEFAULT {default}" if default else ""
        print(f"  {col_name:25} {data_type:20} {nullable_str:10} {default_str}")
    
    required_columns = {
        'injury_id': 'integer',
        'player_id': 'integer',
        'report_date': 'date',
        'injury_status': 'character varying',
        'injury_description': 'text',
        'games_missed': 'integer',
        'return_date': 'date',
        'updated_at': 'timestamp without time zone',
        'source': 'character varying',
        'created_at': 'timestamp without time zone'
    }
    
    print("\n" + "="*60)
    print("VERIFICATION RESULTS")
    print("="*60)
    
    existing_columns = {col[0]: col[1] for col in columns}
    all_good = True
    
    for req_col, req_type in required_columns.items():
        if req_col in existing_columns:
            actual_type = existing_columns[req_col]
            type_match = (
                req_type in actual_type.lower() or 
                actual_type.lower() in req_type or
                (req_type == 'character varying' and 'varchar' in actual_type.lower()) or
                (req_type == 'timestamp without time zone' and 'timestamp' in actual_type.lower())
            )
            if type_match:
                print(f"[OK] {req_col:25} - OK")
            else:
                print(f"[WARN] {req_col:25} - EXISTS but type mismatch (expected {req_type}, got {actual_type})")
                all_good = False
        else:
            print(f"[MISSING] {req_col:25} - MISSING")
            all_good = False
    
    print("\n" + "="*60)
    print("CHECKING INDEXES")
    print("="*60)
    
    cur.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'injuries'
    """)
    
    indexes = cur.fetchall()
    print(f"\nFound {len(indexes)} indexes:")
    for idx_name, idx_def in indexes:
        print(f"  {idx_name}")
        print(f"    {idx_def}")
    
    print("\n" + "="*60)
    print("DATA SAMPLE CHECK")
    print("="*60)
    
    cur.execute("SELECT COUNT(*) FROM injuries")
    total_count = cur.fetchone()[0]
    print(f"\nTotal injuries: {total_count}")
    
    if total_count > 0:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(report_date) as has_report_date,
                COUNT(return_date) as has_return_date,
                COUNT(games_missed) as has_games_missed,
                COUNT(injury_description) as has_injury_description
            FROM injuries
        """)
        
        stats = cur.fetchone()
        print(f"\nData completeness:")
        print(f"  Total records: {stats[0]}")
        print(f"  Has report_date: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"  Has return_date: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
        print(f"  Has games_missed: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)")
        print(f"  Has injury_description: {stats[4]} ({stats[4]/stats[0]*100:.1f}%)")
        
        cur.execute("""
            SELECT i.injury_id, p.full_name, 
                   i.report_date as injury_start,
                   i.injury_status, i.return_date, i.games_missed
            FROM injuries i
            JOIN players p ON i.player_id = p.player_id
            ORDER BY i.report_date DESC
            LIMIT 5
        """)
        
        samples = cur.fetchall()
        print(f"\nSample of 5 most recent injuries:")
        for injury_id, name, start_date, status, return_date, games_missed in samples:
            return_str = f"Returned: {return_date}" if return_date else "Still injured"
            games_str = f"({games_missed} games missed)" if games_missed else ""
            print(f"  {name}: {status} since {start_date} - {return_str} {games_str}")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*60)
    if all_good:
        print("SCHEMA VERIFICATION PASSED - All required columns exist!")
    else:
        print("SCHEMA VERIFICATION FAILED - Some columns are missing or incorrect")
        print("  Please run fix_injuries_schema.sql to fix the issues")
    print("="*60)
    
    return all_good

if __name__ == "__main__":
    verify_injuries_schema()

