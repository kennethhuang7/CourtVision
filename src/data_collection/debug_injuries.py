import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_db_connection

conn = get_db_connection()
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM injuries")
print(f"Total injuries in table: {cur.fetchone()[0]}")

cur.execute("""
    SELECT injury_status, COUNT(*) 
    FROM injuries 
    GROUP BY injury_status
    ORDER BY COUNT(*) DESC
""")
print("\nInjuries by status:")
for status, count in cur.fetchall():
    print(f"  {status}: {count}")

cur.execute("""
    SELECT p.full_name, i.injury_status, i.report_date, i.injury_description
    FROM injuries i
    JOIN players p ON i.player_id = p.player_id
    ORDER BY i.report_date DESC
    LIMIT 10
""")
print("\n10 Most recent injuries:")
for name, status, date, desc in cur.fetchall():
    print(f"  {name}: {status} on {date} - {desc}")

cur.execute("""
    SELECT DISTINCT ON (i.player_id) 
        i.injury_id, i.player_id, p.full_name, i.report_date, i.injury_status
    FROM injuries i
    JOIN players p ON i.player_id = p.player_id
    WHERE i.injury_status IN ('Out', 'Day-To-Day', 'Questionable')
    ORDER BY i.player_id, i.report_date DESC, i.injury_id DESC
    LIMIT 10
""")
print("\n10 Sample active injuries (DISTINCT ON query):")
for injury_id, player_id, name, date, status in cur.fetchall():
    print(f"  {name}: {status} (report_date: {date})")

cur.close()
conn.close()