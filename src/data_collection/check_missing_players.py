import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.utils import get_db_connection

search_terms = [
    ('Kristaps Porzingis', ['Porzingis', 'Kristaps']),
    ('Bogdan Bogdanovic', ['Bogdanovic', 'Bogdan']),
    ('Luka Doncic', ['Doncic', 'Luka']),
    ('Nikola Topic', ['Topic', 'Nikola']),
    ('Dennis Schroder', ['Schroder', 'Schroder', 'Dennis']),
    ('Jusuf Nurkic', ['Nurkic', 'Jusuf'])
]

conn = get_db_connection()
cur = conn.cursor()

print("Checking for players with partial name matches...\n")
print("="*70)

for espn_name, terms in search_terms:
    print(f"\nSearching for: {espn_name}")
    
    found = False
    for term in terms:
        cur.execute("""
            SELECT player_id, full_name, team_id, is_active
            FROM players
            WHERE full_name ILIKE %s
            OR first_name ILIKE %s
            OR last_name ILIKE %s
        """, (f'%{term}%', f'%{term}%', f'%{term}%'))
        
        results = cur.fetchall()
        
        if results:
            found = True
            for player_id, full_name, team_id, is_active in results:
                status = "ACTIVE" if is_active else "INACTIVE"
                print(f"  FOUND: {full_name} (ID: {player_id}, Team: {team_id}, {status})")
            break
    
    if not found:
        print(f"  NOT FOUND in database")

print("\n" + "="*70)

cur.close()
conn.close()