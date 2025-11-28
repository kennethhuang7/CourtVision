import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def add_defense_tables():
    print("Adding defensive stats tables...")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        with open('src/database/add_team_defense_stats.sql', 'r') as f:
            sql = f.read()
        
        cur.execute(sql)
        conn.commit()
        
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cur.fetchall()
        
        print("Defense tables added!")
        print("\nAll tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    add_defense_tables()