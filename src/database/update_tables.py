import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def update_tables():
    print("Updating database schema with all contextual factors...")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        with open('src/database/update_schema_complete.sql', 'r') as f:
            update_sql = f.read()
        
        cur.execute(update_sql)
        conn.commit()
        
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cur.fetchall()
        
        print("Schema updated successfully!")
        print("\nAll tables in database:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error updating schema: {e}")
        raise

if __name__ == "__main__":
    update_tables()