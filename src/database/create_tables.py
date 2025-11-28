import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def create_tables():
    print("Creating database tables...\n")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        with open('src/database/schema.sql', 'r') as f:
            schema_sql = f.read()
        
        cur.execute(schema_sql)
        conn.commit()
        
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cur.fetchall()
        
        print("Tables created successfully!\n")
        print("Tables in database:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cur.close()
        conn.close()
        
        print("\nDatabase schema setup complete!")
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

if __name__ == "__main__":
    create_tables()