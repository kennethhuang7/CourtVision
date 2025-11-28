import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def test_database_connection():
    print("Testing database connection...\n")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            print("Using DATABASE_URL connection string...")
            conn = psycopg2.connect(database_url)
        else:
            print("Using individual credentials...")
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
        
        cur = conn.cursor()
        
        cur.execute('SELECT version();')
        db_version = cur.fetchone()
        
        print(f"Connected to database!")
        print(f"PostgreSQL version: {db_version[0][:80]}...\n")
        
        cur.execute('SELECT current_database();')
        db_name = cur.fetchone()[0]
        print(f"Connected to database: {db_name}")
        
        cur.close()
        conn.close()
        
        print("\nDatabase connection successful!")
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("\nTroubleshooting:")
        print("1. Check your .env file has correct credentials")
        print("2. If using Supabase, make sure project is active")
        print("3. Check your internet connection (for cloud database)")
        print("4. Make sure you replaced [YOUR_PASSWORD] with actual password")
        raise

if __name__ == "__main__":
    test_database_connection()