import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/database/setup_supabase_api_access.py
# This sets up RLS policies for read-only API access to predictions and confidence_components tables

import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

def get_db_connection():
    connection_params = {
        'connect_timeout': 10,
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5
    }
    
    if os.getenv('DATABASE_URL'):
        database_url = os.getenv('DATABASE_URL')
        parsed = urlparse(database_url)
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password,
            **connection_params
        )
    else:
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
            **connection_params
        )
    
    return conn

def setup_rls_and_policies():
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("Setting up Supabase REST API access")
    print("=" * 70)
    print()
    
    tables = ['predictions', 'confidence_components', 'players', 'teams', 'games']
    
    for table_name in tables:
        print(f"Setting up {table_name}...")
        
        try:
            cur.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;")
            print(f"  ✓ Enabled RLS on {table_name}")
        except Exception as e:
            print(f"  ⚠ RLS already enabled or error: {e}")
        
        try:
            cur.execute(f"""
                DROP POLICY IF EXISTS "Allow public read access" ON {table_name};
            """)
            
            cur.execute(f"""
                CREATE POLICY "Allow public read access" ON {table_name}
                FOR SELECT
                USING (true);
            """)
            print(f"  ✓ Created read-only policy for {table_name}")
        except Exception as e:
            print(f"  ⚠ Policy creation error: {e}")
        
        print()
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("=" * 70)
    print("Setup complete!")
    print("=" * 70)
    print()
    print("Your Supabase REST API is now accessible at:")
    print("  https://[your-project-ref].supabase.co/rest/v1/")
    print()
    print("Example endpoints:")
    print("  GET https://[project-ref].supabase.co/rest/v1/predictions")
    print("  GET https://[project-ref].supabase.co/rest/v1/confidence_components")
    print("  GET https://[project-ref].supabase.co/rest/v1/players")
    print("  GET https://[project-ref].supabase.co/rest/v1/teams")
    print("  GET https://[project-ref].supabase.co/rest/v1/games")
    print()
    print("Authentication:")
    print("  Use your Supabase 'anon' key in the 'apikey' header")
    print("  Or use 'Authorization: Bearer [anon-key]' header")
    print()
    print("Find your project URL and keys in Supabase Dashboard:")
    print("  Settings → API")
    print()

if __name__ == "__main__":
    setup_rls_and_policies()

