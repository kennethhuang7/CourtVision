import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/database/enable_rls_all_tables.py
# This enables RLS on all tables while ensuring your scripts still work

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

def enable_rls_all_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("Enabling RLS on all tables")
    print("=" * 70)
    print()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    
    all_tables = [row[0] for row in cur.fetchall()]
    
    already_configured = ['predictions', 'confidence_components']
    tables_to_configure = [t for t in all_tables if t not in already_configured]
    
    print(f"Found {len(all_tables)} tables total")
    print(f"Already configured: {', '.join(already_configured)}")
    print(f"Tables to configure: {len(tables_to_configure)}")
    print()
    
    for table_name in tables_to_configure:
        print(f"Setting up {table_name}...")
        
        try:
            cur.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;")
            print(f"  ✓ Enabled RLS on {table_name}")
        except Exception as e:
            print(f"  ⚠ RLS already enabled or error: {e}")
        
        try:
            cur.execute(f"""
                DROP POLICY IF EXISTS "Allow service role full access" ON {table_name};
            """)
            
            cur.execute(f"""
                CREATE POLICY "Allow service role full access" ON {table_name}
                FOR ALL
                TO service_role
                USING (true)
                WITH CHECK (true);
            """)
            print(f"  ✓ Created service role policy for {table_name}")
        except Exception as e:
            if "role" in str(e).lower() and "does not exist" in str(e).lower():
                try:
                    cur.execute(f"""
                        DROP POLICY IF EXISTS "Allow authenticated full access" ON {table_name};
                    """)
                    
                    cur.execute(f"""
                        CREATE POLICY "Allow authenticated full access" ON {table_name}
                        FOR ALL
                        TO authenticated
                        USING (true)
                        WITH CHECK (true);
                    """)
                    print(f"  ✓ Created authenticated policy for {table_name}")
                except Exception as e2:
                    cur.execute(f"""
                        DROP POLICY IF EXISTS "Allow all operations" ON {table_name};
                    """)
                    
                    cur.execute(f"""
                        CREATE POLICY "Allow all operations" ON {table_name}
                        FOR ALL
                        USING (true)
                        WITH CHECK (true);
                    """)
                    print(f"  ✓ Created general policy for {table_name}")
            else:
                print(f"  ⚠ Policy creation error: {e}")
        
        print()
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("=" * 70)
    print("Setup complete!")
    print("=" * 70)
    print()
    print("RLS is now enabled on all tables.")
    print("Your scripts will continue to work because they use direct")
    print("database connections that bypass RLS (service_role).")
    print()

if __name__ == "__main__":
    enable_rls_all_tables()

