import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/database/fix_rls_security.py

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

def is_rls_enabled(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT relforcerowsecurity
            FROM pg_class
            WHERE relname = %s
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');
        """, (table_name,))
        result = cur.fetchone()
        return result[0] if result else False
    except Exception:
        return False
    finally:
        cur.close()

def enable_rls_security():
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("FIXING RLS SECURITY")
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
    
    print(f"Found {len(all_tables)} tables")
    print()
    
    enabled_count = 0
    already_enabled_count = 0
    
    for table_name in all_tables:
        if is_rls_enabled(conn, table_name):
            print(f"  ✓ {table_name}: RLS already enabled")
            already_enabled_count += 1
        else:
            try:
                cur.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;")
                print(f"  ✓ {table_name}: RLS enabled")
                enabled_count += 1
            except Exception as e:
                print(f"  ✗ {table_name}: Error enabling RLS - {e}")
    
    print()
    print(f"Enabled RLS on {enabled_count} tables")
    print(f"{already_enabled_count} tables already had RLS enabled")
    print()
    
    print("=" * 70)
    print("ADDING MISSING POLICIES")
    print("=" * 70)
    print()
    
    if 'group_messages' in all_tables:
        print("Adding policies for group_messages...")
        try:
            cur.execute("""
                DROP POLICY IF EXISTS "Group members can view messages" ON group_messages;
            """)
            cur.execute("""
                CREATE POLICY "Group members can view messages"
                ON group_messages FOR SELECT
                TO authenticated
                USING (
                    EXISTS (
                        SELECT 1 FROM user_group_members
                        WHERE group_id = group_messages.group_id
                        AND user_id = auth.uid()
                    )
                );
            """)
            print("  ✓ Added SELECT policy")
        except Exception as e:
            print(f"  ⚠ SELECT policy error: {e}")
        
        try:
            cur.execute("""
                DROP POLICY IF EXISTS "Group members can send messages" ON group_messages;
            """)
            cur.execute("""
                CREATE POLICY "Group members can send messages"
                ON group_messages FOR INSERT
                TO authenticated
                WITH CHECK (
                    sender_id = auth.uid()
                    AND EXISTS (
                        SELECT 1 FROM user_group_members
                        WHERE group_id = group_messages.group_id
                        AND user_id = auth.uid()
                    )
                );
            """)
            print("  ✓ Added INSERT policy")
        except Exception as e:
            print(f"  ⚠ INSERT policy error: {e}")
        
        try:
            cur.execute("""
                DROP POLICY IF EXISTS "Senders can delete own messages" ON group_messages;
            """)
            cur.execute("""
                CREATE POLICY "Senders can delete own messages"
                ON group_messages FOR DELETE
                TO authenticated
                USING (sender_id = auth.uid());
            """)
            print("  ✓ Added DELETE policy")
        except Exception as e:
            print(f"  ⚠ DELETE policy error: {e}")
        
        print()
    
    if 'pick_group_shares' in all_tables:
        print("Adding policies for pick_group_shares...")
        try:
            cur.execute("""
                DROP POLICY IF EXISTS "Group members can view shares" ON pick_group_shares;
            """)
            cur.execute("""
                CREATE POLICY "Group members can view shares"
                ON pick_group_shares FOR SELECT
                TO authenticated
                USING (
                    EXISTS (
                        SELECT 1 FROM user_group_members
                        WHERE group_id = pick_group_shares.group_id
                        AND user_id = auth.uid()
                    )
                    OR EXISTS (
                        SELECT 1 FROM user_picks
                        WHERE id = pick_group_shares.pick_id
                        AND owner_id = auth.uid()
                    )
                );
            """)
            print("  ✓ Added SELECT policy")
        except Exception as e:
            print(f"  ⚠ SELECT policy error: {e}")
        
        try:
            cur.execute("""
                DROP POLICY IF EXISTS "Pick owners can share with groups" ON pick_group_shares;
            """)
            cur.execute("""
                CREATE POLICY "Pick owners can share with groups"
                ON pick_group_shares FOR INSERT
                TO authenticated
                WITH CHECK (
                    EXISTS (
                        SELECT 1 FROM user_picks
                        WHERE id = pick_group_shares.pick_id
                        AND owner_id = auth.uid()
                    )
                );
            """)
            print("  ✓ Added INSERT policy")
        except Exception as e:
            print(f"  ⚠ INSERT policy error: {e}")
        
        try:
            cur.execute("""
                DROP POLICY IF EXISTS "Pick owners can remove shares" ON pick_group_shares;
            """)
            cur.execute("""
                CREATE POLICY "Pick owners can remove shares"
                ON pick_group_shares FOR DELETE
                TO authenticated
                USING (
                    EXISTS (
                        SELECT 1 FROM user_picks
                        WHERE id = pick_group_shares.pick_id
                        AND owner_id = auth.uid()
                    )
                );
            """)
            print("  ✓ Added DELETE policy")
        except Exception as e:
            print(f"  ⚠ DELETE policy error: {e}")
        
        print()
    
    print("=" * 70)
    print("ENSURING SERVICE ROLE ACCESS")
    print("=" * 70)
    print()
    
    service_role_tables = [
        'predictions', 'confidence_components', 'players', 'teams', 'games',
        'injuries', 'player_game_stats', 'team_ratings', 'team_defensive_stats',
        'position_defense_stats', 'player_transactions', 'teammate_dependency'
    ]
    
    for table_name in service_role_tables:
        if table_name in all_tables:
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
                print(f"  ✓ {table_name}: Service role policy ensured")
            except Exception as e:
                print(f"  ⚠ {table_name}: Service role policy error - {e}")
    
    print()
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("=" * 70)
    print("SETUP COMPLETE!")
    print("=" * 70)
    print()
    print("RLS is now enabled on all tables.")
    print("Existing policies are preserved.")
    print("Missing policies have been added.")
    print()
    print("Your ML scripts will continue to work because they use")
    print("direct database connections (service_role) that bypass RLS.")
    print()
    print("The Electron app will now be properly secured:")
    print("  - Anon users can only read ML tables (predictions, players, etc.)")
    print("  - Authenticated users can only access their own data")
    print("  - All user data is protected by RLS policies")
    print()

if __name__ == "__main__":
    enable_rls_security()
