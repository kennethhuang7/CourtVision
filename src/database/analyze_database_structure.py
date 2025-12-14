import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# RUN THIS:
# python src/database/analyze_database_structure.py
# To save output to a file:
# python src/database/analyze_database_structure.py --output database_structure.txt

import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse
from datetime import datetime

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

def get_all_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables

def get_table_columns(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    columns = cur.fetchall()
    cur.close()
    return columns

def get_primary_keys(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            kcu.column_name,
            kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
        AND tc.table_name = %s
        ORDER BY kcu.ordinal_position;
    """, (table_name,))
    pks = [row[0] for row in cur.fetchall()]
    cur.close()
    return pks

def get_foreign_keys(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints AS rc
            ON rc.constraint_name = tc.constraint_name
            AND rc.constraint_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
        AND tc.table_name = %s
        ORDER BY kcu.ordinal_position;
    """, (table_name,))
    fks = cur.fetchall()
    cur.close()
    return fks

def get_unique_constraints(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            tc.constraint_name,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS columns
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = 'public'
        AND tc.table_name = %s
        GROUP BY tc.constraint_name
        ORDER BY tc.constraint_name;
    """, (table_name,))
    uniques = cur.fetchall()
    cur.close()
    return uniques

def get_check_constraints(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            tc.constraint_name,
            cc.check_clause
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.check_constraints AS cc
            ON tc.constraint_name = cc.constraint_name
            AND tc.table_schema = cc.constraint_schema
        WHERE tc.constraint_type = 'CHECK'
        AND tc.table_schema = 'public'
        AND tc.table_name = %s
        ORDER BY tc.constraint_name;
    """, (table_name,))
    checks = cur.fetchall()
    cur.close()
    return checks

def get_indexes(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                pg_indexes.indexname,
                pg_indexes.indexdef
            FROM pg_indexes
            WHERE pg_indexes.schemaname = 'public'
            AND pg_indexes.tablename = %s
            AND pg_indexes.indexname NOT LIKE '%%_pkey'
            ORDER BY pg_indexes.indexname;
        """, (table_name,))
        indexes = cur.fetchall()
    except Exception:
        indexes = []
    finally:
        cur.close()
    return indexes

def format_data_type(col_info):
    data_type = col_info[1]
    max_length = col_info[2]
    precision = col_info[3]
    scale = col_info[4]
    
    if data_type == 'character varying':
        if max_length:
            return f'VARCHAR({max_length})'
        return 'VARCHAR'
    elif data_type == 'character':
        if max_length:
            return f'CHAR({max_length})'
        return 'CHAR'
    elif data_type == 'numeric':
        if precision and scale:
            return f'DECIMAL({precision},{scale})'
        elif precision:
            return f'DECIMAL({precision})'
        return 'DECIMAL'
    elif data_type == 'integer':
        return 'INTEGER'
    elif data_type == 'bigint':
        return 'BIGINT'
    elif data_type == 'smallint':
        return 'SMALLINT'
    elif data_type == 'boolean':
        return 'BOOLEAN'
    elif data_type == 'date':
        return 'DATE'
    elif data_type == 'timestamp without time zone':
        return 'TIMESTAMP'
    elif data_type == 'timestamp with time zone':
        return 'TIMESTAMPTZ'
    elif data_type == 'text':
        return 'TEXT'
    elif data_type == 'double precision':
        return 'DOUBLE PRECISION'
    elif data_type == 'real':
        return 'REAL'
    elif data_type == 'serial':
        return 'SERIAL'
    elif data_type == 'bigserial':
        return 'BIGSERIAL'
    else:
        return data_type.upper()

def analyze_database(output_file=None):
    conn = get_db_connection()
    
    output_lines = []
    
    output_lines.append("=" * 80)
    output_lines.append("DATABASE STRUCTURE ANALYSIS")
    output_lines.append("=" * 80)
    output_lines.append(f"Generated at: {datetime.now()}")
    output_lines.append("")
    
    tables = get_all_tables(conn)
    
    output_lines.append(f"Total Tables: {len(tables)}")
    output_lines.append("")
    
    for table_name in tables:
        output_lines.append("=" * 80)
        output_lines.append(f"TABLE: {table_name}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        columns = get_table_columns(conn, table_name)
        primary_keys = get_primary_keys(conn, table_name)
        foreign_keys = get_foreign_keys(conn, table_name)
        unique_constraints = get_unique_constraints(conn, table_name)
        check_constraints = get_check_constraints(conn, table_name)
        indexes = get_indexes(conn, table_name)
        
        output_lines.append("COLUMNS:")
        output_lines.append("-" * 80)
        for col in columns:
            col_name = col[0]
            data_type = format_data_type(col)
            is_nullable = col[5]
            default = col[6]
            
            props = []
            if col_name in primary_keys:
                props.append("PRIMARY KEY")
            if is_nullable == 'NO':
                props.append("NOT NULL")
            if default:
                props.append(f"DEFAULT: {default}")
            
            prop_str = f" [{', '.join(props)}]" if props else ""
            output_lines.append(f"  {col_name:40} {data_type:30}{prop_str}")
        output_lines.append("")
        
        if foreign_keys:
            output_lines.append("FOREIGN KEY RELATIONSHIPS:")
            output_lines.append("-" * 80)
            for fk in foreign_keys:
                col_name = fk[0]
                ref_table = fk[1]
                ref_col = fk[2]
                update_rule = fk[3]
                delete_rule = fk[4]
                output_lines.append(f"  {col_name} -> {ref_table}.{ref_col}")
                output_lines.append(f"    ON UPDATE: {update_rule}, ON DELETE: {delete_rule}")
            output_lines.append("")
        
        if unique_constraints:
            output_lines.append("UNIQUE CONSTRAINTS:")
            output_lines.append("-" * 80)
            for uc in unique_constraints:
                constraint_name = uc[0]
                columns = uc[1]
                output_lines.append(f"  {constraint_name}: ({columns})")
            output_lines.append("")
        
        if check_constraints:
            output_lines.append("CHECK CONSTRAINTS:")
            output_lines.append("-" * 80)
            for cc in check_constraints:
                constraint_name = cc[0]
                check_clause = cc[1]
                output_lines.append(f"  {constraint_name}: {check_clause}")
            output_lines.append("")
        
        if indexes:
            output_lines.append("INDEXES:")
            output_lines.append("-" * 80)
            for idx in indexes:
                if len(idx) >= 2:
                    index_name = idx[0]
                    index_def = idx[1] if idx[1] else 'N/A'
                    output_lines.append(f"  {index_name}:")
                    output_lines.append(f"    {index_def}")
            output_lines.append("")
        
        output_lines.append("")
    
    output_lines.append("=" * 80)
    output_lines.append("RELATIONSHIP SUMMARY")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    for table_name in tables:
        foreign_keys = get_foreign_keys(conn, table_name)
        if foreign_keys:
            output_lines.append(f"{table_name}:")
            for fk in foreign_keys:
                col_name = fk[0]
                ref_table = fk[1]
                ref_col = fk[2]
                output_lines.append(f"  -> {col_name} references {ref_table}.{ref_col}")
            output_lines.append("")
    
    conn.close()
    
    output_text = "\n".join(output_lines)
    
    print(output_text)
    
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output_text)
        print(f"\nOutput also saved to: {output_file}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze database structure')
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (e.g., database_structure.txt)'
    )
    
    args = parser.parse_args()
    
    analyze_database(output_file=args.output)
