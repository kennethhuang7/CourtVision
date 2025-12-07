import psycopg2
import os
import time    
import random 
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

def check_connection(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        return True
    except:
        return False

def ensure_connection(conn, cur=None):
    if not check_connection(conn):
        try:
            if cur:
                cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
        new_conn = get_db_connection()
        return new_conn, new_conn.cursor()
    return conn, cur if cur else conn.cursor()

def rate_limit(seconds=40.0):
    time.sleep(seconds + random.uniform(5.0, 10.0))