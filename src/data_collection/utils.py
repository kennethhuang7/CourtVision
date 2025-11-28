import psycopg2
from dotenv import load_dotenv
import os
import time
import random

load_dotenv()

def get_db_connection():
    database_url = os.getenv('DATABASE_URL')
    return psycopg2.connect(database_url)

def rate_limit(seconds=40.0):
    time.sleep(seconds + random.uniform(5.0, 10.0))