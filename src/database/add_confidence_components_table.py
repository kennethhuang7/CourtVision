import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def add_confidence_components_table():
    print("Creating confidence_components table...\n")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        with open('src/database/add_confidence_components_table.sql', 'r') as f:
            sql = f.read()
        
        cur.execute(sql)
        conn.commit()
        
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'confidence_components'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        
        print("Table created successfully!\n")
        print("Columns in confidence_components table:")
        for col in columns:
            print(f"  - {col[0]}: {col[1]}")
        
        cur.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'confidence_components';
        """)
        
        indexes = cur.fetchall()
        
        print("\nIndexes created:")
        for idx in indexes:
            print(f"  - {idx[0]}")
        
        cur.close()
        conn.close()
        
        print("\nconfidence_components table setup complete!")
        
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

if __name__ == "__main__":
    add_confidence_components_table()

