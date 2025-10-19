import requests
import os
import csv
import warnings
import time
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime
# Suppress the urllib3/OpenSSL warning
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

load_dotenv()

# Polygon API Configuration
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Snowflake Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "STOCK_DATA")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "TICKERS")

LIMIT = 1000

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def create_table_if_not_exists(conn, cursor):
    """Create the tickers table if it doesn't exist"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        ticker VARCHAR(20),
        name VARCHAR(500),
        market VARCHAR(50),
        locale VARCHAR(10),
        primary_exchange VARCHAR(50),
        type VARCHAR(50),
        active BOOLEAN,
        currency_name VARCHAR(50),
        cik VARCHAR(20),
        composite_figi VARCHAR(20),
        share_class_figi VARCHAR(20),
        last_updated_utc TIMESTAMP,
        ds DATE
    )
    """
    try:
        cursor.execute(create_table_sql)
        print(f"Table {SNOWFLAKE_TABLE} created or already exists")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_tickers_to_snowflake(tickers):
    """Insert ticker data into Snowflake table"""
    if not tickers:
        print("No tickers to insert")
        return
    
    conn = get_snowflake_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_if_not_exists(conn, cursor)
        
        # Clear existing data (optional - you might want to keep historical data)
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
        
        # Prepare insert statement
        insert_sql = f"""
        INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
        (ticker, name, market, locale, primary_exchange, type, active, currency_name, 
         cik, composite_figi, share_class_figi, last_updated_utc, ds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare data for insertion
        insert_data = []
        for ticker in tickers:
            row = (
                ticker.get('ticker', ''),
                ticker.get('name', ''),
                ticker.get('market', ''),
                ticker.get('locale', ''),
                ticker.get('primary_exchange', ''),
                ticker.get('type', ''),
                ticker.get('active', False),
                ticker.get('currency_name', ''),
                ticker.get('cik', ''),
                ticker.get('composite_figi', ''),
                ticker.get('share_class_figi', ''),
                ticker.get('last_updated_utc', None),
                ticker.get('ds', datetime.now().strftime('%Y-%m-%d'))
            )
            insert_data.append(row)
        
        # Execute batch insert
        cursor.executemany(insert_sql, insert_data)
        conn.commit()
        
        print(f'Successfully inserted {len(tickers)} tickers into Snowflake table {SNOWFLAKE_TABLE}')
        
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def run_stock_job():
    print("Starting stock job...")
    print(f"Polygon API Key configured: {'Yes' if POLYGON_API_KEY else 'No'}")
    print(f"Snowflake credentials configured: {'Yes' if SNOWFLAKE_USER and SNOWFLAKE_PASSWORD else 'No'}")
    
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    print(f"Making request to Polygon API...")
    response = requests.get(url)
    data = response.json()

    tickers = []

    # Check if 'results' is in the initial response and the status is not an error
    if 'results' in data and data.get('status') != 'ERROR':
        for ticker in data['results']:
            tickers.append(ticker)

    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        time.sleep(1)  # Add 1 second delay to avoid rate limiting
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        # Check if 'results' is in the subsequent response and the status is not an error
        if 'results' in data and data.get('status') != 'ERROR':
            for ticker in data['results']:
                ticker['ds'] = datetime.now().strftime('%Y-%m-%d')
                tickers.append(ticker)
        else:
            print("Error retrieving next page:", data.get('error', 'Unknown error'))
            break # Exit the loop if there's an error
    else:
        print("Error retrieving initial data:", data.get('error', 'Unknown error'))


    print(f"Retrieved {len(tickers)} tickers")

    # Insert tickers to Snowflake
    if tickers:  # Check if tickers list is not empty
        insert_tickers_to_snowflake(tickers)
    else:
        print("No tickers retrieved to insert into Snowflake.")

if __name__ == "__main__":
    run_stock_job()