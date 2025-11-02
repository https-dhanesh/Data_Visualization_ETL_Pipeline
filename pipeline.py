import pandas as pd
import requests
import sqlalchemy
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy import create_engine
from pathlib import Path
import time

BASE_DIR = Path(__file__).resolve().parent

DB_URL = "postgresql://YOUR_USER:YOUR_PASSWORD@localhost:5432/olist_db"

API_URL = "http://127.0.0.1:8000/api/v1/customers"

@task(retries=3, retry_delay_seconds=5)
def extract_csv(file_name: str) -> pd.DataFrame:

    file_path = BASE_DIR / file_name
    print(f"Reading {file_path}...")
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print(f"ERROR: File not found at {file_path}")
        return pd.DataFrame()
    
@task(retries=3, retry_delay_seconds=10)
def extract_api(url: str) -> pd.DataFrame:
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Could not connect to API at {url}.")
        return pd.DataFrame()    
    
@task
def clean_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:

    print(f"Cleaning data for {table_name}...")
    
    for col in df.columns:
        if "timestamp" in col or "_date" in col:
            df[col] = pd.to_datetime(df[col], errors='coerce') 
  
    df.drop_duplicates(inplace=True)
    
    if 'customer_zip_code_prefix' in df.columns:
        df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].fillna(0)
    
    return df

@task
def get_db_engine():
    print("Connecting to database...")
    try:
        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            print("Database connection successful.")
        return engine
    except Exception as e:
        print(f"ERROR: Could not create database engine. Check DB_URL. Error: {e}")
        return None
    
@task(cache_policy=NO_CACHE)
def load_to_postgres(df: pd.DataFrame, table_name: str, engine):

    if df.empty or engine is None:
        print(f"Skipping load for {table_name}. Data is empty or DB engine failed.")
        return
        
    print(f"Loading data into Postgres table: {table_name}...")   
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Successfully loaded {table_name}.")

@task(cache_policy=NO_CACHE)
def create_analytics_tables(engine):
    
    if engine is None:
        print("Skipping analytics tables. DB engine failed.")
        return

    print("Creating analytics tables in Postgres...")
    
    sales_summary_sql = """
    DROP TABLE IF EXISTS analytics_sales_summary;
    CREATE TABLE analytics_sales_summary AS
    SELECT
        c.customer_unique_id,
        c.customer_state AS region,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(p.payment_value) AS total_sales
    FROM
        raw_orders AS o
    JOIN
        raw_customers AS c ON o.customer_id = c.customer_id
    JOIN
        (
            SELECT order_id, SUM(payment_value) AS payment_value 
            FROM raw_payments 
            GROUP BY order_id
        ) AS p ON o.order_id = p.order_id
    WHERE
        o.order_status = 'delivered'
    GROUP BY
        c.customer_unique_id, c.customer_state;
    """

    delivery_performance_sql = """
    DROP TABLE IF EXISTS analytics_delivery_performance;
    CREATE TABLE analytics_delivery_performance AS
    SELECT
        order_id,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        

        EXTRACT(DAY FROM (order_delivered_customer_date - order_purchase_timestamp)) AS delivery_time_days,
 
        EXTRACT(DAY FROM (order_estimated_delivery_date - order_delivered_customer_date)) AS delivery_diff_days,
        
        CASE
            WHEN order_status != 'delivered' THEN 'in_transit_or_failed'
            WHEN order_delivered_customer_date IS NULL THEN 'in_transit_or_failed'
            WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'on_time'
            ELSE 'late'
        END AS delivery_status
    FROM
        raw_orders;
    """
    
    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(sqlalchemy.text(sales_summary_sql))
            print("Successfully created analytics_sales_summary.")
        
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(sqlalchemy.text(delivery_performance_sql))
            print("Successfully created analytics_delivery_performance.")
    except Exception as e:
        print(f"ERROR: Could not create analytics tables. SQL Error: {e}")

@flow(log_prints=True)
def olist_etl_flow():
    
    print("--- Starting ETL Flow ---")
    
    print("Step 1: Extracting data...")
    orders_df = extract_csv.submit("olist_orders_dataset.csv").result()
    payments_df = extract_csv.submit("olist_order_payments_dataset.csv").result()
    customers_df = extract_api.submit(API_URL).result()
    print("Data extraction complete.")

    print("Step 2: Cleaning data...")
    orders_clean = clean_data.submit(orders_df, "raw_orders").result()
    payments_clean = clean_data.submit(payments_df, "raw_payments").result()
    customers_clean = clean_data.submit(customers_df, "raw_customers").result()
    print("Data cleaning complete.")


    print("Step 3: Loading data to Postgres...")
    engine = get_db_engine()
    
    load_to_postgres(orders_clean, "raw_orders", engine)
    load_to_postgres(payments_clean, "raw_payments", engine)
    load_to_postgres(customers_clean, "raw_customers", engine)
    print("Data loading complete.")

    print("Step 4: Creating analytics tables...")
    create_analytics_tables(engine)
    print("Analytics tables created.")
    
    print("--- ETL Flow Finished Successfully ---")

if __name__ == "__main__":
  
    print("Waiting 5 seconds for API server to be ready...")
    time.sleep(5) 
    
    olist_etl_flow()