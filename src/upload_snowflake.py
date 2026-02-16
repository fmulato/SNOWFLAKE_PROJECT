import os
import json
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# -----------------------------
# Project paths
# -----------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "..", "data", "raw")
CSV_PATH = os.path.join(RAW_DATA_DIR, "ecommerce_customer_behavior_dataset_v2.csv")
SECRETS_PATH = os.path.join(PROJECT_ROOT, "..", "secrets", "snowflake.json")

# -----------------------------
# Load Snowflake credentials
# -----------------------------
with open(SECRETS_PATH, "r") as f:
    creds = json.load(f)

SNOWFLAKE_USER = creds["user"]
SNOWFLAKE_PASSWORD = creds["password"]
SNOWFLAKE_ACCOUNT = creds["account"]
SNOWFLAKE_WAREHOUSE = creds["warehouse"]
SNOWFLAKE_DATABASE = creds["database"]
SNOWFLAKE_SCHEMA = creds["schema"]
TABLE_NAME = creds["table_name"]

# -----------------------------
# Main function
# -----------------------------
def main():
    # -----------------------------
    # Load CSV
    # -----------------------------
    df = pd.read_csv(CSV_PATH)
    df.columns = [col.upper() for col in df.columns]
    
    print(f"CSV loaded: {CSV_PATH}")
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print("Preview:")
    print(df.head())

    if df.isnull().sum().sum() == 0:
        print("No missing values detected")
    else:
        print("Warning: missing values detected")

    # -----------------------------
    # Connect to Snowflake
    # -----------------------------
    ctx = connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cs = ctx.cursor()
    print("Connected to Snowflake")

    # -----------------------------
    # Create database and schema if not exists
    # -----------------------------
    cs.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
    cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # -----------------------------
    # Create table if not exists
    # -----------------------------
    # Todos os tipos de coluna como STRING (simples e seguro)
    columns_with_types = ", ".join([f"{col} STRING" for col in df.columns])
    create_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns_with_types})"
    cs.execute(create_sql)
    print(f"Table '{TABLE_NAME}' is ready")

    # -----------------------------
    # Clear table before insert
    # -----------------------------
    cs.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    print(f"Table '{TABLE_NAME}' truncated")

    # -----------------------------
    # Upload data usando write_pandas
    # -----------------------------
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx,
        df=df,
        table_name=TABLE_NAME,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        chunk_size=5000  # define chunk para upload eficiente
    )

    if success:
        print(f"Data uploaded successfully to {TABLE_NAME} ({nrows} rows in {nchunks} chunks)")
    else:
        print("Upload failed")

    # -----------------------------
    # Close connection
    # -----------------------------
    cs.close()
    ctx.close()
    print("Connection closed")

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    main()