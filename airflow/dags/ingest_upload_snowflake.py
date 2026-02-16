from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x
from datetime import datetime
import os
import sys

# -----------------------------
# Adjust paths to import scripts
# -----------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # /opt/airflow/dags
SRC_DIR = os.path.join(PROJECT_ROOT, "../src")  # ajusta para apontar para src
sys.path.insert(0, SRC_DIR)  # adiciona src ao Python path

# -----------------------------
# Import functions
# -----------------------------
from upload_snowflake import main as upload_to_snowflake
from ingest_kaggle import main as ingest_kaggle_dataset  # agora src está no path

# -----------------------------
# DAG definition
# -----------------------------
default_args = {
    "owner": "fabricio",
    "depends_on_past": False,
    "retries": 0,
}

dag = DAG(
    "kaggle_to_snowflake",
    default_args=default_args,
    description="Download Kaggle dataset and upload to Snowflake",
    schedule_interval=None,  # manual trigger
    start_date=datetime(2026, 2, 16),
    catchup=False,
)

# -----------------------------
# Tasks
# -----------------------------
t1 = PythonOperator(
    task_id="ingest_kaggle_dataset",
    python_callable=ingest_kaggle_dataset,
    dag=dag,
)

t2 = PythonOperator(
    task_id="upload_to_snowflake",
    python_callable=upload_to_snowflake,
    dag=dag,
)

# -----------------------------
# Set task dependencies
# -----------------------------
t1 >> t2