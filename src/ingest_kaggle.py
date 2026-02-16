import os
import pandas as pd
from ingestion.kaggle_ingestion import download_dataset

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

DATASET_ID = "umuttuygurr/e-commerce-customer-behavior-and-sales-analysis-tr"

if __name__ == "__main__":
    # Step 1: download dataset
    download_dataset(DATASET_ID, RAW_DATA_DIR)

    # Step 2: list CSVs
    csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("No CSV files found in data/raw")

    # Step 3: load first CSV for validation
    df = pd.read_csv(os.path.join(RAW_DATA_DIR, csv_files[0]))
    print(df.head())