import os
import shutil
import kagglehub

def download_dataset(dataset_id: str, target_dir: str) -> None:
    """Download CSV dataset from Kaggle and copy to data/raw folder."""
    os.makedirs(target_dir, exist_ok=True)

    download_path = kagglehub.dataset_download(dataset_id)

    # Copy CSV files to target folder
    for root, _, files in os.walk(download_path):
        for file in files:
            if file.endswith(".csv"):
                src = os.path.join(root, file)
                dst = os.path.join(target_dir, file)
                shutil.copy(src, dst)