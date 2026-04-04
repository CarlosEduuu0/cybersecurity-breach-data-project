import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import kagglehub
import hashlib

dataset_path = kagglehub.dataset_download("algozee/cyber-security")

print("path to dataset files:", dataset_path)


bronze_path = Path("data/bronze") 

bronze_path.mkdir(parents=True, exist_ok=True)


def load_file(file_path):
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path)
        else:
            return None

        print(f"loaded: {file_path} | rows: {len(df)}")
        return df

    except Exception as e:
        print(f"error loading {file_path}: {e}")
        return None


def save_parquet(df, file_name, source_path):
    output_file = bronze_path / (file_name.split("/")[-1].replace(".csv", ".parquet").replace(".json", ".parquet"))
    
    
    file_hash = hashlib.md5()
    with open(source_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            file_hash.update(chunk)
    file_hash_value = file_hash.hexdigest()
    
    
    ingestion_timestamp = datetime.now()
    row_count = len(df)
    
    df = df.assign(
        ingestion_timestamp=ingestion_timestamp,
        source_file=source_path,
        row_count=row_count,
        file_hash=file_hash_value
    )
    
    df.to_parquet(output_file, index=False)
    print(f"saved: {output_file} | rows: {row_count} | hash: {file_hash_value} | timestamp: {ingestion_timestamp}")


def run_pipeline():
    print("starting ingestion...")

    for root, dirs, files in os.walk(dataset_path):
        for file in files:
            full_path = os.path.join(root, file)

            df = load_file(full_path)

            if df is not None:
                save_parquet(df, file, full_path)

    print("ingestion finished")

if __name__ == "__main__":
    run_pipeline()