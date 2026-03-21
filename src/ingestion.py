import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import kagglehub

dataset_path = kagglehub.dataset_download("algozee/cyber-security")

print("path to dataset files:", dataset_path)

today = datetime.now().strftime("%Y-%m-%d")
bronze_path = Path("data/bronze") / today

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


def save_parquet(df, file_name):
    output_file = bronze_path / (file_name.split("/")[-1].replace(".csv", ".parquet"))
    df.to_parquet(output_file, index=False)
    print(f"saved: {output_file}")


def run_pipeline():
    print("starting ingestion...")

    for root, dirs, files in os.walk(dataset_path):
        for file in files:
            full_path = os.path.join(root, file)

            df = load_file(full_path)

            if df is not None:
                save_parquet(df, file)

    print("ingestion finished")

if __name__ == "__main__":
    run_pipeline()