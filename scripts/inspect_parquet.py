import sys
import pandas as pd
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.common.parquet import load_parquet_dataset

def inspect_parquet(path: str):
    print(f"Inspecting dataset at {path}...")
    df = load_parquet_dataset(Path(path))
    
    print("\nDataFrame Shape:", df.shape)
    print("\nColumns:", df.columns.tolist())
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nSource counts:")
    print(df["source"].value_counts())
    
    print("\nMarket types:")
    print(df["market_type"].value_counts())

if __name__ == "__main__":
    if len(sys.argv) > 1:
        inspect_parquet(sys.argv[1])
    else:
        # Try to find the latest unified dataset
        data_dir = Path("data/datasets")
        datasets = sorted(list(data_dir.glob("v*_unified")))
        if datasets:
            inspect_parquet(str(datasets[-1]))
        else:
            print("No datasets found in data/datasets.")

