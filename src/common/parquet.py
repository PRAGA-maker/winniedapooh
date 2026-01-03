import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from src.common.config import config
from src.common.logging import logger

def write_parquet_dataset(df: pd.DataFrame, dataset_name: str, version: str = None):
    """Write a dataframe to a versioned parquet dataset directory."""
    if version is None:
        version = datetime.now().strftime("%Y%m%d_%H%M")
    
    output_dir = config.datasets_dir / f"v{version}_{dataset_name}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = output_dir / "data.parquet"
    logger.info(f"Writing parquet dataset to {file_path}...")
    
    # Ensure all datetime columns are UTC
    for col in df.select_dtypes(include=['datetime64']).columns:
        if df[col].dt.tz is None:
            df[col] = df[col].dt.set_tz('UTC')
            
    df.to_parquet(file_path, index=False)
    
    # Write metadata/manifest
    manifest = {
        "version": version,
        "dataset_name": dataset_name,
        "created_at": datetime.now().isoformat(),
        "row_count": len(df)
    }
    import json
    with open(output_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
        
    return output_dir

def load_parquet_dataset(path: Path) -> pd.DataFrame:
    """Load a parquet dataset from a directory or file."""
    if path.is_dir():
        file_path = path / "data.parquet"
        if not file_path.exists():
            # Try loading all parquets in dir
            return pd.read_parquet(path)
        return pd.read_parquet(file_path)
    return pd.read_parquet(path)

