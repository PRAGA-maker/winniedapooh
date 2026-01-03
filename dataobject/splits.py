import hashlib
import json
import pandas as pd
from typing import Optional
from pathlib import Path
from dataobject.dataset import MarketDataset, DatasetView

class SplitManager:
    def __init__(self, dataset: MarketDataset, splits_df: pd.DataFrame):
        self.dataset = dataset
        self.splits_df = splits_df

    @classmethod
    def build(cls, dataset: MarketDataset, seed: int = 42) -> "SplitManager":
        """Build deterministic splits based on market_id hash."""
        records = []
        for _, row in dataset.df.iterrows():
            m_id = row["market_id"]
            source = row["source"]
            
            # Deterministic hash
            hash_input = f"{m_id}:{source}:{seed}".encode()
            h = hashlib.sha256(hash_input).hexdigest()
            h_int = int(h, 16)
            
            # Assignment logic:
            # If source == metaculus: 50% test, 50% bench
            # If source == kalshi: 70% train, 15% test, 15% bench
            
            if source == "metaculus":
                split = "test" if (h_int % 100) < 50 else "bench"
            else: # kalshi
                mod = h_int % 100
                if mod < 70:
                    split = "train"
                elif mod < 85:
                    split = "test"
                else:
                    split = "bench"
                    
            records.append({
                "market_id": m_id,
                "source": source,
                "split": split
            })
            
        return cls(dataset, pd.DataFrame(records))

    def view(self, split_name: str) -> DatasetView:
        """Get a view of the dataset for a specific split."""
        split_ids = self.splits_df[self.splits_df["split"] == split_name]
        merged = pd.merge(
            self.dataset.df, 
            split_ids[["market_id", "source"]], 
            on=["market_id", "source"]
        )
        return DatasetView(merged)

    def save(self, path: Path):
        self.splits_df.to_parquet(path / "splits.parquet", index=False)
        
    @classmethod
    def load(cls, dataset: MarketDataset, path: Path) -> "SplitManager":
        splits_df = pd.read_parquet(path / "splits.parquet")
        return cls(dataset, splits_df)

# --- LESSONS LEARNED ---
# 1. Deterministic Splitting: Using SHA256 hash of market_id + seed is much 
#    more stable than random sampling for forecasting datasets.
# 2. Split Logic: Metaculus is almost always OOD (bench/test), while Kalshi 
#    is the primary source for training.

