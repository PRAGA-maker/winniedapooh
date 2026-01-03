import pandas as pd
from typing import Optional, List, Dict, Any
from pathlib import Path
from src.common.parquet import load_parquet_dataset
from src.common.schema import MarketRecord

class MarketRecordWrapper:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        
    def __getattr__(self, name):
        return self.data.get(name)
        
    def to_record(self) -> MarketRecord:
        return MarketRecord(**{k: v for k, v in self.data.items() if k in MarketRecord.__fields__})

class DatasetView:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def records(self) -> List[MarketRecordWrapper]:
        return [MarketRecordWrapper(row.to_dict()) for _, row in self.df.iterrows()]
        
    def to_frame(self) -> pd.DataFrame:
        return self.df

class MarketDataset:
    def __init__(self, df: pd.DataFrame, path: Optional[Path] = None):
        self.df = df
        self.path = path
        
    @classmethod
    def load(cls, path: str) -> "MarketDataset":
        p = Path(path)
        df = load_parquet_dataset(p)
        return cls(df, path=p)
        
    def slice(self, source: Optional[str] = None, status: Optional[str] = None, market_type: Optional[str] = None) -> DatasetView:
        filtered_df = self.df.copy()
        if source:
            filtered_df = filtered_df[filtered_df["source"] == source]
        if status:
            filtered_df = filtered_df[filtered_df["status"] == status]
        if market_type:
            filtered_df = filtered_df[filtered_df["market_type"] == market_type]
        return DatasetView(filtered_df)
        
    def get_record(self, market_id: str, source: str) -> Optional[MarketRecordWrapper]:
        match = self.df[(self.df["market_id"] == market_id) & (self.df["source"] == source)]
        if match.empty:
            return None
        return MarketRecordWrapper(match.iloc[0].to_dict())

# --- LESSONS LEARNED ---
# 1. Parquet Performance: Loading a single unified table is significantly faster 
#    than joining hundreds of small CSVs or JSONs.
# 2. Wrapper Objects: Using a wrapper for market rows allows for cleaner .dict() 
#    and validation logic without cluttering the main DataFrame.

