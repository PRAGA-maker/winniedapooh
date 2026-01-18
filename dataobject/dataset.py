import json
import pandas as pd
from typing import Optional, List, Dict, Any
from pathlib import Path
from src.common.parquet import load_parquet_dataset
from src.common.schema import EventRecord

class EventRecordWrapper:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self._options_cache: Optional[List[Dict[str, Any]]] = None
        
    def __getattr__(self, name):
        return self.data.get(name)

    @property
    def options(self) -> List[Dict[str, Any]]:
        if self._options_cache is None:
            raw = self.data.get("options_json")
            if raw:
                try:
                    self._options_cache = json.loads(raw)
                except json.JSONDecodeError:
                    self._options_cache = []
            else:
                self._options_cache = []
        return self._options_cache
        
    def to_record(self) -> EventRecord:
        return EventRecord(**{k: v for k, v in self.data.items() if k in EventRecord.__fields__})

class DatasetView:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def records(self) -> List[EventRecordWrapper]:
        return [EventRecordWrapper(row.to_dict()) for _, row in self.df.iterrows()]
        
    def to_frame(self) -> pd.DataFrame:
        return self.df

class EventDataset:
    def __init__(self, df: pd.DataFrame, path: Optional[Path] = None):
        self.df = df
        self.path = path
        
    @classmethod
    def load(cls, path: str) -> "EventDataset":
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
        
    def get_record(self, event_id: str, source: str) -> Optional[EventRecordWrapper]:
        match = self.df[(self.df["event_id"] == event_id) & (self.df["source"] == source)]
        if match.empty:
            return None
        return EventRecordWrapper(match.iloc[0].to_dict())

# --- LESSONS LEARNED ---
# 1. Parquet Performance: Loading a single unified table is significantly faster 
#    than joining hundreds of small CSVs or JSONs.
# 2. Wrapper Objects: Use a wrapper for event rows to keep options_json parsing
#    lazy and localized.
# 3. Event Keys: event_id is the stable primary key after aggregation.

