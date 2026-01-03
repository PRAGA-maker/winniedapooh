from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field

from enum import Enum

class MarketType(str, Enum):
    BINARY = "binary"
    MULTIPLE_CHOICE = "multiple_choice"
    NUMERIC = "numeric"
    OTHER = "other"

class MarketStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    RESOLVED = "resolved"
    UNKNOWN = "unknown"

class MarketRecord(BaseModel):
    source: str  # "kalshi" | "metaculus"
    market_id: str
    event_id: Optional[str] = None
    title: str
    description: str
    url: str
    market_type: MarketType
    answer_options_json: str  # JSON list of options
    end_time: datetime
    status: MarketStatus
    resolved_value_json: Optional[str] = None
    created_time: Optional[datetime] = None
    metadata_json: str  # JSON blob of extra fields

class TimeSeriesPoint(BaseModel):
    source: str
    market_id: str
    ts: datetime
    belief_scalar: Optional[float] = None
    belief_json: Optional[str] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    open_interest: Optional[float] = None
    raw_json: Optional[str] = None

    class Config:
        # Ensure only one of belief_scalar or belief_json is set for the final canonical form
        # but the model allows both during ingestion before normalization if needed.
        pass

# --- LESSONS LEARNED ---
# 1. Pydantic V2: All fields MUST have type annotations or they are ignored/error out.
# 2. Enums: Use (str, Enum) for easy JSON serialization in parquet/pandas.
# 3. Canonical Schema: Keeping it flat (except for JSON blobs) makes pandas/parquet much happier.

