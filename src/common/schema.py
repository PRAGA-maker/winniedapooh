from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field

from enum import Enum

class MarketType(str, Enum):
    BINARY = "binary"
    MULTIPLE_CHOICE = "multiple_choice"
    NUMERIC = "numeric"
    OTHER = "other"
    EVENT = "event"

class MarketStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    RESOLVED = "resolved"
    UNKNOWN = "unknown"

class EventRecord(BaseModel):
    source: str  # "kalshi" | "metaculus"
    event_id: str
    title: str
    description: str
    url: str
    market_type: MarketType
    options_json: str  # JSON list of option objects
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
    bid: Optional[float] = None  # Best buy price (Kalshi: from candlesticks, daily OHLC)
    ask: Optional[float] = None  # Best sell price (Kalshi: from candlesticks, daily OHLC)
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
# 4. Bid/Ask Coverage (2026-01-18): Investigation revealed ~18.8% of Kalshi options lack bid/ask:
#    - 15.3%: Non-synthetic with 0 volume (expected - no trading activity)
#    - 4.1%: Synthetic options (expected - no real orderbook exists)
#    - 0.9%: Ultra-short-lived daily markets (Kalshi data retention limitation)
#    Bid/ask are sourced from /markets/candlesticks (daily OHLC), not instantaneous orderbook
#    snapshots. Historical orderbook data is not available via Kalshi API. This is acceptable
#    for forecasting research (~81% option coverage, ~30% point coverage in Jan 2024).

