import json
from datetime import datetime
from typing import Dict, Any, List
from src.common.schema import MarketRecord, TimeSeriesPoint, MarketType, MarketStatus

def map_kalshi_market(raw: Dict[str, Any]) -> MarketRecord:
    """Map raw Kalshi market JSON to canonical MarketRecord."""
    # Market type mapping
    raw_type = raw.get("category", "").lower()
    if "binary" in raw_type or not raw.get("yes_sub_title"):
        market_type = MarketType.BINARY
    else:
        # Default to binary for v1 as per PRD
        market_type = MarketType.BINARY
        
    # Status mapping
    raw_status = raw.get("status", "").lower()
    if raw_status == "active":
        status = MarketStatus.OPEN
    elif raw_status == "closed":
        status = MarketStatus.CLOSED
    elif raw_status == "settled":
        status = MarketStatus.RESOLVED
    else:
        status = MarketStatus.UNKNOWN

    # Answer options
    options = ["NO", "YES"]
    
    # End time
    end_time_str = raw.get("expiration_time")
    if end_time_str:
        end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
    else:
        end_time = datetime.now() # Fallback

    return MarketRecord(
        source="kalshi",
        market_id=raw["ticker"],
        event_id=raw.get("event_ticker"),
        title=raw["title"],
        description=raw.get("subtitle", ""),
        url=f"https://kalshi.com/markets/{raw['ticker']}",
        market_type=market_type,
        answer_options_json=json.dumps(options),
        end_time=end_time,
        status=status,
        resolved_value_json=json.dumps(raw.get("result")) if raw.get("result") else None,
        created_time=datetime.fromisoformat(raw["open_time"].replace("Z", "+00:00")) if raw.get("open_time") else None,
        metadata_json=json.dumps(raw)
    )

def map_kalshi_trade(ticker: str, trade: Dict[str, Any]) -> TimeSeriesPoint:
    """Map raw Kalshi trade to canonical TimeSeriesPoint."""
    # price is in cents, yes_price_dollars is in dollars
    price = trade.get("yes_price_dollars")
    
    # created_time is ISO format
    ts_str = trade.get("created_time")
    if ts_str:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    else:
        ts = datetime.now()
        
    return TimeSeriesPoint(
        source="kalshi",
        market_id=ticker,
        ts=ts,
        belief_scalar=float(price) if price is not None else None,
        volume=float(trade.get("count", 1)),
        raw_json=json.dumps(trade)
    )

def map_kalshi_candle(ticker: str, candle: Dict[str, Any]) -> TimeSeriesPoint:
    """Map raw Kalshi candlestick to canonical TimeSeriesPoint."""
    # price.mean_dollars is the recommended belief
    price = candle.get("price", {})
    mean_price = price.get("mean_dollars")
    
    # ts is in unix seconds
    ts = datetime.fromtimestamp(candle["start_period_ts"])
    
    return TimeSeriesPoint(
        source="kalshi",
        market_id=ticker,
        ts=ts,
        belief_scalar=float(mean_price) if mean_price is not None else None,
        volume=float(candle.get("volume", 0)),
        open_interest=float(candle.get("open_interest", 0)),
        raw_json=json.dumps(candle)
    )

# --- LESSONS LEARNED ---
# 1. Price Scalar: 'yes_price_dollars' is the cleanest mapping for binary belief.
# 2. Timestamps: Kalshi uses ISO strings with 'Z' for UTC. Standardize early.

