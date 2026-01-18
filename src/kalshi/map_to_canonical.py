import json
from datetime import datetime
from typing import Dict, Any, List
from src.common.schema import TimeSeriesPoint, MarketType, MarketStatus


def _strip_year_suffix(ticker: str) -> str | None:
    if not ticker:
        return None
    parts = ticker.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 2:
        return parts[0]
    return None


def build_kalshi_url(market_id: str, event_ticker: str = None) -> str:
    """
    Build a stable Kalshi URL.
    Prefer event-market pages: /markets/{event}/{market_id}.
    """
    market_id = (market_id or "").strip()
    event_segment = (event_ticker or "").strip()
    if event_segment.upper().startswith("KX"):
        event_segment = event_segment[2:]

    if event_segment:
        base_segment = _strip_year_suffix(event_segment) or event_segment
        if base_segment:
            return f"https://kalshi.com/markets/{base_segment}/{market_id}"

    fallback_segment = _strip_year_suffix(market_id)
    if fallback_segment:
        return f"https://kalshi.com/markets/{fallback_segment}/{market_id}"

    if market_id:
        return f"https://kalshi.com/markets/{market_id}"
    return ""

def _coerce_numeric(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned == "":
                return None
            return float(cleaned)
        return float(value)
    except (ValueError, TypeError):
        return value

def _map_kalshi_market_type(raw: Dict[str, Any]) -> MarketType:
    raw_type = (raw.get("market_type") or raw.get("category") or "").lower()
    if "binary" in raw_type:
        return MarketType.BINARY
    if "scalar" in raw_type:
        return MarketType.NUMERIC
    return MarketType.OTHER

def _build_kalshi_answer_options(raw: Dict[str, Any], market_type: MarketType) -> List[Any]:
    if market_type == MarketType.BINARY:
        no_title = raw.get("no_sub_title") or "NO"
        yes_title = raw.get("yes_sub_title") or "YES"
        return [no_title, yes_title]
    if market_type == MarketType.NUMERIC:
        option = {
            "type": "scalar",
            "strike_type": raw.get("strike_type"),
            "floor_strike": raw.get("floor_strike"),
            "cap_strike": raw.get("cap_strike"),
            "tick_size": raw.get("tick_size"),
            "response_price_units": raw.get("response_price_units")
        }
        return [option]
    return []

def _normalize_kalshi_resolution(raw: Dict[str, Any], market_type: MarketType) -> Any:
    result = raw.get("result")
    if isinstance(result, str):
        cleaned = result.strip()
        if cleaned:
            return cleaned.lower() if market_type == MarketType.BINARY else cleaned
    if market_type == MarketType.BINARY:
        settlement_value = raw.get("settlement_value_dollars")
        if settlement_value is None:
            settlement_value = raw.get("settlement_value")
        if settlement_value is not None:
            coerced = _coerce_numeric(settlement_value)
            if isinstance(coerced, (int, float)):
                return "yes" if coerced > 0 else "no"
    if market_type == MarketType.NUMERIC:
        expiration_value = raw.get("expiration_value")
        if expiration_value is not None:
            return _coerce_numeric(expiration_value)
        settlement_value = raw.get("settlement_value_dollars")
        if settlement_value is None:
            settlement_value = raw.get("settlement_value")
        if settlement_value is not None:
            return _coerce_numeric(settlement_value)
    return None

def _build_kalshi_description(raw: Dict[str, Any]) -> str:
    """
    Build a natural text description from available Kalshi API fields.
    Prefers subtitle, then rules_primary/rules_secondary, then title.
    If multiple fields are available, concatenates them naturally.
    """
    parts = []
    
    # Prefer subtitle if present and non-empty
    subtitle = raw.get("subtitle")
    if subtitle and isinstance(subtitle, str) and subtitle.strip():
        parts.append(subtitle.strip())
    
    # Add rules_primary if present
    rules_primary = raw.get("rules_primary")
    if rules_primary and isinstance(rules_primary, str) and rules_primary.strip():
        parts.append(rules_primary.strip())
    
    # Add rules_secondary if present
    rules_secondary = raw.get("rules_secondary")
    if rules_secondary and isinstance(rules_secondary, str) and rules_secondary.strip():
        parts.append(rules_secondary.strip())
    
    # If we have parts, join them; otherwise fall back to title or empty string
    if parts:
        return " ".join(parts)
    
    # Fallback to title if nothing else is available
    title = raw.get("title")
    if title and isinstance(title, str) and title.strip():
        return title.strip()
    
    return ""

def map_kalshi_market(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map raw Kalshi market JSON to canonical market record dict."""
    # Market type mapping
    market_type = _map_kalshi_market_type(raw)
        
    # Status mapping
    raw_status = raw.get("status", "").lower()
    if raw_status in ["active", "open"]:
        status = MarketStatus.OPEN
    elif raw_status == "closed":
        status = MarketStatus.CLOSED
    elif raw_status in ["settled", "finalized", "determined", "resolved"]:
        status = MarketStatus.RESOLVED
    else:
        status = MarketStatus.UNKNOWN

    # Answer options
    options = _build_kalshi_answer_options(raw, market_type)

    # Resolved value
    resolved_value = _normalize_kalshi_resolution(raw, market_type)
    
    # End time
    end_time_str = raw.get("expiration_time")
    if end_time_str:
        end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
    else:
        end_time = datetime.now() # Fallback

    # Build description from available fields
    description = _build_kalshi_description(raw)

    return {
        "source": "kalshi",
        "market_id": raw["ticker"],
        "event_id": raw.get("event_ticker"),
        "title": raw["title"],
        "description": description,
        "url": build_kalshi_url(raw.get("ticker"), raw.get("event_ticker")),
        "market_type": market_type,
        "answer_options_json": json.dumps(options),
        "end_time": end_time,
        "status": status,
        "resolved_value_json": json.dumps(resolved_value) if resolved_value is not None else None,
        "created_time": datetime.fromisoformat(raw["open_time"].replace("Z", "+00:00")) if raw.get("open_time") else None,
        "metadata_json": json.dumps(raw)
    }

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
# 3. URL Stability: Event-market paths /markets/{event}/{market_id} resolve more reliably than ticker-only URLs.
# 4. Market Type: Kalshi API exposes market_type (binary/scalar); don't hardcode binary.
# 5. Resolutions: Normalize result/settlement fields into resolved_value_json consistently.
# 6. Status Mapping: Treat finalized/determined/settled as resolved for canonical status.
# 7. Answer Options: Kalshi binary markets can be non-YES/NO wordings; preserve subtitles as options.
# 6. Description Mapping: Kalshi API provides multiple description fields (subtitle, rules_primary, 
#    rules_secondary). Build description by concatenating available fields in priority order.
#    S3 bulk records lack description fields, so use API enrichment to populate proper descriptions.
# 7. Scalar Absence: Exhaustive API/S3 scans (Feb-Mar 2025) confirm 0% scalar markets exist in practice. 
#    The mapping logic for MarketType.NUMERIC is placeholder/spec-compliant only. If scalars appear, 
#    verify if S3 reporting includes them or if API-only history (candles) is required.

