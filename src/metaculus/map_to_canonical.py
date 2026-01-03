import json
from datetime import datetime
from typing import Dict, Any, List
from src.common.schema import MarketRecord, TimeSeriesPoint, MarketType, MarketStatus

def map_metaculus_question(raw_post: Dict[str, Any], raw_q: Dict[str, Any]) -> MarketRecord:
    """Map raw Metaculus post/question JSON to canonical MarketRecord."""
    if not raw_q:
        raise ValueError("raw_q cannot be None")
        
    # Market type mapping
    raw_type = str(raw_q.get("type") or "").lower()
    if raw_type == "binary":
        market_type = MarketType.BINARY
    elif raw_type == "multiple_choice":
        market_type = MarketType.MULTIPLE_CHOICE
    elif raw_type in ["numeric", "continuous"]:
        market_type = MarketType.NUMERIC
    else:
        market_type = MarketType.OTHER
        
    # Status mapping
    status_str = str(raw_q.get("status") or raw_post.get("status") or "").lower()
    if "resolved" in status_str:
        status = MarketStatus.RESOLVED
    elif "closed" in status_str:
        status = MarketStatus.CLOSED
    elif "active" in status_str or "open" in status_str:
        status = MarketStatus.OPEN
    else:
        status = MarketStatus.UNKNOWN

    # Answer options
    possibilities = raw_q.get("possibilities") or {}
    options = possibilities.get("labels") or possibilities.get("categories") or ["NO", "YES"]
    
    # End time
    end_time_str = raw_q.get("scheduled_resolve_time") or raw_q.get("actual_resolve_time") or raw_post.get("scheduled_resolve_time")
    if end_time_str:
        try:
            end_time = datetime.fromisoformat(str(end_time_str).replace("Z", "+00:00"))
        except:
            end_time = datetime.now()
    else:
        end_time = datetime.now()

    return MarketRecord(
        source="metaculus",
        market_id=str(raw_q.get("id", "unknown")),
        title=str(raw_q.get("title") or raw_post.get("title") or ""),
        description=str(raw_q.get("description") or raw_post.get("description") or ""),
        url=f"https://www.metaculus.com/questions/{raw_q.get('id', '')}",
        market_type=market_type,
        answer_options_json=json.dumps(options),
        end_time=end_time,
        status=status,
        resolved_value_json=json.dumps(raw_q.get("resolution")) if raw_q.get("resolution") is not None else None,
        created_time=datetime.fromisoformat(str(raw_q["created_at"]).replace("Z", "+00:00")) if raw_q.get("created_at") else None,
        metadata_json=json.dumps({"post": raw_post, "question": raw_q})
    )

def map_metaculus_history_point(q_id: str, point: Dict[str, Any]) -> TimeSeriesPoint:
    """Map raw Metaculus history point to canonical TimeSeriesPoint."""
    # Metaculus history in aggregations format:
    # { "start_time": ..., "centers": [prob], ... }
    
    if "start_time" in point:
        ts = datetime.fromtimestamp(point["start_time"])
        centers = point.get("centers")
        if centers and isinstance(centers, list) and len(centers) > 0:
            belief_scalar = float(centers[0])
            belief_json = None
        else:
            belief_scalar = None
            belief_json = json.dumps(point.get("forecast_values"))
    elif isinstance(point, list):
        # [timestamp, community_prediction, ...]
        ts = datetime.fromtimestamp(point[0])
        belief_scalar = float(point[1])
        belief_json = None
    else:
        # Fallback for old/other formats
        ts = datetime.fromisoformat(point["t"].replace("Z", "+00:00")) if "t" in point else datetime.now()
        belief = point.get("cp") or point.get("community_prediction")
        belief_scalar = float(belief) if isinstance(belief, (int, float)) else None
        belief_json = json.dumps(belief) if belief_scalar is None else None

    return TimeSeriesPoint(
        source="metaculus",
        market_id=q_id,
        ts=ts,
        belief_scalar=belief_scalar,
        belief_json=belief_json,
        raw_json=json.dumps(point)
    )

# --- LESSONS LEARNED ---
# 1. History Location: History lives in aggregations -> recency_weighted -> history.
# 2. Probability Field: 'centers' list in the history point is usually the prob.
# 3. Unpacking: Metaculus is deeply nested. Defensive .get() and type checking are mandatory.

