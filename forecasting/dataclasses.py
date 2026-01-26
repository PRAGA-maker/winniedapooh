from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Dict

@dataclass
class OptionHistory:
    option_id: str
    market_id: Optional[str]
    title: str
    history_ts: List[datetime]
    history_belief: List[float]
    history_bid: Optional[List[float]] = None
    history_ask: Optional[List[float]] = None
    history_volume: Optional[List[float]] = None
    history_open_interest: Optional[List[float]] = None

@dataclass
class Example:
    event_id: str
    source: str
    cutoff_ts: datetime
    options: List[OptionHistory]
    static_features: Dict[str, Any]
    target: Any

@dataclass
class Batch:
    examples: List[Example]

def validate_no_future(example: Example):
    """Ensure no data point in history is after the cutoff."""
    for option in example.options:
        for ts in option.history_ts:
            if ts > example.cutoff_ts:
                raise ValueError(f"Future leakage: {ts} > {example.cutoff_ts}")

# --- LESSONS LEARNED ---
# 1. OptionHistory: Keep per-option histories explicit to avoid implicit
#    list alignment bugs.
# 2. Leakage Checks: Validate against cutoff across all options, not just one.

