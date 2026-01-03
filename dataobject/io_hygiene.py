from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Dict

@dataclass
class Example:
    market_id: str
    source: str
    cutoff_ts: datetime
    history_ts: List[datetime]
    history_belief: List[float]
    static_features: Dict[str, Any]
    target: Any

@dataclass
class Batch:
    examples: List[Example]

def validate_no_future(example: Example):
    """Ensure no data point in history is after the cutoff."""
    for ts in example.history_ts:
        if ts > example.cutoff_ts:
            raise ValueError(f"Future leakage: {ts} > {example.cutoff_ts}")

