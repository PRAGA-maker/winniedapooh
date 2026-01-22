import json
import random
from typing import List, Dict, Any, Optional
from datetime import datetime
from forecasting.tasks.base import Task
from forecasting.dataset import EventRecordWrapper
from forecasting.dataclasses import Example, OptionHistory
from forecasting.metrics import multiclass_brier, multiclass_logloss

class ResolveEventTask(Task):
    name = "resolve_event"
    
    def __init__(self, relax_status: bool = False, min_history_points: int = 5):
        self.relax_status = relax_status
        self.min_history_points = min_history_points
    
    def _to_datetime(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def _load_json_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def _parse_resolution(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ["yes", "true", "1"]:
                return 1.0
            if lowered in ["no", "false", "0"]:
                return 0.0
            try:
                return float(lowered)
            except ValueError:
                return None
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def _normalize(self, values: List[float]) -> Optional[List[float]]:
        total = sum(values)
        if total <= 0:
            return None
        return [v / total for v in values]

    def make_examples(self, record: EventRecordWrapper, rng: random.Random) -> List[Example]:
        options = record.options
        if not options:
            return []

        is_resolved = record.status == "resolved"
        is_closed = record.status == "closed"

        if not is_resolved and not (self.relax_status and is_closed):
            return []

        option_histories = []
        min_len = None
        for option in options:
            ts_list = option.get("ts") or []
            belief_list = option.get("belief") or []
            if not ts_list or not belief_list:
                return []
            if len(ts_list) != len(belief_list):
                return []
            if min_len is None or len(ts_list) < min_len:
                min_len = len(ts_list)

        if min_len is None or min_len < self.min_history_points:
            return []

        idx_low = max(1, min_len // 5)
        idx_high = max(idx_low, min_len * 4 // 5)
        idx = rng.randint(idx_low, idx_high)

        cutoff_candidates = []
        for option in options:
            ts_val = self._to_datetime(option.get("ts")[idx])
            if ts_val:
                cutoff_candidates.append(ts_val)
        if not cutoff_candidates:
            return []
        cutoff_ts = min(cutoff_candidates)

        for option in options:
            raw_ts = option.get("ts")[: idx + 1]
            ts_list = [self._to_datetime(ts) for ts in raw_ts]
            if any(ts is None for ts in ts_list):
                return []
            belief_list = option.get("belief")[: idx + 1]
            option_histories.append(OptionHistory(
                option_id=str(option.get("option_id") or option.get("market_id") or ""),
                market_id=option.get("market_id"),
                title=str(option.get("title") or ""),
                history_ts=ts_list,
                history_belief=[float(b) if b is not None else 0.0 for b in belief_list],
                history_bid=(option.get("bid") or [])[: idx + 1],
                history_ask=(option.get("ask") or [])[: idx + 1],
                history_volume=(option.get("volume") or [])[: idx + 1],
                history_open_interest=(option.get("open_interest") or [])[: idx + 1]
            ))

        try:
            if is_resolved:
                resolved_scores = []
                for option in options:
                    raw_val = self._load_json_value(option.get("resolved_value_json"))
                    resolved_scores.append(self._parse_resolution(raw_val))
                resolved_indices = [i for i, v in enumerate(resolved_scores) if v is not None and v > 0.5]
                if not resolved_indices:
                    return []
                share = 1.0 / len(resolved_indices)
                target = [share if i in resolved_indices else 0.0 for i in range(len(options))]
            else:
                # For relaxed status, use normalized last beliefs as a dummy target
                last_beliefs = []
                for option in options:
                    belief_list = option.get("belief") or []
                    last_beliefs.append(float(belief_list[-1]) if belief_list else 0.0)
                normalized = self._normalize(last_beliefs)
                if normalized is None:
                    return []
                target = normalized
        except Exception as e:
            return []
        
        static_features = {
            "title": record.title,
            "description": record.description,
            "end_time": record.end_time,
            "option_count": len(options)
        }
        
        return [Example(
            event_id=record.event_id,
            source=record.source,
            cutoff_ts=cutoff_ts,
            options=option_histories,
            static_features=static_features,
            target=target
        )]

    def metric_fns(self) -> Dict[str, Any]:
        return {
            "brier": multiclass_brier,
            "logloss": multiclass_logloss
        }

# --- LESSONS LEARNED ---
# 1. Resolution: Allow multi-YES; distribute weight across resolved options.
# 2. Cutoff: Use the shortest option history to avoid misaligned slices.
# 3. Relaxed Mode: Normalize last beliefs to keep targets on the simplex.
# 4. min_history_points Filtering (2026-01-22): Default value of 5 ensures sufficient historical
#    context for meaningful forecasting. This threshold is critical for data quality but requires
#    datasets built over adequate time windows. S3 bulk data provides daily snapshots, so a
#    5-point minimum requires at least 5 days of data. Short-window datasets (e.g., 2 days)
#    result in 0% example generation rate. For production, build datasets over 30+ days to ensure
#    high pass rates. Lowering min_history_points improves coverage but reduces forecast quality.

