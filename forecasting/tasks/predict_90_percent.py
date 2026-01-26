"""
Predict 90% Duration Task - predict belief at 90% through market duration.
Used for RLM evaluation where we want to maximize available history.
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
from forecasting.tasks.base import Task
from forecasting.dataset import EventRecordWrapper
from forecasting.dataclasses import Example, OptionHistory
from forecasting.metrics import multiclass_brier, multiclass_logloss


class Predict90PercentTask(Task):
    """Task to predict belief at 90% through market duration."""
    name = "predict_90_percent"
    
    def __init__(self, relax_status: bool = False, min_history_points: int = 5):
        """
        Args:
            relax_status: If True, allow markets without resolution (use last belief as target)
            min_history_points: Minimum number of data points required before cutoff
        """
        self.relax_status = relax_status
        self.min_history_points = min_history_points
    
    def _to_datetime(self, value: Any) -> Optional[datetime]:
        """Convert value to timezone-naive datetime for consistent comparisons."""
        if value is None:
            return None
        # Handle pandas Timestamp
        if hasattr(value, 'to_pydatetime'):
            dt = value.to_pydatetime()
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        if isinstance(value, datetime):
            return value.replace(tzinfo=None) if value.tzinfo else value
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.replace(tzinfo=None)
            except ValueError:
                return None
        return None

    def _normalize(self, values: List[float]) -> Optional[List[float]]:
        total = sum(values)
        if total <= 0:
            return None
        return [v / total for v in values]

    def make_examples(self, record: EventRecordWrapper, rng: Any) -> List[Example]:
        # Need end_time to calculate cutoff
        if not record.end_time:
            return []
        
        if not self.relax_status and record.status not in ["closed", "resolved"]:
            return []

        options = record.options
        if not options:
            return []
        
        # Find earliest timestamp across all options
        first_ts = None
        for option in options:
            raw_ts = option.get("ts") or []
            for ts_val in raw_ts:
                ts = self._to_datetime(ts_val)
                if ts is not None:
                    if first_ts is None or ts < first_ts:
                        first_ts = ts
        
        if first_ts is None:
            return []
        
        # Calculate cutoff: 90% through market duration
        end_time = self._to_datetime(record.end_time)
        if end_time is None:
            return []
        duration = end_time - first_ts
        cutoff_time = first_ts + (duration * 0.9)

        option_histories = []
        target_values = []
        for option in options:
            raw_ts = option.get("ts") or []
            raw_belief = option.get("belief") or []
            if not raw_ts or not raw_belief or len(raw_ts) != len(raw_belief):
                return []
            ts_list = [self._to_datetime(ts) for ts in raw_ts]
            if any(ts is None for ts in ts_list):
                return []

            cutoff_idx = None
            for i, ts in enumerate(ts_list):
                if ts >= cutoff_time:
                    cutoff_idx = i
                    break
            if cutoff_idx is None or cutoff_idx < self.min_history_points:
                return []

            history_ts = ts_list[:cutoff_idx]
            history_belief = raw_belief[:cutoff_idx]
            target = raw_belief[cutoff_idx]
            if target is None:
                return []

            option_histories.append(OptionHistory(
                option_id=str(option.get("option_id") or option.get("market_id") or ""),
                market_id=option.get("market_id"),
                title=str(option.get("title") or ""),
                history_ts=history_ts,
                history_belief=[float(b) if b is not None else 0.0 for b in history_belief],
                history_bid=(option.get("bid") or [])[:cutoff_idx],
                history_ask=(option.get("ask") or [])[:cutoff_idx],
                history_volume=(option.get("volume") or [])[:cutoff_idx],
                history_open_interest=(option.get("open_interest") or [])[:cutoff_idx]
            ))
            target_values.append(float(target))

        normalized_target = self._normalize(target_values)
        if normalized_target is None:
            return []
        
        static_features = {
            "title": record.title,
            "description": record.description,
            "end_time": record.end_time,
            "option_count": len(options),
            "first_ts": first_ts,
            "cutoff_ts": cutoff_time,
            "duration_hours": duration.total_seconds() / 3600
        }
        
        return [Example(
            event_id=record.event_id,
            source=record.source,
            cutoff_ts=cutoff_time,
            options=option_histories,
            static_features=static_features,
            target=normalized_target
        )]
    
    def metric_fns(self) -> Dict[str, Any]:
        return {
            "brier": multiclass_brier,
            "logloss": multiclass_logloss
        }


# --- LESSONS LEARNED ---
# 1. Dynamic cutoff: Using 90% of duration gives more history than fixed week-out.
# 2. Cross-option first_ts: Must scan all options for the true earliest timestamp.
# 3. min_history_points: Use count-based threshold since duration varies wildly.
# 4. Timezone handling: Pandas Timestamps and ISO strings have different tz-awareness.
#    Always normalize to tz-naive via _to_datetime() before datetime arithmetic.
