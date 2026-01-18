"""
Predict Week Out Task - predict belief 7 days before market close.
Created for DX testing.
"""
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from dataobject.tasks.base import Task
from dataobject.dataset import EventRecordWrapper
from dataobject.io_hygiene import Example, OptionHistory
from dataobject.metrics import multiclass_brier, multiclass_logloss


class PredictWeekOutTask(Task):
    """Task to predict belief 7 days before market close."""
    name = "predict_week_out"
    
    def __init__(self, relax_status: bool = False, min_history_days: int = 14):
        """
        Args:
            relax_status: If True, allow markets without resolution (use last belief as target)
            min_history_days: Minimum days of history required
        """
        self.relax_status = relax_status
        self.min_history_days = min_history_days
    
    def _to_datetime(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
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
        
        # Calculate cutoff: 7 days before end_time
        end_time = record.end_time
        cutoff_time = end_time - timedelta(days=7)

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
            if len(ts_list) < self.min_history_days:
                return []

            cutoff_idx = None
            for i, ts in enumerate(ts_list):
                if ts >= cutoff_time:
                    cutoff_idx = i
                    break
            if cutoff_idx is None or cutoff_idx == 0:
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
            "option_count": len(options)
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


# DX TEST NOTES:
# Time to create: ~15 minutes
# Files modified: 2 (this file + runner.py TASK_REGISTRY)
# Pain points:
#   - Task interface is clear, but calculating cutoff requires understanding datetime handling
#   - The Example dataclass is intuitive
#   - ResolveEventTask is a good reference
#   - Would be helpful to have more examples of different task types
# Clarity: 4/5 - Good but could use more task examples

# --- LESSONS LEARNED ---
# 1. Cutoff logic must be computed per option to avoid missing history.
# 2. Normalize targets to keep a valid distribution across options.
