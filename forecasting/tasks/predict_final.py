"""
Predict Final Task - predict resolution from early market data.
Uses early cutoff (configurable %) but targets actual resolution or final belief.
This is the key task for testing RLM reasoning capabilities.
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
import random
from forecasting.tasks.base import Task
from forecasting.dataset import EventRecordWrapper
from forecasting.dataclasses import Example, OptionHistory
from forecasting.metrics import multiclass_brier, multiclass_logloss


class PredictFinalTask(Task):
    """
    Task to predict final market state from early data.

    Key design decisions:
    - cutoff_percent: How much of the market history to show (default 25%)
    - use_resolution: If True, use actual resolution; if False, use final belief
    - This creates a meaningful prediction task where RLM reasoning can help
    """
    name = "predict_final"

    def __init__(
        self,
        cutoff_percent: float = 0.25,  # Show first 25% of history
        use_resolution: bool = True,    # Use actual resolution as target
        relax_status: bool = False,     # Allow non-resolved markets
        min_history_points: int = 3     # Minimum points before cutoff
    ):
        """
        Args:
            cutoff_percent: Fraction of market duration to use as cutoff (0.0-1.0)
            use_resolution: If True, target is resolution; if False, target is final belief
            relax_status: If True, allow closed markets (use final belief as target)
            min_history_points: Minimum data points required before cutoff
        """
        self.cutoff_percent = cutoff_percent
        self.use_resolution = use_resolution
        self.relax_status = relax_status
        self.min_history_points = min_history_points

    def _to_datetime(self, value: Any) -> Optional[datetime]:
        """Convert value to timezone-naive datetime."""
        if value is None:
            return None
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

    def _parse_resolution(self, value: Any) -> Optional[float]:
        """Parse resolution value to float."""
        if value is None:
            return None
        if isinstance(value, str):
            import json
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
            lowered = str(value).strip().lower()
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

    def make_examples(self, record: EventRecordWrapper, rng: random.Random) -> List[Example]:
        options = record.options
        if not options:
            return []

        # Check status
        is_resolved = record.status == "resolved"
        is_closed = record.status == "closed"

        if self.use_resolution and not is_resolved and not (self.relax_status and is_closed):
            return []

        if not self.use_resolution and not is_resolved and not is_closed and not self.relax_status:
            return []

        # Find timestamp range across all options
        first_ts = None
        last_ts = None

        for option in options:
            raw_ts = option.get("ts") or []
            for ts_val in raw_ts:
                ts = self._to_datetime(ts_val)
                if ts is not None:
                    if first_ts is None or ts < first_ts:
                        first_ts = ts
                    if last_ts is None or ts > last_ts:
                        last_ts = ts

        if first_ts is None or last_ts is None:
            return []

        # Calculate cutoff
        duration = last_ts - first_ts
        if duration.total_seconds() <= 0:
            return []
        cutoff_time = first_ts + (duration * self.cutoff_percent)

        # Build option histories
        option_histories = []
        final_beliefs = []

        for option in options:
            raw_ts = option.get("ts") or []
            raw_belief = option.get("belief") or []

            if not raw_ts or not raw_belief or len(raw_ts) != len(raw_belief):
                return []

            ts_list = [self._to_datetime(ts) for ts in raw_ts]
            if any(ts is None for ts in ts_list):
                return []

            # Find cutoff index
            cutoff_idx = None
            for i, ts in enumerate(ts_list):
                if ts >= cutoff_time:
                    cutoff_idx = i
                    break

            if cutoff_idx is None:
                cutoff_idx = len(ts_list)

            if cutoff_idx < self.min_history_points:
                return []

            # History up to cutoff
            history_ts = ts_list[:cutoff_idx]
            history_belief = raw_belief[:cutoff_idx]

            # Final belief (last observation)
            final_belief = raw_belief[-1] if raw_belief else 0.0
            final_beliefs.append(float(final_belief) if final_belief else 0.0)

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

        # Build target
        if self.use_resolution and is_resolved:
            # Use actual resolution
            resolved_scores = []
            for option in options:
                raw_val = option.get("resolved_value_json")
                resolved_scores.append(self._parse_resolution(raw_val))

            resolved_indices = [i for i, v in enumerate(resolved_scores) if v is not None and v > 0.5]
            if not resolved_indices:
                return []

            share = 1.0 / len(resolved_indices)
            target = [share if i in resolved_indices else 0.0 for i in range(len(options))]
        else:
            # Use final belief as target
            normalized = self._normalize(final_beliefs)
            if normalized is None:
                return []
            target = normalized

        static_features = {
            "title": record.title,
            "description": record.description,
            "end_time": record.end_time,
            "option_count": len(options),
            "first_ts": first_ts,
            "cutoff_ts": cutoff_time,
            "cutoff_percent": self.cutoff_percent,
            "duration_hours": duration.total_seconds() / 3600
        }

        return [Example(
            event_id=record.event_id,
            source=record.source,
            cutoff_ts=cutoff_time,
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
# 1. Early cutoff + final target is the key design for meaningful RLM evaluation
# 2. cutoff_percent controls difficulty: 25% is hard, 75% is easy
# 3. use_resolution=True gives proper ground truth but limits data
# 4. relax_status + use_resolution=False gives more data but softer target
# 5. The task tests if RLM can predict where the market will end up, not just next tick
#
