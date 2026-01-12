"""
Predict Week Out Task - predict belief 7 days before market close.
Created for DX testing.
"""
import json
import random
from typing import List, Dict, Any
from datetime import datetime, timedelta
from dataobject.tasks.base import Task
from dataobject.dataset import MarketRecordWrapper
from dataobject.io_hygiene import Example


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
    
    def make_examples(self, record: MarketRecordWrapper, rng: random.Random) -> List[Example]:
        # Only binary markets
        if record.market_type != "binary":
            return []
        
        # Need end_time to calculate cutoff
        if not record.end_time:
            return []
        
        # Need history
        ts_list = record.ts
        belief_list = record.belief
        
        if ts_list is None or belief_list is None or len(ts_list) < self.min_history_days:
            return []
        
        # Calculate cutoff: 7 days before end_time
        end_time = record.end_time
        cutoff_time = end_time - timedelta(days=7)
        
        # Find the cutoff point in history
        cutoff_idx = None
        for i, ts in enumerate(ts_list):
            if ts >= cutoff_time:
                cutoff_idx = i
                break
        
        if cutoff_idx is None or cutoff_idx == 0:
            return []  # No history before cutoff
        
        # Get history up to cutoff
        history_ts = ts_list[:cutoff_idx]
        history_belief = belief_list[:cutoff_idx]
        
        # Target is the belief at cutoff (or shortly after)
        target_idx = min(cutoff_idx + 1, len(belief_list) - 1)
        target = belief_list[target_idx]
        
        if target is None:
            return []
        
        static_features = {
            "title": record.title,
            "description": record.description,
            "end_time": record.end_time
        }
        
        return [Example(
            market_id=record.market_id,
            source=record.source,
            cutoff_ts=cutoff_time,
            history_ts=history_ts,
            history_belief=history_belief,
            static_features=static_features,
            target=float(target)
        )]
    
    def metric_fns(self) -> Dict[str, Any]:
        from sklearn.metrics import mean_squared_error, mean_absolute_error
        
        def mse(y_true, y_pred):
            return mean_squared_error(y_true, y_pred)
        
        def mae(y_true, y_pred):
            return mean_absolute_error(y_true, y_pred)
        
        return {
            "mse": mse,
            "mae": mae
        }


# DX TEST NOTES:
# Time to create: ~15 minutes
# Files modified: 2 (this file + runner.py TASK_REGISTRY)
# Pain points:
#   - Task interface is clear, but calculating cutoff requires understanding datetime handling
#   - The Example dataclass is intuitive
#   - ResolveBinaryTask is a good reference
#   - Would be helpful to have more examples of different task types
# Clarity: 4/5 - Good but could use more task examples
