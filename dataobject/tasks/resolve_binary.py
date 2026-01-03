import json
import random
from typing import List, Dict, Any
from datetime import datetime, timedelta
from dataobject.tasks.base import Task
from dataobject.dataset import MarketRecordWrapper
from dataobject.io_hygiene import Example

class ResolveBinaryTask(Task):
    name = "resolve_binary"
    
    def make_examples(self, record: MarketRecordWrapper, rng: random.Random) -> List[Example]:
        # Only binary markets
        if record.market_type != "binary":
            return []
            
        # Needs a resolution
        if record.status != "resolved" or record.resolved_value_json is None:
            return []
            
        try:
            res_val = json.loads(record.resolved_value_json)
            # Standardize resolution to 0 or 1
            if isinstance(res_val, str):
                target = 1.0 if res_val.lower() in ["yes", "true", "1"] else 0.0
            else:
                target = float(res_val)
        except:
            return []

        ts_list = record.ts
        belief_list = record.belief
        
        if ts_list is None or len(ts_list) < 5:
            return []
            
        # Pick a random cutoff between 20% and 80% of the history
        idx = rng.randint(len(ts_list) // 5, len(ts_list) * 4 // 5)
        cutoff_ts = ts_list[idx]
        
        history_ts = ts_list[:idx+1]
        history_belief = belief_list[:idx+1]
        
        static_features = {
            "title": record.title,
            "description": record.description,
            "end_time": record.end_time
        }
        
        return [Example(
            market_id=record.market_id,
            source=record.source,
            cutoff_ts=cutoff_ts,
            history_ts=history_ts,
            history_belief=history_belief,
            static_features=static_features,
            target=target
        )]

    def metric_fns(self) -> Dict[str, Any]:
        from sklearn.metrics import brier_score_loss, log_loss
        return {
            "brier": brier_score_loss,
            "logloss": log_loss
        }

