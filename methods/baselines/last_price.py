from typing import List, Dict, Any
from methods.base import ForecastMethod
from dataobject.io_hygiene import Batch

class LastPriceBaseline(ForecastMethod):
    name = "last_price"
    
    def _normalize(self, values: List[float]) -> List[float]:
        total = sum(values)
        if total <= 0:
            return [1.0 / len(values) for _ in values]
        return [v / total for v in values]

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[List[float]]:
        preds = []
        for ex in batch.examples:
            option_scores = []
            for option in ex.options:
                if option.history_belief:
                    option_scores.append(option.history_belief[-1])
                else:
                    option_scores.append(0.0)
            preds.append(self._normalize(option_scores))
        return preds

# --- LESSONS LEARNED ---
# 1. Baselines must emit a probability distribution, not a scalar.
# 2. Normalize last beliefs to avoid invalid sums.

