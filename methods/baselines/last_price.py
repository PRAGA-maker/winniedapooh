from typing import List, Dict, Any
import numpy as np
from methods.base import ForecastMethod
from dataobject.io_hygiene import Batch

class LastPriceBaseline(ForecastMethod):
    name = "last_price"
    
    def predict(self, batch: Batch, spec: Dict[str, Any]) -> np.ndarray:
        preds = []
        for ex in batch.examples:
            if ex.history_belief:
                preds.append(ex.history_belief[-1])
            else:
                preds.append(0.5) # Fallback
        return np.array(preds)

