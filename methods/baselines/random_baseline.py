"""
Random baseline that returns random predictions between 0 and 1.
Simple test method for DX evaluation.
"""
import numpy as np
from typing import Dict, Any
from methods.base import ForecastMethod
from dataobject.io_hygiene import Batch


class RandomBaseline(ForecastMethod):
    name = "random_baseline"
    
    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = np.random.RandomState(seed)
    
    def predict(self, batch: Batch, spec: Dict[str, Any]) -> np.ndarray:
        """Return random predictions for each example."""
        return self.rng.uniform(0.0, 1.0, size=len(batch.examples))


# DX TEST NOTES:
# Time to create: ~5 minutes
# Files modified: 2 (this file + registry.py)
# Pain points:
#   - None significant. The ForecastMethod interface is clear.
#   - Examples (LastPriceBaseline) were helpful.
#   - The base class has minimal required methods (predict is main one).
# Clarity: 5/5 - Very straightforward
