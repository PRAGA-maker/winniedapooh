from typing import Any, Dict
from dataobject.io_hygiene import Batch

class ForecastMethod:
    name: str

    def fit(self, train_batches: Any, spec: Dict[str, Any]) -> None:
        pass
        
    def predict(self, batch: Batch, spec: Dict[str, Any]) -> Any:
        raise NotImplementedError
        
    def save(self, path: str) -> None:
        pass
        
    @classmethod
    def load(cls, path: str) -> "ForecastMethod":
        raise NotImplementedError

