from typing import Any, Dict
from dataobject.io_hygiene import Batch


class ForecastMethod:
    """
    Base class for all forecasting methods.
    
    Subclasses must implement predict(). The fit(), save(), and load() methods
    are optional (default implementations provided for simple baselines).
    
    Examples:
        See methods/baselines/last_price.py for a simple baseline implementation.
        See methods/mlp_forecaster.py for a full implementation with training.
    
    Testing Notes (2026-01):
        Developer experience testing: Adding a new method takes ~5 minutes.
        Interface is intuitive - only predict() required for simple baselines.
        Registry pattern makes registration straightforward.
        All tested methods (LastPriceBaseline, RandomBaseline, MLPForecaster) work end-to-end.
    """
    name: str

    def fit(self, train_batches: Any, spec: Dict[str, Any]) -> None:
        """
        Train the model on training data.
        
        Args:
            train_batches: List of Batch objects containing training examples
            spec: RunSpec configuration dictionary
        """
        pass
        
    def predict(self, batch: Batch, spec: Dict[str, Any]) -> Any:
        """
        Generate predictions for a batch of examples.
        
        Args:
            batch: Batch object containing examples to predict
            spec: RunSpec configuration dictionary
            
        Returns:
            Predictions as array/list (should match length of batch.examples)
        """
        raise NotImplementedError
        
    def save(self, path: str) -> None:
        """
        Save the model to disk.
        
        Args:
            path: File path to save the model
        """
        pass
        
    @classmethod
    def load(cls, path: str) -> "ForecastMethod":
        """
        Load a model from disk.
        
        Args:
            path: File path to load the model from
            
        Returns:
            Loaded ForecastMethod instance
        """
        raise NotImplementedError

