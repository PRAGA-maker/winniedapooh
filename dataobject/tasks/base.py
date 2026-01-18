from typing import List, Dict, Any
from dataobject.dataset import EventRecordWrapper
from dataobject.io_hygiene import Example, Batch


class Task:
    """
    Base class for defining prediction tasks.
    
    A Task converts EventRecord objects into Example objects for training/evaluation,
    and defines which metrics to compute.
    
    Examples:
        See dataobject/tasks/resolve_binary.py for event-level resolution task.
        See dataobject/tasks/predict_week_out.py for time-series prediction task.
    
    Testing Notes (2026-01):
        Developer experience testing: Creating a new task takes ~15 minutes.
        Interface is clear but datetime handling requires some thought.
        Example dataclass is intuitive.
        ResolveEventTask is a good reference implementation.
        All tested tasks work end-to-end with the runner.
    """
    name: str
    
    def make_examples(self, record: EventRecordWrapper, rng: Any) -> List[Example]:
        """
        Convert an EventRecord into Example objects for training/evaluation.
        
        Args:
            record: EventRecord to convert
            rng: Random number generator for reproducibility
            
        Returns:
            List of Example objects (empty list if record doesn't qualify)
        """
        raise NotImplementedError
        
    def collate(self, examples: List[Example]) -> Batch:
        """
        Collate examples into a Batch for processing.
        
        Default implementation simply wraps examples in a Batch.
        Override if you need custom batching logic.
        
        Args:
            examples: List of Example objects
            
        Returns:
            Batch object containing the examples
        """
        return Batch(examples=examples)
        
    def metric_fns(self) -> Dict[str, Any]:
        """
        Return dictionary of metric functions to compute.
        
        Returns:
            Dictionary mapping metric names to sklearn-compatible functions
            Functions should accept (y_true, y_pred) and return a scalar
        """
        raise NotImplementedError

# --- LESSONS LEARNED ---
# 1. Event-first: Task input is an event row with variable-length options.
# 2. Metrics: Prefer multiclass-safe metrics for distribution targets.

