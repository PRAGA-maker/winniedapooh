import random
import numpy as np
from typing import Any, Dict
from dataobject.dataset import DatasetView
from dataobject.tasks.base import Task
from methods.base import ForecastMethod
from runner.experiment import RunSpec


def evaluate(method: ForecastMethod, view: DatasetView, task: Task, spec: RunSpec) -> Dict[str, float]:
    """
    Evaluate a forecasting method on a dataset view.
    
    Args:
        method: ForecastMethod to evaluate
        view: DatasetView containing records to evaluate on
        task: Task to use for creating examples and metrics
        spec: RunSpec configuration
        
    Returns:
        Dictionary mapping metric names to computed values (empty if no examples)
    
    Testing Notes (2026-01):
        Returns empty dict if no examples generated - this is expected behavior
        when test data has no resolved markets. Could add warning message for 
        better visibility (recommendation: P1 priority).
        All tested methods and tasks work correctly with this evaluator.
    """
    rng = random.Random(spec.seed)
    examples = []
    from tqdm import tqdm
    for record in tqdm(view.records(), desc="Creating evaluation examples", leave=False):
        examples.extend(task.make_examples(record, rng))
        
    if not examples:
        import warnings
        warnings.warn(f"No examples generated for evaluation - task.make_examples() returned empty for all records in view", UserWarning)
        return {}
        
    if not examples:
        return {}
        
    batch = task.collate(examples)
    preds = method.predict(batch, spec.to_dict())
    targets = np.array([ex.target for ex in examples])
    
    metrics = {}
    for name, fn in task.metric_fns().items():
        try:
            metrics[name] = float(fn(targets, preds))
        except Exception as e:
            print(f"Error computing metric {name}: {e}")
            
    return metrics

