import random
import numpy as np
from typing import Any, Dict
from dataobject.dataset import DatasetView
from dataobject.tasks.base import Task
from methods.base import ForecastMethod
from runner.experiment import RunSpec

def evaluate(method: ForecastMethod, view: DatasetView, task: Task, spec: RunSpec) -> Dict[str, float]:
    rng = random.Random(spec.seed)
    examples = []
    for record in view.records():
        examples.extend(task.make_examples(record, rng))
        
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

