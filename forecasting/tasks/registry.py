from typing import Dict, Any, Type
from forecasting.tasks.base import Task
from forecasting.tasks.resolve_binary import ResolveEventTask
from forecasting.tasks.predict_week_out import PredictWeekOutTask
from forecasting.tasks.predict_90_percent import Predict90PercentTask

TASKS: Dict[str, Type[Task]] = {
    "resolve_event": ResolveEventTask,
    "predict_week_out": PredictWeekOutTask,
    "predict_90_percent": Predict90PercentTask,
}

def build_task(name: str, params: Dict[str, Any]) -> Task:
    if name not in TASKS:
        raise ValueError(f"Unknown task: {name}")
    return TASKS[name](**params)
