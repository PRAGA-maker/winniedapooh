from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class RunSpec:
    run_name: str
    dataset_path: str
    split_path: Optional[str]
    method: str
    method_params: Dict[str, Any]
    task: str
    task_params: Dict[str, Any]
    seed: int
    train_view: Dict[str, Any]
    test_view: Dict[str, Any]
    bench_view: Dict[str, Any]
    description: str = ""

    def to_dict(self):
        return asdict(self)

