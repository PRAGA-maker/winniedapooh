import json
import pandas as pd
from pathlib import Path
from src.common.config import config
from runner.experiment import RunSpec

class ResultStore:
    @staticmethod
    def write(run_id: str, spec: RunSpec, test_metrics: dict, bench_metrics: dict):
        run_dir = config.repo_root / "data" / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        
        with open(run_dir / "spec.json", "w") as f:
            json.dump(spec.to_dict(), f, indent=2)
            
        metrics = {
            "test": test_metrics,
            "bench": bench_metrics
        }
        with open(run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
            
        print(f"Results written to {run_dir}")

