import sys
import random
import time
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from dataobject.dataset import MarketDataset
from dataobject.splits import SplitManager
from dataobject.tasks.resolve_binary import ResolveBinaryTask
from methods.registry import build_method
from runner.experiment import RunSpec
from runner.result_store import ResultStore
from runner.evaluator import evaluate

TASK_REGISTRY = {
    "resolve_binary": ResolveBinaryTask
}

def run_experiment(spec: RunSpec):
    print(f"Starting run: {spec.run_name}")
    
    dataset = MarketDataset.load(spec.dataset_path)
    if spec.split_path:
        splits = SplitManager.load(dataset, Path(spec.split_path))
    else:
        splits = SplitManager.build(dataset, seed=spec.seed)
        
    task = TASK_REGISTRY[spec.task](**spec.task_params)
    method = build_method(spec.method, spec.method_params)
    
    # Training (if applicable)
    train_view = splits.view("train")
    # For baseline, we skip fit or just call it
    # method.fit(...)
    
    # Evaluation
    test_view = splits.view("test")
    bench_view = splits.view("bench")
    
    test_metrics = evaluate(method, test_view, task, spec)
    bench_metrics = evaluate(method, bench_view, task, spec)
    
    run_id = f"{spec.run_name}_{int(time.time())}"
    ResultStore.write(run_id, spec, test_metrics, bench_metrics)
    
    print("Run complete.")

if __name__ == "__main__":
    # Example execution
    data_dir = Path("data/datasets")
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if not datasets:
        print("No datasets found. Run src/build_unified_parquet.py first.")
        sys.exit(1)
        
    latest_ds = str(datasets[-1])
    
    spec = RunSpec(
        run_name="baseline_run",
        dataset_path=latest_ds,
        split_path=None,
        method="last_price",
        method_params={},
        task="resolve_binary",
        task_params={},
        seed=42,
        train_view={"split": "train"},
        test_view={"split": "test"},
        bench_view={"split": "bench"}
    )
    
    run_experiment(spec)

# --- LESSONS LEARNED ---
# 1. Reproducibility: Hash-based splitting on market_id ensures the same markets 
#    end up in the same splits across runs, even if the dataset grows.
# 2. Experiment Specs: Keeping the RunSpec serializable allows for auditing and diffing later.

