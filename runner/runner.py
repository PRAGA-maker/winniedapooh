import sys
import random
import time
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, List

# Add project root to sys.path at the beginning to avoid package shadowing
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataobject.dataset import MarketDataset
from dataobject.splits import SplitManager
from dataobject.tasks.resolve_binary import ResolveBinaryTask
from dataobject.tasks.predict_week_out import PredictWeekOutTask
from methods.registry import build_method
from runner.experiment import RunSpec
from runner.evaluator import evaluate

TASK_REGISTRY = {
    "resolve_binary": ResolveBinaryTask,
    "predict_week_out": PredictWeekOutTask
}

def get_run_dir(spec: RunSpec) -> Path:
    # Create a unique hash for the model configuration
    params_str = json.dumps(spec.method_params, sort_keys=True)
    config_hash = hashlib.md5(params_str.encode()).hexdigest()[:8]
    
    model_folder_name = f"{spec.method}_{config_hash}"
    timestamp = int(time.time())
    run_id = f"run_{timestamp}_{spec.run_name}"
    
    base_dir = Path("data/outputs") / model_folder_name / run_id
    return base_dir

def run_experiment(spec: RunSpec):
    print(f"Starting run: {spec.run_name}")
    
    # Setup directories
    run_dir = get_run_dir(spec)
    model_dir = run_dir / "model"
    logs_dir = run_dir / "logs"
    plots_dir = run_dir / "plots"
    metrics_dir = run_dir / "metrics"
    
    for d in [model_dir, logs_dir, plots_dir, metrics_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Create info.txt
    timestamp_str = time.strftime("%Y%m%d_%H%M%S")
    info_file = run_dir / f"{run_dir.parent.name}_{timestamp_str}.txt"
    with open(info_file, "w") as f:
        f.write(f"Model: {spec.method}\n")
        f.write(f"Run Name: {spec.run_name}\n")
        f.write(f"Timestamp: {timestamp_str}\n")
        f.write(f"Description: {spec.description}\n")
        f.write(f"Method Params: {json.dumps(spec.method_params, indent=2)}\n")

    dataset = MarketDataset.load(spec.dataset_path)
    if spec.split_path:
        splits = SplitManager.load(dataset, Path(spec.split_path))
    else:
        splits = SplitManager.build(dataset, seed=spec.seed)
        
    task = TASK_REGISTRY[spec.task](**spec.task_params)
    method = build_method(spec.method, spec.method_params)
    
    # 1. Training Phase
    train_view = splits.view("train")
    print(f"Training on {len(train_view.df)} records from 'train' split...")
    
    rng = random.Random(spec.seed)
    train_examples = []
    for record in train_view.records():
        train_examples.extend(task.make_examples(record, rng))
    
    if train_examples:
        train_batch = task.collate(train_examples)
        method.fit([train_batch], spec.to_dict())
        
        # Save model
        model_path = model_dir / "model.joblib"
        method.save(str(model_path))
    
    # 2. Evaluation Phase
    test_view = splits.view("test")
    bench_view = splits.view("bench")
    
    print(f"Evaluating on 'test' split ({len(test_view.df)} records)...")
    test_metrics = evaluate(method, test_view, task, spec)
    
    print(f"Evaluating on 'bench' split ({len(bench_view.df)} records)...")
    bench_metrics = evaluate(method, bench_view, task, spec)
    
    # 3. Store Results
    with open(metrics_dir / "spec.json", "w") as f:
        json.dump(spec.to_dict(), f, indent=2)
    
    metrics = {
        "test": test_metrics,
        "bench": bench_metrics
    }
    with open(metrics_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    
    # Log to file
    log_file = logs_dir / "run.log"
    with open(log_file, "w") as f:
        f.write(f"Run ID: {run_dir.name}\n")
        f.write(f"Spec: {json.dumps(spec.to_dict(), indent=2)}\n")
        f.write(f"Test Metrics: {json.dumps(test_metrics, indent=2)}\n")
        f.write(f"Bench Metrics: {json.dumps(bench_metrics, indent=2)}\n")
    
    print(f"Run complete. Results saved to {run_dir}")
    return test_metrics, bench_metrics, run_dir

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Winnie Da Pooh Experiment Runner")
    parser.add_argument("--name", type=str, default="experiment", help="Run name")
    parser.add_argument("--method", type=str, default="last_price", help="Forecasting method to use")
    parser.add_argument("--method-params", type=str, default="{}", help="JSON string of method parameters")
    parser.add_argument("--task", type=str, default="resolve_binary", help="Task to evaluate on")
    parser.add_argument("--task-params", type=str, default="{}", help="JSON string of task parameters")
    parser.add_argument("--dataset", type=str, help="Path to unified parquet dataset (defaults to latest)")
    parser.add_argument("--split", type=str, help="Path to splits directory (optional)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--desc", type=str, default="", help="Description of the experiment")
    
    args = parser.parse_args()

    # 1. Resolve dataset path
    if args.dataset:
        dataset_path = args.dataset
    else:
        data_dir = Path("data/datasets")
        datasets = sorted(list(data_dir.glob("v*_unified")))
        if not datasets:
            print("No datasets found. Run scripts/build_db.py first.")
            sys.exit(1)
        dataset_path = str(datasets[-1])
        print(f"Using latest dataset: {dataset_path}")

    # 2. Parse JSON params
    try:
        method_params = json.loads(args.method_params)
        task_params = json.loads(args.task_params)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON parameters: {e}")
        sys.exit(1)

    # 3. Create RunSpec
    spec = RunSpec(
        run_name=args.name,
        dataset_path=dataset_path,
        split_path=args.split,
        method=args.method,
        method_params=method_params,
        task=args.task,
        task_params=task_params,
        seed=args.seed,
        train_view={"split": "train"},
        test_view={"split": "test"},
        bench_view={"split": "bench"},
        description=args.desc
    )
    
    # 4. Run Experiment
    run_experiment(spec)

if __name__ == "__main__":
    main()
