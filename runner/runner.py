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
from runner.results_db import ResultsDatabase

TASK_REGISTRY = {
    "resolve_binary": ResolveBinaryTask,
    "predict_week_out": PredictWeekOutTask
}

def _ensure_dataset_available(data_dir: Path) -> Path:
    """
    Ensure dataset exists locally, downloading from Hugging Face if needed.
    
    Args:
        data_dir: Path to data/datasets directory
        
    Returns:
        Path to dataset directory (either existing or newly downloaded)
        
    Raises:
        SystemExit: If download fails or no dataset can be obtained
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Check for existing local datasets
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if datasets:
        return datasets[-1]
    
    # No local dataset found - download from Hugging Face
    print("No local dataset found. Downloading from Hugging Face...")
    print("Dataset: carpetxie/winniethepooh")
    
    try:
        from datasets import load_dataset
        import pandas as pd
        
        # Load dataset from Hugging Face
        print("Downloading dataset...")
        hf_dataset = load_dataset("carpetxie/winniethepooh", split="train")
        
        # Convert to pandas DataFrame
        df = hf_dataset.to_pandas()
        
        # Create versioned directory matching local build format
        from datetime import datetime
        version = datetime.now().strftime("%Y%m%d_%H%M")
        output_dir = data_dir / f"v{version}_unified"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as parquet to match local build structure
        parquet_path = output_dir / "data.parquet"
        df.to_parquet(parquet_path, index=False)
        
        # Create manifest to match local build format
        manifest = {
            "version": version,
            "dataset_name": "unified",
            "created_at": datetime.now().isoformat(),
            "row_count": len(df),
            "source": "huggingface"
        }
        manifest_path = output_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        
        print(f"Dataset downloaded to: {output_dir}")
        print(f"Loaded {len(df)} rows")
        
        return output_dir
        
    except Exception as e:
        print(f"Failed to download dataset from Hugging Face: {e}")
        print("Please run scripts/build_db.py to build a local dataset, or")
        print("ensure you have network access and huggingface_hub installed.")
        sys.exit(1)

def get_run_dir(spec: RunSpec) -> Path:
    """
    Generate a unique output directory for an experiment run.
    
    Directory structure: data/outputs/{method}_{config_hash}/run_{timestamp}_{name}
    Config hash is based on method_params to group runs with same hyperparameters.
    
    Args:
        spec: RunSpec configuration
        
    Returns:
        Path to the run directory
    """
    # Create a unique hash for the model configuration
    params_str = json.dumps(spec.method_params, sort_keys=True)
    config_hash = hashlib.md5(params_str.encode()).hexdigest()[:8]
    
    model_folder_name = f"{spec.method}_{config_hash}"
    timestamp = int(time.time())
    run_id = f"run_{timestamp}_{spec.run_name}"
    
    base_dir = Path("data/outputs") / model_folder_name / run_id
    return base_dir

def run_experiment(spec: RunSpec):
    """
    Run a complete forecasting experiment.
    
    Orchestrates data loading, split management, training, evaluation, and result storage.
    Creates organized output directory with model weights, metrics, logs, and plots.
    
    Args:
        spec: RunSpec configuration specifying method, task, dataset, and parameters
        
    Returns:
        Tuple of (test_metrics, bench_metrics, run_dir)
    
    Testing Notes (2026-01):
        CLI is intuitive and works well end-to-end. Output organization is excellent
        (organized by method + config hash + run). All tested methods and tasks work correctly.
        Could add progress bars for long-running evaluations (recommendation: P2 priority).
    """
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
    from tqdm import tqdm
    for record in tqdm(train_view.records(), desc="Creating training examples", leave=False):
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
    
    # 4. Index to Results Database
    try:
        db = ResultsDatabase()
        db.index_run(run_dir, spec.to_dict(), metrics)
        print(f"Run indexed to database")
    except Exception as e:
        print(f"Warning: Failed to index run to database: {e}")
    
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
        dataset_dir = _ensure_dataset_available(data_dir)
        dataset_path = str(dataset_dir)
        print(f"Using dataset: {dataset_path}")

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
