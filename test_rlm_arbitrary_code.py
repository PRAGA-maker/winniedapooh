"""Test RLM with arbitrary code execution (n=1)."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from methods.rlm_forecaster import RLMForecaster
from forecasting.dataclasses import Batch
from forecasting.dataset import EventDataset
from forecasting.tasks.predict_90_percent import Predict90PercentTask
import os
from dotenv import load_dotenv

load_dotenv()

print("="*60)
print("RLM ARBITRARY CODE EXECUTION TEST (n=1)")
print("="*60)

# Find latest dataset
import glob
datasets = glob.glob("data/datasets/*/data.parquet")
if not datasets:
    print("ERROR: No datasets found")
    sys.exit(1)

dataset_path = max(datasets, key=os.path.getmtime)
print(f"\nDataset: {dataset_path}")

# Load dataset
dataset = EventDataset.load(dataset_path)
print(f"Total records: {len(dataset.df)}")

# Get one example
task = Predict90PercentTask()
record = dataset.df.iloc[0]
print(f"\nTest market: {record['event_id']}")
print(f"Source: {record['source']}")
print(f"Title: {record['title'][:100]}")

# Create batch
from forecasting.dataset import EventRecordWrapper
wrapped_record = EventRecordWrapper(record)
examples = task.make_examples(wrapped_record, rng=None)
if not examples:
    print("ERROR: Failed to create examples")
    sys.exit(1)

example = examples[0]
batch = Batch(examples=examples)
print(f"Options: {example.option_count}")
print(f"Cutoff: {example.cutoff_ts}")

# Initialize RLM with parquet path
print("\n" + "="*60)
print("Initializing RLM Forecaster")
print("="*60)

rlm = RLMForecaster(
    model='gemini-3-flash',  # Using gemini-3-flash as requested
    max_iterations=5,
    call_budget=20,
    verbose=False,  # Disable verbose to avoid Windows Unicode issues
    use_repl=True,
    diagnostic_mode=True,
    parquet_path=dataset_path,  # Pass parquet path for arbitrary code!
)

# Fit (builds search index)
print("Building search index...")
rlm.fit([batch], {})

# Run prediction
print("\n" + "="*60)
print("Running Prediction (watch the model's reasoning!)")
print("="*60 + "\n")

try:
    pred, stats = rlm._predict_with_repl(example)

    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"Prediction: {[f'{p:.3f}' for p in pred]}")
    print(f"Target: {[f'{t:.3f}' for t in example.target]}")
    print(f"\nStats:")
    print(f"  Fallback used: {stats.fallback_used}")
    print(f"  API calls: {stats.api_calls}")
    print(f"  Code blocks executed: {stats.code_blocks_executed}")
    print(f"  Iterations: {stats.iterations_used}")
    print(f"  Execution time: {stats.execution_time:.2f}s")

    if stats.raw_response:
        print(f"\nRaw response (first 500 chars):")
        print(stats.raw_response[:500])

    # Check diagnostic log
    log_path = rlm._diagnostics.get_log_path()
    if log_path and Path(log_path).exists():
        print(f"\nDiagnostic log: {log_path}")
        with open(log_path, 'r') as f:
            log_content = f.read()
        print(f"Log length: {len(log_content)} chars")

except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*60)
print("Test complete!")
print("="*60)
