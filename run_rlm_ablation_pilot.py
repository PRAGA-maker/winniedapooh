"""
Standalone script to run RLM ablation pilot experiment.

This bypasses CLI JSON parsing issues and runs the experiment directly in Python.
"""
import sys
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent))

from runner.experiment import RunSpec
from runner.runner import run_experiment

# Configuration
DATASET_PATH = Path("data/datasets/v20260121_0105_rlm_full_unified")
SPLIT_PATH = Path("data/splits/fast_pilot_n10")
SEED = 42

# Experiment 1: RLM with market prices
print("=" * 80)
print("EXPERIMENT 1: RLM (with market prices)")
print("=" * 80)

spec_rlm = RunSpec(
    run_name="rlm_ablation_pilot",
    method="rlm",
    method_params={
        "parquet_path": str(DATASET_PATH / "data.parquet"),
        "diagnostic_mode": True,
        "verbose": False,
    },
    task="resolve_event",
    task_params={
        "relax_status": True,
        "min_history_points": 2,
    },
    dataset_path=DATASET_PATH,
    split_path=SPLIT_PATH,
    seed=SEED,
    description="RLM ablation pilot: WITH market price history"
)

try:
    test_metrics, bench_metrics, run_dir = run_experiment(spec_rlm)
    print(f"\n✓ RLM completed successfully!")
    print(f"Test Brier: {test_metrics.get('multiclass_brier', 'N/A')}")
    print(f"Results: {run_dir}")
except Exception as e:
    print(f"\n✗ RLM failed: {e}")
    import traceback
    traceback.print_exc()

# Experiment 2: RLM-no-market (ablation)
print("\n" + "=" * 80)
print("EXPERIMENT 2: RLM-no-market (WITHOUT market prices - ablation)")
print("=" * 80)

spec_rlm_no_market = RunSpec(
    run_name="rlm_no_market_ablation_pilot",
    method="rlm-no-market",
    method_params={
        "parquet_path": str(DATASET_PATH / "data.parquet"),
        "diagnostic_mode": True,
        "verbose": False,
    },
    task="resolve_event",
    task_params={
        "relax_status": True,
        "min_history_points": 2,
    },
    dataset_path=DATASET_PATH,
    split_path=SPLIT_PATH,
    seed=SEED,
    description="RLM ablation pilot: WITHOUT market price history"
)

try:
    test_metrics, bench_metrics, run_dir = run_experiment(spec_rlm_no_market)
    print(f"\n✓ RLM-no-market completed successfully!")
    print(f"Test Brier: {test_metrics.get('multiclass_brier', 'N/A')}")
    print(f"Results: {run_dir}")
except Exception as e:
    print(f"\n✗ RLM-no-market failed: {e}")
    import traceback
    traceback.print_exc()

# Experiment 3: Baseline
print("\n" + "=" * 80)
print("EXPERIMENT 3: Last Price Baseline")
print("=" * 80)

spec_baseline = RunSpec(
    run_name="baseline_ablation_pilot",
    method="last_price",
    method_params={},
    task="resolve_event",
    task_params={
        "relax_status": True,
        "min_history_points": 2,
    },
    dataset_path=DATASET_PATH,
    split_path=SPLIT_PATH,
    seed=SEED,
    description="Baseline for RLM ablation comparison"
)

try:
    test_metrics, bench_metrics, run_dir = run_experiment(spec_baseline)
    print(f"\n✓ Baseline completed successfully!")
    print(f"Test Brier: {test_metrics.get('multiclass_brier', 'N/A')}")
    print(f"Results: {run_dir}")
except Exception as e:
    print(f"\n✗ Baseline failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("PILOT EXPERIMENT COMPLETE")
print("=" * 80)
