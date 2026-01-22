"""
Analyze RLM ablation experiment results.

Compares:
1. RLM (with market prices)
2. RLM-no-market (without market prices)
3. Last price baseline

Interpretation matrix:
- If rlm-no-market ≈ random (0.25): Model copying crowd prices (no edge)
- If rlm-no-market ≈ rlm: Model reasoning independently
- If rlm-no-market < rlm < baseline: Model adds value, crowd helps
- If rlm-no-market > baseline: REAL EDGE (beats baseline without prices!)
"""
import json
from pathlib import Path
import numpy as np
from scipy import stats

def load_run_results(run_dir):
    """Load metrics from a run directory."""
    metrics_file = Path(run_dir) / "metrics" / "metrics.json"
    if not metrics_file.exists():
        return None

    with open(metrics_file) as f:
        return json.load(f)

def find_latest_run(pattern):
    """Find most recent run matching pattern."""
    runs = sorted(Path("data/outputs").glob(f"*/{pattern}"))
    return runs[-1] if runs else None

# Find runs
rlm_run = find_latest_run("run_*rlm_ablation_pilot")
rlm_no_market_run = find_latest_run("run_*rlm_no_market_ablation_pilot")
baseline_run = find_latest_run("run_*baseline_ablation_pilot")

print("=" * 80)
print("RLM ABLATION EXPERIMENT RESULTS")
print("=" * 80)

# Load results
results = {}
for name, run_dir in [
    ("RLM (with prices)", rlm_run),
    ("RLM-no-market", rlm_no_market_run),
    ("Baseline (last_price)", baseline_run)
]:
    if run_dir:
        metrics = load_run_results(run_dir)
        if metrics:
            results[name] = metrics
            print(f"\n{name}:")
            print(f"  Run: {run_dir}")
            print(f"  Test Brier: {metrics['test']['brier']:.4f}")
            print(f"  Test LogLoss: {metrics['test']['logloss']:.4f}")
        else:
            print(f"\n{name}: Run found but no metrics yet")
    else:
        print(f"\n{name}: No run found")

if len(results) < 3:
    print("\n\nWaiting for all runs to complete...")
    exit(0)

# Analysis
print("\n" + "=" * 80)
print("ANALYSIS")
print("=" * 80)

rlm_brier = results["RLM (with prices)"]["test"]["brier"]
rlm_no_market_brier = results["RLM-no-market"]["test"]["brier"]
baseline_brier = results["Baseline (last_price)"]["test"]["brier"]
random_brier = 0.25  # Expected Brier for random predictions

print(f"\nBrier Score Comparison:")
print(f"  Random baseline:      {random_brier:.4f}")
print(f"  Last price baseline:  {baseline_brier:.4f}")
print(f"  RLM (with prices):    {rlm_brier:.4f}")
print(f"  RLM-no-market:        {rlm_no_market_brier:.4f}")

print(f"\nDifferences:")
print(f"  RLM vs Baseline:              {rlm_brier - baseline_brier:+.4f}")
print(f"  RLM-no-market vs Baseline:    {rlm_no_market_brier - baseline_brier:+.4f}")
print(f"  RLM vs RLM-no-market:         {rlm_brier - rlm_no_market_brier:+.4f}")

# Interpretation
print("\n" + "=" * 80)
print("INTERPRETATION")
print("=" * 80)

if abs(rlm_no_market_brier - random_brier) < 0.05:
    print("\n🔴 RLM-no-market ≈ random")
    print("   → Model was just copying crowd prices (no genuine edge)")
elif abs(rlm_no_market_brier - rlm_brier) < 0.02:
    print("\n🟢 RLM-no-market ≈ RLM")
    print("   → Model reasoning independently! Not relying on crowd wisdom")
elif rlm_no_market_brier > baseline_brier:
    print("\n🔴 RLM-no-market worse than baseline")
    print("   → Model struggles without crowd prices")
elif rlm_no_market_brier < baseline_brier and rlm_brier < rlm_no_market_brier:
    print("\n🟡 RLM-no-market between baseline and RLM")
    print("   → Model adds value, but crowd wisdom helps")
elif rlm_no_market_brier < baseline_brier:
    print("\n🟢🟢 RLM-no-market BEATS baseline!")
    print("   → MODEL HAS REAL EDGE! Genuine predictive power without crowd")

# Statistical significance (if we had per-example data)
print("\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)
print("\n1. Expand to n=30-100 for statistical significance")
print("2. Compute confidence intervals and p-values")
print("3. Analyze by market type / difficulty")
print("4. Check fallback rates and API usage")
print("5. Update RLM_HANDOFF.md with findings")

print("\n" + "=" * 80)
