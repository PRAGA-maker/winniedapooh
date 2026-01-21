"""
Debug script to observe RLM behavior on n=3 examples.
Purpose: Verify RLM is working correctly before large-scale evaluation.

Usage:
    uv run scripts/debug_rlm.py
    uv run scripts/debug_rlm.py --n 5 --task predict_90_percent
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import random
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from forecasting.dataset import EventDataset
from forecasting.splits import SplitManager
from forecasting.tasks.registry import build_task
from forecasting.dataclasses import Example, Batch
from methods.rlm_forecaster import RLMForecaster


def print_example_summary(ex: Example, idx: int):
    """Print a human-readable summary of an example."""
    print(f"\n{'='*60}")
    print(f"EXAMPLE {idx+1}")
    print(f"{'='*60}")
    print(f"Event ID: {ex.event_id}")
    print(f"Source: {ex.source}")
    print(f"Title: {ex.static_features.get('title', 'N/A')[:80]}")
    print(f"Cutoff: {ex.cutoff_ts}")
    print(f"End Time: {ex.static_features.get('end_time', 'N/A')}")
    print(f"Options: {len(ex.options)}")

    for i, opt in enumerate(ex.options):
        last_belief = opt.history_belief[-1] if opt.history_belief else None
        lb_str = f"{last_belief:.3f}" if last_belief is not None else "N/A"
        print(f"  [{i+1}] {opt.title[:40]}: last_belief={lb_str}, "
              f"history_len={len(opt.history_belief)}")

    print(f"Target: {[f'{t:.3f}' for t in ex.target]}")


def debug_rlm(n: int = 3, task_name: str = "predict_90_percent", verbose: bool = True):
    """
    Run RLM on n examples with full verbose output.

    This helps us observe:
    1. What prompts are sent to Gemini
    2. What responses come back
    3. Whether tools are being used
    4. Whether the REPL loop is being exercised
    """
    # Load dataset
    data_dir = Path("data/datasets")
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if not datasets:
        print("ERROR: No dataset found. Run 'uv run runner/runner.py' first.")
        return

    dataset_path = datasets[-1]
    print(f"Using dataset: {dataset_path}")

    dataset = EventDataset.load(str(dataset_path))
    splits = SplitManager.build(dataset, seed=42)

    # Build task and get examples
    task = build_task(task_name, {})

    rng = random.Random(42)
    train_view = splits.view("train")

    examples: List[Example] = []
    for record in train_view.records():
        ex_list = task.make_examples(record, rng)
        examples.extend(ex_list)
        if len(examples) >= n:
            break

    examples = examples[:n]
    print(f"\nCollected {len(examples)} examples for debugging")

    # Print example summaries
    for i, ex in enumerate(examples):
        print_example_summary(ex, i)

    # Initialize RLM with verbose=True
    print(f"\n{'='*60}")
    print("INITIALIZING RLM FORECASTER")
    print(f"{'='*60}")

    # Set budget based on n
    call_budget = max(100, n * 5)  # ~5 calls per example

    rlm = RLMForecaster(
        model="gemini-2.0-flash",
        max_recursions=3,
        call_budget=call_budget,
        verbose=verbose
    )

    # Build search index from examples
    batch = Batch(examples=examples)
    rlm.fit([batch], {})

    # Run predictions one by one with detailed logging
    print(f"\n{'='*60}")
    print("RUNNING PREDICTIONS")
    print(f"{'='*60}")

    predictions = []
    for i, ex in enumerate(examples):
        print(f"\n{'='*60}")
        print(f"PREDICTING EXAMPLE {i+1}/{len(examples)}: {ex.event_id}")
        print(f"{'='*60}")

        # Get the context prompt that will be sent
        context = rlm._build_context_prompt(ex)
        print(f"\n--- CONTEXT PROMPT (first 1500 chars) ---")
        print(context[:1500])
        print("...")

        # Make prediction
        single_batch = Batch(examples=[ex])
        pred = rlm.predict(single_batch, {})[0]
        predictions.append(pred)

        print(f"\n--- PREDICTION ---")
        print(f"Predicted: {[f'{p:.3f}' for p in pred]}")
        print(f"Target:    {[f'{t:.3f}' for t in ex.target]}")

        # Calculate per-example Brier
        brier = sum((t - p) ** 2 for t, p in zip(ex.target, pred))
        print(f"Example Brier: {brier:.6f}")

        # Last price baseline for comparison
        last_price = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in ex.options]
        total = sum(last_price)
        last_price_norm = [v/total if total > 0 else 1/len(last_price) for v in last_price]
        last_price_brier = sum((t - p) ** 2 for t, p in zip(ex.target, last_price_norm))
        print(f"Last Price Baseline: {[f'{p:.3f}' for p in last_price_norm]}")
        print(f"Last Price Brier:    {last_price_brier:.6f}")

    # Print session stats
    print(f"\n{'='*60}")
    print("SESSION STATISTICS")
    print(f"{'='*60}")
    rlm.print_stats()

    # Final comparison
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    total_brier_rlm = 0
    total_brier_baseline = 0

    for i, (ex, pred) in enumerate(zip(examples, predictions)):
        brier_rlm = sum((t - p) ** 2 for t, p in zip(ex.target, pred))

        last_price = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in ex.options]
        total = sum(last_price)
        last_price_norm = [v/total if total > 0 else 1/len(last_price) for v in last_price]
        brier_baseline = sum((t - p) ** 2 for t, p in zip(ex.target, last_price_norm))

        total_brier_rlm += brier_rlm
        total_brier_baseline += brier_baseline

        print(f"Ex {i+1}: RLM Brier={brier_rlm:.6f}, Baseline Brier={brier_baseline:.6f}, "
              f"{'RLM WINS' if brier_rlm < brier_baseline else 'BASELINE WINS'}")

    avg_brier_rlm = total_brier_rlm / len(examples)
    avg_brier_baseline = total_brier_baseline / len(examples)

    print(f"\nAVERAGE BRIER:")
    print(f"  RLM:      {avg_brier_rlm:.6f}")
    print(f"  Baseline: {avg_brier_baseline:.6f}")
    if avg_brier_baseline > 0:
        print(f"  Ratio:    {avg_brier_rlm / avg_brier_baseline:.2f}x {'worse' if avg_brier_rlm > avg_brier_baseline else 'better'}")
    else:
        print(f"  NOTE: Baseline Brier is ~0 - target equals last price (task is trivial)")

    stats = rlm.get_usage_stats()
    print(f"\nTOOL USAGE:")
    print(f"  Search calls:    {stats['session']['total_tool_calls']['search']}")
    print(f"  Trend calls:     {stats['session']['total_tool_calls']['trend']}")
    print(f"  Base rate calls: {stats['session']['total_tool_calls']['base_rate']}")
    print(f"  Avg recursions:  {stats['session']['avg_recursions']:.2f}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Debug RLM on small sample")
    parser.add_argument("--n", type=int, default=3, help="Number of examples")
    parser.add_argument("--task", type=str, default="resolve_event", help="Task name (resolve_event for real target)")
    args = parser.parse_args()

    debug_rlm(n=args.n, task_name=args.task)


# --- LESSONS LEARNED ---
# 2026-01-21 Debug Session Findings:
#
# ISSUE 1: TASK DESIGN FLAW (Critical)
# - predict_90_percent uses raw_belief[cutoff_idx] as target (next timestep's price)
# - Since prices evolve slowly, last_price ≈ target → Brier ≈ 0 for baseline
# - This makes the task trivially solvable without any reasoning
# - SOLUTION: Use resolve_event task (target = actual resolution, one-hot)
#
# ISSUE 2: RLM NOT USING TOOLS
# - Model outputs valid JSON on iteration 1 (avg_recursions = 1.0)
# - Never triggers SEARCH/TREND/BASE_RATE keywords
# - The REPL pattern is completely bypassed
# - ROOT CAUSE: Prompt says tools are optional; LLM takes the shortcut
# - SOLUTION: Force at least one tool call before allowing final answer
#
# ISSUE 3: WRONG METRIC INTERPRETATION
# - Brier = 0.00004 for last_price seemed suspicious → now confirmed as task issue
# - The metric is correct; the task is too easy
#
# ISSUE 4: DEPRECATED API
# - google.generativeai is deprecated; should use google.genai
#
# NEXT STEPS:
# 1. Fix RLM prompt to require tool usage before answering
# 2. Use resolve_event task for proper evaluation
# 3. Create configurable cutoff task for ablation studies (25/50/75/90%)
# 4. Migrate to google.genai package
#
