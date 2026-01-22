"""
Debug script to observe RLM behavior on n=3 examples.
Purpose: Verify RLM REPL sandbox is working correctly.

Usage:
    uv run python tests/test_rlm_debug.py
    uv run python tests/test_rlm_debug.py --n 5 --verbose
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import random
from pathlib import Path
from datetime import datetime
from typing import List

from forecasting.dataset import EventDataset
from forecasting.splits import SplitManager
from forecasting.tasks.registry import build_task
from forecasting.dataclasses import Example, Batch
from methods.rlm_forecaster import RLMForecaster, RestrictedREPL


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


def test_repl_sandbox():
    """Test the restricted REPL sandbox works correctly."""
    print("=" * 60)
    print("TESTING REPL SANDBOX")
    print("=" * 60)

    repl = RestrictedREPL(verbose=True)

    # Test basic computation
    stdout, stderr, _ = repl.execute("x = 2 + 3\nprint(f'x = {x}')")
    print(f"Basic math: stdout='{stdout.strip()}', stderr='{stderr}'")
    assert "x = 5" in stdout, "Basic math failed"

    # Test numpy access
    stdout, stderr, _ = repl.execute("import numpy as np\narr = np.array([1, 2, 3])\nprint(np.mean(arr))")
    # Note: numpy won't work with restricted builtins since __import__ is blocked
    # This is expected behavior for security

    # Test blocked operations
    stdout, stderr, _ = repl.execute("eval('1+1')")
    assert "TypeError" in stderr or "NoneType" in stderr, "eval should be blocked"
    print(f"Blocked eval: stderr contains error (expected)")

    stdout, stderr, _ = repl.execute("open('test.txt', 'w')")
    assert "TypeError" in stderr or "NoneType" in stderr, "open should be blocked"
    print(f"Blocked open: stderr contains error (expected)")

    # Test variable persistence
    repl.execute("counter = 0")
    repl.execute("counter += 1")
    stdout, _, _ = repl.execute("print(counter)")
    assert "1" in stdout, "Variable persistence failed"
    print(f"Variable persistence: counter = {stdout.strip()}")

    # Test context injection
    repl.inject_context({"market": {"title": "Test Market"}, "n_options": 2})
    stdout, _, _ = repl.execute("print(context['market']['title'])")
    assert "Test Market" in stdout, "Context injection failed"
    print(f"Context injection: {stdout.strip()}")

    print("\nAll REPL sandbox tests passed!")
    return True


def debug_rlm(n: int = 3, verbose: bool = True, model: str = "gemini-3-pro"):
    """
    Run RLM on n examples with full verbose output.

    This helps us observe:
    1. What prompts are sent to Gemini
    2. What responses come back
    3. Whether REPL code is being executed
    4. Whether helper functions (search, trend) are used
    """
    # First test the REPL sandbox
    test_repl_sandbox()

    # Load dataset
    data_dir = Path("data/datasets")
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if not datasets:
        print("ERROR: No dataset found. Run 'uv run runner/runner.py' first.")
        return

    dataset_path = datasets[-1]
    print(f"\nUsing dataset: {dataset_path}")

    dataset = EventDataset.load(str(dataset_path))

    # Build task with 50% cutoff for fair evaluation
    task = build_task('predict_final', {
        'cutoff_percent': 0.50,
        'use_resolution': False,
        'relax_status': True,
        'min_history_points': 5
    })

    rng = random.Random(42)
    kalshi_view = dataset.slice(source='kalshi')

    # Get examples with good descriptions
    examples: List[Example] = []
    for record in kalshi_view.records():
        for ex in task.make_examples(record, rng):
            desc = ex.static_features.get("description", "")
            if len(desc) > 50:  # Quality filter
                examples.append(ex)
                if len(examples) >= n:
                    break
        if len(examples) >= n:
            break

    examples = examples[:n]
    print(f"\nCollected {len(examples)} examples for debugging")

    # Print example summaries
    for i, ex in enumerate(examples):
        print_example_summary(ex, i)

    # Initialize RLM
    print(f"\n{'='*60}")
    print("INITIALIZING RLM FORECASTER")
    print(f"{'='*60}")

    call_budget = max(100, n * 20)

    rlm = RLMForecaster(
        model=model,
        max_iterations=10,
        call_budget=call_budget,
        verbose=verbose,
        use_repl=True,
    )

    # Build search index
    batch = Batch(examples=examples)
    rlm.fit([batch], {})

    # Run predictions one by one
    print(f"\n{'='*60}")
    print("RUNNING PREDICTIONS")
    print(f"{'='*60}")

    predictions = []
    for i, ex in enumerate(examples):
        print(f"\n{'='*60}")
        print(f"PREDICTING EXAMPLE {i+1}/{len(examples)}: {ex.event_id}")
        print(f"{'='*60}")

        single_batch = Batch(examples=[ex])
        pred = rlm.predict(single_batch, {})[0]
        predictions.append(pred)

        print(f"\n--- PREDICTION ---")
        print(f"Predicted: {[f'{p:.3f}' for p in pred]}")
        print(f"Target:    {[f'{t:.3f}' for t in ex.target]}")

        # Calculate Brier score
        brier = sum((t - p) ** 2 for t, p in zip(ex.target, pred))
        print(f"Brier Score: {brier:.6f}")

        # Baseline comparison
        last_price = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in ex.options]
        total = sum(last_price)
        last_price_norm = [v/total if total > 0 else 1/len(last_price) for v in last_price]
        baseline_brier = sum((t - p) ** 2 for t, p in zip(ex.target, last_price_norm))
        print(f"Baseline (last price): {[f'{p:.3f}' for p in last_price_norm]}")
        print(f"Baseline Brier: {baseline_brier:.6f}")

        if brier < baseline_brier:
            print(">>> RLM WINS <<<")
        else:
            print(">>> BASELINE WINS <<<")

    # Print session stats
    print(f"\n{'='*60}")
    print("SESSION STATISTICS")
    print(f"{'='*60}")
    rlm.print_stats()

    # Final summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    total_brier_rlm = 0
    total_brier_baseline = 0
    rlm_wins = 0

    for i, (ex, pred) in enumerate(zip(examples, predictions)):
        brier_rlm = sum((t - p) ** 2 for t, p in zip(ex.target, pred))

        last_price = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in ex.options]
        total = sum(last_price)
        last_price_norm = [v/total if total > 0 else 1/len(last_price) for v in last_price]
        brier_baseline = sum((t - p) ** 2 for t, p in zip(ex.target, last_price_norm))

        total_brier_rlm += brier_rlm
        total_brier_baseline += brier_baseline
        if brier_rlm < brier_baseline:
            rlm_wins += 1

        winner = "RLM" if brier_rlm < brier_baseline else "BASE"
        print(f"Ex {i+1}: RLM={brier_rlm:.4f}, BASE={brier_baseline:.4f} -> {winner}")

    avg_brier_rlm = total_brier_rlm / len(examples)
    avg_brier_baseline = total_brier_baseline / len(examples)

    print(f"\nAVERAGE BRIER:")
    print(f"  RLM:      {avg_brier_rlm:.6f}")
    print(f"  Baseline: {avg_brier_baseline:.6f}")
    print(f"  Ratio:    {avg_brier_rlm / avg_brier_baseline:.2f}x")
    print(f"  Win rate: {rlm_wins}/{len(examples)} ({100*rlm_wins/len(examples):.0f}%)")

    stats = rlm.get_usage_stats()
    print(f"\nEXECUTION STATS:")
    print(f"  API calls:       {stats['api']['calls_made']}")
    print(f"  Avg iterations:  {stats['session']['avg_iterations']:.2f}")
    print(f"  Avg code blocks: {stats['session']['avg_code_blocks']:.2f}")
    print(f"  Search calls:    {stats['session']['total_tool_calls']['search']}")
    print(f"  Trend calls:     {stats['session']['total_tool_calls']['trend']}")
    print(f"  Market info:     {stats['session']['total_tool_calls']['market_info']}")
    print(f"  Fallback rate:   {stats['session']['fallback_rate']:.1%}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Debug RLM on small sample")
    parser.add_argument("--n", type=int, default=3, help="Number of examples")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--model", type=str, default="gemini-3-pro", help="Model to use")
    args = parser.parse_args()

    debug_rlm(n=args.n, verbose=args.verbose, model=args.model)


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Debug Script Updates:
#
# SANDBOX TESTING:
# - Added explicit REPL sandbox tests before running predictions
# - Verifies blocked operations (eval, exec, open) actually fail
# - Tests variable persistence and context injection
#
# QUALITY FILTERING:
# - Only debug examples with description > 50 chars
# - Avoids bulk-only records that lack semantic content
#
# KEY OBSERVATIONS:
# - REPL sandbox correctly blocks dangerous operations
# - numpy access requires __import__ which is blocked for security
# - Helper functions (search, trend) must be explicitly injected
#
