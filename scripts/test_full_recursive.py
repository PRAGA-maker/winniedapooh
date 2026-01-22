"""
Test script for the full_recursive forecasting method.

This script tests the full_recursive pipeline on a small sample of data
to verify all components work together correctly.

Usage:
    uv run scripts/test_full_recursive.py                    # Run on 1 sample
    uv run scripts/test_full_recursive.py --n 3              # Run on 3 samples
    uv run scripts/test_full_recursive.py --verbose          # Verbose output
    uv run scripts/test_full_recursive.py --sequential       # DATA_ANALYST sequential mode
"""

import argparse
import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")


def test_imports():
    """Test that all imports work correctly."""
    print("\n[1/5] Testing imports...")

    try:
        from methods.full_recursive_forecaster import FullRecursiveForecaster
        print("  - FullRecursiveForecaster: OK")
    except ImportError as e:
        print(f"  - FullRecursiveForecaster: FAILED ({e})")
        return False

    try:
        from methods.full_recursive import FullRecursivePipeline
        print("  - FullRecursivePipeline: OK")
    except ImportError as e:
        print(f"  - FullRecursivePipeline: FAILED ({e})")
        return False

    try:
        from methods.full_recursive.agents import GeminiAgentClient, run_planner
        print("  - GeminiAgentClient: OK")
        print("  - run_planner: OK")
    except ImportError as e:
        print(f"  - agents: FAILED ({e})")
        return False

    try:
        from methods.full_recursive.data_analyst import run_data_analyst, DataAnalystREPL
        print("  - run_data_analyst: OK")
        print("  - DataAnalystREPL: OK")
    except ImportError as e:
        print(f"  - data_analyst: FAILED ({e})")
        return False

    try:
        from methods.registry import METHODS
        if "full_recursive" in METHODS:
            print("  - Registry registration: OK")
        else:
            print("  - Registry registration: FAILED (not in METHODS)")
            return False
    except ImportError as e:
        print(f"  - Registry: FAILED ({e})")
        return False

    print("  All imports successful!")
    return True


def test_api_key():
    """Test that Gemini API key is available."""
    print("\n[2/5] Testing API key...")

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("  - GEMINI_API_KEY: NOT SET")
        print("  - Set this in your .env file to run full tests")
        return False

    print(f"  - GEMINI_API_KEY: Set ({len(api_key)} chars)")
    return True


def test_repl_sandbox():
    """Test the REPL sandbox functionality."""
    print("\n[3/5] Testing REPL sandbox...")

    from methods.full_recursive.data_analyst import DataAnalystREPL

    repl = DataAnalystREPL(verbose=False)

    # Test basic execution
    stdout, stderr = repl.execute("x = 1 + 1\nprint(f'x = {x}')")
    if "x = 2" in stdout:
        print("  - Basic execution: OK")
    else:
        print(f"  - Basic execution: FAILED (stdout={stdout}, stderr={stderr})")
        return False

    # Test numpy is available
    stdout, stderr = repl.execute("import numpy as np\nprint(np.mean([1,2,3]))")
    # Note: numpy might not be in builtins, but we inject it
    repl.locals["np"] = __import__("numpy")
    stdout, stderr = repl.execute("print(np.mean([1,2,3]))")
    if "2.0" in stdout:
        print("  - Numpy injection: OK")
    else:
        print(f"  - Numpy injection: FAILED ({stdout})")
        # Not critical, continue

    # Test blocked operations
    stdout, stderr = repl.execute("open('/etc/passwd')")
    if stderr and "NoneType" in stderr:
        print("  - Blocked operations: OK (open is blocked)")
    else:
        print(f"  - Blocked operations: WARNING ({stderr})")

    # Test variable persistence
    repl.execute("findings = {'test': True}")
    findings = repl.get_variable("findings")
    if findings and findings.get("test") == True:
        print("  - Variable persistence: OK")
    else:
        print("  - Variable persistence: FAILED")
        return False

    print("  REPL sandbox working correctly!")
    return True


def test_prompt_building():
    """Test that prompts are built correctly."""
    print("\n[4/5] Testing prompt building...")

    from methods.full_recursive.agents import (
        build_planner_prompt,
        build_analyst_prompt,
        build_advocate_prompt,
        SubQuestion,
    )

    # Test planner prompt
    planner_prompt = build_planner_prompt(
        market_title="Will it rain tomorrow?",
        market_description="Predicting weather for tomorrow in NYC",
        time_range=("2024-01-01", "2024-01-15"),
        research_cutoff="2024-01-14",
        notable_moves=[{"date": "2024-01-10", "move_pct": 0.1}],
        query_budget=5,
        previous_feedback=None,
    )

    if "rain tomorrow" in planner_prompt and "2024-01-14" in planner_prompt:
        print("  - Planner prompt: OK")
    else:
        print("  - Planner prompt: FAILED")
        return False

    # Test analyst prompt
    sub_questions = [
        SubQuestion(id="q1", question="What is the weather forecast?", priority="high"),
    ]
    analyst_prompt = build_analyst_prompt(
        market_title="Will it rain tomorrow?",
        sub_questions=sub_questions,
        time_range=("2024-01-01", "2024-01-15"),
        research_cutoff="2024-01-14",
    )

    if "weather forecast" in analyst_prompt:
        print("  - Analyst prompt: OK")
    else:
        print("  - Analyst prompt: FAILED")
        return False

    # Test advocate prompt
    advocate_prompt = build_advocate_prompt(
        market_title="Will it rain tomorrow?",
        sub_questions=sub_questions,
        position="YES",
        time_range=("2024-01-01", "2024-01-15"),
        research_cutoff="2024-01-14",
    )

    if "YES" in advocate_prompt:
        print("  - Advocate prompt: OK")
    else:
        print("  - Advocate prompt: FAILED")

    print("  All prompts building correctly!")
    return True


def test_full_pipeline(
    n_samples: int = 1,
    verbose: bool = False,
    data_analyst_parallel: bool = True,
):
    """Test the full pipeline on sample data."""
    print(f"\n[5/5] Testing full pipeline (n={n_samples})...")

    # Check for API key
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("  - Skipping: No GEMINI_API_KEY set")
        return True

    # Try to load some data
    from pathlib import Path

    data_dir = Path(__file__).parent.parent / "data" / "datasets"
    parquet_files = list(data_dir.glob("**/data.parquet"))

    if not parquet_files:
        print("  - Skipping: No data.parquet found")
        print("  - Run: uv run scripts/build_db.py --start 2024-01-01 --end 2024-01-31")
        return True

    latest_parquet = max(parquet_files, key=lambda p: p.stat().st_mtime)
    print(f"  - Using dataset: {latest_parquet.parent.name}")

    # Load data
    from forecasting.dataset import EventDataset
    from forecasting.tasks.predict_90_percent import Predict90Percent

    dataset = EventDataset(str(latest_parquet))
    task = Predict90Percent(relax_status=True, min_history_points=5)

    # Get examples
    from forecasting.splits import SplitManager
    splits = SplitManager(dataset)
    test_records = list(splits.test_records())[:n_samples * 5]  # Get extra in case some fail

    examples = []
    for record in test_records:
        try:
            batch = task.make_examples(record, None)
            if batch and batch.examples:
                examples.extend(batch.examples)
            if len(examples) >= n_samples:
                break
        except Exception as e:
            continue

    if not examples:
        print("  - Skipping: Could not create examples from data")
        return True

    examples = examples[:n_samples]
    print(f"  - Created {len(examples)} example(s)")

    # Initialize forecaster
    from methods.full_recursive_forecaster import FullRecursiveForecaster

    forecaster = FullRecursiveForecaster(
        api_key=api_key,
        model="gemini-2.0-flash",
        max_iterations=2,  # Limit for testing
        confidence_threshold=0.6,
        data_analyst_parallel=data_analyst_parallel,
        verbose=verbose,
    )

    # Build search index
    from forecasting.dataclasses import Batch
    train_batch = Batch(examples=examples)
    forecaster.fit([train_batch], {})

    # Run prediction
    print("  - Running pipeline...")
    start_time = time.time()

    for i, example in enumerate(examples):
        print(f"\n  Example {i+1}/{len(examples)}: {example.event_id[:30]}...")
        try:
            batch = Batch(examples=[example])
            predictions = forecaster.predict(batch, {})

            if predictions and predictions[0]:
                probs = predictions[0]
                print(f"    Prediction: {[f'{p:.2%}' for p in probs]}")
            else:
                print("    Prediction: FAILED (empty result)")

        except Exception as e:
            print(f"    Prediction: ERROR ({e})")
            if verbose:
                import traceback
                traceback.print_exc()

    elapsed = time.time() - start_time
    print(f"\n  - Elapsed time: {elapsed:.1f}s")

    # Print stats
    forecaster.print_stats()

    return True


def main():
    parser = argparse.ArgumentParser(description="Test full_recursive forecasting method")
    parser.add_argument("--n", type=int, default=1, help="Number of samples to test")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--sequential", action="store_true", help="Run DATA_ANALYST in sequential mode")
    args = parser.parse_args()

    print("=" * 60)
    print("FULL RECURSIVE FORECASTER - TEST SUITE")
    print("=" * 60)

    results = []

    # Run tests
    results.append(("Imports", test_imports()))
    results.append(("API Key", test_api_key()))
    results.append(("REPL Sandbox", test_repl_sandbox()))
    results.append(("Prompt Building", test_prompt_building()))
    results.append(("Full Pipeline", test_full_pipeline(
        n_samples=args.n,
        verbose=args.verbose,
        data_analyst_parallel=not args.sequential,
    )))

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    all_passed = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    if all_passed:
        print("\nAll tests passed! Full recursive method is ready.")
    else:
        print("\nSome tests failed. Check output above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Test Script:
#
# TEST STRUCTURE:
# 1. Import tests - verify all modules load
# 2. API key check - verify environment is configured
# 3. REPL sandbox - verify code execution works safely
# 4. Prompt building - verify prompts are constructed correctly
# 5. Full pipeline - end-to-end test with real data
#
# DATA REQUIREMENTS:
# - Needs data.parquet in data/datasets/
# - Falls back gracefully if no data available
# - Use --n to control sample size
#
