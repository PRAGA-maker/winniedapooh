"""
NeuralLambda Experiment 0 Sanity Test.

Purpose: Verify NeuralLambda training loop works with real market data.
This is NOT a full experiment - it's a quick check that:
1. Data loading works
2. Training loop runs without errors
3. Loss decreases (at least somewhat)
4. No NaN encountered
5. Predictions are valid probabilities

Usage:
    uv run python temp_neurallambda_sanity.py
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import random
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

import pandas as pd

from forecasting.dataclasses import Example, OptionHistory, Batch

# Import NeuralLambdaForecaster
from methods.neurallambda_forecaster import NeuralLambdaForecaster, NeuralLambdaConfig

# =============================================================================
# Data Loading (based on tests/test_rlm_debug.py pattern)
# =============================================================================

def load_examples_from_parquet(parquet_path: str, n: int = 10, seed: int = 42) -> List[Example]:
    """
    Load a small subset of binary markets from parquet.

    Args:
        parquet_path: Path to data.parquet
        n: Number of examples to load
        seed: Random seed for reproducibility

    Returns:
        List of Example objects
    """
    print(f"Loading data from: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    print(f"Total records: {len(df)}")

    # Filter for binary markets only (2 options)
    # First, let's inspect the data structure
    print(f"Columns: {df.columns.tolist()}")

    # Check for status column
    if 'status' in df.columns:
        # Filter for resolved markets
        resolved_mask = df['status'].isin(['settled', 'resolved', 'finalized', 'SETTLED'])
        df = df[resolved_mask]
        print(f"Resolved records: {len(df)}")

    # Shuffle and take n samples
    rng = random.Random(seed)
    indices = list(range(len(df)))
    rng.shuffle(indices)

    examples = []
    for idx in indices[:n * 3]:  # Try more records in case some fail
        if len(examples) >= n:
            break

        row = df.iloc[idx]

        try:
            example = _row_to_example(row)
            if example is not None and len(example.options) == 2:  # Binary only
                examples.append(example)
        except Exception as e:
            print(f"  Skip row {idx}: {e}")
            continue

    print(f"Loaded {len(examples)} binary examples")
    return examples


def _row_to_example(row) -> Example:
    """Convert a DataFrame row to an Example object."""
    import json

    event_id = str(row.get('event_id', row.get('ticker', row.name)))
    source = str(row.get('source', 'unknown'))

    # Parse options from JSON if needed
    options_data = row.get('options_json', row.get('options', '[]'))
    if isinstance(options_data, str):
        options_data = json.loads(options_data)

    if not options_data or len(options_data) < 2:
        return None

    # Get timestamps
    end_time_raw = row.get('end_time', row.get('settle_time', None))
    if end_time_raw is not None:
        if isinstance(end_time_raw, str):
            end_time = datetime.fromisoformat(end_time_raw.replace('Z', '+00:00'))
        else:
            end_time = pd.to_datetime(end_time_raw)
    else:
        end_time = datetime.now()

    # Use 50% cutoff
    create_time_raw = row.get('create_time', row.get('created_time', None))
    if create_time_raw is not None:
        if isinstance(create_time_raw, str):
            create_time = datetime.fromisoformat(create_time_raw.replace('Z', '+00:00'))
        else:
            create_time = pd.to_datetime(create_time_raw)
    else:
        create_time = end_time

    # Make sure times are datetime objects
    if hasattr(end_time, 'to_pydatetime'):
        end_time = end_time.to_pydatetime()
    if hasattr(create_time, 'to_pydatetime'):
        create_time = create_time.to_pydatetime()

    # Remove timezone if present
    if hasattr(end_time, 'replace') and end_time.tzinfo is not None:
        end_time = end_time.replace(tzinfo=None)
    if hasattr(create_time, 'replace') and create_time.tzinfo is not None:
        create_time = create_time.replace(tzinfo=None)

    duration = (end_time - create_time).total_seconds()
    cutoff_ts = create_time + pd.Timedelta(seconds=duration * 0.5)

    # Build options
    options = []
    for opt_data in options_data:
        opt_id = str(opt_data.get('option_id', opt_data.get('id', len(options))))
        opt_title = str(opt_data.get('title', opt_data.get('name', f"Option {len(options)}")))

        # Get belief history
        ts_data = opt_data.get('time_series', opt_data.get('history', []))
        if isinstance(ts_data, str):
            ts_data = json.loads(ts_data)

        history_ts = []
        history_belief = []

        for point in ts_data:
            if isinstance(point, dict):
                ts_raw = point.get('timestamp', point.get('ts', None))
                belief = point.get('belief', point.get('price', point.get('last_price', 0.5)))
            else:
                continue

            if ts_raw is not None:
                if isinstance(ts_raw, str):
                    ts = datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                else:
                    ts = pd.to_datetime(ts_raw)

                if hasattr(ts, 'to_pydatetime'):
                    ts = ts.to_pydatetime()
                if hasattr(ts, 'replace') and ts.tzinfo is not None:
                    ts = ts.replace(tzinfo=None)

                # Only include points before cutoff
                if ts <= cutoff_ts:
                    history_ts.append(ts)
                    history_belief.append(float(belief))

        if not history_belief:
            history_belief = [0.5]
            history_ts = [create_time]

        option = OptionHistory(
            option_id=opt_id,
            market_id=event_id,
            title=opt_title,
            history_ts=history_ts,
            history_belief=history_belief,
        )
        options.append(option)

    if len(options) < 2:
        return None

    # Build target (use resolution if available, else last prices)
    target = []
    for opt_data in options_data:
        # Try to get resolution
        resolution = opt_data.get('resolution', opt_data.get('result', None))
        if resolution is not None:
            target.append(1.0 if resolution in [1, True, 'Yes', 'yes'] else 0.0)
        else:
            # Fallback to last belief
            if options[len(target)].history_belief:
                target.append(options[len(target)].history_belief[-1])
            else:
                target.append(0.5)

    # Normalize target to sum to 1
    total = sum(target)
    if total > 0:
        target = [t / total for t in target]
    else:
        target = [1.0 / len(target)] * len(target)

    # Static features
    static_features = {
        'title': str(row.get('title', row.get('event_title', event_id))),
        'description': str(row.get('description', '')),
        'end_time': str(end_time),
    }

    return Example(
        event_id=event_id,
        source=source,
        cutoff_ts=cutoff_ts,
        options=options,
        static_features=static_features,
        target=target,
    )


# =============================================================================
# Sanity Check
# =============================================================================

def run_sanity_check():
    """Run the NeuralLambda sanity check."""
    print("=" * 60)
    print("NEURALLAMBDA SANITY CHECK - EXPERIMENT 0")
    print("=" * 60)

    # Parameters
    parquet_path = "data/datasets/v20260121_0105_rlm_full_unified/data.parquet"
    n_examples = 10

    # Config with conservative settings
    config = NeuralLambdaConfig(
        learning_rate=1e-5,
        warmup_steps=10,
        epochs=5,
        batch_size=2,
        gradient_clip_norm=1.0,
    )

    print(f"\nConfig:")
    print(f"  learning_rate: {config.learning_rate}")
    print(f"  warmup_steps: {config.warmup_steps}")
    print(f"  epochs: {config.epochs}")
    print(f"  batch_size: {config.batch_size}")

    # Load data
    print(f"\n{'='*60}")
    print("LOADING DATA")
    print(f"{'='*60}")

    examples = load_examples_from_parquet(parquet_path, n=n_examples)

    if len(examples) == 0:
        print("ERROR: No examples loaded. Check parquet file.")
        return False

    # Print example summaries
    print(f"\nLoaded {len(examples)} examples:")
    for i, ex in enumerate(examples):
        print(f"  [{i+1}] {ex.event_id}: {len(ex.options)} options, "
              f"target={[f'{t:.2f}' for t in ex.target]}")

    # Initialize forecaster
    print(f"\n{'='*60}")
    print("INITIALIZING FORECASTER")
    print(f"{'='*60}")

    try:
        forecaster = NeuralLambdaForecaster(config=config, verbose=True)
    except Exception as e:
        print(f"ERROR: Failed to initialize forecaster: {e}")
        return False

    # Create batches for training
    train_batches = [Batch(examples=examples)]

    # Train
    print(f"\n{'='*60}")
    print("TRAINING")
    print(f"{'='*60}")

    try:
        forecaster.fit(train_batches, {})
    except Exception as e:
        print(f"ERROR during training: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Get training stats
    stats = forecaster.get_stats()
    print(f"\nTraining Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Check for NaN
    if stats.get('nan_encountered', False):
        print("\nFAILED: NaN encountered during training")
        return False

    # Check loss decrease
    train_losses = forecaster.stats.train_losses
    if len(train_losses) >= 2:
        if train_losses[-1] < train_losses[0]:
            print(f"\nPASSED: Loss decreased ({train_losses[0]:.4f} -> {train_losses[-1]:.4f})")
        else:
            print(f"\nWARNING: Loss did not decrease ({train_losses[0]:.4f} -> {train_losses[-1]:.4f})")

    # Test predictions
    print(f"\n{'='*60}")
    print("TESTING PREDICTIONS")
    print(f"{'='*60}")

    test_batch = Batch(examples=examples[:3])

    try:
        predictions = forecaster.predict(test_batch, {})
    except Exception as e:
        print(f"ERROR during prediction: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Validate predictions
    all_valid = True
    for i, (pred, ex) in enumerate(zip(predictions, test_batch.examples)):
        # Check probabilities sum to 1
        prob_sum = sum(pred)
        is_valid = abs(prob_sum - 1.0) < 1e-4

        # Check all probs in [0, 1]
        in_range = all(0.0 <= p <= 1.0 for p in pred)

        status = "VALID" if (is_valid and in_range) else "INVALID"
        print(f"  [{i+1}] Pred: {[f'{p:.3f}' for p in pred]}, "
              f"Target: {[f'{t:.3f}' for t in ex.target]}, "
              f"Sum: {prob_sum:.4f} [{status}]")

        if not (is_valid and in_range):
            all_valid = False

    if all_valid:
        print("\nPASSED: All predictions are valid probabilities")
    else:
        print("\nFAILED: Some predictions are invalid")
        return False

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Examples loaded: {len(examples)}")
    print(f"  Epochs completed: {stats.get('epochs_completed', 0)}")
    print(f"  Final train loss: {stats.get('final_train_loss', 'N/A')}")
    print(f"  NaN encountered: {stats.get('nan_encountered', False)}")
    print(f"  Training time: {stats.get('training_time', 0):.2f}s")
    print(f"  Predictions valid: {all_valid}")

    if not stats.get('nan_encountered', False) and all_valid:
        print("\n*** SANITY CHECK PASSED ***")
        return True
    else:
        print("\n*** SANITY CHECK FAILED ***")
        return False


if __name__ == "__main__":
    success = run_sanity_check()
    sys.exit(0 if success else 1)


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-22 Sanity Check Implementation:
#
# PURPOSE:
# - Quick verification that NeuralLambda training works with real data
# - NOT a full experiment - just checks basic functionality
#
# DATA LOADING:
# - Based on tests/test_rlm_debug.py patterns
# - Filters for binary markets only (2 options)
# - Uses 50% cutoff for history
# - Handles various date formats and timezone issues
#
# CONSERVATIVE SETTINGS:
# - learning_rate=1e-5 (very conservative)
# - warmup_steps=10 (short for quick test)
# - epochs=5 (minimal)
# - batch_size=2 (small for testing)
#
# CHECKS:
# 1. Data loads correctly
# 2. Training runs without errors
# 3. Loss decreases (at least somewhat)
# 4. No NaN encountered
# 5. Predictions are valid probabilities (sum to 1, in [0,1])
#
# EXPECTED OUTPUT:
# - Should see loss decrease over 5 epochs
# - Predictions should be valid probability distributions
# - Training time ~10-30s depending on hardware
#
# IF THIS FAILS:
# - Check NeuralLambdaForecaster imports
# - Check parquet file exists and has expected columns
# - Check CUDA/GPU availability if using GPU
# - Look at traceback for specific error
#
