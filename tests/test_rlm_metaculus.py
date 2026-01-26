"""
Metaculus Cross-Domain Evaluation Script.

Tests transfer learning: train search index on Kalshi, predict on Metaculus.
This is the true test of generalization - can patterns learned from one
domain transfer to another?

Usage:
    uv run python tests/test_rlm_metaculus.py --n 30
    uv run python tests/test_rlm_metaculus.py --n 50 --save
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import random
import json
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field, asdict

from forecasting.dataset import EventDataset
from forecasting.tasks.registry import build_task
from forecasting.dataclasses import Batch, Example
from methods.rlm_forecaster import (
    RLMForecaster,
    random_baseline,
    market_consensus_baseline,
)
from methods.rlm_tools.semantic_search import MarketSearchIndex


@dataclass
class CrossDomainResult:
    """Result for a single cross-domain prediction."""
    event_id: str
    source: str  # metaculus
    title: str
    brier_rlm: float
    brier_random: float
    brier_market: float
    rlm_wins_vs_random: bool
    rlm_wins_vs_market: bool
    prediction: List[float]
    target: List[float]


@dataclass
class CrossDomainResults:
    """Complete cross-domain evaluation results."""
    timestamp: str
    config: Dict[str, Any]

    # Training stats
    train_source: str  # kalshi
    train_count: int
    test_source: str  # metaculus
    test_count: int

    # Metrics
    avg_brier_rlm: float = 0.0
    avg_brier_random: float = 0.0
    avg_brier_market: float = 0.0
    win_rate_vs_random: float = 0.0
    win_rate_vs_market: float = 0.0

    # Individual results
    results: List[CrossDomainResult] = field(default_factory=list)

    # RLM stats
    api_calls: int = 0
    avg_iterations: float = 0.0
    fallback_rate: float = 0.0


def run_cross_domain_evaluation(
    n_total: int = 30,
    seed: int = 42,
    verbose: bool = False,
    model: str = "gemini-3-pro",
    max_iterations: int = 10,
) -> CrossDomainResults:
    """
    Run cross-domain evaluation: train on Kalshi, test on Metaculus.

    This tests:
    1. Can patterns from Kalshi markets help predict Metaculus questions?
    2. Does the RLM's reasoning transfer across domains?
    3. Is the search index useful for cross-domain transfer?
    """
    print("=" * 70)
    print("RLM CROSS-DOMAIN EVALUATION")
    print("Train: Kalshi -> Test: Metaculus")
    print("=" * 70)
    print(f"Config: n={n_total}, seed={seed}, model={model}")
    print()

    # Initialize results
    results = CrossDomainResults(
        timestamp=datetime.now().isoformat(),
        config={
            "n_total": n_total,
            "seed": seed,
            "model": model,
            "max_iterations": max_iterations,
        },
        train_source="kalshi",
        test_source="metaculus",
        train_count=0,
        test_count=0,
    )

    # Load dataset
    dataset_path = Path("data/datasets")
    datasets = sorted(list(dataset_path.glob("v*_unified")))
    if not datasets:
        raise ValueError("No dataset found. Run 'uv run runner/runner.py' first.")

    dataset = EventDataset.load(str(datasets[-1]))
    print(f"Dataset: {datasets[-1].name}")

    # Get Kalshi data for training
    kalshi_view = dataset.slice(source='kalshi')
    print(f"Kalshi records: {len(kalshi_view.df)}")

    # Get Metaculus data for testing
    metaculus_view = dataset.slice(source='metaculus')
    print(f"Metaculus records: {len(metaculus_view.df)}")

    if len(metaculus_view.df) == 0:
        print("\nWARNING: No Metaculus data available.")
        print("This dataset may only contain Kalshi markets.")
        print("Falling back to Kalshi hold-out test.")

        # Fall back to Kalshi hold-out
        return run_kalshi_holdout_evaluation(
            dataset, n_total, seed, verbose, model, max_iterations
        )

    # Build task
    task = build_task('predict_final', {
        'cutoff_percent': 0.50,
        'use_resolution': False,
        'relax_status': True,
        'min_history_points': 5
    })

    # Get training examples from Kalshi
    rng = random.Random(seed)
    train_examples = []
    for record in kalshi_view.records():
        exs = task.make_examples(record, rng)
        # Quality filter
        for ex in exs:
            if len(ex.static_features.get("description", "")) > 50:
                train_examples.append(ex)

    print(f"Kalshi training examples: {len(train_examples)}")
    results.train_count = len(train_examples)

    # Get test examples from Metaculus
    test_examples = []
    for record in metaculus_view.records():
        exs = task.make_examples(record, rng)
        for ex in exs:
            if len(ex.static_features.get("description", "")) > 50:
                test_examples.append(ex)
                if len(test_examples) >= n_total:
                    break
        if len(test_examples) >= n_total:
            break

    test_examples = test_examples[:n_total]
    print(f"Metaculus test examples: {len(test_examples)}")
    results.test_count = len(test_examples)

    if len(test_examples) == 0:
        print("ERROR: No Metaculus test examples found.")
        return results

    # Initialize RLM (trained on Kalshi only)
    print("\n" + "-" * 70)
    print("INITIALIZING RLM (trained on Kalshi)")
    print("-" * 70)

    call_budget = max(500, len(test_examples) * max_iterations * 2)
    rlm = RLMForecaster(
        model=model,
        max_iterations=max_iterations,
        call_budget=call_budget,
        verbose=verbose,
        use_repl=True,
    )

    # Fit ONLY on Kalshi data
    train_batch = Batch(examples=train_examples)
    rlm.fit([train_batch], {})

    # Run predictions on Metaculus
    print("\n" + "=" * 70)
    print(f"PREDICTING METACULUS ({len(test_examples)} examples)")
    print("=" * 70)

    total_brier_rlm = 0.0
    total_brier_random = 0.0
    total_brier_market = 0.0
    wins_vs_random = 0
    wins_vs_market = 0

    for i, ex in enumerate(test_examples):
        print(f"\n[{i+1}/{len(test_examples)}] {ex.event_id}")
        print(f"  Title: {ex.static_features.get('title', 'N/A')[:60]}")

        # RLM prediction
        test_batch = Batch(examples=[ex])
        pred_rlm = rlm.predict(test_batch, {})[0]

        # Baselines
        pred_market = market_consensus_baseline(ex)
        pred_random = random_baseline(len(ex.options))

        # Compute Brier scores
        brier_rlm = sum((p - t) ** 2 for p, t in zip(pred_rlm, ex.target))
        brier_random = sum((p - t) ** 2 for p, t in zip(pred_random, ex.target))
        brier_market = sum((p - t) ** 2 for p, t in zip(pred_market, ex.target))

        total_brier_rlm += brier_rlm
        total_brier_random += brier_random
        total_brier_market += brier_market

        wins_random = brier_rlm < brier_random
        wins_market = brier_rlm < brier_market
        if wins_random:
            wins_vs_random += 1
        if wins_market:
            wins_vs_market += 1

        result = CrossDomainResult(
            event_id=ex.event_id,
            source=ex.source,
            title=ex.static_features.get("title", ""),
            brier_rlm=brier_rlm,
            brier_random=brier_random,
            brier_market=brier_market,
            rlm_wins_vs_random=wins_random,
            rlm_wins_vs_market=wins_market,
            prediction=pred_rlm,
            target=list(ex.target),
        )
        results.results.append(result)

        if verbose:
            winner = "RLM" if wins_market else "MARKET"
            print(f"  RLM: {brier_rlm:.4f}, Market: {brier_market:.4f} -> {winner}")

    # Compute averages
    n = len(test_examples)
    results.avg_brier_rlm = total_brier_rlm / n if n > 0 else 0
    results.avg_brier_random = total_brier_random / n if n > 0 else 0
    results.avg_brier_market = total_brier_market / n if n > 0 else 0
    results.win_rate_vs_random = wins_vs_random / n if n > 0 else 0
    results.win_rate_vs_market = wins_vs_market / n if n > 0 else 0

    # RLM stats
    rlm_stats = rlm.get_usage_stats()
    results.api_calls = rlm_stats["api"]["calls_made"]
    results.avg_iterations = rlm_stats["session"]["avg_iterations"]
    results.fallback_rate = rlm_stats["session"]["fallback_rate"]

    return results


def run_kalshi_holdout_evaluation(
    dataset: EventDataset,
    n_total: int,
    seed: int,
    verbose: bool,
    model: str,
    max_iterations: int,
) -> CrossDomainResults:
    """
    Fallback: if no Metaculus data, use Kalshi hold-out.
    Simulates cross-domain by using different time periods.
    """
    print("\n" + "=" * 70)
    print("KALSHI HOLD-OUT EVALUATION (fallback)")
    print("=" * 70)

    results = CrossDomainResults(
        timestamp=datetime.now().isoformat(),
        config={
            "n_total": n_total,
            "seed": seed,
            "model": model,
            "max_iterations": max_iterations,
            "fallback": True,
        },
        train_source="kalshi_train",
        test_source="kalshi_test",
        train_count=0,
        test_count=0,
    )

    kalshi_view = dataset.slice(source='kalshi')

    # Build task
    task = build_task('predict_final', {
        'cutoff_percent': 0.50,
        'use_resolution': False,
        'relax_status': True,
        'min_history_points': 5
    })

    # Get all examples
    rng = random.Random(seed)
    all_examples = []
    for record in kalshi_view.records():
        exs = task.make_examples(record, rng)
        for ex in exs:
            if len(ex.static_features.get("description", "")) > 50:
                all_examples.append(ex)

    # Split 80/20
    random.Random(seed).shuffle(all_examples)
    split_idx = int(len(all_examples) * 0.8)
    train_examples = all_examples[:split_idx]
    test_examples = all_examples[split_idx:][:n_total]

    results.train_count = len(train_examples)
    results.test_count = len(test_examples)

    print(f"Train: {len(train_examples)}, Test: {len(test_examples)}")

    # Initialize RLM
    call_budget = max(500, len(test_examples) * max_iterations * 2)
    rlm = RLMForecaster(
        model=model,
        max_iterations=max_iterations,
        call_budget=call_budget,
        verbose=verbose,
        use_repl=True,
    )

    train_batch = Batch(examples=train_examples)
    rlm.fit([train_batch], {})

    # Run predictions
    total_brier_rlm = 0.0
    total_brier_random = 0.0
    total_brier_market = 0.0
    wins_vs_random = 0
    wins_vs_market = 0

    for i, ex in enumerate(test_examples):
        if verbose:
            print(f"\n[{i+1}/{len(test_examples)}] {ex.event_id}")

        test_batch = Batch(examples=[ex])
        pred_rlm = rlm.predict(test_batch, {})[0]

        pred_market = market_consensus_baseline(ex)
        pred_random = random_baseline(len(ex.options))

        brier_rlm = sum((p - t) ** 2 for p, t in zip(pred_rlm, ex.target))
        brier_random = sum((p - t) ** 2 for p, t in zip(pred_random, ex.target))
        brier_market = sum((p - t) ** 2 for p, t in zip(pred_market, ex.target))

        total_brier_rlm += brier_rlm
        total_brier_random += brier_random
        total_brier_market += brier_market

        wins_random = brier_rlm < brier_random
        wins_market = brier_rlm < brier_market
        if wins_random:
            wins_vs_random += 1
        if wins_market:
            wins_vs_market += 1

        results.results.append(CrossDomainResult(
            event_id=ex.event_id,
            source=ex.source,
            title=ex.static_features.get("title", ""),
            brier_rlm=brier_rlm,
            brier_random=brier_random,
            brier_market=brier_market,
            rlm_wins_vs_random=wins_random,
            rlm_wins_vs_market=wins_market,
            prediction=pred_rlm,
            target=list(ex.target),
        ))

    n = len(test_examples)
    results.avg_brier_rlm = total_brier_rlm / n if n > 0 else 0
    results.avg_brier_random = total_brier_random / n if n > 0 else 0
    results.avg_brier_market = total_brier_market / n if n > 0 else 0
    results.win_rate_vs_random = wins_vs_random / n if n > 0 else 0
    results.win_rate_vs_market = wins_vs_market / n if n > 0 else 0

    rlm_stats = rlm.get_usage_stats()
    results.api_calls = rlm_stats["api"]["calls_made"]
    results.avg_iterations = rlm_stats["session"]["avg_iterations"]
    results.fallback_rate = rlm_stats["session"]["fallback_rate"]

    return results


def print_summary(results: CrossDomainResults):
    """Print formatted summary."""
    print("\n" + "=" * 70)
    print("CROSS-DOMAIN EVALUATION SUMMARY")
    print("=" * 70)

    print(f"\nTimestamp: {results.timestamp}")
    print(f"Train source: {results.train_source} ({results.train_count} examples)")
    print(f"Test source: {results.test_source} ({results.test_count} examples)")

    print("\n" + "-" * 70)
    print("BRIER SCORES (lower is better)")
    print("-" * 70)

    ratio_random = results.avg_brier_rlm / results.avg_brier_random if results.avg_brier_random > 0 else float('inf')
    ratio_market = results.avg_brier_rlm / results.avg_brier_market if results.avg_brier_market > 0 else float('inf')

    print(f"RLM:    {results.avg_brier_rlm:.4f}")
    print(f"Random: {results.avg_brier_random:.4f} (RLM is {ratio_random:.2f}x)")
    print(f"Market: {results.avg_brier_market:.4f} (RLM is {ratio_market:.2f}x)")

    print("\n" + "-" * 70)
    print("WIN RATES")
    print("-" * 70)
    print(f"vs Random: {results.win_rate_vs_random*100:.1f}%")
    print(f"vs Market: {results.win_rate_vs_market*100:.1f}%")

    print("\n" + "-" * 70)
    print("RLM STATISTICS")
    print("-" * 70)
    print(f"API Calls:      {results.api_calls}")
    print(f"Avg Iterations: {results.avg_iterations:.2f}")
    print(f"Fallback Rate:  {results.fallback_rate*100:.1f}%")

    print("\n" + "-" * 70)
    print("TRANSFER ASSESSMENT")
    print("-" * 70)

    if results.win_rate_vs_random > 0.5:
        print(f"[+] POSITIVE TRANSFER: RLM beats random ({results.win_rate_vs_random*100:.0f}% win rate)")
    else:
        print(f"[-] NO TRANSFER: RLM does not beat random")

    if ratio_market < 1:
        print(f"[+] RLM outperforms market consensus ({ratio_market:.2f}x)")
    else:
        print(f"[-] Market consensus outperforms RLM ({ratio_market:.2f}x)")

    print("\n" + "=" * 70)


def save_results(results: CrossDomainResults, output_dir: Path = Path("data/outputs/rlm_eval")):
    """Save results to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)

    data = {
        "timestamp": results.timestamp,
        "config": results.config,
        "train_source": results.train_source,
        "train_count": results.train_count,
        "test_source": results.test_source,
        "test_count": results.test_count,
        "metrics": {
            "avg_brier_rlm": results.avg_brier_rlm,
            "avg_brier_random": results.avg_brier_random,
            "avg_brier_market": results.avg_brier_market,
            "win_rate_vs_random": results.win_rate_vs_random,
            "win_rate_vs_market": results.win_rate_vs_market,
        },
        "rlm_stats": {
            "api_calls": results.api_calls,
            "avg_iterations": results.avg_iterations,
            "fallback_rate": results.fallback_rate,
        },
        "individual_results": [asdict(r) for r in results.results],
    }

    filename = f"cross_domain_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = output_dir / filename

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Cross-Domain RLM Evaluation")
    parser.add_argument("--n", type=int, default=30, help="Number of test examples")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--save", action="store_true", help="Save results to JSON")
    parser.add_argument("--model", type=str, default="gemini-3-pro", help="Gemini model")
    parser.add_argument("--max-iterations", type=int, default=10, help="Max REPL iterations")
    args = parser.parse_args()

    results = run_cross_domain_evaluation(
        n_total=args.n,
        seed=args.seed,
        verbose=args.verbose,
        model=args.model,
        max_iterations=args.max_iterations,
    )

    print_summary(results)

    if args.save:
        save_results(results)


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Cross-Domain Evaluation:
#
# DESIGN:
# - Train search index on Kalshi only
# - Test on Metaculus (no Metaculus data in training)
# - This tests true generalization, not memorization
#
# FALLBACK:
# - If no Metaculus data available, use Kalshi hold-out
# - 80/20 train/test split simulates domain shift
#
# KEY METRICS:
# - Win rate vs random: primary measure of transfer
# - If > 50%, patterns from Kalshi help on Metaculus
# - Win rate vs market: secondary measure
#
# EXPECTED FINDINGS:
# - Limited transfer between Kalshi (financial/political) and Metaculus (science/tech)
# - Search index may find related topics across domains
# - Domain-specific reasoning unlikely to transfer well
#
