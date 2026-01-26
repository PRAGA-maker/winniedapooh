"""
Comprehensive RLM Evaluation Script with Baselines and Ablations.

Runs evaluation comparing:
- RLM with REPL (code execution)
- RLM without REPL (direct prompt - ablation)
- Random baseline
- Market consensus (last price)
- Historical average (from similar markets)

Usage:
    uv run python tests/test_rlm_evaluate.py --n 30
    uv run python tests/test_rlm_evaluate.py --n 100 --cutoff 0.5 --save
    uv run python tests/test_rlm_evaluate.py --ablation  # Compare REPL vs no-REPL
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import random
import json
import time
import math
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field, asdict
from collections import defaultdict

from forecasting.dataset import EventDataset
from forecasting.tasks.registry import build_task
from forecasting.dataclasses import Batch, Example
from methods.rlm_forecaster import (
    RLMForecaster,
    random_baseline,
    market_consensus_baseline,
)
from methods.rlm_tools.semantic_search import MarketSearchIndex


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ExampleResult:
    """Result for a single example."""
    event_id: str
    difficulty: str  # easy, medium, hard
    diff_from_baseline: float

    # Brier scores for each method
    brier_rlm: float
    brier_rlm_no_repl: Optional[float]
    brier_random: float
    brier_market: float
    brier_historical: float

    # Which method won
    rlm_vs_market: str  # "RLM" or "MARKET"
    rlm_vs_random: str  # "RLM" or "RANDOM"

    # Predictions
    prediction_rlm: List[float]
    prediction_market: List[float]
    target: List[float]

    # Leakage detection
    leakage_flagged: bool = False


@dataclass
class MethodMetrics:
    """Aggregated metrics for a single method."""
    name: str
    n_examples: int = 0
    total_brier: float = 0.0
    wins_vs_market: int = 0
    wins_vs_random: int = 0

    @property
    def avg_brier(self) -> float:
        return self.total_brier / self.n_examples if self.n_examples > 0 else 0.0

    @property
    def win_rate_vs_market(self) -> float:
        return self.wins_vs_market / self.n_examples if self.n_examples > 0 else 0.0

    @property
    def win_rate_vs_random(self) -> float:
        return self.wins_vs_random / self.n_examples if self.n_examples > 0 else 0.0


@dataclass
class DifficultyBucket:
    """Metrics aggregated by difficulty."""
    n_examples: int = 0
    rlm_brier: float = 0.0
    market_brier: float = 0.0
    random_brier: float = 0.0
    historical_brier: float = 0.0
    rlm_wins: int = 0


@dataclass
class EvaluationResults:
    """Complete evaluation results."""
    timestamp: str
    config: Dict[str, Any]

    # Overall metrics by method
    rlm: MethodMetrics = field(default_factory=lambda: MethodMetrics("RLM"))
    rlm_no_repl: MethodMetrics = field(default_factory=lambda: MethodMetrics("RLM_NO_REPL"))
    market: MethodMetrics = field(default_factory=lambda: MethodMetrics("MARKET"))
    random: MethodMetrics = field(default_factory=lambda: MethodMetrics("RANDOM"))
    historical: MethodMetrics = field(default_factory=lambda: MethodMetrics("HISTORICAL"))

    # By difficulty
    easy: DifficultyBucket = field(default_factory=DifficultyBucket)
    medium: DifficultyBucket = field(default_factory=DifficultyBucket)
    hard: DifficultyBucket = field(default_factory=DifficultyBucket)

    # RLM-specific stats
    api_calls: int = 0
    total_predictions: int = 0
    avg_iterations: float = 0.0
    avg_code_blocks: float = 0.0
    fallback_rate: float = 0.0
    leakage_count: int = 0

    # Timing
    total_time_seconds: float = 0.0
    avg_time_per_prediction: float = 0.0

    # Individual results
    results: List[ExampleResult] = field(default_factory=list)


# =============================================================================
# Baseline Methods
# =============================================================================

def compute_historical_baseline(
    example: Example,
    search_index: MarketSearchIndex,
) -> List[float]:
    """
    Compute historical average from similar markets.
    Uses TF-IDF search to find similar markets and averages their resolutions.
    """
    if search_index is None:
        return random_baseline(len(example.options))

    title = example.static_features.get("title", "")
    results = search_index.search(title, top_k=5)

    # Filter out current market
    results = [(mid, score, meta) for mid, score, meta in results if mid != example.event_id]

    if not results:
        return random_baseline(len(example.options))

    # Average the targets from similar markets
    n_options = len(example.options)
    targets = []
    for mid, score, meta in results:
        target = meta.get("target", [])
        if len(target) == n_options:
            targets.append(target)

    if not targets:
        return random_baseline(n_options)

    avg = [sum(t[i] for t in targets) / len(targets) for i in range(n_options)]
    total = sum(avg)
    if total <= 0:
        return random_baseline(n_options)
    return [v / total for v in avg]


def brier_score(pred: List[float], target: List[float]) -> float:
    """Compute Brier score (sum of squared errors)."""
    return sum((p - t) ** 2 for p, t in zip(pred, target))


# =============================================================================
# Difficulty Classification
# =============================================================================

def categorize_difficulty(ex: Example) -> Tuple[str, float]:
    """
    Categorize example difficulty based on price movement from last price to target.
    Returns (category, movement_magnitude).
    """
    last_beliefs = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in ex.options]
    total = sum(last_beliefs)
    last_norm = [b/total if total > 0 else 1/len(last_beliefs) for b in last_beliefs]
    diff = sum(abs(t - b) for t, b in zip(ex.target, last_norm))

    if diff < 0.1:
        return "easy", diff
    elif diff < 0.3:
        return "medium", diff
    else:
        return "hard", diff


# =============================================================================
# Statistical Tests
# =============================================================================

def paired_t_test(scores_a: List[float], scores_b: List[float]) -> Tuple[float, float]:
    """
    Perform paired t-test to compare two methods.
    Returns (t_statistic, p_value).
    """
    if len(scores_a) != len(scores_b) or len(scores_a) < 2:
        return 0.0, 1.0

    n = len(scores_a)
    diffs = [a - b for a, b in zip(scores_a, scores_b)]
    mean_diff = sum(diffs) / n
    variance = sum((d - mean_diff) ** 2 for d in diffs) / (n - 1)

    if variance <= 0:
        return 0.0, 1.0

    se = math.sqrt(variance / n)
    t_stat = mean_diff / se

    # Approximate p-value using normal distribution for large n
    # For small n, this is an approximation
    p_value = 2 * (1 - min(0.9999, abs(0.5 + 0.5 * math.erf(abs(t_stat) / math.sqrt(2)))))

    return t_stat, p_value


def confidence_interval(wins: int, n: int, confidence: float = 0.95) -> Tuple[float, float]:
    """
    Wilson score interval for proportion confidence interval.
    Returns (lower_bound, upper_bound).
    """
    if n == 0:
        return 0.0, 1.0

    z = 1.96 if confidence == 0.95 else 1.645  # 95% or 90%
    p = wins / n

    denominator = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denominator
    spread = z * math.sqrt((p*(1-p) + z**2/(4*n)) / n) / denominator

    return max(0, center - spread), min(1, center + spread)


# =============================================================================
# Main Evaluation
# =============================================================================

def run_evaluation(
    n_total: int = 30,
    cutoff_percent: float = 0.50,
    seed: int = 42,
    verbose: bool = False,
    model: str = "gemini-3-pro",
    run_ablation: bool = False,
    max_iterations: int = 10,
) -> EvaluationResults:
    """
    Run comprehensive RLM evaluation.

    Args:
        n_total: Total number of test examples
        cutoff_percent: Cutoff percentage for predict_final task
        seed: Random seed
        verbose: Print detailed progress
        model: Gemini model to use
        run_ablation: Also run RLM without REPL for comparison
        max_iterations: Max REPL iterations per prediction

    Returns:
        EvaluationResults with all metrics
    """
    print("=" * 70)
    print("RLM COMPREHENSIVE EVALUATION")
    print("=" * 70)
    print(f"Config: n={n_total}, cutoff={cutoff_percent}, model={model}")
    print(f"Ablation mode: {run_ablation}")
    print()

    # Initialize results
    results = EvaluationResults(
        timestamp=datetime.now().isoformat(),
        config={
            "n_total": n_total,
            "cutoff_percent": cutoff_percent,
            "seed": seed,
            "model": model,
            "run_ablation": run_ablation,
            "max_iterations": max_iterations,
        }
    )

    # Load dataset
    dataset_path = Path("data/datasets")
    datasets = sorted(list(dataset_path.glob("v*_unified")))
    if not datasets:
        raise ValueError("No dataset found. Run 'uv run runner/runner.py' first.")

    dataset = EventDataset.load(str(datasets[-1]))
    kalshi_view = dataset.slice(source='kalshi')
    print(f"Dataset: {datasets[-1].name}")
    print(f"Kalshi records: {len(kalshi_view.df)}")

    # Build task
    task = build_task('predict_final', {
        'cutoff_percent': cutoff_percent,
        'use_resolution': False,
        'relax_status': True,
        'min_history_points': 5
    })

    # Get all examples
    rng = random.Random(seed)
    all_examples = []
    for record in kalshi_view.records():
        all_examples.extend(task.make_examples(record, rng))

    print(f"Total examples: {len(all_examples)}")

    # Filter for quality (non-empty descriptions)
    quality_examples = [
        ex for ex in all_examples
        if len(ex.static_features.get("description", "")) > 50
    ]
    print(f"Quality examples (description > 50 chars): {len(quality_examples)}")

    # Categorize by difficulty
    categorized = {"easy": [], "medium": [], "hard": []}
    for ex in quality_examples:
        diff_cat, diff_val = categorize_difficulty(ex)
        categorized[diff_cat].append((ex, diff_val))

    print(f"Easy: {len(categorized['easy'])}, Medium: {len(categorized['medium'])}, Hard: {len(categorized['hard'])}")

    # Sample balanced test set
    n_per_cat = n_total // 3
    test_examples = []
    test_diffs = []

    for cat in ["easy", "medium", "hard"]:
        random.Random(seed).shuffle(categorized[cat])
        sample = categorized[cat][:n_per_cat]
        test_examples.extend([ex for ex, _ in sample])
        test_diffs.extend([diff for _, diff in sample])

    # Remaining for training
    train_examples = []
    for cat in ["easy", "medium", "hard"]:
        train_examples.extend([ex for ex, _ in categorized[cat][n_per_cat:]])

    print(f"\nTest set: {len(test_examples)} ({n_per_cat} per difficulty)")
    print(f"Train set: {len(train_examples)}")

    # Build search index for historical baseline
    search_index = MarketSearchIndex()
    for ex in train_examples:
        title = ex.static_features.get("title", "")
        description = ex.static_features.get("description", "")
        search_index.add_market(ex.event_id, title, description, {
            "target": ex.target,
            "title": title,
        })
    search_index.build_index()

    # Initialize RLM with REPL
    print("\n" + "-" * 70)
    print("INITIALIZING RLM (with REPL)")
    print("-" * 70)

    call_budget = max(500, len(test_examples) * max_iterations * 2)
    rlm = RLMForecaster(
        model=model,
        max_iterations=max_iterations,
        call_budget=call_budget,
        verbose=verbose,
        use_repl=True,
    )

    # Fit
    train_batch = Batch(examples=train_examples)
    rlm.fit([train_batch], {})

    # Initialize RLM without REPL (ablation)
    rlm_no_repl = None
    if run_ablation:
        print("\n" + "-" * 70)
        print("INITIALIZING RLM (without REPL - ablation)")
        print("-" * 70)
        rlm_no_repl = RLMForecaster(
            model=model,
            max_iterations=1,  # Only one pass without REPL
            call_budget=call_budget,
            verbose=verbose,
            use_repl=False,
        )
        rlm_no_repl.fit([train_batch], {})

    # Run predictions
    print("\n" + "=" * 70)
    print(f"RUNNING PREDICTIONS ({len(test_examples)} examples)")
    print("=" * 70)

    start_time = time.time()

    # Storage for statistical tests
    rlm_briers = []
    market_briers = []
    random_briers = []
    rlm_no_repl_briers = []

    for i, (ex, diff_val) in enumerate(zip(test_examples, test_diffs)):
        print(f"\n[{i+1}/{len(test_examples)}] {ex.event_id}")

        # Get predictions from all methods
        # RLM with REPL
        test_batch = Batch(examples=[ex])
        pred_rlm = rlm.predict(test_batch, {})[0]

        # RLM without REPL (ablation)
        pred_rlm_no_repl = None
        if rlm_no_repl:
            pred_rlm_no_repl = rlm_no_repl.predict(test_batch, {})[0]

        # Baselines
        pred_market = market_consensus_baseline(ex)
        pred_random = random_baseline(len(ex.options))
        pred_historical = compute_historical_baseline(ex, search_index)

        # Compute Brier scores
        brier_rlm = brier_score(pred_rlm, ex.target)
        brier_market = brier_score(pred_market, ex.target)
        brier_random = brier_score(pred_random, ex.target)
        brier_historical = brier_score(pred_historical, ex.target)
        brier_rlm_no_repl = brier_score(pred_rlm_no_repl, ex.target) if pred_rlm_no_repl else None

        # Store for statistical tests
        rlm_briers.append(brier_rlm)
        market_briers.append(brier_market)
        random_briers.append(brier_random)
        if brier_rlm_no_repl is not None:
            rlm_no_repl_briers.append(brier_rlm_no_repl)

        # Determine difficulty
        diff_cat, _ = categorize_difficulty(ex)

        # Determine winners
        rlm_vs_market = "RLM" if brier_rlm < brier_market else "MARKET"
        rlm_vs_random = "RLM" if brier_rlm < brier_random else "RANDOM"

        # Check for leakage
        leakage = False
        if rlm.session_stats.per_prediction:
            last_stats = rlm.session_stats.per_prediction[-1]
            leakage = last_stats.leakage_warning

        # Store result
        result = ExampleResult(
            event_id=ex.event_id,
            difficulty=diff_cat,
            diff_from_baseline=diff_val,
            brier_rlm=brier_rlm,
            brier_rlm_no_repl=brier_rlm_no_repl,
            brier_random=brier_random,
            brier_market=brier_market,
            brier_historical=brier_historical,
            rlm_vs_market=rlm_vs_market,
            rlm_vs_random=rlm_vs_random,
            prediction_rlm=pred_rlm,
            prediction_market=pred_market,
            target=list(ex.target),
            leakage_flagged=leakage,
        )
        results.results.append(result)

        # Update method metrics
        results.rlm.n_examples += 1
        results.rlm.total_brier += brier_rlm
        if rlm_vs_market == "RLM":
            results.rlm.wins_vs_market += 1
        if rlm_vs_random == "RLM":
            results.rlm.wins_vs_random += 1

        results.market.n_examples += 1
        results.market.total_brier += brier_market

        results.random.n_examples += 1
        results.random.total_brier += brier_random

        results.historical.n_examples += 1
        results.historical.total_brier += brier_historical

        if brier_rlm_no_repl is not None:
            results.rlm_no_repl.n_examples += 1
            results.rlm_no_repl.total_brier += brier_rlm_no_repl

        # Update difficulty buckets
        bucket = getattr(results, diff_cat)
        bucket.n_examples += 1
        bucket.rlm_brier += brier_rlm
        bucket.market_brier += brier_market
        bucket.random_brier += brier_random
        bucket.historical_brier += brier_historical
        if rlm_vs_market == "RLM":
            bucket.rlm_wins += 1

        if leakage:
            results.leakage_count += 1

        if verbose:
            print(f"  RLM: {brier_rlm:.4f}, Market: {brier_market:.4f}, Winner: {rlm_vs_market}")

    end_time = time.time()
    results.total_time_seconds = end_time - start_time
    results.avg_time_per_prediction = results.total_time_seconds / len(test_examples)

    # Get RLM stats
    rlm_stats = rlm.get_usage_stats()
    results.api_calls = rlm_stats["api"]["calls_made"]
    results.total_predictions = rlm_stats["session"]["total_predictions"]
    results.avg_iterations = rlm_stats["session"]["avg_iterations"]
    results.avg_code_blocks = rlm_stats["session"]["avg_code_blocks"]
    results.fallback_rate = rlm_stats["session"]["fallback_rate"]

    # Run statistical tests
    print("\n" + "=" * 70)
    print("STATISTICAL ANALYSIS")
    print("=" * 70)

    t_stat, p_value = paired_t_test(rlm_briers, market_briers)
    print(f"RLM vs Market: t={t_stat:.3f}, p={p_value:.4f}")

    t_stat_rand, p_value_rand = paired_t_test(rlm_briers, random_briers)
    print(f"RLM vs Random: t={t_stat_rand:.3f}, p={p_value_rand:.4f}")

    if rlm_no_repl_briers:
        t_stat_abl, p_value_abl = paired_t_test(rlm_briers, rlm_no_repl_briers)
        print(f"RLM (REPL) vs RLM (no REPL): t={t_stat_abl:.3f}, p={p_value_abl:.4f}")

    # Win rate confidence intervals
    lower, upper = confidence_interval(results.rlm.wins_vs_market, results.rlm.n_examples)
    print(f"\nRLM win rate vs Market: {results.rlm.win_rate_vs_market*100:.1f}% (95% CI: [{lower*100:.1f}%, {upper*100:.1f}%])")

    return results


def print_summary(results: EvaluationResults):
    """Print formatted summary of results."""
    print("\n" + "=" * 70)
    print("EVALUATION SUMMARY")
    print("=" * 70)

    print(f"\nTimestamp: {results.timestamp}")
    print(f"Config: cutoff={results.config['cutoff_percent']}, n={results.config['n_total']}")

    # Overall Brier scores
    print("\n" + "-" * 70)
    print("BRIER SCORES (lower is better)")
    print("-" * 70)
    print(f"{'Method':<20} {'Brier':>10} {'vs Market':>12} {'vs Random':>12}")
    print("-" * 70)

    ratio_market = results.rlm.avg_brier / results.market.avg_brier if results.market.avg_brier > 0 else float('inf')
    ratio_random = results.rlm.avg_brier / results.random.avg_brier if results.random.avg_brier > 0 else float('inf')

    print(f"{'RLM (REPL)':<20} {results.rlm.avg_brier:>10.4f} {ratio_market:>10.2f}x {ratio_random:>10.2f}x")
    print(f"{'Market Consensus':<20} {results.market.avg_brier:>10.4f} {'-':>12} {'-':>12}")
    print(f"{'Random':<20} {results.random.avg_brier:>10.4f} {'-':>12} {'-':>12}")
    print(f"{'Historical Avg':<20} {results.historical.avg_brier:>10.4f} {'-':>12} {'-':>12}")

    if results.rlm_no_repl.n_examples > 0:
        ratio_abl = results.rlm.avg_brier / results.rlm_no_repl.avg_brier if results.rlm_no_repl.avg_brier > 0 else float('inf')
        print(f"{'RLM (no REPL)':<20} {results.rlm_no_repl.avg_brier:>10.4f} {'-':>12} {ratio_abl:>10.2f}x vs REPL")

    # By difficulty
    print("\n" + "-" * 70)
    print("BY DIFFICULTY")
    print("-" * 70)
    print(f"{'Difficulty':<12} {'n':>6} {'RLM':>10} {'Market':>10} {'Ratio':>10} {'Win Rate':>10}")
    print("-" * 70)

    for name, bucket in [("Easy", results.easy), ("Medium", results.medium), ("Hard", results.hard)]:
        if bucket.n_examples > 0:
            rlm_avg = bucket.rlm_brier / bucket.n_examples
            mkt_avg = bucket.market_brier / bucket.n_examples
            ratio = rlm_avg / mkt_avg if mkt_avg > 0 else float('inf')
            win_rate = bucket.rlm_wins / bucket.n_examples
            print(f"{name:<12} {bucket.n_examples:>6} {rlm_avg:>10.4f} {mkt_avg:>10.4f} {ratio:>9.2f}x {win_rate*100:>9.1f}%")

    # RLM Statistics
    print("\n" + "-" * 70)
    print("RLM EXECUTION STATISTICS")
    print("-" * 70)
    print(f"API Calls:           {results.api_calls}")
    print(f"Predictions:         {results.total_predictions}")
    print(f"Avg Iterations:      {results.avg_iterations:.2f}")
    print(f"Avg Code Blocks:     {results.avg_code_blocks:.2f}")
    print(f"Fallback Rate:       {results.fallback_rate*100:.1f}%")
    print(f"Leakage Warnings:    {results.leakage_count}")
    print(f"Total Time:          {results.total_time_seconds:.1f}s")
    print(f"Avg Time/Prediction: {results.avg_time_per_prediction:.2f}s")

    # Key findings
    print("\n" + "-" * 70)
    print("KEY FINDINGS")
    print("-" * 70)

    if ratio_market < 1:
        print(f"[+] RLM beats Market Consensus ({ratio_market:.2f}x, {results.rlm.win_rate_vs_market*100:.0f}% win rate)")
    else:
        print(f"[-] Market Consensus beats RLM ({ratio_market:.2f}x)")

    if ratio_random < 1:
        print(f"[+] RLM beats Random Baseline ({ratio_random:.2f}x)")
    else:
        print(f"[-] Random Baseline beats RLM ({ratio_random:.2f}x)")

    # Check if target Brier < 0.05 achieved
    if results.rlm.avg_brier < 0.05:
        print(f"[+] TARGET ACHIEVED: Brier {results.rlm.avg_brier:.4f} < 0.05")
    else:
        print(f"[-] Target not achieved: Brier {results.rlm.avg_brier:.4f} >= 0.05")

    print("\n" + "=" * 70)


def save_results(results: EvaluationResults, output_dir: Path = Path("data/outputs/rlm_eval")):
    """Save results to JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert to serializable dict
    def bucket_to_dict(b):
        return {
            "n_examples": b.n_examples,
            "rlm_brier": b.rlm_brier,
            "market_brier": b.market_brier,
            "random_brier": b.random_brier,
            "historical_brier": b.historical_brier,
            "rlm_wins": b.rlm_wins,
        }

    def method_to_dict(m):
        return {
            "name": m.name,
            "n_examples": m.n_examples,
            "total_brier": m.total_brier,
            "avg_brier": m.avg_brier,
            "wins_vs_market": m.wins_vs_market,
            "win_rate_vs_market": m.win_rate_vs_market,
        }

    data = {
        "timestamp": results.timestamp,
        "config": results.config,
        "methods": {
            "rlm": method_to_dict(results.rlm),
            "rlm_no_repl": method_to_dict(results.rlm_no_repl),
            "market": method_to_dict(results.market),
            "random": method_to_dict(results.random),
            "historical": method_to_dict(results.historical),
        },
        "by_difficulty": {
            "easy": bucket_to_dict(results.easy),
            "medium": bucket_to_dict(results.medium),
            "hard": bucket_to_dict(results.hard),
        },
        "rlm_stats": {
            "api_calls": results.api_calls,
            "total_predictions": results.total_predictions,
            "avg_iterations": results.avg_iterations,
            "avg_code_blocks": results.avg_code_blocks,
            "fallback_rate": results.fallback_rate,
            "leakage_count": results.leakage_count,
            "total_time_seconds": results.total_time_seconds,
            "avg_time_per_prediction": results.avg_time_per_prediction,
        },
        "individual_results": [asdict(r) for r in results.results],
    }

    filename = f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = output_dir / filename

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive RLM Evaluation")
    parser.add_argument("--n", type=int, default=30, help="Total test examples")
    parser.add_argument("--cutoff", type=float, default=0.50, help="Cutoff percentage")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--save", action="store_true", help="Save results to JSON")
    parser.add_argument("--model", type=str, default="gemini-3-pro", help="Gemini model")
    parser.add_argument("--ablation", action="store_true", help="Run ablation (REPL vs no-REPL)")
    parser.add_argument("--max-iterations", type=int, default=10, help="Max REPL iterations")
    args = parser.parse_args()

    results = run_evaluation(
        n_total=args.n,
        cutoff_percent=args.cutoff,
        seed=args.seed,
        verbose=args.verbose,
        model=args.model,
        run_ablation=args.ablation,
        max_iterations=args.max_iterations,
    )

    print_summary(results)

    if args.save:
        save_results(results)


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Evaluation Infrastructure Redesign:
#
# BASELINES ADDED:
# 1. Random baseline - uniform distribution
# 2. Market consensus - last price normalized
# 3. Historical average - TF-IDF search for similar markets, average their resolutions
#
# ABLATION SUPPORT:
# - --ablation flag compares REPL vs no-REPL modes
# - Tracks all metrics separately for fair comparison
#
# STATISTICAL TESTS:
# 1. Paired t-test for Brier score comparison
# 2. Wilson score interval for win rate confidence intervals
#
# DIFFICULTY BUCKETS:
# - Easy: <10% movement from last price to target
# - Medium: 10-30% movement
# - Hard: >30% movement
#
# KEY METRICS:
# - Brier score (primary)
# - Win rate vs market/random
# - Ratio to baseline
# - Leakage count
#
# QUALITY FILTERING:
# - Only use examples with description > 50 chars
# - Filters out bulk-only records with cryptic IDs
#
