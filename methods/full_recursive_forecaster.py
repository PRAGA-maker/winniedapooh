"""
Full Recursive Forecaster - ForecastMethod implementation.

This forecaster combines:
1. Web-grounded agents for broad context (news, events, opinions)
2. RLM REPL for high-granularity data analysis (trends, patterns, base rates)

The pipeline runs recursively until confidence threshold is met or max iterations reached.

Usage:
    uv run runner/runner.py --method full_recursive

Configuration via method_params:
    --method-params '{"data_analyst_parallel": false}'  # Sequential mode
    --method-params '{"max_iterations": 3}'             # Limit iterations
    --method-params '{"confidence_threshold": 0.8}'     # Higher threshold
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

from methods.base import ForecastMethod
from forecasting.dataclasses import Batch, Example
from methods.rlm_tools.semantic_search import MarketSearchIndex
from methods.full_recursive import FullRecursivePipeline, PipelineResult

# Load environment variables
load_dotenv(Path(__file__).parent.parent / ".env")


# =============================================================================
# Statistics Tracking
# =============================================================================

@dataclass
class FullRecursiveStats:
    """Statistics for a full_recursive session."""
    total_predictions: int = 0
    total_iterations: int = 0
    total_commits: int = 0
    total_retries: int = 0
    total_cost_usd: float = 0.0
    total_latency_ms: int = 0
    avg_confidence: float = 0.0
    per_prediction: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, result: PipelineResult, event_id: str):
        """Add a prediction result to stats."""
        self.per_prediction.append({
            "event_id": event_id,
            "prediction": result.prediction,
            "probability": result.probability,
            "confidence": result.confidence,
            "iterations": result.iterations_used,
            "cost_usd": result.total_cost_usd,
            "latency_ms": result.total_latency_ms,
        })
        self.total_predictions += 1
        self.total_iterations += result.iterations_used
        self.total_cost_usd += result.total_cost_usd
        self.total_latency_ms += result.total_latency_ms

        # Update averages
        confidences = [p["confidence"] for p in self.per_prediction]
        self.avg_confidence = sum(confidences) / len(confidences)

    def summary(self) -> Dict[str, Any]:
        """Return summary statistics."""
        return {
            "total_predictions": self.total_predictions,
            "total_iterations": self.total_iterations,
            "avg_iterations": self.total_iterations / max(1, self.total_predictions),
            "total_cost_usd": self.total_cost_usd,
            "avg_cost_usd": self.total_cost_usd / max(1, self.total_predictions),
            "total_latency_ms": self.total_latency_ms,
            "avg_latency_ms": self.total_latency_ms / max(1, self.total_predictions),
            "avg_confidence": self.avg_confidence,
        }


# =============================================================================
# Main Forecaster Class
# =============================================================================

class FullRecursiveForecaster(ForecastMethod):
    """
    Full Recursive Forecaster using web-grounded agents and RLM REPL.

    This method implements the kalshi-research-agents recursive ensemble pipeline:
    1. PLANNER decomposes problem into sub-questions
    2. ANALYST, ADVOCATE_YES, ADVOCATE_NO research in parallel
    3. DATA_ANALYST analyzes .parquet data via RLM REPL
    4. VERIFIER judges debate and finds issues
    5. SYNTHESIZER decides COMMIT or RETRY

    The recursive loop continues until confidence >= threshold or max_iterations.

    Attributes:
        name: Method name for registry ("full_recursive")

    Configuration:
        api_key: Gemini API key (from GEMINI_API_KEY env var)
        model: Gemini model to use (default: gemini-2.0-flash)
        max_iterations: Max recursive iterations (default: 5)
        confidence_threshold: Confidence required to commit (default: 0.7)
        data_analyst_parallel: DATA_ANALYST timing mode (default: True)
            - True: Runs in parallel with web agents (faster, independent)
            - False: Runs after web agents (slower, can use their findings)
        verbose: Print debug information
    """

    name = "full_recursive"

    # Supported Gemini models
    MODELS = {
        "gemini-2.0-flash": "gemini-2.0-flash-exp",
        "gemini-2.5-flash": "gemini-2.5-flash",
        "gemini-3-flash": "gemini-3-flash-preview",
        "gemini-3-pro": "gemini-3-pro-preview",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.0-flash",
        max_iterations: int = 5,
        confidence_threshold: float = 0.7,
        data_analyst_parallel: bool = True,
        verbose: bool = False,
        log_dir: Optional[str] = None,
    ):
        """
        Initialize the Full Recursive Forecaster.

        Args:
            api_key: Gemini API key (falls back to GEMINI_API_KEY env var)
            model: Gemini model name
            max_iterations: Maximum recursive iterations per prediction
            confidence_threshold: Confidence required to commit (0.0-1.0)
            data_analyst_parallel: DATA_ANALYST timing ablation flag
            verbose: Print debug information during prediction
            log_dir: Directory for JSONL logs (RLM visualizer compatible). None disables.
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY required (set env var or pass api_key)")

        self.model_key = model
        self.model = self.MODELS.get(model, model)
        self.max_iterations = max_iterations
        self.confidence_threshold = confidence_threshold
        self.data_analyst_parallel = data_analyst_parallel
        self.verbose = verbose
        self.log_dir = log_dir

        # Lazy-initialized
        self._search_index: Optional[MarketSearchIndex] = None
        self._all_examples: List[Example] = []

        # Session statistics
        self.session_stats = FullRecursiveStats()

        if self.verbose:
            print(f"[FullRecursive] Model: {self.model}")
            print(f"[FullRecursive] Max iterations: {max_iterations}")
            print(f"[FullRecursive] Confidence threshold: {confidence_threshold:.0%}")
            print(f"[FullRecursive] DATA_ANALYST mode: {'parallel' if data_analyst_parallel else 'sequential'}")
            if log_dir:
                print(f"[FullRecursive] Logging to: {log_dir}")

    def _build_search_index(self, examples: List[Example]) -> None:
        """Build TF-IDF search index from examples."""
        if self._search_index is not None:
            return

        self._search_index = MarketSearchIndex()
        seen_ids = set()

        for ex in examples:
            if ex.event_id in seen_ids:
                continue
            seen_ids.add(ex.event_id)

            title = ex.static_features.get("title", "")
            description = ex.static_features.get("description", "")

            metadata = {
                "target": ex.target,
                "option_count": len(ex.options),
                "source": ex.source,
                "title": title,
                "description": description,
            }

            self._search_index.add_market(ex.event_id, title, description, metadata)

        self._search_index.build_index()

        if self.verbose:
            print(f"[FullRecursive] Built search index with {len(seen_ids)} markets")

    def fit(self, train_batches: List[Batch], spec: Dict[str, Any]) -> None:
        """
        Build search index from training data.

        The search index is used by DATA_ANALYST to find similar historical markets
        for base rate calculations and pattern matching.

        Args:
            train_batches: List of Batch objects containing training examples
            spec: RunSpec configuration dictionary
        """
        all_examples = []
        for batch in train_batches:
            all_examples.extend(batch.examples)

        self._all_examples = all_examples
        self._build_search_index(all_examples)

        if self.verbose:
            print(f"[FullRecursive] Fit complete: {len(all_examples)} training examples")

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[List[float]]:
        """
        Generate predictions for a batch of examples.

        Runs the full recursive pipeline for each example, returning
        probability distributions over options.

        Args:
            batch: Batch object containing examples to predict
            spec: RunSpec configuration dictionary

        Returns:
            List of probability lists, one per example
        """
        # Ensure search index exists
        if self._search_index is None:
            self._build_search_index(batch.examples)

        predictions = []

        for i, example in enumerate(batch.examples):
            if self.verbose:
                print(f"\n[FullRecursive] Predicting {i+1}/{len(batch.examples)}: {example.event_id}")

            try:
                # Create pipeline instance
                pipeline = FullRecursivePipeline(
                    api_key=self.api_key,
                    search_index=self._search_index,
                    model=self.model,
                    max_iterations=self.max_iterations,
                    confidence_threshold=self.confidence_threshold,
                    data_analyst_parallel=self.data_analyst_parallel,
                    verbose=self.verbose,
                    log_dir=self.log_dir,
                )

                # Run pipeline
                result = pipeline.run(example)

                # Update stats
                self.session_stats.add(result, example.event_id)

                # Get probabilities
                pred = result.probabilities

            except Exception as e:
                if self.verbose:
                    print(f"[FullRecursive] Error: {e}")

                # Fallback to last price
                pred = self._fallback_last_price(example)

            predictions.append(pred)

        return predictions

    def _fallback_last_price(self, example: Example) -> List[float]:
        """Fallback to last price when pipeline fails."""
        scores = []
        for opt in example.options:
            if opt.history_belief:
                scores.append(opt.history_belief[-1])
            else:
                scores.append(0.5)

        total = sum(scores)
        if total <= 0:
            return [1.0 / len(scores)] * len(scores)
        return [s / total for s in scores]

    def get_usage_stats(self) -> Dict[str, Any]:
        """Return session usage statistics."""
        return self.session_stats.summary()

    def print_stats(self):
        """Print detailed session statistics."""
        stats = self.get_usage_stats()
        print("\n" + "=" * 60)
        print("FULL RECURSIVE SESSION STATISTICS")
        print("=" * 60)
        print(f"Predictions: {stats['total_predictions']}")
        print(f"Avg Iterations: {stats['avg_iterations']:.2f}")
        print(f"Total Cost: ${stats['total_cost_usd']:.4f}")
        print(f"Avg Cost/Prediction: ${stats['avg_cost_usd']:.4f}")
        print(f"Total Latency: {stats['total_latency_ms']/1000:.1f}s")
        print(f"Avg Latency: {stats['avg_latency_ms']/1000:.1f}s")
        print(f"Avg Confidence: {stats['avg_confidence']:.0%}")
        print("=" * 60 + "\n")


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Full Recursive Forecaster Implementation:
#
# DESIGN DECISIONS:
# 1. ForecastMethod wrapper follows same pattern as RLMForecaster
# 2. Search index shared between fit() and predict()
# 3. Session stats track cost, latency, iterations
#
# ABLATION SUPPORT:
# 1. data_analyst_parallel flag controls DATA_ANALYST timing
# 2. Can be configured via method_params in runner
# 3. Sequential mode allows targeted data analysis
#
# INTEGRATION:
# 1. Uses existing MarketSearchIndex from rlm_tools
# 2. Uses existing analyze_trend from rlm_tools
# 3. Gemini client pattern similar to RLMForecaster
#
# ERROR HANDLING:
# 1. Pipeline failures fall back to last price
# 2. Missing API key raises clear error
# 3. Stats track failures separately
#
# COST TRACKING:
# 1. Per-prediction cost tracked
# 2. Session total tracked
# 3. Based on token estimates from Gemini
#
# OBSERVABILITY (added 2026-01-21):
# 1. log_dir parameter enables RLM visualizer-compatible logging
# 2. Each pipeline run creates a JSONL log file
# 3. Usage via runner:
#    uv run runner/runner.py --method full_recursive \
#      --method-params '{"log_dir": "logs/full_recursive", "verbose": true}'
# 4. View logs:
#    - Copy JSONL files to external/rlm/visualizer/public/logs/
#    - cd external/rlm/visualizer && npm run dev
#    - Open http://localhost:3000 and select log file
# 5. Helper function available:
#    from methods.full_recursive import copy_logs_to_visualizer
#    copy_logs_to_visualizer()  # Copies recent logs to visualizer
#
