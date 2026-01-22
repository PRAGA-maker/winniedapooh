"""
RLM Forecaster V2 - Using external/rlm library properly.

This version uses the full RLM library from external/rlm with:
- Proper RLM orchestration with max_iterations
- LocalREPL with setup_code for helper functions
- Context payload with parquet schema and market info
- Sub-LLM queries via llm_query() for complex analysis

Key differences from v1:
- Uses external/rlm's RLM class for the main loop
- Uses LocalREPL's built-in llm_query() for sub-calls
- Passes full parquet schema documentation
- Helper functions injected via setup_code
"""
import os
import sys
import re
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv
import numpy as np

# Add external/rlm to path FIRST
sys.path.insert(0, str(Path(__file__).parent.parent / "external" / "rlm"))

from methods.base import ForecastMethod
from forecasting.dataclasses import Batch, Example
from methods.rlm_tools.semantic_search import MarketSearchIndex
from methods.rlm_tools.data_analysis import analyze_trend

# Now import from external/rlm
from rlm import RLM
from rlm.environments.local_repl import LocalREPL
from rlm.core.lm_handler import LMHandler
from rlm.clients import get_client

load_dotenv(Path(__file__).parent.parent / ".env")


# =============================================================================
# Statistics Tracking (same as v1)
# =============================================================================

@dataclass
class RLMPredictionStats:
    """Stats for a single prediction."""
    event_id: str
    iterations_used: int = 0
    code_blocks_executed: int = 0
    search_calls: int = 0
    trend_calls: int = 0
    fallback_used: bool = False
    leakage_warning: bool = False
    execution_time: float = 0.0


@dataclass
class RLMSessionStats:
    """Aggregate stats for an RLM session."""
    total_predictions: int = 0
    total_iterations: int = 0
    total_code_blocks: int = 0
    total_fallbacks: int = 0
    leakage_warnings: int = 0
    per_prediction: List[RLMPredictionStats] = field(default_factory=list)

    def add(self, stats: RLMPredictionStats):
        self.per_prediction.append(stats)
        self.total_predictions += 1
        self.total_iterations += stats.iterations_used
        self.total_code_blocks += stats.code_blocks_executed
        if stats.fallback_used:
            self.total_fallbacks += 1
        if stats.leakage_warning:
            self.leakage_warnings += 1

    def summary(self) -> Dict[str, Any]:
        return {
            "total_predictions": self.total_predictions,
            "total_iterations": self.total_iterations,
            "avg_iterations": self.total_iterations / max(1, self.total_predictions),
            "total_code_blocks": self.total_code_blocks,
            "fallback_rate": self.total_fallbacks / max(1, self.total_predictions),
            "leakage_warnings": self.leakage_warnings,
        }


# =============================================================================
# Forecaster System Prompt - Tuned for RLM library
# =============================================================================

FORECASTER_SYSTEM_PROMPT = """You are an expert forecaster for prediction markets using a REPL environment.

CRITICAL RULES:
1. You are making predictions AS OF the cutoff_ts in the context. Do NOT use future knowledge.
2. Write Python code in ```repl blocks to analyze the data
3. Use the helper functions: search(), trend(), get_similar_resolutions()
4. Set your prediction in a variable called `prediction` and call FINAL_VAR(prediction)

CONTEXT STRUCTURE:
The `context` variable is a dictionary with:
- market: Dict with title, description, options, end_time, source
- price_history: Dict mapping option_idx to {title, prices, last_price}
- cutoff_ts: The prediction date (string) - pretend it's this date NOW
- n_options: Number of options (your prediction must have this many probabilities)
- parquet_schema: Information about the full dataset available

AVAILABLE HELPER FUNCTIONS:
1. search(query: str) -> List[Tuple[market_id, score, metadata]]
   Find similar markets using TF-IDF semantic search

2. trend(option_idx: int) -> Dict[str, float]
   Returns {slope, volatility, min, max, mean, last_value, length}

3. get_similar_resolutions(query: str) -> List[float]
   Get resolution probabilities from similar markets (base rates)

4. llm_query(prompt: str) -> str
   Query a sub-LLM for complex reasoning (use sparingly)

ANALYSIS APPROACH:
1. First, examine context['market'] and context['price_history']
2. Use search() to find similar historical markets
3. Use trend() to analyze price movements
4. Apply domain knowledge (but NOT future knowledge!)
5. Set prediction = [p1, p2, ...] where sum = 1.0

OUTPUT:
```repl
# Your analysis code here
prediction = [0.6, 0.4]  # Example for 2 options
```
FINAL_VAR(prediction)

IMPORTANT: Probabilities must sum to 1.0 and have exactly n_options values.
"""


# =============================================================================
# Setup Code for LocalREPL
# =============================================================================

def build_setup_code(search_index: MarketSearchIndex, example: Example) -> str:
    """
    Build setup code that defines helper functions in the REPL namespace.
    These functions are serialized into Python code that will execute in LocalREPL.
    """

    # Serialize the trend data for the current example
    trend_data = {}
    for i, opt in enumerate(example.options):
        trend_data[i] = analyze_trend(opt.history_belief)

    # Serialize search results (pre-compute top results)
    title = example.static_features.get("title", "")
    search_results = []
    if search_index and search_index.is_built:
        results = search_index.search(title, top_k=5)
        for mid, score, meta in results:
            if mid != example.event_id:
                search_results.append({
                    "market_id": mid,
                    "score": score,
                    "title": meta.get("title", ""),
                    "target": meta.get("target", []),
                })

    setup_code = f'''
# Pre-computed data for this market
_trend_data = {json.dumps(trend_data)}
_search_results = {json.dumps(search_results)}
_event_id = "{example.event_id}"

def search(query: str):
    """Find similar markets using pre-computed TF-IDF search."""
    # Return pre-computed results (actual search done at setup time)
    results = []
    for r in _search_results[:3]:
        results.append((r["market_id"], r["score"], {{"title": r["title"], "target": r["target"]}}))
    return results

def trend(option_idx: int):
    """Get trend analysis for an option."""
    idx_str = str(option_idx)
    if idx_str in _trend_data:
        return _trend_data[idx_str]
    return {{"error": f"Invalid option index: {{option_idx}}"}}

def get_similar_resolutions(query: str):
    """Get resolution probabilities from similar markets (base rates)."""
    targets = []
    for r in _search_results:
        if r["target"]:
            targets.append(r["target"])
    if not targets:
        return None
    # Average the targets
    n = len(targets[0]) if targets else 0
    if n == 0:
        return None
    avg = [sum(t[i] for t in targets if len(t) > i) / len(targets) for i in range(n)]
    return avg

# Also make numpy available
import numpy as np
'''
    return setup_code


# =============================================================================
# Context Payload Builder
# =============================================================================

def build_context_payload(example: Example, parquet_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Build context payload with full market info and parquet schema documentation.
    """
    cutoff_ts = example.cutoff_ts
    if isinstance(cutoff_ts, datetime):
        cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M")
    else:
        cutoff_str = str(cutoff_ts)

    # Build price history
    price_history = {}
    for i, opt in enumerate(example.options):
        price_history[str(i)] = {
            "title": opt.title,
            "prices": opt.history_belief[-50:] if opt.history_belief else [],
            "last_price": opt.history_belief[-1] if opt.history_belief else 0.5,
        }

    # Parquet schema documentation
    parquet_schema = """
The full dataset is stored in a Parquet file with the following schema:

COLUMNS:
- event_id: str - Unique identifier for the market/event
- source: str - Data source ('kalshi' or 'metaculus')
- title: str - Market question/title
- description: str - Detailed description of the market
- status: str - Market status ('open', 'closed', 'resolved')
- end_time: datetime - When the market closes
- created_at: datetime - When the market was created
- time_series: List[TimeSeriesPoint] - Price/probability history
- options: List[OptionInfo] - Available options and their metadata
- resolution: Optional[List[float]] - Final resolution (one-hot encoded)

Each TimeSeriesPoint has:
- ts: datetime - Timestamp
- raw_belief: List[float] - Raw probabilities per option

The data contains prediction markets from Kalshi (financial/political) and Metaculus (science/technology).
"""

    return {
        "market": {
            "title": example.static_features.get("title", "Unknown"),
            "description": example.static_features.get("description", "")[:3000],
            "end_time": str(example.static_features.get("end_time", "Unknown")),
            "source": example.source,
            "event_id": example.event_id,
            "options": [opt.title for opt in example.options],
        },
        "price_history": price_history,
        "cutoff_ts": cutoff_str,
        "n_options": len(example.options),
        "parquet_schema": parquet_schema,
        "parquet_path": parquet_path or "data/clean/unified_market_data.parquet",
    }


# =============================================================================
# Main RLM Forecaster V2
# =============================================================================

class RLMForecasterV2(ForecastMethod):
    """
    RLM-based forecaster using the external/rlm library.

    Features:
    - Uses RLM class for orchestration
    - LocalREPL with setup_code for helper functions
    - Full parquet schema documentation in context
    - llm_query() available for sub-LLM calls
    """
    name = "rlm_v2"

    MODELS = {
        "gemini-3-pro": "gemini-3-pro-preview",
        "gemini-3-flash": "gemini-3-flash-preview",
        "gemini-2.5-flash": "gemini-2.5-flash",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-3-pro",
        max_iterations: int = 10,
        verbose: bool = False,
    ):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY required")

        self.model_key = model
        self.model = self.MODELS.get(model, model)
        self.max_iterations = max_iterations
        self.verbose = verbose

        # Lazy-initialized
        self._search_index: Optional[MarketSearchIndex] = None
        self._all_examples: List[Example] = []

        # Session statistics
        self.session_stats = RLMSessionStats()

        if self.verbose:
            print(f"[RLM-V2] Model: {self.model}, Max iterations: {max_iterations}")

    def _build_search_index(self, examples: List[Example]) -> None:
        """Build TF-IDF index from training examples."""
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
                "target": list(ex.target),
                "title": title,
                "description": description,
            }

            self._search_index.add_market(ex.event_id, title, description, metadata)

        self._search_index.build_index()
        if self.verbose:
            print(f"[RLM-V2] Built search index with {len(seen_ids)} markets")

    def _fallback_last_price(self, example: Example) -> List[float]:
        """Fallback to last price."""
        scores = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in example.options]
        total = sum(scores)
        if total <= 0:
            return [1.0 / len(scores)] * len(scores)
        return [s / total for s in scores]

    def _extract_prediction(self, response: str, n_options: int) -> Optional[List[float]]:
        """Extract prediction from RLM response."""
        # Look for array pattern
        array_match = re.search(r'\[[\d.,\s]+\]', response)
        if array_match:
            try:
                probs = json.loads(array_match.group())
                if len(probs) == n_options:
                    total = sum(max(0, p) for p in probs)
                    if total > 0:
                        return [max(0, p) / total for p in probs]
            except json.JSONDecodeError:
                pass
        return None

    def _predict_single(self, example: Example) -> Tuple[List[float], RLMPredictionStats]:
        """Run prediction using external/rlm RLM class."""
        stats = RLMPredictionStats(event_id=example.event_id)
        start_time = time.time()

        # Build context and setup code
        context = build_context_payload(example)
        setup_code = build_setup_code(self._search_index, example)

        try:
            # Create RLM instance
            rlm = RLM(
                backend="gemini",
                backend_kwargs={
                    "model_name": self.model,
                    "api_key": self.api_key,
                },
                environment="local",
                environment_kwargs={
                    "setup_code": setup_code,
                },
                max_depth=1,
                max_iterations=self.max_iterations,
                custom_system_prompt=FORECASTER_SYSTEM_PROMPT,
                verbose=self.verbose,
            )

            # Build user prompt
            user_prompt = self._build_user_prompt(context)

            # Run RLM completion
            result = rlm.completion(context, root_prompt=user_prompt)

            stats.execution_time = time.time() - start_time

            # Extract prediction from response
            response = result.response if hasattr(result, 'response') else str(result)
            prediction = self._extract_prediction(response, len(example.options))

            if prediction is not None:
                if self.verbose:
                    print(f"[RLM-V2] Got prediction: {[f'{p:.3f}' for p in prediction]}")
                return prediction, stats

        except Exception as e:
            if self.verbose:
                print(f"[RLM-V2] Error: {e}")

        # Fallback
        stats.fallback_used = True
        stats.execution_time = time.time() - start_time
        return self._fallback_last_price(example), stats

    def _build_user_prompt(self, context: Dict[str, Any]) -> str:
        """Build the user prompt."""
        market = context["market"]
        return f"""Predict this market: {market['title']}

Cutoff date: {context['cutoff_ts']} (use NO information after this date)
Options: {market['options']}
Source: {market['source']}

Analyze the data and provide your prediction."""

    def fit(self, train_batches: List[Batch], spec: Dict[str, Any]) -> None:
        """Build search index from training data."""
        all_examples = []
        for batch in train_batches:
            all_examples.extend(batch.examples)

        self._all_examples = all_examples
        self._build_search_index(all_examples)

        if self.verbose:
            print(f"[RLM-V2] Fit complete: {len(all_examples)} training examples")

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[List[float]]:
        """Generate predictions for a batch."""
        if self._search_index is None:
            self._build_search_index(batch.examples)

        predictions = []
        for i, example in enumerate(batch.examples):
            if self.verbose:
                print(f"\n[RLM-V2] Predicting {i+1}/{len(batch.examples)}: {example.event_id}")

            try:
                pred, stats = self._predict_single(example)
                self.session_stats.add(stats)
            except Exception as e:
                if self.verbose:
                    print(f"[RLM-V2] Error: {e}")
                pred = self._fallback_last_price(example)
                stats = RLMPredictionStats(event_id=example.event_id, fallback_used=True)
                self.session_stats.add(stats)

            predictions.append(pred)

        return predictions

    def get_usage_stats(self) -> Dict[str, Any]:
        """Return session statistics."""
        return {
            "session": self.session_stats.summary(),
        }

    def print_stats(self):
        """Print session statistics."""
        stats = self.get_usage_stats()
        print("\n" + "="*50)
        print("RLM-V2 SESSION STATISTICS")
        print("="*50)
        print(f"Predictions: {stats['session']['total_predictions']}")
        print(f"Avg Iterations: {stats['session']['avg_iterations']:.2f}")
        print(f"Fallback Rate: {stats['session']['fallback_rate']:.1%}")
        print("="*50 + "\n")


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 RLM V2 Implementation:
#
# KEY DESIGN DECISIONS:
# 1. Use external/rlm RLM class for orchestration - handles iteration, parsing
# 2. Use LocalREPL with setup_code to inject helper functions
# 3. Pass parquet schema documentation in context for awareness
# 4. Helper functions pre-compute results at setup time (serialized into code)
#
# LIMITATIONS:
# 1. setup_code must be serializable Python code (no closures)
# 2. Search results are pre-computed, not dynamic
# 3. llm_query() requires LMHandler running (handled by RLM class)
#
# INTEGRATION NOTES:
# - external/rlm expects context as dict or str
# - FINAL_VAR(prediction) pattern used for output
# - System prompt must tell model about available functions
#
# FUTURE IMPROVEMENTS:
# - Dynamic search queries via llm_query() callback
# - Better error handling for REPL execution
# - Token tracking from RLM usage summary
#
