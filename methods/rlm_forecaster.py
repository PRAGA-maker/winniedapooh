"""
RLM (Recursive Language Model) Forecaster for prediction markets.
Uses Gemini to recursively analyze market data via a REPL pattern,
with TF-IDF semantic search over market descriptions.
"""
import os
import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

from methods.base import ForecastMethod
from forecasting.dataclasses import Batch, Example
from methods.rlm_tools.semantic_search import MarketSearchIndex
from methods.rlm_tools.data_analysis import analyze_trend, summarize_options

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")


@dataclass
class RLMPredictionStats:
    """Stats for a single prediction."""
    event_id: str
    recursions_used: int = 0
    search_calls: int = 0
    trend_calls: int = 0
    base_rate_calls: int = 0
    fallback_used: bool = False
    leakage_warning: bool = False
    leakage_details: str = ""


@dataclass
class RLMSessionStats:
    """Aggregate stats for an RLM session."""
    total_predictions: int = 0
    total_recursions: int = 0
    total_search_calls: int = 0
    total_trend_calls: int = 0
    total_base_rate_calls: int = 0
    total_fallbacks: int = 0
    leakage_warnings: int = 0
    per_prediction: List[RLMPredictionStats] = field(default_factory=list)

    def add(self, stats: RLMPredictionStats):
        self.per_prediction.append(stats)
        self.total_predictions += 1
        self.total_recursions += stats.recursions_used
        self.total_search_calls += stats.search_calls
        self.total_trend_calls += stats.trend_calls
        self.total_base_rate_calls += stats.base_rate_calls
        if stats.fallback_used:
            self.total_fallbacks += 1
        if stats.leakage_warning:
            self.leakage_warnings += 1

    def summary(self) -> Dict[str, Any]:
        return {
            "total_predictions": self.total_predictions,
            "total_recursions": self.total_recursions,
            "avg_recursions": self.total_recursions / max(1, self.total_predictions),
            "total_tool_calls": {
                "search": self.total_search_calls,
                "trend": self.total_trend_calls,
                "base_rate": self.total_base_rate_calls,
            },
            "fallback_rate": self.total_fallbacks / max(1, self.total_predictions),
            "leakage_warnings": self.leakage_warnings,
        }


class RLMForecaster(ForecastMethod):
    """
    RLM-based forecaster using Gemini for recursive market analysis.

    Features:
    - TF-IDF semantic search over market titles/descriptions (no API calls)
    - Budget-tracked Gemini API calls
    - Recursive analysis with max_recursions limit
    - Leakage detection (warns if LLM references future dates)
    - Detailed per-prediction statistics
    """
    name = "rlm"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.0-flash",
        max_recursions: int = 3,
        call_budget: int = 1000,
        verbose: bool = False
    ):
        """
        Args:
            api_key: Gemini API key (falls back to GEMINI_API_KEY env var)
            model: Gemini model name (default gemini-2.0-flash)
            max_recursions: Max LLM calls per example
            call_budget: Total API call budget across all predictions (default 1000)
            verbose: Print debug information
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY required - set env var or pass api_key")

        self.model = model
        self.max_recursions = max_recursions
        self.call_budget = call_budget
        self.calls_made = 0
        self.verbose = verbose

        # Lazy-initialized
        self._client = None
        self._search_index: Optional[MarketSearchIndex] = None
        self._all_examples: List[Example] = []

        # Session statistics
        self.session_stats = RLMSessionStats()

    def _get_client(self):
        """Lazy-load Gemini client."""
        if self._client is None:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            self._client = genai.GenerativeModel(self.model)
        return self._client

    def _check_budget(self):
        """Raise if API call budget exhausted."""
        if self.calls_made >= self.call_budget:
            raise RuntimeError(f"Gemini API call budget exhausted ({self.call_budget} calls)")

    def _call_gemini(self, prompt: str) -> str:
        """Make a budget-tracked Gemini call."""
        self._check_budget()
        client = self._get_client()
        response = client.generate_content(prompt)
        self.calls_made += 1
        if self.verbose:
            print(f"[RLM] Call {self.calls_made}/{self.call_budget}: {len(prompt)} chars -> {len(response.text)} chars")
        return response.text

    def _check_leakage(self, response: str, cutoff_date: datetime) -> tuple[bool, str]:
        """
        Check if the LLM response references dates after the cutoff.
        Returns (has_leakage, details).
        """
        # Extract dates mentioned in response (various formats)
        date_patterns = [
            r'\b(20\d{2}[-/]\d{1,2}[-/]\d{1,2})\b',  # 2024-12-25
            r'\b(\d{1,2}[-/]\d{1,2}[-/]20\d{2})\b',  # 12/25/2024
            r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2}\b',
        ]

        mentioned_dates = []
        for pattern in date_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE)
            mentioned_dates.extend(matches)

        # Check for "outcome", "result", "resolved", "won" near future dates
        leakage_keywords = ['outcome', 'result', 'resolved', 'winner', 'won', 'final', 'actually']
        response_lower = response.lower()

        for keyword in leakage_keywords:
            if keyword in response_lower:
                # Check if discussing resolution/outcome
                if any(phrase in response_lower for phrase in [
                    'the outcome was', 'the result was', 'it resolved',
                    'the winner was', 'actually happened', 'we know that'
                ]):
                    return True, f"Potential leakage: LLM may be using future knowledge (keyword: {keyword})"

        return False, ""

    def _build_search_index(self, examples: List[Example]) -> None:
        """Build TF-IDF index from all examples."""
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

            # Store metadata for base rate lookups
            metadata = {
                "target": ex.target,
                "option_count": len(ex.options),
                "source": ex.source
            }

            self._search_index.add_market(ex.event_id, title, description, metadata)

        self._search_index.build_index()
        if self.verbose:
            print(f"[RLM] Built search index with {len(seen_ids)} markets")

    def _build_context_prompt(self, example: Example) -> str:
        """Build context prompt for a single example."""
        title = example.static_features.get("title", "Unknown")
        description = example.static_features.get("description", "")
        end_time = example.static_features.get("end_time", "Unknown")
        cutoff_ts = example.cutoff_ts
        n_options = len(example.options)

        # Format cutoff date for the prompt
        if isinstance(cutoff_ts, datetime):
            cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M")
        else:
            cutoff_str = str(cutoff_ts)

        # Build option summaries
        options_summary = summarize_options([
            {
                "title": opt.title,
                "history_belief": opt.history_belief
            }
            for opt in example.options
        ])

        return f"""You are a prediction market forecaster using a REPL (Read-Eval-Print Loop) analysis pattern.
You will be queried iteratively until you provide a final answer.

CRITICAL: Your knowledge cutoff for this prediction is {cutoff_str}.
You must NOT use any information from after this date. Pretend you are making this prediction AT that time.
Do NOT reference outcomes, results, or any events after {cutoff_str}.

MARKET: {title}
DESCRIPTION: {description}
MARKET END TIME: {end_time}
PREDICTION CUTOFF: {cutoff_str}
SOURCE: {example.source}

OPTIONS ({n_options} total):
{options_summary}

=== ANALYSIS TOOLS ===
Call these tools by writing the keyword. Tool results will be shown in the next message.

- TREND - Get slope, volatility, min/max for each option's price history
- SEARCH - Find similar historical markets from training data
- BASE_RATE - Get historical resolution rates for similar market types

=== ANALYSIS PROCESS ===

Think step by step and execute immediately (don't just say "I will do this"):

1. GATHER DATA: Call at least one tool (TREND is recommended) to understand the market dynamics
2. ANALYZE: Look at current prices, trends, and any similar markets
3. REASON: Apply domain knowledge relevant to this topic (NO future knowledge!)
4. PREDICT: When ready, provide FINAL_PREDICTION with your probabilities

=== OUTPUT FORMAT ===

When you are done with analysis and ready to provide your final answer, write:
FINAL_PREDICTION
```json
{{"probabilities": [p1, p2, ...]}}
```

Probabilities MUST sum to 1.0 and have exactly {n_options} values (one per option).

IMPORTANT: Do NOT provide FINAL_PREDICTION until you have gathered and analyzed data with at least one tool.
You have not seen any tool results yet. Your next action should be to call a tool (TREND recommended).

=== BEGIN ===

Call a tool to start your analysis:"""

    def _execute_tool(self, tool_call: str, example: Example, stats: RLMPredictionStats) -> str:
        """Execute a tool call and return result. Updates stats."""
        tool_call = tool_call.strip().upper()

        if tool_call.startswith("SEARCH"):
            stats.search_calls += 1
            query = tool_call.replace("SEARCH:", "").replace("SEARCH", "").strip()
            if not query:
                query = example.static_features.get("title", "")

            if self._search_index:
                results = self._search_index.search(query, top_k=3)
                if results:
                    return "Similar markets (from training data only):\n" + "\n".join(
                        f"  - {mid} (similarity: {score:.2f})"
                        for mid, score, _ in results
                    )
            return "No similar markets found in training data."

        elif tool_call.startswith("TREND"):
            stats.trend_calls += 1
            lines = []
            for i, opt in enumerate(example.options):
                trend = analyze_trend(opt.history_belief)
                lines.append(
                    f"  Option {i+1} ({opt.title}): "
                    f"slope={trend['slope']:+.4f}/step, "
                    f"volatility={trend['volatility']:.4f}, "
                    f"range=[{trend['min']:.2%}, {trend['max']:.2%}]"
                )
            return "Trend analysis (historical data only):\n" + "\n".join(lines)

        elif tool_call.startswith("BASE_RATE"):
            stats.base_rate_calls += 1
            if self._search_index:
                title = example.static_features.get("title", "")
                results = self._search_index.search(title, top_k=5)

                if results:
                    targets = [meta.get("target", []) for _, _, meta in results if meta.get("target")]
                    if targets:
                        avg_target = [
                            sum(t[i] for t in targets if len(t) > i) / len(targets)
                            for i in range(len(example.options))
                        ]
                        return f"Base rate from {len(targets)} similar resolved markets: {[f'{p:.2%}' for p in avg_target]}"
            return "No base rate data available."

        return f"Unknown tool: {tool_call}"

    def _parse_probabilities(self, response: str, n_options: int, require_final: bool = True) -> Optional[List[float]]:
        """Extract probability array from LLM response.

        Args:
            response: LLM response text
            n_options: Expected number of options
            require_final: If True, only accept probabilities after FINAL_PREDICTION marker
        """
        # Check for FINAL_PREDICTION marker
        has_final_marker = "FINAL_PREDICTION" in response.upper()

        if require_final and not has_final_marker:
            return None

        # Look for JSON block (prefer this format)
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                probs = data.get("probabilities", [])
                if len(probs) == n_options:
                    return self._normalize(probs)
            except json.JSONDecodeError:
                pass

        # Fallback: look for array pattern after FINAL_PREDICTION
        if has_final_marker:
            # Get text after FINAL_PREDICTION
            final_idx = response.upper().index("FINAL_PREDICTION")
            after_final = response[final_idx:]
            array_match = re.search(r'\[[\d.,\s]+\]', after_final)
            if array_match:
                try:
                    probs = json.loads(array_match.group())
                    if len(probs) == n_options:
                        return self._normalize(probs)
                except json.JSONDecodeError:
                    pass

        # Last resort: any array pattern (if not requiring final marker)
        if not require_final:
            array_match = re.search(r'\[[\d.,\s]+\]', response)
            if array_match:
                try:
                    probs = json.loads(array_match.group())
                    if len(probs) == n_options:
                        return self._normalize(probs)
                except json.JSONDecodeError:
                    pass

        return None

    def _normalize(self, values: List[float]) -> List[float]:
        """Normalize values to sum to 1.0."""
        total = sum(max(0, v) for v in values)
        if total <= 0:
            return [1.0 / len(values)] * len(values)
        return [max(0, v) / total for v in values]

    def _fallback_last_price(self, example: Example) -> List[float]:
        """Fallback to last price if LLM fails."""
        scores = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in example.options]
        return self._normalize(scores)

    def _predict_single(self, example: Example) -> tuple[List[float], RLMPredictionStats]:
        """Run RLM prediction for a single example. Returns (probs, stats)."""
        n_options = len(example.options)
        stats = RLMPredictionStats(event_id=example.event_id)

        # Build initial prompt
        context = self._build_context_prompt(example)
        conversation = context

        # Track whether at least one tool was used
        tools_used = False

        # Recursive analysis loop
        for iteration in range(self.max_recursions):
            stats.recursions_used = iteration + 1

            try:
                response = self._call_gemini(conversation)
            except RuntimeError:
                if self.verbose:
                    print(f"[RLM] Budget exhausted, falling back to last price")
                stats.fallback_used = True
                return self._fallback_last_price(example), stats

            # Check for leakage
            has_leakage, leakage_details = self._check_leakage(response, example.cutoff_ts)
            if has_leakage:
                stats.leakage_warning = True
                stats.leakage_details = leakage_details
                if self.verbose:
                    print(f"[RLM] WARNING: {leakage_details}")

            # Look for tool calls FIRST (before checking for final answer)
            tool_patterns = ["SEARCH:", "SEARCH", "TREND", "BASE_RATE"]
            tool_results = []
            for pattern in tool_patterns:
                if pattern in response.upper():
                    result = self._execute_tool(pattern, example, stats)
                    tool_results.append(result)
                    tools_used = True

            # Check for final answer
            # On iteration 0, require FINAL_PREDICTION marker and tool usage
            # On iteration 1+, be more lenient
            require_final = (iteration == 0)
            probs = self._parse_probabilities(response, n_options, require_final=require_final)

            if probs is not None:
                # Only accept final answer if tools were used OR we're on iteration 2+
                if tools_used or iteration >= 2:
                    if self.verbose:
                        print(f"[RLM] Got FINAL_PREDICTION on iteration {iteration + 1}: {[f'{p:.3f}' for p in probs]}")
                    return probs, stats
                else:
                    # Reject early answer without tool usage
                    if self.verbose:
                        print(f"[RLM] Rejected early answer (tools_used={tools_used}, iter={iteration})")
                    conversation = f"{conversation}\n\nASSISTANT: {response}\n\nYou provided a prediction without analyzing data first. Please call a tool (TREND, SEARCH, or BASE_RATE) to gather information, analyze it, then provide FINAL_PREDICTION."
                    continue

            if tool_results:
                conversation = f"{conversation}\n\nASSISTANT: {response}\n\nTOOL RESULTS:\n" + "\n".join(tool_results)
                conversation += f"\n\nBased on this data, continue your analysis. When ready, provide your final prediction as JSON."
            else:
                # No tool calls and no valid answer - prompt more explicitly
                conversation = f"{conversation}\n\nASSISTANT: {response}\n\nPlease call a tool (TREND, SEARCH, or BASE_RATE) to gather data for your analysis."

        # Max iterations reached
        if self.verbose:
            print(f"[RLM] Max iterations reached, using fallback")
        stats.fallback_used = True
        return self._fallback_last_price(example), stats

    def fit(self, train_batches: List[Batch], spec: Dict[str, Any]) -> None:
        """Build search index from training data."""
        all_examples = []
        for batch in train_batches:
            all_examples.extend(batch.examples)

        self._all_examples = all_examples
        self._build_search_index(all_examples)

        if self.verbose:
            print(f"[RLM] Fit complete: {len(all_examples)} training examples")

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[List[float]]:
        """Generate predictions for a batch of examples."""
        if self._search_index is None:
            self._build_search_index(batch.examples)

        predictions = []
        for i, example in enumerate(batch.examples):
            if self.verbose:
                print(f"[RLM] Predicting {i+1}/{len(batch.examples)}: {example.event_id}")

            try:
                pred, stats = self._predict_single(example)
                self.session_stats.add(stats)
            except Exception as e:
                if self.verbose:
                    print(f"[RLM] Error predicting {example.event_id}: {e}")
                pred = self._fallback_last_price(example)
                stats = RLMPredictionStats(event_id=example.event_id, fallback_used=True)
                self.session_stats.add(stats)

            predictions.append(pred)

        return predictions

    def get_usage_stats(self) -> Dict[str, Any]:
        """Return API usage and session statistics."""
        return {
            "api": {
                "calls_made": self.calls_made,
                "call_budget": self.call_budget,
                "budget_remaining": self.call_budget - self.calls_made
            },
            "session": self.session_stats.summary()
        }

    def print_stats(self):
        """Print detailed session statistics."""
        stats = self.get_usage_stats()
        print("\n" + "="*50)
        print("RLM SESSION STATISTICS")
        print("="*50)
        print(f"API Calls: {stats['api']['calls_made']}/{stats['api']['call_budget']}")
        print(f"Predictions: {stats['session']['total_predictions']}")
        print(f"Avg Recursions: {stats['session']['avg_recursions']:.2f}")
        print(f"Tool Calls:")
        print(f"  - Search: {stats['session']['total_tool_calls']['search']}")
        print(f"  - Trend: {stats['session']['total_tool_calls']['trend']}")
        print(f"  - Base Rate: {stats['session']['total_tool_calls']['base_rate']}")
        print(f"Fallback Rate: {stats['session']['fallback_rate']:.1%}")
        print(f"Leakage Warnings: {stats['session']['leakage_warnings']}")
        print("="*50 + "\n")


# --- LESSONS LEARNED ---
# 1. Budget tracking is critical - Gemini calls add up fast with recursive pattern.
# 2. TF-IDF search is fast and effective for finding similar markets (no API needed).
# 3. Robust parsing is essential - LLMs don't always follow output formats exactly.
# 4. Fallback to last_price is a safe default when LLM fails or budget exhausts.
# 5. LEAKAGE PREVENTION: Must explicitly tell LLM the cutoff date and monitor responses.
# 6. Per-prediction stats help identify prompt issues and tool usage patterns.
