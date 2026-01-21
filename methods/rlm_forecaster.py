"""
RLM (Recursive Language Model) Forecaster for prediction markets.
Uses Gemini to recursively analyze market data via a REPL pattern,
with TF-IDF semantic search over market descriptions.
"""
import os
import re
import json
from typing import Any, Dict, List, Optional
from pathlib import Path
from dotenv import load_dotenv

from methods.base import ForecastMethod
from forecasting.dataclasses import Batch, Example
from methods.rlm_tools.semantic_search import MarketSearchIndex
from methods.rlm_tools.data_analysis import analyze_trend, summarize_options

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")


class RLMForecaster(ForecastMethod):
    """
    RLM-based forecaster using Gemini for recursive market analysis.

    Features:
    - TF-IDF semantic search over market titles/descriptions (no API calls)
    - Budget-tracked Gemini API calls
    - Recursive analysis with max_recursions limit
    """
    name = "rlm"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.0-flash",
        max_recursions: int = 3,
        call_budget: int = 100,
        verbose: bool = False
    ):
        """
        Args:
            api_key: Gemini API key (falls back to GEMINI_API_KEY env var)
            model: Gemini model name (default gemini-2.0-flash, use gemini-2.5-pro for higher quality)
            max_recursions: Max LLM calls per example
            call_budget: Total API call budget across all predictions
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
        self._all_examples: List[Example] = []  # For base rate computation

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
        n_options = len(example.options)

        # Build option summaries
        options_summary = summarize_options([
            {
                "title": opt.title,
                "history_belief": opt.history_belief
            }
            for opt in example.options
        ])

        return f"""You are a prediction market forecaster tasked with predicting probability distributions.

CONTEXT: You have market data to analyze. Think step-by-step about what factors influence the outcome.

MARKET: {title}
DESCRIPTION: {description}
END TIME: {end_time}
SOURCE: {example.source}
OPTIONS ({n_options} total):
{options_summary}

ANALYSIS PROCESS:
1. First, examine the current price/belief data for each option - these represent current market consensus
2. Consider the trend (is probability rising, falling, or stable?)
3. Consider volatility (are beliefs bouncing around or steady?)
4. Factor in any domain knowledge about the topic
5. Adjust from current beliefs based on your analysis

TOOLS (mention these keywords and I'll provide results):
- SEARCH: Find similar historical markets
- TREND: Get detailed trend statistics
- BASE_RATE: Historical resolution rates for similar markets

IMPORTANT: Think step-by-step. After analysis, provide your FINAL answer as:
```json
{{"probabilities": [p1, p2, ...]}}
```
Probabilities MUST sum to 1.0 and have exactly {n_options} values.

Begin step-by-step analysis:"""

    def _execute_tool(self, tool_call: str, example: Example) -> str:
        """Execute a tool call and return result."""
        tool_call = tool_call.strip().upper()

        if tool_call.startswith("SEARCH"):
            # Extract query from SEARCH: <query>
            query = tool_call.replace("SEARCH:", "").replace("SEARCH", "").strip()
            if not query:
                query = example.static_features.get("title", "")

            if self._search_index:
                results = self._search_index.search(query, top_k=3)
                if results:
                    return "Similar markets:\n" + "\n".join(
                        f"  - {mid} (similarity: {score:.2f})"
                        for mid, score, _ in results
                    )
            return "No similar markets found."

        elif tool_call.startswith("TREND"):
            # Analyze trends for all options
            lines = []
            for i, opt in enumerate(example.options):
                trend = analyze_trend(opt.history_belief)
                lines.append(
                    f"  Option {i+1} ({opt.title}): "
                    f"slope={trend['slope']:+.4f}/step, "
                    f"volatility={trend['volatility']:.4f}, "
                    f"range=[{trend['min']:.2%}, {trend['max']:.2%}]"
                )
            return "Trend analysis:\n" + "\n".join(lines)

        elif tool_call.startswith("BASE_RATE"):
            # Get base rate from similar resolved markets
            if self._search_index:
                title = example.static_features.get("title", "")
                results = self._search_index.search(title, top_k=5)

                # Compute average target from similar markets
                if results:
                    targets = [meta.get("target", []) for _, _, meta in results if meta.get("target")]
                    if targets:
                        avg_target = [
                            sum(t[i] for t in targets if len(t) > i) / len(targets)
                            for i in range(len(example.options))
                        ]
                        return f"Base rate from {len(targets)} similar markets: {[f'{p:.2%}' for p in avg_target]}"
            return "No base rate data available."

        return f"Unknown tool: {tool_call}"

    def _parse_probabilities(self, response: str, n_options: int) -> Optional[List[float]]:
        """Extract probability array from LLM response."""
        # Look for JSON block
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                probs = data.get("probabilities", [])
                if len(probs) == n_options:
                    return self._normalize(probs)
            except json.JSONDecodeError:
                pass

        # Fallback: look for array pattern
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

    def _predict_single(self, example: Example) -> List[float]:
        """Run RLM prediction for a single example."""
        n_options = len(example.options)

        # Build initial prompt
        context = self._build_context_prompt(example)
        conversation = context

        # Recursive analysis loop
        for iteration in range(self.max_recursions):
            try:
                response = self._call_gemini(conversation)
            except RuntimeError:
                # Budget exhausted
                if self.verbose:
                    print(f"[RLM] Budget exhausted, falling back to last price")
                return self._fallback_last_price(example)

            # Check for final answer
            probs = self._parse_probabilities(response, n_options)
            if probs is not None:
                if self.verbose:
                    print(f"[RLM] Got probabilities on iteration {iteration + 1}: {probs}")
                return probs

            # Look for tool calls (simple pattern matching)
            tool_patterns = ["SEARCH:", "TREND", "BASE_RATE"]
            tool_results = []
            for pattern in tool_patterns:
                if pattern in response.upper():
                    result = self._execute_tool(pattern, example)
                    tool_results.append(result)

            if tool_results:
                # Add tool results to conversation and continue
                conversation = f"{conversation}\n\nASSISTANT: {response}\n\nTOOL RESULTS:\n" + "\n".join(tool_results)
                conversation += "\n\nContinue your analysis and provide final probabilities."
            else:
                # No tools called and no valid output - ask for probabilities directly
                conversation = f"{conversation}\n\nASSISTANT: {response}\n\nPlease provide your final probability prediction in the required JSON format."

        # Max iterations reached - try one more time for probabilities
        if self.verbose:
            print(f"[RLM] Max iterations reached, using last response")
        return self._fallback_last_price(example)

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
        # Ensure search index is built
        if self._search_index is None:
            self._build_search_index(batch.examples)

        predictions = []
        for i, example in enumerate(batch.examples):
            if self.verbose:
                print(f"[RLM] Predicting {i+1}/{len(batch.examples)}: {example.event_id}")

            try:
                pred = self._predict_single(example)
            except Exception as e:
                if self.verbose:
                    print(f"[RLM] Error predicting {example.event_id}: {e}")
                pred = self._fallback_last_price(example)

            predictions.append(pred)

        return predictions

    def get_usage_stats(self) -> Dict[str, Any]:
        """Return API usage statistics."""
        return {
            "calls_made": self.calls_made,
            "call_budget": self.call_budget,
            "budget_remaining": self.call_budget - self.calls_made
        }


# --- LESSONS LEARNED ---
# 1. Budget tracking is critical - Gemini calls add up fast with recursive pattern.
# 2. TF-IDF search is fast and effective for finding similar markets (no API needed).
# 3. Robust parsing is essential - LLMs don't always follow output formats exactly.
# 4. Fallback to last_price is a safe default when LLM fails or budget exhausts.
# 5. The RLM pattern works well for market analysis - tools provide structured data access.
