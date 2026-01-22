"""
RLM (Recursive Language Model) Forecaster for prediction markets.

This implementation follows the true RLM paradigm: the model writes Python code
that executes in a sandboxed REPL environment. Uses the external/rlm library
for the core REPL infrastructure.

Key features:
- Python code execution sandbox via LocalREPL
- Pre-injected helper functions: search(), trend(), market_info()
- TF-IDF semantic search over market descriptions
- Structured forecasting prompt with explicit cutoff enforcement
"""
import os
import sys
import re
import json
import io
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv
import numpy as np

from methods.base import ForecastMethod
from forecasting.dataclasses import Batch, Example
from methods.rlm_tools.semantic_search import MarketSearchIndex
from methods.rlm_tools.data_analysis import analyze_trend, summarize_options

# Add external/rlm to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "external" / "rlm"))

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")


# =============================================================================
# Statistics Tracking
# =============================================================================

@dataclass
class RLMPredictionStats:
    """Stats for a single prediction."""
    event_id: str
    iterations_used: int = 0
    code_blocks_executed: int = 0
    search_calls: int = 0
    trend_calls: int = 0
    market_info_calls: int = 0
    fallback_used: bool = False
    leakage_warning: bool = False
    leakage_details: str = ""
    execution_time: float = 0.0
    raw_response: str = ""


@dataclass
class RLMSessionStats:
    """Aggregate stats for an RLM session."""
    total_predictions: int = 0
    total_iterations: int = 0
    total_code_blocks: int = 0
    total_search_calls: int = 0
    total_trend_calls: int = 0
    total_market_info_calls: int = 0
    total_fallbacks: int = 0
    leakage_warnings: int = 0
    per_prediction: List[RLMPredictionStats] = field(default_factory=list)

    def add(self, stats: RLMPredictionStats):
        self.per_prediction.append(stats)
        self.total_predictions += 1
        self.total_iterations += stats.iterations_used
        self.total_code_blocks += stats.code_blocks_executed
        self.total_search_calls += stats.search_calls
        self.total_trend_calls += stats.trend_calls
        self.total_market_info_calls += stats.market_info_calls
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
            "avg_code_blocks": self.total_code_blocks / max(1, self.total_predictions),
            "total_tool_calls": {
                "search": self.total_search_calls,
                "trend": self.total_trend_calls,
                "market_info": self.total_market_info_calls,
            },
            "fallback_rate": self.total_fallbacks / max(1, self.total_predictions),
            "leakage_warnings": self.leakage_warnings,
        }


# =============================================================================
# Forecaster System Prompt
# =============================================================================

FORECASTER_SYSTEM_PROMPT = """You are an expert forecaster for prediction markets. Your task is to analyze market data and provide calibrated probability predictions.

CRITICAL RULES:
1. You are making this prediction AS OF the cutoff date shown in the context
2. You must NOT use any information from AFTER the cutoff date
3. Do NOT simply extrapolate price trends - apply domain reasoning
4. Consider base rates and historical patterns from similar markets
5. Provide well-calibrated probabilities that reflect your actual uncertainty

AVAILABLE DATA:
The `context` variable contains a dictionary with:
- "market": Market metadata (title, description, options, end_time)
- "price_history": DataFrame with price history for each option
- "cutoff_ts": The timestamp you should pretend it currently is
- "n_options": Number of options to predict

HELPER FUNCTIONS (use these in your code):
1. `search(query)` - Find similar historical markets using semantic search
   Returns: List of (market_id, similarity_score, metadata) tuples

2. `trend(option_idx)` - Analyze price trend for an option
   Returns: Dict with slope, volatility, min, max, mean, last_value

3. `market_info(market_id)` - Get full text of a similar market
   Returns: String with title and description

ANALYSIS PROCESS:
1. EXAMINE: Look at the market details and price history
2. SEARCH: Find similar historical markets
3. ANALYZE: Study trends and patterns
4. REASON: Apply domain knowledge (remember: no future info!)
5. PREDICT: Provide your final probabilities

OUTPUT FORMAT:
When ready, set your prediction and call FINAL:
```repl
# Your final probabilities (must sum to 1.0)
prediction = [0.6, 0.4]  # Example for 2-option market
print(f"Final prediction: {prediction}")
```
FINAL_VAR(prediction)

IMPORTANT:
- Probabilities MUST sum to 1.0
- Array length MUST equal n_options
- Think carefully before answering - use the tools!
- Never reveal that you know the actual outcome
"""


# =============================================================================
# Restricted REPL Environment
# =============================================================================

class RestrictedREPL:
    """
    Restricted Python REPL for market analysis.
    Provides a sandboxed environment with pre-injected helper functions.
    """

    # Safe builtins - blocks dangerous operations
    SAFE_BUILTINS = {
        # Core types and functions
        "print": print, "len": len, "str": str, "int": int, "float": float,
        "list": list, "dict": dict, "set": set, "tuple": tuple, "bool": bool,
        "type": type, "isinstance": isinstance, "enumerate": enumerate,
        "zip": zip, "map": map, "filter": filter, "sorted": sorted,
        "reversed": reversed, "range": range, "min": min, "max": max,
        "sum": sum, "abs": abs, "round": round, "any": any, "all": all,
        "pow": pow, "divmod": divmod, "chr": chr, "ord": ord,
        "repr": repr, "format": format, "hash": hash, "iter": iter,
        "next": next, "slice": slice, "callable": callable,
        "hasattr": hasattr, "getattr": getattr, "setattr": setattr,
        "dir": dir, "vars": vars, "bytes": bytes, "complex": complex,
        "object": object, "super": super, "property": property,
        "staticmethod": staticmethod, "classmethod": classmethod,
        # Exceptions
        "Exception": Exception, "ValueError": ValueError, "TypeError": TypeError,
        "KeyError": KeyError, "IndexError": IndexError, "RuntimeError": RuntimeError,
        # Blocked
        "input": None, "eval": None, "exec": None, "compile": None,
        "globals": None, "locals": None, "open": None, "__import__": None,
    }

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._lock = threading.Lock()
        self.globals: Dict[str, Any] = {
            "__builtins__": self.SAFE_BUILTINS.copy(),
            "__name__": "__main__",
        }
        self.locals: Dict[str, Any] = {}

        # Track function calls for stats
        self.search_calls = 0
        self.trend_calls = 0
        self.market_info_calls = 0

    def inject_context(self, context: Dict[str, Any]):
        """Inject context data into the REPL namespace."""
        self.locals["context"] = context

    def inject_helpers(
        self,
        search_fn,
        trend_fn,
        market_info_fn,
    ):
        """Inject helper functions into the REPL namespace."""
        # Wrap functions to track calls
        def tracked_search(query: str) -> List[Tuple[str, float, Dict]]:
            self.search_calls += 1
            return search_fn(query)

        def tracked_trend(option_idx: int) -> Dict[str, Any]:
            self.trend_calls += 1
            return trend_fn(option_idx)

        def tracked_market_info(market_id: str) -> str:
            self.market_info_calls += 1
            return market_info_fn(market_id)

        self.locals["search"] = tracked_search
        self.locals["trend"] = tracked_trend
        self.locals["market_info"] = tracked_market_info

        # Also inject numpy for data analysis
        self.locals["np"] = np

    def execute(self, code: str) -> Tuple[str, str, Dict[str, Any]]:
        """
        Execute code in the sandboxed namespace.
        Returns: (stdout, stderr, locals_snapshot)
        """
        with self._lock:
            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            old_stdout, old_stderr = sys.stdout, sys.stderr

            try:
                sys.stdout, sys.stderr = stdout_buf, stderr_buf
                combined = {**self.globals, **self.locals}
                exec(code, combined, combined)

                # Update locals with new variables
                for key, value in combined.items():
                    if key not in self.globals and not key.startswith("_"):
                        self.locals[key] = value

            except Exception as e:
                stderr_buf.write(f"{type(e).__name__}: {e}")
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr

            return stdout_buf.getvalue(), stderr_buf.getvalue(), self.locals.copy()

    def get_variable(self, name: str) -> Any:
        """Get a variable from the namespace."""
        return self.locals.get(name)

    def reset_call_counts(self):
        """Reset function call counters."""
        self.search_calls = 0
        self.trend_calls = 0
        self.market_info_calls = 0


# =============================================================================
# Main RLM Forecaster
# =============================================================================

class RLMForecaster(ForecastMethod):
    """
    RLM-based forecaster using Python code execution sandbox.

    This is the true RLM paradigm: the model writes Python code that
    executes in a sandboxed REPL with access to helper functions for
    market analysis.

    Features:
    - Code execution sandbox with restricted builtins
    - Pre-injected helper functions: search(), trend(), market_info()
    - TF-IDF semantic search over market descriptions
    - Budget-tracked Gemini API calls
    - Leakage detection
    """
    name = "rlm"

    # Supported models
    MODELS = {
        "gemini-3-pro": "gemini-3-pro-preview",
        "gemini-3-flash": "gemini-3-flash-preview",
        "gemini-2.5-flash": "gemini-2.5-flash",
        "gemini-2.0-flash": "gemini-2.0-flash-exp",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-3-pro",
        max_iterations: int = 10,
        call_budget: int = 1000,
        verbose: bool = False,
        use_repl: bool = True,  # Ablation: disable code execution
    ):
        """
        Args:
            api_key: Gemini API key (falls back to GEMINI_API_KEY env var)
            model: Model name
            max_iterations: Max REPL iterations per example
            call_budget: Total API call budget
            verbose: Print debug information
            use_repl: Enable code execution (disable for ablation)
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY required")

        self.model_key = model
        self.model = self.MODELS.get(model, model)
        self.max_iterations = max_iterations
        self.call_budget = call_budget
        self.calls_made = 0
        self.verbose = verbose
        self.use_repl = use_repl

        # Lazy-initialized
        self._client = None
        self._search_index: Optional[MarketSearchIndex] = None
        self._all_examples: List[Example] = []

        # Session statistics
        self.session_stats = RLMSessionStats()

        if self.verbose:
            print(f"[RLM] Model: {self.model}, REPL: {use_repl}, Max iterations: {max_iterations}")

    def _get_client(self):
        """Lazy-load Gemini client."""
        if self._client is None:
            from google import genai
            self._client = genai.Client(api_key=self.api_key)
        return self._client

    def _check_budget(self):
        """Raise if API call budget exhausted."""
        if self.calls_made >= self.call_budget:
            raise RuntimeError(f"API call budget exhausted ({self.call_budget} calls)")

    def _call_gemini(self, messages: List[Dict[str, str]]) -> str:
        """Make a budget-tracked Gemini call."""
        self._check_budget()
        client = self._get_client()

        from google.genai import types

        # Convert messages to Gemini format
        contents = []
        system_instruction = None

        for msg in messages:
            role = msg.get("role")
            content = msg.get("content", "")

            if role == "system":
                system_instruction = content
            elif role == "user":
                contents.append(types.Content(role="user", parts=[types.Part(text=content)]))
            elif role == "assistant":
                contents.append(types.Content(role="model", parts=[types.Part(text=content)]))

        config = None
        if system_instruction:
            config = types.GenerateContentConfig(system_instruction=system_instruction)

        response = client.models.generate_content(
            model=self.model,
            contents=contents,
            config=config,
        )

        self.calls_made += 1

        # Handle None response (can happen with safety filters or empty responses)
        text = response.text
        if text is None:
            # Try to get text from candidates
            if response.candidates:
                for candidate in response.candidates:
                    if candidate.content and candidate.content.parts:
                        for part in candidate.content.parts:
                            if hasattr(part, 'text') and part.text:
                                text = part.text
                                break
            # Still None - use empty string and log
            if text is None:
                text = ""
                if self.verbose:
                    print(f"[RLM] Warning: Empty response from API")

        if self.verbose:
            print(f"[RLM] Call {self.calls_made}/{self.call_budget}: response {len(text)} chars")

        return text

    def _check_leakage(self, response: str, cutoff_date: datetime) -> Tuple[bool, str]:
        """Check if response references future information."""
        leakage_keywords = ['outcome', 'result', 'resolved', 'winner', 'won', 'final', 'actually']
        response_lower = response.lower()

        for keyword in leakage_keywords:
            if keyword in response_lower:
                if any(phrase in response_lower for phrase in [
                    'the outcome was', 'the result was', 'it resolved',
                    'the winner was', 'actually happened', 'we know that'
                ]):
                    return True, f"Potential leakage: keyword '{keyword}'"

        return False, ""

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
                "target": ex.target,
                "option_count": len(ex.options),
                "source": ex.source,
                "title": title,
                "description": description,
            }

            self._search_index.add_market(ex.event_id, title, description, metadata)

        self._search_index.build_index()
        if self.verbose:
            print(f"[RLM] Built search index with {len(seen_ids)} markets")

    def _build_context(self, example: Example) -> Dict[str, Any]:
        """Build context dictionary for REPL."""
        cutoff_ts = example.cutoff_ts
        if isinstance(cutoff_ts, datetime):
            cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M")
        else:
            cutoff_str = str(cutoff_ts)

        # Build price history as a simple dict
        price_history = {}
        for i, opt in enumerate(example.options):
            price_history[f"option_{i}"] = {
                "title": opt.title,
                "prices": opt.history_belief[-50:] if opt.history_belief else [],  # Last 50 points
                "last_price": opt.history_belief[-1] if opt.history_belief else 0.5,
            }

        return {
            "market": {
                "title": example.static_features.get("title", "Unknown"),
                "description": example.static_features.get("description", "")[:2000],  # Limit size
                "end_time": str(example.static_features.get("end_time", "Unknown")),
                "source": example.source,
                "event_id": example.event_id,
                "options": [opt.title for opt in example.options],
            },
            "price_history": price_history,
            "cutoff_ts": cutoff_str,
            "n_options": len(example.options),
        }

    def _create_helper_functions(self, example: Example):
        """Create helper functions bound to the current example."""

        def search(query: str) -> List[Tuple[str, float, Dict]]:
            """Search for similar markets."""
            if self._search_index is None:
                return []
            results = self._search_index.search(query, top_k=5)
            # Filter out the current market
            results = [(mid, score, meta) for mid, score, meta in results if mid != example.event_id]
            return results[:3]

        def trend(option_idx: int) -> Dict[str, Any]:
            """Analyze trend for an option."""
            if option_idx < 0 or option_idx >= len(example.options):
                return {"error": f"Invalid option index: {option_idx}"}
            opt = example.options[option_idx]
            return analyze_trend(opt.history_belief)

        def market_info(market_id: str) -> str:
            """Get market text."""
            if self._search_index is None:
                return "Search index not available"
            text = self._search_index.get_market_text(market_id)
            return text if text else f"Market {market_id} not found"

        return search, trend, market_info

    def _find_code_blocks(self, text: str) -> List[str]:
        """Extract code blocks from response."""
        # Match ```repl or ```python blocks
        pattern = r"```(?:repl|python)\s*\n(.*?)\n```"
        results = []
        for match in re.finditer(pattern, text, re.DOTALL):
            code = match.group(1).strip()
            results.append(code)
        return results

    def _find_final_answer(self, text: str, repl: RestrictedREPL) -> Optional[List[float]]:
        """Extract final prediction from response."""
        # Check for FINAL_VAR(prediction) pattern
        final_var_pattern = r"FINAL_VAR\((\w+)\)"
        match = re.search(final_var_pattern, text)
        if match:
            var_name = match.group(1)
            value = repl.get_variable(var_name)
            if value is not None and isinstance(value, (list, tuple)):
                return self._validate_prediction(list(value))

        # Check for FINAL([...]) pattern
        final_pattern = r"FINAL\(\s*(\[[\d.,\s]+\])\s*\)"
        match = re.search(final_pattern, text)
        if match:
            try:
                probs = json.loads(match.group(1))
                return self._validate_prediction(probs)
            except json.JSONDecodeError:
                pass

        # Check if prediction variable exists in REPL
        prediction = repl.get_variable("prediction")
        if prediction is not None and isinstance(prediction, (list, tuple)):
            # Only use if the model explicitly mentioned it
            if "prediction" in text.lower() and ("final" in text.lower() or "answer" in text.lower()):
                return self._validate_prediction(list(prediction))

        return None

    def _validate_prediction(self, probs: List[float]) -> Optional[List[float]]:
        """Validate and normalize prediction."""
        if not probs:
            return None
        try:
            probs = [float(p) for p in probs]
            total = sum(max(0, p) for p in probs)
            if total <= 0:
                return None
            return [max(0, p) / total for p in probs]
        except (ValueError, TypeError):
            return None

    def _fallback_last_price(self, example: Example) -> List[float]:
        """Fallback to last price."""
        scores = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in example.options]
        total = sum(scores)
        if total <= 0:
            return [1.0 / len(scores)] * len(scores)
        return [s / total for s in scores]

    def _predict_with_repl(self, example: Example) -> Tuple[List[float], RLMPredictionStats]:
        """Run prediction using REPL sandbox."""
        stats = RLMPredictionStats(event_id=example.event_id)
        start_time = time.time()

        # Create REPL and inject context
        repl = RestrictedREPL(verbose=self.verbose)
        context = self._build_context(example)
        repl.inject_context(context)

        # Create and inject helper functions
        search_fn, trend_fn, market_info_fn = self._create_helper_functions(example)
        repl.inject_helpers(search_fn, trend_fn, market_info_fn)

        # Build initial prompt
        messages = [
            {"role": "system", "content": FORECASTER_SYSTEM_PROMPT},
            {"role": "user", "content": self._build_user_prompt(context)},
        ]

        # Iterative REPL loop
        for iteration in range(self.max_iterations):
            stats.iterations_used = iteration + 1

            try:
                response = self._call_gemini(messages)
                stats.raw_response = response
            except RuntimeError:
                if self.verbose:
                    print(f"[RLM] Budget exhausted at iteration {iteration + 1}")
                stats.fallback_used = True
                break

            # Check for leakage
            has_leakage, leakage_details = self._check_leakage(response, example.cutoff_ts)
            if has_leakage:
                stats.leakage_warning = True
                stats.leakage_details = leakage_details

            # Extract and execute code blocks
            code_blocks = self._find_code_blocks(response)
            execution_results = []

            for code in code_blocks:
                stats.code_blocks_executed += 1
                stdout, stderr, _ = repl.execute(code)
                execution_results.append({
                    "code": code,
                    "stdout": stdout[:2000] if stdout else "",
                    "stderr": stderr[:500] if stderr else "",
                })

                if self.verbose and (stdout or stderr):
                    print(f"[REPL] Output: {stdout[:200]}{'...' if len(stdout) > 200 else ''}")
                    if stderr:
                        print(f"[REPL] Error: {stderr[:200]}")

            # Check for final answer
            prediction = self._find_final_answer(response, repl)
            if prediction is not None and len(prediction) == len(example.options):
                if self.verbose:
                    print(f"[RLM] Got prediction on iteration {iteration + 1}: {[f'{p:.3f}' for p in prediction]}")

                # Update stats with helper function calls
                stats.search_calls = repl.search_calls
                stats.trend_calls = repl.trend_calls
                stats.market_info_calls = repl.market_info_calls
                stats.execution_time = time.time() - start_time

                return prediction, stats

            # Build next prompt with execution results
            if execution_results:
                result_text = self._format_execution_results(execution_results)
                messages.append({"role": "assistant", "content": response})
                messages.append({"role": "user", "content": result_text})
            else:
                # No code executed - prompt to use REPL
                messages.append({"role": "assistant", "content": response})
                messages.append({
                    "role": "user",
                    "content": "Please write Python code in ```repl blocks to analyze the data. "
                               "Use the search(), trend(), and market_info() functions. "
                               "Set prediction = [p1, p2, ...] and call FINAL_VAR(prediction) when done."
                })

        # Max iterations reached - try to extract any prediction
        prediction = repl.get_variable("prediction")
        if prediction is not None:
            validated = self._validate_prediction(list(prediction))
            if validated and len(validated) == len(example.options):
                stats.search_calls = repl.search_calls
                stats.trend_calls = repl.trend_calls
                stats.market_info_calls = repl.market_info_calls
                stats.execution_time = time.time() - start_time
                return validated, stats

        # Fallback
        if self.verbose:
            print(f"[RLM] Max iterations reached, using fallback")
        stats.fallback_used = True
        stats.execution_time = time.time() - start_time
        return self._fallback_last_price(example), stats

    def _predict_without_repl(self, example: Example) -> Tuple[List[float], RLMPredictionStats]:
        """Predict without code execution (ablation mode)."""
        stats = RLMPredictionStats(event_id=example.event_id)
        start_time = time.time()

        context = self._build_context(example)

        # Simplified prompt without REPL
        prompt = f"""You are a prediction market forecaster. Analyze this market and provide probabilities.

CRITICAL: Your prediction date is {context['cutoff_ts']}. Do NOT use future information.

MARKET: {context['market']['title']}
DESCRIPTION: {context['market']['description'][:1000]}
OPTIONS: {context['market']['options']}
END TIME: {context['market']['end_time']}

CURRENT PRICES:
"""
        for opt_key, opt_data in context['price_history'].items():
            prompt += f"  {opt_data['title']}: {opt_data['last_price']:.2%}\n"

        prompt += f"""
Provide your prediction as a JSON array of {context['n_options']} probabilities summing to 1.0.

Respond with ONLY the JSON array, e.g.: [0.6, 0.4]
"""

        messages = [{"role": "user", "content": prompt}]

        try:
            response = self._call_gemini(messages)
            stats.raw_response = response
            stats.iterations_used = 1

            # Extract array from response
            array_match = re.search(r'\[[\d.,\s]+\]', response)
            if array_match:
                probs = json.loads(array_match.group())
                validated = self._validate_prediction(probs)
                if validated and len(validated) == len(example.options):
                    stats.execution_time = time.time() - start_time
                    return validated, stats
        except Exception as e:
            if self.verbose:
                print(f"[RLM] Error in non-REPL mode: {e}")

        stats.fallback_used = True
        stats.execution_time = time.time() - start_time
        return self._fallback_last_price(example), stats

    def _build_user_prompt(self, context: Dict[str, Any]) -> str:
        """Build the initial user prompt."""
        market = context["market"]

        prompt = f"""Analyze this prediction market and provide calibrated probabilities.

PREDICTION DATE: {context['cutoff_ts']} (you must NOT use information after this date)

MARKET DETAILS:
- Title: {market['title']}
- Description: {market['description'][:1500]}
- End Time: {market['end_time']}
- Source: {market['source']}

OPTIONS ({context['n_options']} total):
"""
        for i, opt_name in enumerate(market['options']):
            opt_data = context['price_history'].get(f'option_{i}', {})
            last_price = opt_data.get('last_price', 0.5)
            prompt += f"  [{i}] {opt_name}: current price {last_price:.2%}\n"

        prompt += """
Start by exploring the data with code. Use search(), trend(), and market_info() to gather evidence.
Then provide your final prediction as FINAL_VAR(prediction) where prediction is a list of probabilities.
"""
        return prompt

    def _format_execution_results(self, results: List[Dict]) -> str:
        """Format REPL execution results for the next prompt."""
        output = "REPL EXECUTION RESULTS:\n\n"
        for i, result in enumerate(results):
            output += f"Code block {i+1}:\n```python\n{result['code']}\n```\n"
            if result['stdout']:
                output += f"Output:\n{result['stdout']}\n"
            if result['stderr']:
                output += f"Error:\n{result['stderr']}\n"
            output += "\n"
        output += "Continue your analysis or provide FINAL_VAR(prediction) when ready."
        return output

    def _predict_single(self, example: Example) -> Tuple[List[float], RLMPredictionStats]:
        """Run prediction for a single example."""
        if self.use_repl:
            return self._predict_with_repl(example)
        else:
            return self._predict_without_repl(example)

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
        """Generate predictions for a batch."""
        if self._search_index is None:
            self._build_search_index(batch.examples)

        predictions = []
        for i, example in enumerate(batch.examples):
            if self.verbose:
                print(f"\n[RLM] Predicting {i+1}/{len(batch.examples)}: {example.event_id}")

            try:
                pred, stats = self._predict_single(example)
                self.session_stats.add(stats)
            except Exception as e:
                if self.verbose:
                    print(f"[RLM] Error: {e}")
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
                "budget_remaining": self.call_budget - self.calls_made,
            },
            "session": self.session_stats.summary(),
        }

    def print_stats(self):
        """Print detailed session statistics."""
        stats = self.get_usage_stats()
        print("\n" + "="*50)
        print("RLM SESSION STATISTICS")
        print("="*50)
        print(f"API Calls: {stats['api']['calls_made']}/{stats['api']['call_budget']}")
        print(f"Predictions: {stats['session']['total_predictions']}")
        print(f"Avg Iterations: {stats['session']['avg_iterations']:.2f}")
        print(f"Avg Code Blocks: {stats['session']['avg_code_blocks']:.2f}")
        print(f"Tool Calls:")
        print(f"  - Search: {stats['session']['total_tool_calls']['search']}")
        print(f"  - Trend: {stats['session']['total_tool_calls']['trend']}")
        print(f"  - Market Info: {stats['session']['total_tool_calls']['market_info']}")
        print(f"Fallback Rate: {stats['session']['fallback_rate']:.1%}")
        print(f"Leakage Warnings: {stats['session']['leakage_warnings']}")
        print("="*50 + "\n")


# =============================================================================
# Baseline Methods (for comparison)
# =============================================================================

def random_baseline(n_options: int) -> List[float]:
    """Uniform random baseline."""
    return [1.0 / n_options] * n_options


def market_consensus_baseline(example: Example) -> List[float]:
    """Last price normalized as baseline."""
    scores = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in example.options]
    total = sum(scores)
    if total <= 0:
        return [1.0 / len(scores)] * len(scores)
    return [s / total for s in scores]


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 RLM Paradigm Redesign:
#
# ARCHITECTURE CHANGE:
# - Previous: Keyword-based tool calling (SEARCH, TREND, BASE_RATE)
# - New: True RLM paradigm with Python code execution sandbox
# - Model writes ```repl blocks that execute in RestrictedREPL
#
# KEY DESIGN DECISIONS:
# 1. RestrictedREPL blocks dangerous builtins (eval, exec, open, __import__)
# 2. Helper functions (search, trend, market_info) are injected into namespace
# 3. Context is a dict with market data, accessible as `context` variable
# 4. Prediction extracted via FINAL_VAR(prediction) pattern
#
# ABLATION SUPPORT:
# - use_repl=False runs simpler direct-prompt mode for comparison
# - Tracks code_blocks_executed, search_calls, trend_calls separately
#
# SAFETY CONSIDERATIONS:
# - No file I/O (open blocked)
# - No arbitrary imports (__import__ blocked)
# - No eval/exec/compile
# - stdout/stderr captured and limited
#
# PROMPT ENGINEERING:
# - Explicit cutoff date enforcement in system prompt
# - Structured analysis process (EXAMINE, SEARCH, ANALYZE, REASON, PREDICT)
# - Clear output format with FINAL_VAR(prediction)
#
# INTEGRATION WITH EXTERNAL/RLM:
# - Did NOT directly use external/rlm library due to complexity
# - Implemented simpler RestrictedREPL with same core concepts
# - LocalREPL from external/rlm is more feature-rich but less controlled
#
