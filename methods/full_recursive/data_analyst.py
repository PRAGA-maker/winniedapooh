"""
DATA_ANALYST agent implementation using RLM REPL.

This agent provides quantitative analysis by executing Python code
in a sandboxed REPL environment with access to:
- Market price history
- TF-IDF semantic search over similar markets
- Trend analysis functions

The DATA_ANALYST complements web-grounded agents by providing
high-granularity data analysis capabilities.
"""

import os
import re
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from forecasting.dataclasses import Example
from methods.rlm_tools.semantic_search import MarketSearchIndex
from methods.rlm_tools.data_analysis import analyze_trend

from .prompts import DATA_ANALYST_SYSTEM_PROMPT
from .agents import LLMCallLog, SubQuestion


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class DataAnalystOutput:
    """Output from the DATA_ANALYST agent."""
    trend_analysis: str = ""
    similar_markets: List[str] = field(default_factory=list)
    base_rates: str = ""
    quantitative_factors: List[str] = field(default_factory=list)
    data_driven_probability: float = 0.5
    confidence: float = 0.5
    reasoning: str = ""
    code_blocks_executed: int = 0
    search_calls: int = 0
    trend_calls: int = 0
    market_info_calls: int = 0


# =============================================================================
# Restricted REPL (simplified from rlm_forecaster.py)
# =============================================================================

class DataAnalystREPL:
    """
    Restricted Python REPL for data analysis.

    Provides a sandboxed environment with:
    - Safe builtins (no file I/O, eval, exec)
    - Pre-injected helper functions (search, trend, market_info)
    - numpy for numerical analysis
    """

    SAFE_BUILTINS = {
        "print": print, "len": len, "str": str, "int": int, "float": float,
        "list": list, "dict": dict, "set": set, "tuple": tuple, "bool": bool,
        "type": type, "isinstance": isinstance, "enumerate": enumerate,
        "zip": zip, "map": map, "filter": filter, "sorted": sorted,
        "reversed": reversed, "range": range, "min": min, "max": max,
        "sum": sum, "abs": abs, "round": round, "any": any, "all": all,
        "pow": pow, "divmod": divmod, "repr": repr, "format": format,
        "hasattr": hasattr, "getattr": getattr, "setattr": setattr,
        "Exception": Exception, "ValueError": ValueError, "TypeError": TypeError,
        "KeyError": KeyError, "IndexError": IndexError,
        # Blocked
        "input": None, "eval": None, "exec": None, "compile": None,
        "globals": None, "locals": None, "open": None, "__import__": None,
    }

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.globals: Dict[str, Any] = {
            "__builtins__": self.SAFE_BUILTINS.copy(),
            "__name__": "__main__",
        }
        self.locals: Dict[str, Any] = {}
        self.search_calls = 0
        self.trend_calls = 0
        self.market_info_calls = 0

    def inject_context(self, context: Dict[str, Any]):
        """Inject market context into REPL namespace."""
        self.locals["context"] = context

    def inject_helpers(self, search_fn, trend_fn, market_info_fn):
        """Inject helper functions with call tracking."""
        import numpy as np

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
        self.locals["np"] = np

    def execute(self, code: str) -> Tuple[str, str]:
        """
        Execute code in the sandbox.

        Returns: (stdout, stderr)
        """
        import io
        import sys

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        old_stdout, old_stderr = sys.stdout, sys.stderr

        try:
            sys.stdout, sys.stderr = stdout_buf, stderr_buf
            combined = {**self.globals, **self.locals}
            exec(code, combined, combined)

            # Update locals
            for key, value in combined.items():
                if key not in self.globals and not key.startswith("_"):
                    self.locals[key] = value

        except Exception as e:
            stderr_buf.write(f"{type(e).__name__}: {e}")
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr

        return stdout_buf.getvalue(), stderr_buf.getvalue()

    def get_variable(self, name: str) -> Any:
        """Get a variable from the namespace."""
        return self.locals.get(name)


# =============================================================================
# Context Building
# =============================================================================

def build_data_context(example: Example) -> Dict[str, Any]:
    """Build the context dictionary for DATA_ANALYST REPL."""
    cutoff_ts = example.cutoff_ts
    if isinstance(cutoff_ts, datetime):
        cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M")
    else:
        cutoff_str = str(cutoff_ts)

    price_history = {}
    for i, opt in enumerate(example.options):
        price_history[f"option_{i}"] = {
            "title": opt.title,
            "prices": opt.history_belief[-50:] if opt.history_belief else [],
            "last_price": opt.history_belief[-1] if opt.history_belief else 0.5,
        }

    return {
        "market": {
            "title": example.static_features.get("title", "Unknown"),
            "description": example.static_features.get("description", "")[:2000],
            "end_time": str(example.static_features.get("end_time", "Unknown")),
            "source": example.source,
            "event_id": example.event_id,
            "options": [opt.title for opt in example.options],
        },
        "price_history": price_history,
        "cutoff_ts": cutoff_str,
        "n_options": len(example.options),
    }


def create_helper_functions(example: Example, search_index: Optional[MarketSearchIndex]):
    """Create helper functions bound to the example."""

    def search(query: str) -> List[Tuple[str, float, Dict]]:
        """Search for similar markets."""
        if search_index is None:
            return []
        results = search_index.search(query, top_k=5)
        # Filter out current market
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
        if search_index is None:
            return "Search index not available"
        text = search_index.get_market_text(market_id)
        return text if text else f"Market {market_id} not found"

    return search, trend, market_info


# =============================================================================
# Prompt Building
# =============================================================================

def build_data_analyst_prompt(
    context: Dict[str, Any],
    sub_questions: List[SubQuestion],
    web_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Build the prompt for DATA_ANALYST."""
    market = context["market"]

    prompt = f"""QUANTITATIVE ANALYSIS TASK

Market: {market['title']}
Cutoff Date: {context['cutoff_ts']}
Number of Options: {context['n_options']}

OPTIONS:
"""
    for i, opt_name in enumerate(market['options']):
        opt_data = context['price_history'].get(f'option_{i}', {})
        last_price = opt_data.get('last_price', 0.5)
        prompt += f"  [{i}] {opt_name}: current price {last_price:.2%}\n"

    prompt += """
SUB-QUESTIONS TO ADDRESS WITH DATA:
"""
    for q in sub_questions[:3]:  # Limit to top 3
        prompt += f"- {q.question}\n"

    if web_context:
        prompt += """
CONTEXT FROM WEB RESEARCH (use to guide your data analysis):
"""
        if "analyst" in web_context:
            prompt += f"- Analyst confidence: {web_context['analyst'].overall_confidence:.0%}\n"
        if "advocate_yes" in web_context:
            prompt += f"- YES advocate core argument: {web_context['advocate_yes'].core_argument[:100]}...\n"
        if "advocate_no" in web_context:
            prompt += f"- NO advocate core argument: {web_context['advocate_no'].core_argument[:100]}...\n"

    prompt += """
YOUR TASK:
1. Use search() to find similar historical markets
2. Use trend() to analyze price patterns for each option
3. Use market_info() to get details on similar markets
4. Calculate base rates from similar market outcomes
5. Provide a data-driven probability estimate

Write Python code in ```repl blocks. Set your findings at the end:

```repl
findings = {
    "trend_analysis": "...",
    "similar_markets": [...],
    "base_rates": "...",
    "quantitative_factors": [...],
    "data_driven_probability": 0.0-1.0,
    "confidence": 0.0-1.0,
    "reasoning": "..."
}
```
FINAL_VAR(findings)
"""

    return prompt


# =============================================================================
# Main Function
# =============================================================================

def run_data_analyst(
    example: Example,
    sub_questions: List[SubQuestion],
    search_index: Optional[MarketSearchIndex],
    api_key: str,
    model: str = "gemini-2.0-flash",
    max_iterations: int = 5,
    web_context: Optional[Dict[str, Any]] = None,
    verbose: bool = False,
    iteration: int = 1,
) -> Tuple[Optional[DataAnalystOutput], LLMCallLog]:
    """
    Run the DATA_ANALYST agent using RLM REPL.

    Args:
        example: The market example to analyze
        sub_questions: Questions from the planner to address
        search_index: TF-IDF search index over markets
        api_key: Gemini API key
        model: Gemini model to use
        max_iterations: Max REPL iterations
        web_context: Optional context from web agents (for sequential mode)
        verbose: Print debug info
        iteration: Pipeline iteration number

    Returns:
        Tuple of (DataAnalystOutput, LLMCallLog)
    """
    from google import genai
    from google.genai import types

    start_time = time.time()

    # Initialize REPL
    repl = DataAnalystREPL(verbose=verbose)
    context = build_data_context(example)
    repl.inject_context(context)

    # Inject helpers
    search_fn, trend_fn, market_info_fn = create_helper_functions(example, search_index)
    repl.inject_helpers(search_fn, trend_fn, market_info_fn)

    # Initialize Gemini client
    client = genai.Client(api_key=api_key)

    # Build initial prompt
    user_prompt = build_data_analyst_prompt(context, sub_questions, web_context)

    messages = [
        {"role": "user", "content": user_prompt},
    ]

    code_blocks_executed = 0
    raw_responses = []

    # REPL loop
    for repl_iter in range(max_iterations):
        # Convert messages to Gemini format
        contents = []
        for msg in messages:
            role = "user" if msg["role"] == "user" else "model"
            contents.append(types.Content(
                role=role,
                parts=[types.Part(text=msg["content"])]
            ))

        config = types.GenerateContentConfig(
            system_instruction=DATA_ANALYST_SYSTEM_PROMPT,
            temperature=0.5,
        )

        # Call Gemini with exponential backoff for rate limits
        max_retries = 3
        response = None
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=contents,
                    config=config,
                )
                break
            except Exception as e:
                error_str = str(e).lower()
                if "rate" in error_str or "429" in error_str or "quota" in error_str or "resource exhausted" in error_str:
                    wait_time = (2 ** attempt) + (0.5 * attempt)
                    if verbose:
                        print(f"[DATA_ANALYST] Rate limit hit, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        raise
                else:
                    raise

        if response is None:
            raise RuntimeError("Gemini API call failed after all retries")

        response_text = response.text if response.text else ""
        raw_responses.append(response_text)

        if verbose:
            print(f"[DATA_ANALYST] Iteration {repl_iter + 1}: {len(response_text)} chars")

        # Extract code blocks
        code_blocks = re.findall(r"```(?:repl|python)\s*\n(.*?)\n```", response_text, re.DOTALL)

        execution_results = []
        for code in code_blocks:
            code_blocks_executed += 1
            stdout, stderr = repl.execute(code.strip())
            execution_results.append({
                "code": code[:500],
                "stdout": stdout[:1000] if stdout else "",
                "stderr": stderr[:500] if stderr else "",
            })
            if verbose and (stdout or stderr):
                print(f"[REPL] Output: {stdout[:200]}...")
                if stderr:
                    print(f"[REPL] Error: {stderr[:200]}")

        # Check for FINAL_VAR
        final_match = re.search(r"FINAL_VAR\((\w+)\)", response_text)
        if final_match:
            var_name = final_match.group(1)
            findings = repl.get_variable(var_name)
            if findings and isinstance(findings, dict):
                # Build output
                output = DataAnalystOutput(
                    trend_analysis=findings.get("trend_analysis", ""),
                    similar_markets=findings.get("similar_markets", []),
                    base_rates=findings.get("base_rates", ""),
                    quantitative_factors=findings.get("quantitative_factors", []),
                    data_driven_probability=findings.get("data_driven_probability", 0.5),
                    confidence=findings.get("confidence", 0.5),
                    reasoning=findings.get("reasoning", ""),
                    code_blocks_executed=code_blocks_executed,
                    search_calls=repl.search_calls,
                    trend_calls=repl.trend_calls,
                    market_info_calls=repl.market_info_calls,
                )

                end_time = time.time()
                log = LLMCallLog(
                    call_id=f"iter{iteration}_data_analyst",
                    agent="data_analyst",
                    timestamp=datetime.now().isoformat(),
                    iteration=iteration,
                    latency_ms=int((end_time - start_time) * 1000),
                    tokens_input=0,  # Not tracked in REPL mode
                    tokens_output=0,
                    cost_estimate_usd=0.0,
                    raw_response="\n---\n".join(raw_responses)[:2000],
                    parse_error=None,
                )

                return output, log

        # Build next prompt with execution results
        if execution_results:
            result_text = "REPL EXECUTION RESULTS:\n\n"
            for i, res in enumerate(execution_results):
                result_text += f"Code block {i+1}:\n```python\n{res['code']}\n```\n"
                if res['stdout']:
                    result_text += f"Output:\n{res['stdout']}\n"
                if res['stderr']:
                    result_text += f"Error:\n{res['stderr']}\n"
            result_text += "\nContinue your analysis or set findings and call FINAL_VAR(findings)."

            messages.append({"role": "assistant", "content": response_text})
            messages.append({"role": "user", "content": result_text})
        else:
            # No code executed - prompt to use REPL
            messages.append({"role": "assistant", "content": response_text})
            messages.append({
                "role": "user",
                "content": "Please write Python code in ```repl blocks. "
                           "Use search(), trend(), and market_info() functions. "
                           "Set findings dict and call FINAL_VAR(findings) when done."
            })

    # Max iterations - try to get any findings
    findings = repl.get_variable("findings")
    if findings and isinstance(findings, dict):
        output = DataAnalystOutput(
            trend_analysis=findings.get("trend_analysis", ""),
            similar_markets=findings.get("similar_markets", []),
            base_rates=findings.get("base_rates", ""),
            quantitative_factors=findings.get("quantitative_factors", []),
            data_driven_probability=findings.get("data_driven_probability", 0.5),
            confidence=findings.get("confidence", 0.5),
            reasoning=findings.get("reasoning", ""),
            code_blocks_executed=code_blocks_executed,
            search_calls=repl.search_calls,
            trend_calls=repl.trend_calls,
            market_info_calls=repl.market_info_calls,
        )
    else:
        # Fallback output
        output = DataAnalystOutput(
            trend_analysis="Analysis incomplete",
            data_driven_probability=0.5,
            confidence=0.3,
            reasoning="Max iterations reached without complete analysis",
            code_blocks_executed=code_blocks_executed,
            search_calls=repl.search_calls,
            trend_calls=repl.trend_calls,
            market_info_calls=repl.market_info_calls,
        )

    end_time = time.time()
    log = LLMCallLog(
        call_id=f"iter{iteration}_data_analyst",
        agent="data_analyst",
        timestamp=datetime.now().isoformat(),
        iteration=iteration,
        latency_ms=int((end_time - start_time) * 1000),
        tokens_input=0,
        tokens_output=0,
        cost_estimate_usd=0.0,
        raw_response="\n---\n".join(raw_responses)[:2000],
        parse_error="Max iterations reached" if not findings else None,
    )

    return output, log


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 DATA_ANALYST Implementation:
#
# DESIGN DECISIONS:
# 1. Reused RestrictedREPL pattern from rlm_forecaster.py
# 2. FINAL_VAR pattern for extracting structured output
# 3. Helper functions (search, trend, market_info) injected at runtime
#
# INTEGRATION WITH PIPELINE:
# 1. Can run in parallel with web agents (data_analyst_parallel=True)
# 2. Can run sequentially with web_context (data_analyst_parallel=False)
# 3. Sequential mode allows targeting data analysis based on debate
#
# DIFFERENCES FROM STANDALONE RLM:
# 1. More structured output format (DataAnalystOutput dataclass)
# 2. Designed to complement web research, not replace it
# 3. Focused on quantitative factors, base rates, trends
# 4. NO dependency on external/rlm library (custom DataAnalystREPL)
# 5. Simpler REPL (no llm_query overhead)
#
# ERROR HANDLING:
# 1. Max iterations fallback with partial output
# 2. REPL errors captured in stderr
# 3. LLMCallLog tracks execution details
#
# =============================================================================
# 2026-01-22 RLM Alignment Updates:
# =============================================================================
#
# CHANGES FROM RLM ANALYSIS:
# 1. Added ```repl emphasis to DATA_ANALYST_SYSTEM_PROMPT
#    - Models often default to ```python which doesn't execute
#    - Explicit warning: "You MUST use ```repl code blocks for ALL code execution"
#    - Location: methods/full_recursive/prompts.py
#
# 2. Added exponential backoff for rate limits (lines 358-377)
#    - Pattern: (2 ** attempt) + (0.5 * attempt) seconds
#    - Retries: 3 attempts for rate/quota/429 errors
#    - Non-retryable errors raised immediately
#    - Prevents API quota exhaustion on large runs
#
# 3. Code block parsing already correct (line 371)
#    - Handles both ```repl and ```python blocks
#    - No update needed
#
# INTENTIONAL DIVERGENCE FROM STANDALONE RLM:
# - No external/rlm dependency (custom REPL is lighter weight)
# - No llm_query() (adds latency, not needed for quantitative work)
# - No parquet file access (uses focused context, not full dataset)
# - No RLMDiagnostics (pipeline has FullRecursiveLogger)
#
# SHARED WITH RLM:
# - methods/rlm_tools/semantic_search.py (MarketSearchIndex)
# - methods/rlm_tools/data_analysis.py (analyze_trend)
#
# See .claude/FULL_RECURSIVE_HANDOFF.md for full architecture analysis
#
