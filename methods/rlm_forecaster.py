"""
RLM (Recursive Language Model) Forecaster for prediction markets.

This implementation uses the external/rlm library for the true RLM paradigm:
- The model writes Python code in ```repl blocks
- Code executes in LocalREPL from external/rlm
- llm_query() available for sub-LLM reasoning
- Helper functions (search, trend, market_info) pre-injected via setup_code

Key features:
- Python code execution sandbox via LocalREPL
- Pre-injected helper functions for market analysis
- TF-IDF semantic search over market descriptions
- Full parquet schema documentation in context
- Leakage detection and ablation support
"""
import os
import sys
import re
import json
import time
import random
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
from methods.rlm_tools.data_analysis import analyze_trend, summarize_options

# Import from external/rlm
from rlm import RLM
from rlm.environments.local_repl import LocalREPL
from rlm.utils.parsing import find_code_blocks
from rlm.logger import RLMLogger
from rlm.core.types import RLMIteration, RLMMetadata

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")


# =============================================================================
# Diagnostic Logging (Windows-safe)
# =============================================================================

def _safe_str(s: str) -> str:
    """Convert string to Windows-safe ASCII for logging."""
    return s.encode('ascii', errors='replace').decode('ascii')


class DiagnosticRLMLogger(RLMLogger):
    """Custom RLM logger that forwards to our enhanced diagnostics."""

    def __init__(self, diagnostics: 'RLMDiagnostics'):
        # Don't call super().__init__() - we're not writing to a file
        self.diagnostics = diagnostics
        self._iteration_count = 0
        self._metadata_logged = False

    def log_metadata(self, metadata: RLMMetadata):
        """Log RLM metadata."""
        self._metadata_logged = True
        if self.diagnostics.enabled:
            self.diagnostics.log(f"RLM Metadata: max_iterations={metadata.max_iterations}, "
                               f"backend={metadata.backend}, model={metadata.root_model}")

    def log(self, iteration: RLMIteration):
        """Log an RLMIteration to our diagnostics."""
        self._iteration_count += 1

        if not self.diagnostics.enabled:
            return

        # Extract environment locals from code blocks
        env_locals = {}
        code_block_strs = []
        if iteration.code_blocks:
            for block in iteration.code_blocks:
                code_block_strs.append(block.code)
                if block.result and hasattr(block.result, 'locals'):
                    env_locals.update(block.result.locals)

        # Log iteration
        self.diagnostics.log_iteration(
            iteration_num=self._iteration_count,
            response=iteration.response,
            code_blocks=code_block_strs,
            env_locals=env_locals,
            final_answer=iteration.final_answer
        )

    @property
    def iteration_count(self) -> int:
        return self._iteration_count


class RLMDiagnostics:
    """
    Enhanced diagnostic logger for debugging RLM issues.

    Captures iteration-by-iteration execution data including:
    - Code blocks executed per iteration
    - REPL environment variables after each execution
    - Whether FINAL_VAR was called
    - Completion reason (FINAL_VAR vs iteration exhaustion)
    """

    def __init__(self, enabled: bool = False, output_dir: str = "data/outputs", max_iterations: int = 10):
        self.enabled = enabled
        self.output_dir = Path(output_dir)
        self.log_file = None
        self._entries = []
        self.max_iterations = max_iterations
        self.current_event_id = None
        self.iterations_logged = 0
        self.final_var_called = False
        self.completion_reason = "UNKNOWN"
        self.last_env_vars = []

        if enabled:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file = self.output_dir / f"rlm_diagnostics_{timestamp}.log"

    def log(self, message: str, level: str = "INFO"):
        """Log a message (Windows-safe)."""
        if not self.enabled:
            return
        safe_msg = _safe_str(message)
        entry = f"[{datetime.now().isoformat()}] [{level}] {safe_msg}"
        self._entries.append(entry)
        # Write immediately to file
        if self.log_file:
            with open(self.log_file, 'a', encoding='utf-8', errors='replace') as f:
                f.write(entry + '\n')

    def start_prediction(self, event_id: str):
        """Mark the start of a new prediction."""
        if not self.enabled:
            return
        self.current_event_id = event_id
        self.iterations_logged = 0
        self.final_var_called = False
        self.completion_reason = "UNKNOWN"
        self.last_env_vars = []
        self.log(f"\n{'='*70}")
        self.log(f"STARTING PREDICTION: {event_id}")
        self.log(f"Max iterations: {self.max_iterations}")
        self.log(f"{'='*70}")

    def log_iteration(self, iteration_num: int, response: str, code_blocks: list,
                     env_locals: dict, final_answer: Optional[str]):
        """Log a single iteration with full details."""
        if not self.enabled:
            return

        self.iterations_logged = iteration_num
        marker = f"[ITERATION {iteration_num}/{self.max_iterations}]"

        self.log(f"\n{marker} ===== ITERATION {iteration_num} =====")
        self.log(f"Response length: {len(response)} chars")
        self.log(f"Response preview: {response[:300]}...")

        # Log code blocks
        self.log(f"Code blocks executed: {len(code_blocks)}")
        for i, block in enumerate(code_blocks):
            self.log(f"  Block {i+1} (first 150 chars): {block[:150]}...")

        # Log environment variables (filter out functions and large objects)
        filtered_vars = []
        for key in sorted(env_locals.keys()):
            val = env_locals[key]
            # Skip internal/builtin variables and functions
            if key.startswith('_') or callable(val):
                continue
            # Skip large objects
            try:
                val_repr = repr(val)
                if len(val_repr) > 200:
                    val_repr = val_repr[:200] + "..."
                filtered_vars.append(f"{key}={val_repr}")
            except:
                filtered_vars.append(f"{key}=<repr failed>")

        self.last_env_vars = filtered_vars
        self.log(f"Environment variables: {', '.join(filtered_vars) if filtered_vars else 'None'}")

        # Check for prediction variable
        has_prediction = 'prediction' in env_locals
        self.log(f"'prediction' variable exists: {has_prediction}")

        # Check if FINAL_VAR was called
        if final_answer is not None:
            self.final_var_called = True
            self.completion_reason = "FINAL_VAR_CALLED"
            self.log(f"[FINAL_VAR CALLED] Final answer: {str(final_answer)[:100]}...")

        self.log("=" * 70)

    def log_completion(self, total_iterations: int, final_answer: Optional[str],
                      prediction_extracted: bool, fallback_used: bool):
        """Log completion summary."""
        if not self.enabled:
            return

        # Determine completion reason if not already set
        if self.completion_reason == "UNKNOWN":
            if final_answer is not None and self.final_var_called:
                self.completion_reason = "FINAL_VAR_CALLED"
            elif total_iterations >= self.max_iterations:
                self.completion_reason = "ITERATION_LIMIT_REACHED"
            else:
                self.completion_reason = "ERROR_OR_EARLY_EXIT"

        self.log(f"\n{'='*70}")
        self.log("===== EXECUTION SUMMARY =====")
        self.log(f"Event ID: {self.current_event_id}")
        self.log(f"Total iterations: {total_iterations}")
        self.log(f"Max allowed: {self.max_iterations}")
        self.log(f"FINAL_VAR called: {self.final_var_called}")
        self.log(f"Variables in environment: {self.last_env_vars}")
        self.log(f"'prediction' variable exists: {'prediction' in ' '.join(self.last_env_vars)}")
        self.log(f"Completion reason: {self.completion_reason}")
        self.log(f"Prediction extracted: {prediction_extracted}")
        self.log(f"Fallback used: {fallback_used}")
        self.log(f"{'='*70}\n")

    def log_response(self, event_id: str, raw_response: str, code_blocks: list,
                     prediction_extracted: bool, fallback_used: bool):
        """Log full response details (legacy method for compatibility)."""
        if not self.enabled:
            return
        self.log(f"===== LEGACY LOG: {event_id} =====")
        self.log(f"Raw response length: {len(raw_response)}")
        self.log(f"Code blocks found: {len(code_blocks)}")
        self.log(f"Prediction extracted: {prediction_extracted}")
        self.log(f"Fallback used: {fallback_used}")
        self.log("=" * 50)

    def log_setup_code_error(self, event_id: str, error: str):
        """Log setup code execution errors."""
        if not self.enabled:
            return
        self.log(f"SETUP CODE ERROR [{event_id}]: {error}", level="ERROR")

    def get_log_path(self) -> str:
        """Return path to log file."""
        return str(self.log_file) if self.log_file else ""


# =============================================================================
# Statistics Tracking
# =============================================================================

@dataclass
class RLMPredictionStats:
    """Stats for a single prediction."""
    event_id: str
    iterations_used: int = 0
    api_calls: int = 0  # Actual API calls made (from usage_summary)
    input_tokens: int = 0  # Total input tokens used
    output_tokens: int = 0  # Total output tokens used
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
    total_api_calls: int = 0  # Actual API calls (from usage tracking)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
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
        self.total_api_calls += stats.api_calls
        self.total_input_tokens += stats.input_tokens
        self.total_output_tokens += stats.output_tokens
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
            "total_api_calls": self.total_api_calls,
            "avg_api_calls": self.total_api_calls / max(1, self.total_predictions),
            "total_tokens": {
                "input": self.total_input_tokens,
                "output": self.total_output_tokens,
                "total": self.total_input_tokens + self.total_output_tokens,
            },
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

FORECASTER_SYSTEM_PROMPT = """You are an expert forecaster for prediction markets using a REPL environment.

You can access, transform, and analyze market data interactively by writing ARBITRARY Python code.
You will be queried iteratively until you provide a final prediction.

TASK COMPLETION (CRITICAL):
When you're ready to make your final forecast, you MUST follow this TWO-STEP pattern in your response:

STEP 1: Create the prediction variable in a ```repl code block
STEP 2: Call FINAL_VAR("prediction") IMMEDIATELY after (OUTSIDE the code block, as plain text)

Example of CORRECT completion:
  ```repl
  # After all your analysis, create the prediction array
  prediction = [0.6, 0.4]  # Must sum to 1.0 and have option_count elements
  print(f"Final forecast: {prediction}")
  ```
  FINAL_VAR("prediction")

CRITICAL REQUIREMENTS:
- BOTH steps must happen in the SAME response turn
- The prediction variable must be created with EXECUTABLE code (not comments!)
- Pass the VARIABLE NAME to FINAL_VAR as a string, not the value itself
- Do NOT create prediction in one iteration and call FINAL_VAR in a later iteration
- This is MANDATORY - without calling FINAL_VAR(), your work won't be recorded

COMMON MISTAKES TO AVOID:
- ❌ Calling FINAL_VAR("prediction") without creating the variable first
- ❌ Creating prediction in one turn, then calling FINAL_VAR in a later turn
- ❌ Writing only comments in the code block instead of executable code
- ❌ Passing the value to FINAL_VAR: FINAL_VAR([0.6, 0.4]) is WRONG

CRITICAL RULES:
1. You are making predictions AS OF the cutoff_ts - pretend it's that date NOW
2. You must NOT use any information from AFTER the cutoff date
3. All data in the parquet file is PRE-FILTERED to before cutoff (no leakage)
4. Apply domain reasoning - don't just extrapolate price trends
5. Consider base rates and historical patterns from similar markets
6. Provide well-calibrated probabilities that reflect your actual uncertainty

CODE EXECUTION:
- Write code in ```repl blocks (NOT ```python - that won't execute!)
- You can import libraries: pandas, numpy, json, etc.
- Use print() statements to view outputs and continue reasoning
- Variables persist across code blocks in the same conversation

THE REPL ENVIRONMENT CONTAINS:

1. MARKET METADATA (quick access variables):
   - market_id: str - The market you're predicting
   - cutoff_ts: str - The prediction date (ISO format)
   - option_count: int - Number of options
   - source: str - 'kalshi' or 'metaculus'

2. PRE-LOADED DATA (ready to use immediately):
   - df: DataFrame - ALL market data filtered to before cutoff_ts
   - market_row: Series - The current market's data (already filtered from df)

   Schema:
     * event_id: str - Market identifier
     * source: str - Data source
     * title: str - Market question
     * description: str - Detailed market description
     * status: str - Market status
     * end_time: datetime - Market close time
     * options_json: JSON string - List of option objects with time series data
       Each option has: title, ts (List[str] timestamps), belief (List[float] prices)
     * resolved_value_json: JSON string - Resolution outcome if resolved
     * metadata_json: JSON string - Market-level metadata

3. HELPER FUNCTIONS:
   - llm_query(prompt: str) -> str : Query a sub-LLM (500K context) for complex reasoning
   - FINAL_VAR(variable_name: str) : Return a variable as your final answer

GUIDANCE (these are principles, not mandatory steps):
- Explore the data programmatically (analyze trends, query similar markets, examine history)
- Consider historical patterns, base rates, and domain-specific factors
- Use llm_query() for complex semantic reasoning when needed
- Apply your reasoning to create a calibrated probability distribution
- When ready, call FINAL_VAR(prediction) with your final forecast

MINIMAL EXAMPLE:
```repl
import json

# Data is ALREADY LOADED as 'df' and 'market_row'
# Parse options with time series
options = json.loads(market_row['options_json'])
title = market_row['title']
description = market_row['description']

print(f"Market: {title}")
print(f"Description: {description[:200]}")
print(f"Options: {len(options)}")

# Analyze price history
for i, opt in enumerate(options):
    last_price = opt['belief'][-1] if opt['belief'] else 0.5
    print(f"  Option {i}: {opt['title'][:40]} = {last_price:.3f}")

# Create prediction (your reasoning here)
prediction = [0.6, 0.4]  # Must sum to 1.0 and have option_count values
print(f"Final prediction: {prediction}")
```
FINAL_VAR("prediction")  # IMPORTANT: Pass variable name as string!

REQUIREMENTS:
- Probabilities MUST sum to 1.0
- Array length MUST equal option_count
- You MUST call FINAL_VAR(prediction) to complete the task
- Explore systematically - use print() to debug and understand the data
- All code executes in the same persistent namespace
"""


# =============================================================================
# Setup Code Builder
# =============================================================================

def build_setup_code(search_index: MarketSearchIndex, example: Example, parquet_path: Optional[str] = None) -> str:
    """
    Build setup code for TRUE RLM paradigm with arbitrary code execution.

    CRITICAL: This is the RLM paradigm from the paper:
    - Model writes ARBITRARY Python code to query/analyze data
    - Data is PRE-LOADED into environment (not just path)
    - Model can query the dataframe directly (df variable)
    - Model explores systematically via REPL iteration

    Setup code provides:
    - PRE-LOADED dataframe (df) with all market data
    - Pre-imported libraries (pandas, numpy, json)
    - Minimal helper functions (optional)
    - Market metadata (event_id, cutoff_ts, option_count)
    """

    # Serialize minimal market metadata
    metadata = {
        "event_id": example.event_id,
        "cutoff_ts": example.cutoff_ts.isoformat() if isinstance(example.cutoff_ts, datetime) else str(example.cutoff_ts),
        "option_count": len(example.options),
        "source": example.source,
    }

    metadata_json_repr = repr(json.dumps(metadata, ensure_ascii=True))

    # Convert parquet_path to absolute path so REPL can access it
    # REPL runs in temp directory, needs absolute path to find the file
    if parquet_path:
        from pathlib import Path
        abs_parquet_path = str(Path(parquet_path).resolve()).replace('\\', '\\\\')  # Escape backslashes for Windows
        parquet_path_repr = repr(abs_parquet_path)
    else:
        parquet_path_repr = "None"

    setup_code = f'''
# ============================================================
# RLM Forecaster Setup - TRUE RLM PARADIGM
# Data is LOADED with absolute path for REPL access
# ============================================================

import json
import numpy as np
import pandas as pd

# ============================================================
# MARKET METADATA (minimal - query df for everything else)
# ============================================================

market_metadata = json.loads({metadata_json_repr})

# Quick access variables
market_id = market_metadata["event_id"]
cutoff_ts = market_metadata["cutoff_ts"]
option_count = market_metadata["option_count"]
source = market_metadata["source"]

# ============================================================
# LOAD DATA (READ-ONLY ACCESS)
# ============================================================

# Load market data from absolute path (REPL can access this)
parquet_path = {parquet_path_repr}

if parquet_path and parquet_path != "None":
    df = pd.read_parquet(parquet_path)
    # Filter to current market for convenience
    market_row = df[df['event_id'] == market_id].iloc[0] if len(df[df['event_id'] == market_id]) > 0 else None
else:
    df = pd.DataFrame()
    market_row = None

# DATAFRAME SCHEMA (df is pre-loaded and ready to use):
#
# METADATA COLUMNS:
# - source (str): Data source ('kalshi' or 'metaculus')
# - event_id (str): Unique market identifier (matches market_id above)
# - title (str): Market question/title
# - description (str): Detailed description of the market
# - url (str): Web URL to the market
# - market_type (str): Type of market (e.g., 'event')
# - status (str): Market status ('open', 'closed', 'resolved', 'unknown')
#
# TIME COLUMNS:
# - end_time (datetime64[ns, UTC]): When the market closes
# - created_time (datetime64[ns, UTC]): When the market was created
#
# DATA COLUMNS (JSON strings - must parse with json.loads()):
# - options_json (str): JSON array of option objects
#   Structure: List of dicts, each with:
#     * option_id (str): Unique option identifier
#     * market_id (str): Parent market identifier
#     * title (str): Option description/question
#     * ts (List[str]): Time series timestamps (ISO format)
#     * belief (List[float]): Price history aligned with ts (0.0-1.0 probabilities)
#     * bid, ask (List[float or None]): Order book data
#     * volume, open_interest (List[float]): Trading metrics
#     * metadata_json (str): Option-specific metadata (JSON string)
#     * resolved_value_json (str): Resolution outcome if resolved
#
# - resolved_value_json (str): JSON with resolution outcome (if market resolved)
# - metadata_json (str): JSON object with market-level metadata
#
# IMPORTANT: All data is filtered to BEFORE cutoff_ts (no leakage)

print(f"Market ID: {{market_id}}")
print(f"Cutoff: {{cutoff_ts}}")
print(f"Options: {{option_count}}")
print(f"Data loaded: {{len(df)}} markets available")
print(f"Current market row: {{'Available' if market_row is not None else 'Not found'}}")
print("")
print("You can write ARBITRARY Python code to analyze the data!")
print("Examples:")
print("  # Data is already loaded as 'df' and 'market_row'")
print("  print(market_row['title'])")
print("  options = json.loads(market_row['options_json'])")
print("  # Analyze trends, compute statistics, query similar markets, etc.")
print("")
print("When done, call: FINAL_VAR(prediction)")
print("  where prediction = [p1, p2, ...] with {{option_count}} probabilities")
'''
    return setup_code


# =============================================================================
# Context Builder
# =============================================================================

def build_context(example: Example, parquet_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Build MINIMAL context dictionary for RLM.

    CRITICAL: In the true RLM paradigm, data should NOT be dumped into prompts.
    Instead, the model queries what it needs via REPL functions.

    This context provides only the essential metadata - the model must query
    for descriptions, prices, and other data using helper functions.
    """
    cutoff_ts = example.cutoff_ts
    if isinstance(cutoff_ts, datetime):
        cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M")
    else:
        cutoff_str = str(cutoff_ts)

    return {
        "market_id": example.event_id,
        "title": example.static_features.get("title", "Unknown")[:100],  # Truncated!
        "option_count": len(example.options),
        "cutoff_ts": cutoff_str,
        # NO description, NO price_history, NO parquet_info
        # Model MUST query for these via helper functions
    }


# =============================================================================
# Main RLM Forecaster
# =============================================================================

class RLMForecaster(ForecastMethod):
    """
    RLM-based forecaster using the external/rlm library.

    This is the true RLM paradigm: the model writes Python code that
    executes in LocalREPL with access to helper functions and llm_query().

    Features:
    - Uses external/rlm RLM class for orchestration
    - LocalREPL with setup_code for helper functions
    - TF-IDF semantic search over market descriptions
    - Budget-tracked API calls
    - Leakage detection
    - Ablation support (use_repl=False for direct prompting)
    """
    name = "rlm"

    # Supported models
    MODELS = {
        "gemini-3-pro": "gemini-3-pro-preview",
        "gemini-3-flash": "gemini-3-flash-preview",
        "gemini-2.5-flash": "gemini-2.5-flash",  # Recommended - higher quota limits
        "gemini-2.5-flash-image": "gemini-2.5-flash-image",  # Alternative with image support
        "gemini-2.0-flash": "gemini-2.0-flash-exp",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.5-flash",  # Changed from gemini-3-pro (has 500 errors)
        max_iterations: int = 10,
        call_budget: int = 1000,
        verbose: bool = False,
        use_repl: bool = True,  # Ablation: disable code execution
        diagnostic_mode: bool = False,  # Enable detailed logging to file
        parquet_path: Optional[str] = None,  # Path to parquet file for arbitrary code execution
    ):
        """
        Args:
            api_key: Gemini API key (falls back to GEMINI_API_KEY env var)
            model: Model name (key from MODELS dict or full model ID)
            max_iterations: Max REPL iterations per example
            call_budget: Total API call budget for session
            verbose: Print debug information
            use_repl: Enable code execution (disable for ablation comparison)
            diagnostic_mode: Enable detailed logging to data/outputs/rlm_diagnostics_*.log
            parquet_path: Path to parquet file containing market data (enables arbitrary code execution)
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
        self.diagnostic_mode = diagnostic_mode
        self.parquet_path = parquet_path

        # Lazy-initialized
        self._search_index: Optional[MarketSearchIndex] = None
        self._all_examples: List[Example] = []

        # Session statistics
        self.session_stats = RLMSessionStats()

        # Diagnostics logger
        self._diagnostics = RLMDiagnostics(enabled=diagnostic_mode, max_iterations=max_iterations)
        if diagnostic_mode:
            self._diagnostics.log(f"RLMForecaster initialized: model={self.model}, use_repl={use_repl}")

        if self.verbose:
            print(f"[RLM] Model: {self.model}, REPL: {use_repl}, Max iterations: {max_iterations}")
            if diagnostic_mode:
                print(f"[RLM] Diagnostics enabled: {self._diagnostics.get_log_path()}")

    def _check_budget(self):
        """Check if API call budget is exhausted."""
        if self.calls_made >= self.call_budget:
            raise RuntimeError(f"API call budget exhausted ({self.call_budget} calls)")

    def _check_leakage(self, response: str, cutoff_date) -> Tuple[bool, str]:
        """Check if response references future information.

        Detection methods:
        1. Keyword + context: Look for past-tense phrases indicating known outcomes
        2. Date-based: Flag references to dates after the cutoff
        3. False positive reduction: Ignore keywords in quoted text
        """
        response_lower = response.lower()

        # === 1. Remove quoted text to reduce false positives ===
        # Keywords in market titles like "Who will be the winner?" are OK
        response_unquoted = re.sub(r'["\'][^"\']*["\']', '', response_lower)

        # === 2. Keyword + context-based detection ===
        # Only flag keywords when they appear with past-tense indicators
        leakage_phrases = [
            'the outcome was', 'the result was', 'it resolved',
            'the winner was', 'actually happened', 'we know that',
            'ended up', 'turned out', 'was decided', 'was determined',
            'has been resolved', 'has resolved', 'did win', 'did happen'
        ]

        for phrase in leakage_phrases:
            if phrase in response_unquoted:
                return True, f"Potential leakage: phrase '{phrase}'"

        # === 3. Date-based leakage detection ===
        # Pattern matches: "December 2025", "Jan 2026", "12/2025", "2025-12"
        month_map = {
            'jan': 1, 'january': 1, 'feb': 2, 'february': 2,
            'mar': 3, 'march': 3, 'apr': 4, 'april': 4,
            'may': 5, 'jun': 6, 'june': 6,
            'jul': 7, 'july': 7, 'aug': 8, 'august': 8,
            'sep': 9, 'sept': 9, 'september': 9,
            'oct': 10, 'october': 10, 'nov': 11, 'november': 11,
            'dec': 12, 'december': 12
        }

        # Convert cutoff_date to datetime if needed
        if isinstance(cutoff_date, str):
            try:
                cutoff_dt = datetime.fromisoformat(cutoff_date.replace('Z', '+00:00'))
            except:
                cutoff_dt = datetime.now()  # Fallback
        elif isinstance(cutoff_date, datetime):
            cutoff_dt = cutoff_date
        else:
            cutoff_dt = datetime.now()

        # Pattern 1: "Month Year" (e.g., "December 2025", "Jan 2026")
        month_year_pattern = r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s+(\d{4})\b'

        for match in re.finditer(month_year_pattern, response_lower):
            month_str, year_str = match.groups()
            try:
                month = month_map.get(month_str.lower(), 1)
                ref_date = datetime(int(year_str), month, 15)  # Use mid-month
                if ref_date > cutoff_dt:
                    return True, f"Potential leakage: future date reference '{match.group()}' (cutoff: {cutoff_dt.strftime('%Y-%m')})"
            except (ValueError, TypeError):
                pass

        # Pattern 2: "as of [date]" - strong indicator of future knowledge
        as_of_pattern = r'as of\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s+(\d{4})'
        for match in re.finditer(as_of_pattern, response_lower):
            month_str, year_str = match.groups()
            try:
                month = month_map.get(month_str.lower(), 1)
                ref_date = datetime(int(year_str), month, 15)
                if ref_date > cutoff_dt:
                    return True, f"Potential leakage: 'as of' future date '{match.group()}'"
            except (ValueError, TypeError):
                pass

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
                "target": list(ex.target) if ex.target else [],
                "option_count": len(ex.options),
                "source": ex.source,
                "title": title,
                "description": description,
            }

            self._search_index.add_market(ex.event_id, title, description, metadata)

        self._search_index.build_index()
        if self.verbose:
            print(f"[RLM] Built search index with {len(seen_ids)} markets")

    def _fallback_last_price(self, example: Example) -> List[float]:
        """Fallback prediction using last price."""
        scores = [opt.history_belief[-1] if opt.history_belief else 0.5 for opt in example.options]
        total = sum(scores)
        if total <= 0:
            return [1.0 / len(scores)] * len(scores)
        return [s / total for s in scores]

    def _parse_array_string(self, array_str: str, n_options: int) -> Optional[List[float]]:
        """Parse an array string like '[0.3, 0.7]' into normalized probabilities."""
        try:
            # Handle trailing commas by removing them
            cleaned = re.sub(r',\s*\]', ']', array_str)
            probs = json.loads(cleaned)
            if isinstance(probs, list) and len(probs) == n_options:
                # Clamp negatives and normalize
                total = sum(max(0, p) for p in probs)
                if total > 0:
                    return [max(0, p) / total for p in probs]
        except (json.JSONDecodeError, TypeError):
            pass
        return None

    def _extract_prediction_robust(self, response: str, n_options: int) -> Optional[List[float]]:
        """
        Extract prediction from response using multiple patterns.
        More robust than relying solely on FINAL_VAR mechanism.

        Tries in order:
        1. Variable assignment: prediction = [0.3, 0.7]
        2. Array near keywords: "final", "prediction", "forecast", "probab"
        3. Standalone array matching option count
        4. Percentage formats: "30%, 70%"
        """
        # Pattern for arrays: handles negative numbers, decimals, scientific notation
        array_pattern = r'\[\s*-?[\d.]+(?:e[+-]?\d+)?(?:\s*,\s*-?[\d.]+(?:e[+-]?\d+)?)*\s*,?\s*\]'

        # Strategy 1: Variable assignment pattern (most reliable)
        # Matches: prediction = [0.3, 0.7], pred=[0.5,0.5], final_prediction = [...]
        var_patterns = [
            r'prediction\s*=\s*(' + array_pattern + ')',
            r'pred\s*=\s*(' + array_pattern + ')',
            r'probs\s*=\s*(' + array_pattern + ')',
            r'probabilities\s*=\s*(' + array_pattern + ')',
            r'final_prediction\s*=\s*(' + array_pattern + ')',
            r'forecast\s*=\s*(' + array_pattern + ')',
        ]

        for pattern in var_patterns:
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                result = self._parse_array_string(match.group(1), n_options)
                if result:
                    self._diagnostics.log(f"Extracted prediction via variable assignment: {result}")
                    return result

        # Strategy 2: Array near keywords (context-aware)
        keywords = ['prediction', 'final', 'forecast', 'probab', 'answer', 'result']
        response_lower = response.lower()

        for keyword in keywords:
            idx = response_lower.find(keyword)
            if idx >= 0:
                # Search for array within 300 chars after keyword
                snippet = response[idx:idx+300]
                match = re.search(array_pattern, snippet, re.IGNORECASE)
                if match:
                    result = self._parse_array_string(match.group(), n_options)
                    if result:
                        self._diagnostics.log(f"Extracted prediction near '{keyword}': {result}")
                        return result

        # Strategy 3: Find ANY array matching the expected option count
        # Search from the end of response (final arrays more likely to be the answer)
        all_arrays = list(re.finditer(array_pattern, response, re.IGNORECASE))
        for match in reversed(all_arrays):
            result = self._parse_array_string(match.group(), n_options)
            if result:
                self._diagnostics.log(f"Extracted prediction from standalone array: {result}")
                return result

        # Strategy 4: Percentage format (e.g., "30%, 70%" or "Option A: 30%, Option B: 70%")
        pct_pattern = r'(\d+(?:\.\d+)?)\s*%'
        percentages = re.findall(pct_pattern, response)
        if len(percentages) >= n_options:
            # Take the last n_options percentages
            pcts = [float(p) / 100 for p in percentages[-n_options:]]
            total = sum(pcts)
            if total > 0:
                result = [p / total for p in pcts]
                self._diagnostics.log(f"Extracted prediction from percentages: {result}")
                return result

        return None

    def _extract_prediction(self, response: str, n_options: int) -> Optional[List[float]]:
        """Extract prediction array from RLM response.

        This is the main extraction method that uses the robust multi-strategy approach.
        """
        return self._extract_prediction_robust(response, n_options)

    def _predict_with_repl(self, example: Example) -> Tuple[List[float], RLMPredictionStats]:
        """Run prediction using external/rlm RLM class."""
        stats = RLMPredictionStats(event_id=example.event_id)
        start_time = time.time()

        # Build context and setup code
        context = build_context(example)
        setup_code = build_setup_code(self._search_index, example, parquet_path=self.parquet_path)

        # Diagnostic: start prediction logging
        self._diagnostics.start_prediction(example.event_id)

        # Build user prompt (minimal - model must query for details)
        user_prompt = f"""Predict this market: {context['title']}

Market ID: {context['market_id']}
Cutoff date: {context['cutoff_ts']} (you are making this prediction ON this date - no future info!)
Option count: {context['option_count']}

IMPORTANT:
- You have MINIMAL context - you must use helper functions to get data
- Start with get_description() to understand the market
- Use get_option_titles() to see the options
- Use get_prices() and trend() to analyze price history
- You MUST write at least one ```repl code block before FINAL_VAR()

Analyze the data using the helper functions and provide your prediction."""

        response = ""
        code_blocks_found = []

        try:
            self._check_budget()

            # Create custom logger for enhanced diagnostics
            diagnostic_logger = DiagnosticRLMLogger(self._diagnostics) if self._diagnostics.enabled else None

            # Create RLM instance using external/rlm library
            # NOTE: verbose=False to avoid Windows Unicode encoding errors with rich console
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
                logger=diagnostic_logger,
                verbose=False,  # Disabled to prevent Unicode errors on Windows
            )

            # Run RLM completion with exponential backoff for rate limits
            max_retries = 3
            result = None
            for attempt in range(max_retries):
                try:
                    result = rlm.completion(context, root_prompt=user_prompt)
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if "rate" in error_str or "429" in error_str or "quota" in error_str:
                        wait_time = (2 ** attempt) + random.uniform(0, 1)
                        if self.verbose:
                            print(f"[RLM] Rate limit hit, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                        self._diagnostics.log(f"Rate limit retry {attempt + 1}/{max_retries}, waiting {wait_time:.1f}s", level="WARNING")
                        time.sleep(wait_time)
                        if attempt == max_retries - 1:
                            raise
                    else:
                        raise

            if result is None:
                raise RuntimeError("RLM completion failed after all retries")

            # Extract ACTUAL API usage and iterations from result
            total_iterations = diagnostic_logger.iteration_count if diagnostic_logger else 0
            stats.iterations_used = total_iterations

            if hasattr(result, 'usage_summary') and result.usage_summary:
                usage = result.usage_summary
                for model_name, model_usage in usage.model_usage_summaries.items():
                    stats.api_calls += model_usage.total_calls
                    stats.input_tokens += model_usage.total_input_tokens
                    stats.output_tokens += model_usage.total_output_tokens
                self.calls_made += stats.api_calls
                self._diagnostics.log(
                    f"Actual API usage: {stats.api_calls} calls, "
                    f"{stats.input_tokens} input tokens, {stats.output_tokens} output tokens, "
                    f"{total_iterations} iterations"
                )
            else:
                # Fallback to conservative estimate if no usage data
                self.calls_made += 1
                stats.api_calls = 1
                self._diagnostics.log("No usage_summary in result, using estimate", level="WARNING")

            stats.execution_time = time.time() - start_time

            # Extract response and prediction
            if hasattr(result, 'response'):
                response = result.response
            elif hasattr(result, 'final_answer'):
                response = str(result.final_answer)
            else:
                response = str(result)

            stats.raw_response = response[:1000]

            # Diagnostic: check what code blocks were detected
            code_blocks_found = find_code_blocks(response)
            stats.code_blocks_executed = len(code_blocks_found)

            # Also check for ```python blocks (not detected by current parser)
            python_blocks = re.findall(r'```python\s*\n(.*?)\n```', response, re.DOTALL)
            if python_blocks and not code_blocks_found:
                self._diagnostics.log(
                    f"WARNING: Found {len(python_blocks)} ```python blocks but 0 ```repl blocks!",
                    level="WARNING"
                )

            # Check for leakage
            has_leakage, leakage_details = self._check_leakage(response, example.cutoff_ts)
            if has_leakage:
                stats.leakage_warning = True
                stats.leakage_details = leakage_details

            # Extract prediction
            prediction = self._extract_prediction(response, len(example.options))

            # RETRY LOGIC: If extraction failed, try one more time with explicit prompt
            if prediction is None:
                self._diagnostics.log("Initial extraction failed, attempting retry with explicit prompt", level="WARNING")
                try:
                    # Send a follow-up asking for explicit prediction format
                    retry_prompt = f"""Your previous response did not contain a valid prediction array.

REQUIRED: Create a prediction array with EXACTLY {len(example.options)} probabilities that sum to 1.0.

Example format:
```repl
prediction = [0.3, 0.4, 0.3]  # probabilities for each option
print(f"Final prediction: {{prediction}}")
```
FINAL_VAR("prediction")

Do this NOW. Output ONLY the code block above with your actual probability values."""

                    # Create new RLM for retry (simpler, fewer iterations)
                    retry_rlm = RLM(
                        backend="gemini",
                        backend_kwargs={
                            "model_name": self.model,
                            "api_key": self.api_key,
                        },
                        environment="local",
                        environment_kwargs={
                            "setup_code": f"option_count = {len(example.options)}\n",
                        },
                        max_depth=1,
                        max_iterations=3,  # Quick retry
                        custom_system_prompt="You are creating a probability prediction. Output a Python code block that creates a 'prediction' array.",
                        verbose=False,
                    )
                    retry_result = retry_rlm.completion({}, root_prompt=retry_prompt)

                    # Try to extract from retry response
                    retry_response = ""
                    if hasattr(retry_result, 'response'):
                        retry_response = retry_result.response
                    elif hasattr(retry_result, 'final_answer'):
                        retry_response = str(retry_result.final_answer)
                    else:
                        retry_response = str(retry_result)

                    # Combine original and retry responses for extraction
                    combined_response = response + "\n\nRETRY RESPONSE:\n" + retry_response
                    prediction = self._extract_prediction(combined_response, len(example.options))

                    if prediction is not None:
                        self._diagnostics.log(f"Retry successful, extracted: {prediction}")
                        stats.api_calls += 1
                        self.calls_made += 1
                except Exception as retry_error:
                    self._diagnostics.log(f"Retry failed: {str(retry_error)}", level="WARNING")

            # Diagnostic logging - completion summary
            self._diagnostics.log_completion(
                total_iterations=total_iterations,
                final_answer=response if hasattr(result, 'response') else None,
                prediction_extracted=(prediction is not None),
                fallback_used=False
            )

            if prediction is not None:
                if self.verbose:
                    print(f"[RLM] Got prediction: {[f'{p:.3f}' for p in prediction]}")
                return prediction, stats

        except Exception as e:
            self._diagnostics.log(f"Exception in _predict_with_repl: {str(e)}", level="ERROR")
            if self.verbose:
                print(f"[RLM] Error: {e}")
                import traceback
                traceback.print_exc()

        # Fallback
        stats.fallback_used = True
        stats.execution_time = time.time() - start_time

        # Diagnostic logging for fallback
        total_iterations = getattr(diagnostic_logger, 'iteration_count', 0) if 'diagnostic_logger' in locals() else 0
        stats.iterations_used = total_iterations
        self._diagnostics.log_completion(
            total_iterations=total_iterations,
            final_answer=response if 'response' in locals() else None,
            prediction_extracted=False,
            fallback_used=True
        )

        return self._fallback_last_price(example), stats

    def _predict_without_repl(self, example: Example) -> Tuple[List[float], RLMPredictionStats]:
        """
        Predict without code execution (ablation mode).

        Note: For ablation comparison, we provide full data in the prompt since
        there's no REPL to query it. This is the OLD approach that violates RLM paradigm.
        """
        stats = RLMPredictionStats(event_id=example.event_id)
        start_time = time.time()

        # Build minimal context for metadata
        context = build_context(example)

        # Extract full data for ablation prompt (bypasses RLM paradigm)
        title = example.static_features.get("title", "Unknown")
        description = example.static_features.get("description", "")[:1000]
        end_time = example.static_features.get("end_time", "Unknown")

        prompt = f"""You are a prediction market forecaster. Analyze this market and provide probabilities.

CRITICAL: Your prediction date is {context['cutoff_ts']}. Do NOT use future information.

MARKET: {title}
DESCRIPTION: {description}
OPTIONS: {[opt.title for opt in example.options]}
END TIME: {end_time}

CURRENT PRICES:
"""
        for i, opt in enumerate(example.options):
            last_price = opt.history_belief[-1] if opt.history_belief else 0.5
            prompt += f"  {opt.title}: {last_price:.2%}\n"

        prompt += f"""
Provide your prediction as a JSON array of {context['option_count']} probabilities summing to 1.0.
Respond with ONLY the JSON array, e.g.: [0.6, 0.4]
"""

        try:
            self._check_budget()

            from google import genai
            from google.genai import types

            client = genai.Client(api_key=self.api_key)

            # Exponential backoff for rate limits
            max_retries = 3
            response = None
            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content(
                        model=self.model,
                        contents=[types.Content(role="user", parts=[types.Part(text=prompt)])],
                    )
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if "rate" in error_str or "429" in error_str or "quota" in error_str:
                        wait_time = (2 ** attempt) + random.uniform(0, 1)
                        if self.verbose:
                            print(f"[RLM] Rate limit hit, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        if attempt == max_retries - 1:
                            raise
                    else:
                        raise

            if response is None:
                raise RuntimeError("Gemini API call failed after all retries")

            # Track actual API usage from response
            stats.api_calls = 1
            self.calls_made += 1
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                stats.input_tokens = response.usage_metadata.prompt_token_count or 0
                stats.output_tokens = response.usage_metadata.candidates_token_count or 0

            text = response.text if response.text else ""
            stats.raw_response = text[:500]
            stats.iterations_used = 1

            array_match = re.search(r'\[[\d.,\s]+\]', text)
            if array_match:
                probs = json.loads(array_match.group())
                total = sum(max(0, p) for p in probs)
                if total > 0 and len(probs) == len(example.options):
                    stats.execution_time = time.time() - start_time
                    return [max(0, p) / total for p in probs], stats

        except Exception as e:
            if self.verbose:
                print(f"[RLM] Error in non-REPL mode: {e}")

        stats.fallback_used = True
        stats.execution_time = time.time() - start_time
        return self._fallback_last_price(example), stats

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
        print(f"Budget: {stats['api']['calls_made']}/{stats['api']['call_budget']} (remaining: {stats['api']['budget_remaining']})")
        print(f"Predictions: {stats['session']['total_predictions']}")
        print(f"Actual API Calls: {stats['session']['total_api_calls']} (avg: {stats['session']['avg_api_calls']:.1f}/prediction)")
        print(f"Tokens Used:")
        print(f"  - Input: {stats['session']['total_tokens']['input']:,}")
        print(f"  - Output: {stats['session']['total_tokens']['output']:,}")
        print(f"  - Total: {stats['session']['total_tokens']['total']:,}")
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
# NOTES (rlm_forecaster.py)
# =============================================================================
#
# PURPOSE: RLM forecaster WITH market prices (baseline for ablation comparison)
#
# DOS:
# - Use robust extraction (_extract_prediction_robust) - 4 strategies
# - Use absolute paths for parquet (REPL runs in temp dir)
# - Pass diagnostic_logger to RLM for observability
#
# DONTS:
# - Don't rely solely on FINAL_VAR pattern (model inconsistent)
# - Don't use gemini-3-* models (500/503 errors as of 2026-01)
#
# EXPERIMENTS + RESULTS:
# - 2026-01-22: Robust extraction reduced fallback from ~50% to ~5%
# - gemini-2.5-flash works; gemini-3-pro-preview has API errors
# - Prompt-only fixes insufficient; code-level extraction required
#
# THINGS THAT DIDNT WORK:
# - Explicit TWO-STEP prompt pattern (model still inconsistent)
# - COMMON MISTAKES section in prompt (helped but not enough)
#
# IMPLEMENTATION NUANCES:
# - Retry logic creates new RLM with max_iterations=3
# - Extraction order: var assignment -> keywords -> arrays -> percentages
# - build_setup_code() uses repr(json.dumps()) for safe escaping
# - verbose=False to avoid Windows Unicode crashes with rich
#
# SEE ALSO: .claude/RLM_HANDOFF.md for full research context
