"""
Full Recursive Forecaster Module

This module implements the full_recursive ForecastMethod, which combines:
- Web-grounded agents (Planner, Analyst, Advocates, Verifier, Synthesizer)
- RLM REPL-based DATA_ANALYST for quantitative analysis

The key insight is that web-grounded agents provide broad context (current events,
news, expert opinions) while RLM REPL provides high-granularity data analysis
(price patterns, trend analysis, historical comparisons).

Reference: kalshi-research-agents TypeScript implementation
Path: C:/Users/prapa/Documents/GitHub/kalshi-research-agents/

Usage:
    # Default
    uv run runner/runner.py --method full_recursive

    # With preset
    from methods.full_recursive.config import get_preset
    config = get_preset("fast")

    # Dry-run mode
    uv run runner/runner.py --method full_recursive --method-params '{"dry_run": true}'
"""

from .pipeline import FullRecursivePipeline, PipelineResult, IterationResult, PipelineLogger
from .agents import (
    GeminiAgentClient,
    PlannerOutput,
    AnalystOutput,
    AdvocateOutput,
    VerifierOutput,
    SynthesizerOutput,
    SubQuestion,
)
from .data_analyst import DataAnalystOutput
from .config import (
    PipelineConfig,
    PRESETS,
    get_preset,
    list_presets,
    FAST_CONFIG,
    BALANCED_CONFIG,
    THOROUGH_CONFIG,
)
from .logger import (
    FullRecursiveLogger,
    create_logger_for_run,
    copy_logs_to_visualizer,
)

__all__ = [
    # Pipeline
    "FullRecursivePipeline",
    "PipelineResult",
    "IterationResult",
    "PipelineLogger",
    # Agents
    "GeminiAgentClient",
    "PlannerOutput",
    "AnalystOutput",
    "AdvocateOutput",
    "VerifierOutput",
    "SynthesizerOutput",
    "SubQuestion",
    "DataAnalystOutput",
    # Config
    "PipelineConfig",
    "PRESETS",
    "get_preset",
    "list_presets",
    "FAST_CONFIG",
    "BALANCED_CONFIG",
    "THOROUGH_CONFIG",
    # Logger (RLM visualizer compatible)
    "FullRecursiveLogger",
    "create_logger_for_run",
    "copy_logs_to_visualizer",
]
