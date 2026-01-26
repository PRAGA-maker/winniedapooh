"""
Configuration presets for the full_recursive pipeline.

Provides pre-configured settings for common use cases:
- FAST: Quick predictions with minimal iterations
- BALANCED: Default balanced configuration
- THOROUGH: More iterations and lower confidence threshold
- ABLATION_SEQUENTIAL: DATA_ANALYST in sequential mode
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class PipelineConfig:
    """Configuration for the full_recursive pipeline."""
    model: str = "gemini-2.0-flash"
    max_iterations: int = 5
    confidence_threshold: float = 0.7
    data_analyst_parallel: bool = True
    verbose: bool = False
    log_dir: Optional[str] = None  # Directory for JSONL logs (RLM visualizer compatible)

    # Agent-specific settings
    planner_temperature: float = 0.7
    analyst_temperature: float = 0.5
    advocate_temperature: float = 0.7
    verifier_temperature: float = 0.5
    synthesizer_temperature: float = 0.5

    # DATA_ANALYST settings
    data_analyst_max_iterations: int = 5

    # Wayback Machine validation settings
    wayback_enabled: bool = True  # Enable/disable Wayback CDX API validation
    wayback_cache_size: int = 256  # LRU cache size for Wayback queries
    wayback_timeout_seconds: float = 10.0  # HTTP request timeout
    wayback_rate_limit: float = 0.2  # Minimum seconds between requests

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for method_params."""
        d = {
            "model": self.model,
            "max_iterations": self.max_iterations,
            "confidence_threshold": self.confidence_threshold,
            "data_analyst_parallel": self.data_analyst_parallel,
            "verbose": self.verbose,
            "wayback_enabled": self.wayback_enabled,
        }
        if self.log_dir:
            d["log_dir"] = self.log_dir
        return d


# =============================================================================
# Configuration Presets
# =============================================================================

# FAST: Quick predictions with minimal iterations
# Good for: Testing, high-volume predictions, time-sensitive scenarios
FAST_CONFIG = PipelineConfig(
    model="gemini-2.0-flash",
    max_iterations=2,
    confidence_threshold=0.6,
    data_analyst_parallel=True,
    verbose=False,
    planner_temperature=0.5,
    analyst_temperature=0.3,
    advocate_temperature=0.5,
    wayback_enabled=False,  # Skip Wayback validation for speed
)

# BALANCED: Default balanced configuration
# Good for: General use, reasonable tradeoff between speed and accuracy
BALANCED_CONFIG = PipelineConfig(
    model="gemini-2.0-flash",
    max_iterations=5,
    confidence_threshold=0.7,
    data_analyst_parallel=True,
    verbose=False,
)

# THOROUGH: More iterations and stricter confidence
# Good for: High-stakes predictions, research, thorough analysis
THOROUGH_CONFIG = PipelineConfig(
    model="gemini-2.0-flash",
    max_iterations=7,
    confidence_threshold=0.8,
    data_analyst_parallel=True,
    verbose=True,
    data_analyst_max_iterations=7,
)

# ABLATION_SEQUENTIAL: DATA_ANALYST in sequential mode
# Good for: Research ablation studies, comparing timing modes
ABLATION_SEQUENTIAL_CONFIG = PipelineConfig(
    model="gemini-2.0-flash",
    max_iterations=5,
    confidence_threshold=0.7,
    data_analyst_parallel=False,  # Sequential mode
    verbose=True,
)

# PRO_MODEL: Use more capable model
# Good for: Complex markets, when accuracy is more important than cost
PRO_MODEL_CONFIG = PipelineConfig(
    model="gemini-3-pro",
    max_iterations=5,
    confidence_threshold=0.7,
    data_analyst_parallel=True,
    verbose=True,
)

# FLASH_MODEL: Use gemini-3-flash for testing/development (higher rate limits)
# Good for: Testing when pro rate limits hit, rapid iteration, development
FLASH_MODEL_CONFIG = PipelineConfig(
    model="gemini-3-flash",
    max_iterations=5,
    confidence_threshold=0.7,
    data_analyst_parallel=True,
    verbose=True,
)


# =============================================================================
# Preset Registry
# =============================================================================

PRESETS: Dict[str, PipelineConfig] = {
    "fast": FAST_CONFIG,
    "balanced": BALANCED_CONFIG,
    "thorough": THOROUGH_CONFIG,
    "sequential": ABLATION_SEQUENTIAL_CONFIG,
    "pro": PRO_MODEL_CONFIG,
    "flash": FLASH_MODEL_CONFIG,
}


def get_preset(name: str) -> Optional[PipelineConfig]:
    """Get a configuration preset by name."""
    return PRESETS.get(name.lower())


def list_presets() -> Dict[str, str]:
    """List available presets with descriptions."""
    return {
        "fast": "Quick predictions (2 iterations, 60% threshold)",
        "balanced": "Default balanced config (5 iterations, 70% threshold)",
        "thorough": "Thorough analysis (7 iterations, 80% threshold)",
        "sequential": "DATA_ANALYST in sequential mode (ablation)",
        "pro": "Use gemini-3-pro model for higher quality",
        "flash": "Use gemini-3-flash model (higher rate limits for testing)",
    }


def get_cli_params(preset_name: str) -> str:
    """Get CLI parameters for a preset.

    Returns a string that can be passed to --method-params.

    Example:
        params = get_cli_params("fast")
        # Run: uv run runner/runner.py --method full_recursive --method-params '{params}'
    """
    preset = get_preset(preset_name)
    if not preset:
        raise ValueError(f"Unknown preset: {preset_name}")

    import json
    return json.dumps(preset.to_dict())


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Configuration Presets:
#
# DESIGN PRINCIPLES:
# 1. Presets provide sensible defaults for common use cases
# 2. All settings can be overridden via method_params
# 3. CLI-friendly output via get_cli_params()
#
# PRESET SELECTION GUIDE:
# - "fast": Testing or when speed matters more than accuracy
# - "balanced": General use, good starting point
# - "thorough": Research, high-stakes predictions
# - "sequential": Ablation studies comparing timing modes
# - "pro": When you need the best model available
# - "flash": When pro rate limits hit; gemini-3-flash has higher limits
#
# TEMPERATURE NOTES:
# - Lower temperature (0.3-0.5) for factual agents (analyst, verifier)
# - Higher temperature (0.7) for creative agents (planner, advocates)
#
# OBSERVABILITY (added 2026-01-21):
# - log_dir field enables JSONL logging for RLM visualizer
# - Set in PipelineConfig or pass via method_params
# - Example: --method-params '{"log_dir": "logs/full_recursive"}'
#
# WAYBACK VALIDATION (added 2026-01-21):
# - wayback_enabled: Set False for fast preset (skip API calls)
# - wayback_cache_size: 256 handles typical citation volume
# - wayback_timeout_seconds: 10s balances reliability vs speed
# - wayback_rate_limit: 0.2s (200ms) prevents 429 errors from archive.org
# - Disable with: --method-params '{"wayback_enabled": false}'
#
# FLASH PRESET (added 2026-01-22):
# - gemini-3-flash has significantly higher rate limits than gemini-3-pro
# - Use when pro rate limits are hit during development/testing
# - Quality may be slightly lower but good enough for iteration
# - Cost: ~$0.004 per prediction (vs ~$0.02 for pro)
# - Latency: ~4 min per prediction with 2 iterations
# - Example: uv run runner/runner.py --method full_recursive \
#            --method-params '{"model": "gemini-3-flash"}'
#
