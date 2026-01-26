"""
Logger for full_recursive pipeline execution traces.

Outputs JSONL files compatible with the RLM visualizer (external/rlm/visualizer/).
This enables visual debugging of pipeline iterations, agent outputs, and
DATA_ANALYST code execution.

Usage:
    logger = FullRecursiveLogger("logs/")
    logger.start(market_title, event_id, config)

    # During pipeline execution
    logger.log_iteration(iteration_result)

    # When complete
    logger.finish(final_result)

    # Log file created at: logs/full_recursive_2026-01-21_12-34-56_abc123.jsonl

Visualizer Usage:
    1. Copy log files to external/rlm/visualizer/public/logs/
    2. cd external/rlm/visualizer && npm run dev
    3. Open http://localhost:3000 and select log file
"""

import json
import os
import uuid
from datetime import datetime
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from .pipeline import IterationResult, PipelineResult


# =============================================================================
# RLM-Compatible Data Types
# =============================================================================

@dataclass
class CodeBlockLog:
    """A code block executed by DATA_ANALYST."""
    code: str
    stdout: str = ""
    stderr: str = ""
    execution_time: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "result": {
                "stdout": self.stdout,
                "stderr": self.stderr,
                "locals": {},
                "execution_time": self.execution_time,
                "rlm_calls": [],
            }
        }


@dataclass
class AgentLog:
    """Log entry for a single agent call."""
    agent: str
    prompt: str
    response: str
    latency_ms: int
    tokens_input: int = 0
    tokens_output: int = 0
    cost_usd: float = 0.0
    parse_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent": self.agent,
            "prompt": self.prompt[:5000],  # Truncate for readability
            "response": self.response[:5000],
            "latency_ms": self.latency_ms,
            "tokens": {"input": self.tokens_input, "output": self.tokens_output},
            "cost_usd": self.cost_usd,
            "parse_error": self.parse_error,
        }


@dataclass
class IterationLog:
    """Log entry for a pipeline iteration, compatible with RLM visualizer."""
    iteration: int
    agents: List[AgentLog] = field(default_factory=list)
    code_blocks: List[CodeBlockLog] = field(default_factory=list)
    decision: str = "RETRY"
    prediction: str = ""
    probability: float = 0.5
    confidence: float = 0.5
    iteration_time: float = 0.0
    total_cost_usd: float = 0.0

    def to_rlm_format(self) -> Dict[str, Any]:
        """Convert to RLM visualizer-compatible format."""
        # Combine all agent prompts/responses
        combined_prompt = "\n\n".join([
            f"=== {a.agent.upper()} ===\n{a.prompt[:2000]}"
            for a in self.agents
        ])

        combined_response = "\n\n".join([
            f"=== {a.agent.upper()} ===\n{a.response[:2000]}"
            for a in self.agents
        ])

        return {
            "type": "iteration",
            "iteration": self.iteration,
            "timestamp": datetime.now().isoformat(),
            "prompt": combined_prompt,
            "response": combined_response,
            "code_blocks": [cb.to_dict() for cb in self.code_blocks],
            "final_answer": f"{self.decision}: {self.prediction} @ {self.probability:.0%} (conf: {self.confidence:.0%})",
            "iteration_time": self.iteration_time,
            # Extra fields for full_recursive
            "full_recursive_data": {
                "agents": [a.to_dict() for a in self.agents],
                "decision": self.decision,
                "prediction": self.prediction,
                "probability": self.probability,
                "confidence": self.confidence,
                "total_cost_usd": self.total_cost_usd,
            }
        }


# =============================================================================
# Main Logger Class
# =============================================================================

class FullRecursiveLogger:
    """
    Logger for full_recursive pipeline execution.

    Writes JSONL files compatible with the RLM visualizer for debugging
    and analysis of pipeline runs.
    """

    def __init__(self, log_dir: str = "logs/full_recursive"):
        """
        Initialize logger.

        Args:
            log_dir: Directory to write log files
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.log_file_path: Optional[Path] = None
        self.run_id: Optional[str] = None
        self.start_time: Optional[float] = None
        self._iteration_count = 0
        self._metadata_logged = False

    def start(
        self,
        market_title: str,
        event_id: str,
        config: Dict[str, Any],
    ) -> str:
        """
        Start logging a new pipeline run.

        Args:
            market_title: Title of the market being predicted
            event_id: Market event ID
            config: Pipeline configuration

        Returns:
            Path to the log file
        """
        import time

        self.start_time = time.time()
        self._iteration_count = 0
        self._metadata_logged = False

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.run_id = str(uuid.uuid4())[:8]
        self.log_file_path = self.log_dir / f"full_recursive_{timestamp}_{self.run_id}.jsonl"

        # Write metadata as first entry
        metadata = {
            "type": "metadata",
            "timestamp": datetime.now().isoformat(),
            "run_id": self.run_id,
            "market_title": market_title[:100],
            "event_id": event_id,
            "config": config,
            "pipeline": "full_recursive",
        }

        self._write_entry(metadata)
        self._metadata_logged = True

        return str(self.log_file_path)

    def log_iteration(self, result: IterationResult) -> None:
        """
        Log a pipeline iteration.

        Args:
            result: IterationResult from the pipeline
        """
        self._iteration_count += 1

        # Convert LLM call logs to agent logs
        agents = []
        for call in result.llm_calls:
            agents.append(AgentLog(
                agent=call.agent,
                prompt="",  # We don't store prompts in LLMCallLog currently
                response=call.raw_response,
                latency_ms=call.latency_ms,
                tokens_input=call.tokens_input,
                tokens_output=call.tokens_output,
                cost_usd=call.cost_estimate_usd,
                parse_error=call.parse_error,
            ))

        # Extract code blocks from DATA_ANALYST if available
        code_blocks = []
        if result.data_analyst_output:
            # DATA_ANALYST doesn't currently expose individual code blocks
            # This is a TODO for better observability
            code_blocks.append(CodeBlockLog(
                code=f"# DATA_ANALYST executed {result.data_analyst_output.code_blocks_executed} blocks",
                stdout=f"Trend: {result.data_analyst_output.trend_analysis[:500]}",
                stderr="",
                execution_time=0.0,
            ))

        iteration_log = IterationLog(
            iteration=self._iteration_count,
            agents=agents,
            code_blocks=code_blocks,
            decision=result.decision,
            prediction=result.synthesizer_output.prediction if result.synthesizer_output else "",
            probability=result.synthesizer_output.probability if result.synthesizer_output else 0.5,
            confidence=result.confidence,
            iteration_time=result.total_latency_ms / 1000.0,
            total_cost_usd=result.total_cost_usd,
        )

        self._write_entry(iteration_log.to_rlm_format())

    def finish(self, result: PipelineResult) -> None:
        """
        Log pipeline completion.

        Args:
            result: Final PipelineResult
        """
        import time

        elapsed = time.time() - self.start_time if self.start_time else 0

        summary = {
            "type": "summary",
            "timestamp": datetime.now().isoformat(),
            "run_id": self.run_id,
            "prediction": result.prediction,
            "probability": result.probability,
            "confidence": result.confidence,
            "iterations_used": result.iterations_used,
            "total_cost_usd": result.total_cost_usd,
            "total_latency_ms": result.total_latency_ms,
            "elapsed_seconds": elapsed,
            "reasoning": result.reasoning[:1000],
            "caveats": result.caveats,
        }

        self._write_entry(summary)

    def _write_entry(self, entry: Dict[str, Any]) -> None:
        """Write an entry to the log file."""
        if not self.log_file_path:
            return

        with open(self.log_file_path, "a") as f:
            json.dump(entry, f, default=str)
            f.write("\n")


# =============================================================================
# Helper Functions
# =============================================================================

def create_logger_for_run(
    log_dir: str = "logs/full_recursive",
    market_title: str = "Unknown",
    event_id: str = "unknown",
    config: Optional[Dict[str, Any]] = None,
) -> FullRecursiveLogger:
    """
    Create and start a logger for a pipeline run.

    Args:
        log_dir: Directory for log files
        market_title: Market title
        event_id: Event ID
        config: Pipeline configuration

    Returns:
        Started logger instance
    """
    logger = FullRecursiveLogger(log_dir)
    logger.start(market_title, event_id, config or {})
    return logger


def copy_logs_to_visualizer(
    source_dir: str = "logs/full_recursive",
    visualizer_dir: str = "external/rlm/visualizer/public/logs",
    max_files: int = 10,
) -> int:
    """
    Copy recent log files to the RLM visualizer's public directory.

    Args:
        source_dir: Source directory with log files
        visualizer_dir: Visualizer's public logs directory
        max_files: Maximum number of files to copy

    Returns:
        Number of files copied
    """
    import shutil

    source = Path(source_dir)
    dest = Path(visualizer_dir)

    if not source.exists():
        return 0

    dest.mkdir(parents=True, exist_ok=True)

    # Get recent log files sorted by modification time
    log_files = sorted(
        source.glob("full_recursive_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:max_files]

    copied = 0
    for log_file in log_files:
        dest_file = dest / log_file.name
        if not dest_file.exists():
            shutil.copy(log_file, dest_file)
            copied += 1

    return copied


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Logger Implementation:
#
# RLM VISUALIZER COMPATIBILITY:
# 1. JSONL format with "type" field for entry type (metadata, iteration, summary)
# 2. RLMIteration format: prompt, response, code_blocks, final_answer, iteration_time
# 3. code_blocks contain: code, result (stdout, stderr, locals, execution_time)
#
# FULL_RECURSIVE EXTENSIONS:
# 1. Added "full_recursive_data" field for pipeline-specific info
# 2. Agents are logged individually but combined for RLM view
# 3. DATA_ANALYST code blocks are currently summarized, not individual
#
# TODO FOR BETTER OBSERVABILITY:
# 1. Capture individual code blocks from DATA_ANALYST REPL
# 2. Store prompts in LLMCallLog for replay
# 3. Add token counts to visualizer display
#
