"""
Full Recursive Pipeline - Orchestrates agents in a recursive loop.

Architecture:
1. PLANNER decomposes problem into sub-questions
2. PARALLEL PHASE: ANALYST + ADVOCATE_YES + ADVOCATE_NO (+ DATA_ANALYST if parallel mode)
3. VERIFIER judges debate and finds issues
4. SYNTHESIZER decides COMMIT or RETRY
5. If RETRY, feedback flows back to PLANNER for next iteration

The DATA_ANALYST timing is controlled by the data_analyst_parallel flag:
- True: Runs in parallel with web agents (faster, independent)
- False: Runs after web agents (can use their findings as context)
"""

import asyncio
import concurrent.futures
import time
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from forecasting.dataclasses import Example
from methods.rlm_tools.semantic_search import MarketSearchIndex

from .agents import (
    GeminiAgentClient,
    LLMCallLog,
    SubQuestion,
    PlannerOutput,
    AnalystOutput,
    AdvocateOutput,
    VerifierOutput,
    SynthesizerOutput,
    Citation,
    Issue,
    run_planner,
    run_analyst,
    run_advocate,
    run_verifier,
    run_synthesizer,
    run_analyst_async,
    run_advocate_async,
)
from .data_analyst import DataAnalystOutput, run_data_analyst
from .wayback_validator import WaybackValidator, ValidationResult, validate_citations_sync

# Import logger (avoid circular import by importing class directly)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .logger import FullRecursiveLogger


# =============================================================================
# Pipeline Data Classes
# =============================================================================

@dataclass
class IterationResult:
    """Result from a single pipeline iteration."""
    iteration: int
    planner_output: Optional[PlannerOutput] = None
    analyst_output: Optional[AnalystOutput] = None
    advocate_yes_output: Optional[AdvocateOutput] = None
    advocate_no_output: Optional[AdvocateOutput] = None
    data_analyst_output: Optional[DataAnalystOutput] = None
    verifier_output: Optional[VerifierOutput] = None
    synthesizer_output: Optional[SynthesizerOutput] = None
    llm_calls: List[LLMCallLog] = field(default_factory=list)
    decision: str = "RETRY"
    confidence: float = 0.0
    total_cost_usd: float = 0.0
    total_latency_ms: int = 0


@dataclass
class PipelineResult:
    """Final result from the full recursive pipeline."""
    prediction: str
    probability: float
    confidence: float
    probabilities: List[float]  # For ForecastMethod interface
    iterations_used: int
    reasoning: str
    caveats: List[str]
    total_cost_usd: float
    total_latency_ms: int
    iteration_history: List[IterationResult] = field(default_factory=list)


# =============================================================================
# Pipeline Logger
# =============================================================================

class PipelineLogger:
    """
    Structured logger for pipeline execution.

    Tracks costs, latencies, and agent outputs across iterations.
    Can output to console and/or JSON file.
    """

    def __init__(self, verbose: bool = True, log_file: Optional[Path] = None):
        self.verbose = verbose
        self.log_file = log_file
        self.start_time: Optional[float] = None
        self.events: List[Dict[str, Any]] = []
        self.costs: Dict[str, float] = {}
        self.latencies: Dict[str, int] = {}

    def start(self, market_title: str, event_id: str):
        """Start logging a new pipeline run."""
        self.start_time = time.time()
        self.events = []
        self.costs = {}
        self.latencies = {}
        self._log_event("pipeline_start", {
            "market_title": market_title[:50],
            "event_id": event_id,
        })

    def log_iteration_start(self, iteration: int, max_iterations: int):
        """Log the start of an iteration."""
        self._log_event("iteration_start", {
            "iteration": iteration,
            "max_iterations": max_iterations,
        })

    def log_agent_complete(
        self,
        agent: str,
        success: bool,
        latency_ms: int,
        cost_usd: float,
        details: Optional[Dict] = None,
    ):
        """Log completion of an agent."""
        self.costs[agent] = self.costs.get(agent, 0) + cost_usd
        self.latencies[agent] = self.latencies.get(agent, 0) + latency_ms

        event_data = {
            "agent": agent,
            "success": success,
            "latency_ms": latency_ms,
            "cost_usd": cost_usd,
        }
        if details:
            event_data.update(details)

        self._log_event("agent_complete", event_data)

    def log_iteration_complete(
        self,
        iteration: int,
        decision: str,
        confidence: float,
        total_cost: float,
        total_latency: int,
    ):
        """Log completion of an iteration."""
        self._log_event("iteration_complete", {
            "iteration": iteration,
            "decision": decision,
            "confidence": confidence,
            "total_cost_usd": total_cost,
            "total_latency_ms": total_latency,
        })

    def log_pipeline_complete(self, result: "PipelineResult"):
        """Log completion of the full pipeline."""
        elapsed = time.time() - self.start_time if self.start_time else 0

        self._log_event("pipeline_complete", {
            "prediction": result.prediction,
            "probability": result.probability,
            "confidence": result.confidence,
            "iterations_used": result.iterations_used,
            "total_cost_usd": result.total_cost_usd,
            "total_latency_ms": result.total_latency_ms,
            "elapsed_seconds": elapsed,
        })

        # Write to file if configured
        if self.log_file:
            self._write_log_file(result)

    def print_summary(self):
        """Print a summary of costs and latencies."""
        if not self.verbose:
            return

        print("\n" + "-" * 50)
        print("PIPELINE EXECUTION SUMMARY")
        print("-" * 50)

        total_cost = sum(self.costs.values())
        total_latency = sum(self.latencies.values())

        print(f"Total Cost: ${total_cost:.4f}")
        print(f"Total Latency: {total_latency/1000:.1f}s")

        if self.costs:
            print("\nCost by Agent:")
            for agent, cost in sorted(self.costs.items(), key=lambda x: -x[1]):
                pct = (cost / total_cost * 100) if total_cost > 0 else 0
                print(f"  {agent}: ${cost:.4f} ({pct:.1f}%)")

        if self.latencies:
            print("\nLatency by Agent:")
            for agent, lat in sorted(self.latencies.items(), key=lambda x: -x[1]):
                pct = (lat / total_latency * 100) if total_latency > 0 else 0
                print(f"  {agent}: {lat/1000:.1f}s ({pct:.1f}%)")

        print("-" * 50)

    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Log an event."""
        event = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            **data,
        }
        self.events.append(event)

        if self.verbose:
            self._print_event(event_type, data)

    def _print_event(self, event_type: str, data: Dict[str, Any]):
        """Print an event to console."""
        if event_type == "pipeline_start":
            print(f"\n{'='*60}")
            print(f"Market: {data.get('market_title', 'Unknown')}")
            print(f"Event ID: {data.get('event_id', 'Unknown')}")
            print(f"{'='*60}")

        elif event_type == "iteration_start":
            print(f"\n--- Iteration {data['iteration']}/{data['max_iterations']} ---")

        elif event_type == "agent_complete":
            status = "OK" if data["success"] else "FAILED"
            agent = data["agent"].upper()
            lat = data["latency_ms"] / 1000
            cost = data["cost_usd"]
            details_str = ""
            if "confidence" in data:
                details_str = f" (confidence: {data['confidence']:.0%})"
            elif "winner" in data:
                details_str = f" (winner: {data['winner']})"
            print(f"[{agent}] {status} - {lat:.1f}s, ${cost:.4f}{details_str}")

        elif event_type == "iteration_complete":
            print(f"Decision: {data['decision']} (confidence: {data['confidence']:.0%})")
            print(f"Iteration cost: ${data['total_cost_usd']:.4f}, latency: {data['total_latency_ms']/1000:.1f}s")

        elif event_type == "pipeline_complete":
            print(f"\n{'='*60}")
            print("PIPELINE COMPLETE")
            print(f"Prediction: {data['prediction']}")
            print(f"Probability: {data['probability']:.0%}")
            print(f"Confidence: {data['confidence']:.0%}")
            print(f"Iterations: {data['iterations_used']}")
            print(f"Total Cost: ${data['total_cost_usd']:.4f}")
            print(f"Total Time: {data['elapsed_seconds']:.1f}s")
            print(f"{'='*60}")

    def _write_log_file(self, result: "PipelineResult"):
        """Write log to JSON file."""
        log_data = {
            "events": self.events,
            "costs": self.costs,
            "latencies": self.latencies,
            "result": {
                "prediction": result.prediction,
                "probability": result.probability,
                "confidence": result.confidence,
                "probabilities": result.probabilities,
                "iterations_used": result.iterations_used,
                "total_cost_usd": result.total_cost_usd,
                "total_latency_ms": result.total_latency_ms,
            },
        }

        with open(self.log_file, "w") as f:
            json.dump(log_data, f, indent=2, default=str)


# =============================================================================
# Helper Functions
# =============================================================================

# Tracking parameters commonly added to URLs (strip for normalization)
TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'ref', 'source', 'mc_cid', 'mc_eid',
    '_ga', '_gl', 'hsCtaTracking', 'mkt_tok',
}


def normalize_url(url: str) -> str:
    """
    Normalize a URL for deduplication purposes.

    - Strips tracking parameters (utm_*, fbclid, etc.)
    - Removes trailing slashes
    - Lowercases the domain
    - Removes fragments (#section)

    Returns the normalized URL string.
    """
    from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

    if not url:
        return ""

    try:
        parsed = urlparse(url)

        # Lowercase the domain
        netloc = parsed.netloc.lower()

        # Strip tracking params from query string
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            filtered_params = {
                k: v for k, v in params.items()
                if k.lower() not in TRACKING_PARAMS
            }
            query = urlencode(filtered_params, doseq=True)
        else:
            query = ""

        # Remove trailing slash from path
        path = parsed.path.rstrip('/')
        if not path:
            path = ""

        # Rebuild URL without fragment
        normalized = urlunparse((
            parsed.scheme,
            netloc,
            path,
            parsed.params,
            query,
            ""  # No fragment
        ))

        return normalized
    except Exception:
        # If parsing fails, return original
        return url


def detect_notable_moves(example: Example, threshold: float = 0.05) -> List[Dict]:
    """Detect notable price moves from example history."""
    moves = []

    for opt_idx, opt in enumerate(example.options):
        if not opt.history_belief or len(opt.history_belief) < 2:
            continue

        for i in range(1, len(opt.history_belief)):
            prev = opt.history_belief[i - 1]
            curr = opt.history_belief[i]
            move = curr - prev

            if abs(move) >= threshold:
                # Try to get date from history_ts
                date_str = "unknown"
                if opt.history_ts and i < len(opt.history_ts):
                    date_str = opt.history_ts[i].strftime("%Y-%m-%d")

                moves.append({
                    "date": date_str,
                    "option_idx": opt_idx,
                    "move_pct": move,
                    "prev_close": prev,
                    "close": curr,
                })

    # Sort by absolute move size
    moves.sort(key=lambda m: abs(m["move_pct"]), reverse=True)
    return moves[:5]


def build_feedback(
    verifier_output: VerifierOutput,
    synthesizer_output: SynthesizerOutput,
    data_analyst_output: Optional[DataAnalystOutput],
) -> str:
    """Build feedback string for the next iteration."""
    parts = []

    # Synthesizer focus areas
    if synthesizer_output.retry_focus:
        parts.append(f"Synthesizer focus areas:\n" +
                     "\n".join(f"- {f}" for f in synthesizer_output.retry_focus))

    # Missing evidence from verifier
    if verifier_output.missing_evidence_needs:
        parts.append(f"Missing evidence:\n" +
                     "\n".join(f"- {e}" for e in verifier_output.missing_evidence_needs))

    # Debate feedback
    if verifier_output.debate_assessment:
        da = verifier_output.debate_assessment
        parts.append(f"Debate feedback:\n"
                     f"- YES weakest point: {da.advocate_yes_weakest_point[:100]}\n"
                     f"- NO weakest point: {da.advocate_no_weakest_point[:100]}")

    # Data analyst findings that need follow-up
    if data_analyst_output and data_analyst_output.confidence < 0.5:
        parts.append(f"Data analysis needs more research:\n"
                     f"- {data_analyst_output.reasoning[:200]}")

    return "\n\n".join(parts) if parts else ""


# =============================================================================
# Main Pipeline Class
# =============================================================================

class FullRecursivePipeline:
    """
    Orchestrates the full recursive agent pipeline.

    The pipeline runs iteratively:
    1. PLANNER generates sub-questions
    2. Parallel agents research (ANALYST, ADVOCATE_YES, ADVOCATE_NO)
    3. DATA_ANALYST analyzes market data (parallel or sequential)
    4. VERIFIER judges and critiques
    5. SYNTHESIZER decides to COMMIT or RETRY
    """

    def __init__(
        self,
        api_key: str,
        search_index: Optional[MarketSearchIndex] = None,
        model: str = "gemini-2.0-flash",
        max_iterations: int = 5,
        confidence_threshold: float = 0.7,
        data_analyst_parallel: bool = True,
        verbose: bool = False,
        log_dir: Optional[str] = None,
        wayback_enabled: bool = True,
        wayback_cache_size: int = 256,
        wayback_timeout_seconds: float = 10.0,
        wayback_rate_limit: float = 0.2,
    ):
        """
        Initialize the pipeline.

        Args:
            api_key: Gemini API key
            search_index: TF-IDF search index for DATA_ANALYST
            model: Gemini model to use
            max_iterations: Maximum pipeline iterations
            confidence_threshold: Confidence required to COMMIT
            data_analyst_parallel: If True, DATA_ANALYST runs in parallel with web agents.
                                   If False, runs after and can use their findings.
            verbose: Print debug information
            log_dir: Directory for JSONL logs (RLM visualizer compatible). None disables logging.
            wayback_enabled: Enable Wayback Machine CDX API validation
            wayback_cache_size: LRU cache size for Wayback queries
            wayback_timeout_seconds: HTTP request timeout for Wayback API
            wayback_rate_limit: Minimum seconds between Wayback API requests
        """
        self.api_key = api_key
        self.search_index = search_index
        self.model = model
        self.max_iterations = max_iterations
        self.confidence_threshold = confidence_threshold
        self.data_analyst_parallel = data_analyst_parallel
        self.verbose = verbose
        self.log_dir = log_dir
        self.wayback_enabled = wayback_enabled

        self.client = GeminiAgentClient(api_key, model, verbose)
        self._logger: Optional["FullRecursiveLogger"] = None

        # Track URLs seen across iterations (for deduplication)
        self.seen_urls: Set[str] = set()

        # Initialize Wayback validator
        self.wayback_validator = WaybackValidator(
            enabled=wayback_enabled,
            cache_size=wayback_cache_size,
            timeout=wayback_timeout_seconds,
            rate_limit=wayback_rate_limit,
            verbose=verbose,
        ) if wayback_enabled else None

    def run(self, example: Example) -> PipelineResult:
        """
        Run the full recursive pipeline on an example.

        Args:
            example: Market example to predict

        Returns:
            PipelineResult with prediction and metadata
        """
        # Initialize logger if log_dir is set
        if self.log_dir:
            from .logger import FullRecursiveLogger
            self._logger = FullRecursiveLogger(self.log_dir)
            config = {
                "model": self.model,
                "max_iterations": self.max_iterations,
                "confidence_threshold": self.confidence_threshold,
                "data_analyst_parallel": self.data_analyst_parallel,
            }
            log_path = self._logger.start(
                market_title=example.static_features.get("title", "Unknown")[:100],
                event_id=example.event_id,
                config=config,
            )
            if self.verbose:
                print(f"[Logger] Writing to: {log_path}")

        # Extract market info
        market_title = example.static_features.get("title", "Unknown Market")
        market_description = example.static_features.get("description", "")

        # Time range
        if example.options and example.options[0].history_ts:
            start_date = example.options[0].history_ts[0].strftime("%Y-%m-%d")
            end_date = example.options[0].history_ts[-1].strftime("%Y-%m-%d")
        else:
            start_date = "unknown"
            end_date = "unknown"
        time_range = (start_date, end_date)

        # Research cutoff is the cutoff_ts
        if isinstance(example.cutoff_ts, datetime):
            research_cutoff = example.cutoff_ts.strftime("%Y-%m-%d")
        else:
            research_cutoff = str(example.cutoff_ts)

        # Detect notable moves
        notable_moves = detect_notable_moves(example)

        if self.verbose:
            print(f"\n{'='*60}")
            print(f"FULL RECURSIVE PIPELINE")
            print(f"{'='*60}")
            print(f"Market: {market_title[:50]}...")
            print(f"Time range: {start_date} to {end_date}")
            print(f"Research cutoff: {research_cutoff}")
            print(f"Notable moves: {len(notable_moves)}")
            print(f"Options: {len(example.options)}")
            print(f"DATA_ANALYST mode: {'parallel' if self.data_analyst_parallel else 'sequential'}")
            print()

        # Reset seen URLs for new run
        self.seen_urls = set()

        # Run iterative loop
        iteration_history: List[IterationResult] = []
        feedback: Optional[str] = None
        final_synth: Optional[SynthesizerOutput] = None

        for iteration in range(1, self.max_iterations + 1):
            if self.verbose:
                print(f"\n--- ITERATION {iteration}/{self.max_iterations} ---")

            iter_result = self._run_iteration(
                example=example,
                market_title=market_title,
                market_description=market_description,
                time_range=time_range,
                research_cutoff=research_cutoff,
                notable_moves=notable_moves,
                iteration=iteration,
                previous_feedback=feedback,
                seen_urls=self.seen_urls,
            )

            iteration_history.append(iter_result)

            # Collect URLs from this iteration for deduplication in next iterations
            iter_urls = self._collect_iteration_urls(
                iter_result.analyst_output,
                iter_result.advocate_yes_output,
                iter_result.advocate_no_output,
            )
            self.seen_urls.update(iter_urls)
            if self.verbose and iter_urls:
                print(f"[DEDUP] Collected {len(iter_urls)} unique URLs, total seen: {len(self.seen_urls)}")

            # Log iteration if logger is active
            if self._logger:
                self._logger.log_iteration(iter_result)

            if iter_result.synthesizer_output:
                final_synth = iter_result.synthesizer_output

                if iter_result.decision == "COMMIT":
                    if self.verbose:
                        print(f"\n[COMMIT] Prediction: {final_synth.prediction}")
                        print(f"         Probability: {final_synth.probability:.0%}")
                        print(f"         Confidence: {final_synth.confidence:.0%}")
                    break

                # Build feedback for next iteration
                feedback = build_feedback(
                    iter_result.verifier_output,
                    iter_result.synthesizer_output,
                    iter_result.data_analyst_output,
                )
            else:
                # Agent failed - try to continue
                if self.verbose:
                    print("[WARNING] Iteration produced no synthesizer output")
                feedback = "Previous iteration failed. Please try again with different approach."

        # Build final result
        total_cost = sum(ir.total_cost_usd for ir in iteration_history)
        total_latency = sum(ir.total_latency_ms for ir in iteration_history)

        if final_synth:
            # Convert probability to list format for ForecastMethod
            if len(example.options) == 2:
                # Binary market: [YES, NO]
                probabilities = [final_synth.probability, 1 - final_synth.probability]
            else:
                # Multi-option: use probability for first option, distribute rest
                probabilities = [final_synth.probability]
                remaining = 1 - final_synth.probability
                for _ in range(len(example.options) - 1):
                    prob = remaining / (len(example.options) - 1)
                    probabilities.append(prob)

            result = PipelineResult(
                prediction=final_synth.prediction,
                probability=final_synth.probability,
                confidence=final_synth.confidence,
                probabilities=probabilities,
                iterations_used=len(iteration_history),
                reasoning=final_synth.reasoning,
                caveats=final_synth.caveats,
                total_cost_usd=total_cost,
                total_latency_ms=total_latency,
                iteration_history=iteration_history,
            )

            # Log final result
            if self._logger:
                self._logger.finish(result)
                if self.verbose:
                    print(f"[Logger] Run complete. Log saved.")

            return result
        else:
            # Complete failure - fallback to last price
            result = self._fallback_result(example, iteration_history, total_cost, total_latency)

            # Log fallback result
            if self._logger:
                self._logger.finish(result)

            return result

    def _collect_all_citations(
        self,
        analyst_out: Optional[AnalystOutput],
        advocate_yes_out: Optional[AdvocateOutput],
        advocate_no_out: Optional[AdvocateOutput],
    ) -> List[Citation]:
        """Collect and deduplicate citations from all agent outputs."""
        citations: List[Citation] = []
        seen_urls: set = set()

        def add_citations(citation_list: List[Citation], source: str):
            for c in citation_list:
                if c.url and c.url not in seen_urls:
                    seen_urls.add(c.url)
                    citations.append(c)

        # Collect from analyst
        if analyst_out:
            for answer in analyst_out.answers:
                add_citations(answer.citations, "analyst")
            add_citations(analyst_out.all_sources_used, "analyst")

        # Collect from advocates
        if advocate_yes_out:
            for evidence in advocate_yes_out.primary_evidence:
                if evidence.source.url and evidence.source.url not in seen_urls:
                    seen_urls.add(evidence.source.url)
                    citations.append(evidence.source)
            add_citations(advocate_yes_out.all_sources_used, "advocate_yes")

        if advocate_no_out:
            for evidence in advocate_no_out.primary_evidence:
                if evidence.source.url and evidence.source.url not in seen_urls:
                    seen_urls.add(evidence.source.url)
                    citations.append(evidence.source)
            add_citations(advocate_no_out.all_sources_used, "advocate_no")

        return citations

    def _collect_iteration_urls(
        self,
        analyst_out: Optional[AnalystOutput],
        advocate_yes_out: Optional[AdvocateOutput],
        advocate_no_out: Optional[AdvocateOutput],
    ) -> Set[str]:
        """
        Collect and normalize all URLs from an iteration's agent outputs.

        Used to track what URLs have been seen across iterations for deduplication.
        """
        urls: Set[str] = set()

        def add_url(url: str):
            if url:
                normalized = normalize_url(url)
                if normalized:
                    urls.add(normalized)

        # Collect from analyst
        if analyst_out:
            for answer in analyst_out.answers:
                for c in answer.citations:
                    add_url(c.url)
            for c in analyst_out.all_sources_used:
                add_url(c.url)

        # Collect from advocates
        if advocate_yes_out:
            for evidence in advocate_yes_out.primary_evidence:
                add_url(evidence.source.url)
            for c in advocate_yes_out.all_sources_used:
                add_url(c.url)

        if advocate_no_out:
            for evidence in advocate_no_out.primary_evidence:
                add_url(evidence.source.url)
            for c in advocate_no_out.all_sources_used:
                add_url(c.url)

        return urls

    def _annotate_citations(
        self,
        analyst_out: Optional[AnalystOutput],
        advocate_yes_out: Optional[AdvocateOutput],
        advocate_no_out: Optional[AdvocateOutput],
        validation_results: List[ValidationResult],
    ) -> None:
        """Annotate Citation objects with Wayback validation results."""
        # Build URL -> validation result mapping
        url_to_result: Dict[str, ValidationResult] = {
            r.url: r for r in validation_results
        }

        def annotate_citation(c: Citation):
            result = url_to_result.get(c.url)
            if result:
                c.wayback_validated = True
                if result.wayback_result and result.wayback_result.earliest_snapshot:
                    c.wayback_earliest = result.wayback_result.earliest_snapshot.strftime("%Y-%m-%d")
                c.wayback_suspicious = result.is_suspicious

        # Annotate analyst citations
        if analyst_out:
            for answer in analyst_out.answers:
                for c in answer.citations:
                    annotate_citation(c)
            for c in analyst_out.all_sources_used:
                annotate_citation(c)

        # Annotate advocate citations
        if advocate_yes_out:
            for evidence in advocate_yes_out.primary_evidence:
                annotate_citation(evidence.source)
            for c in advocate_yes_out.all_sources_used:
                annotate_citation(c)

        if advocate_no_out:
            for evidence in advocate_no_out.primary_evidence:
                annotate_citation(evidence.source)
            for c in advocate_no_out.all_sources_used:
                annotate_citation(c)

    def _build_wayback_context(
        self,
        validation_results: List[ValidationResult],
    ) -> str:
        """Build wayback validation context string for verifier prompt."""
        if not validation_results:
            return ""

        suspicious = [r for r in validation_results if r.is_suspicious]
        verified = [r for r in validation_results if r.wayback_result and r.wayback_result.status == "found"]

        lines = ["=== WAYBACK MACHINE VALIDATION RESULTS ==="]
        lines.append(f"Total URLs checked: {len(validation_results)}")
        lines.append(f"Verified (pre-cutoff archive): {len(verified)}")
        lines.append(f"SUSPICIOUS (no pre-cutoff archive): {len(suspicious)}")
        lines.append("")

        if suspicious:
            lines.append("SUSPICIOUS SOURCES (potential leakage):")
            for r in suspicious[:10]:  # Limit to 10
                title = r.title[:50] if r.title else "Unknown"
                lines.append(f"  - {title}: {r.reason}")

        if verified:
            lines.append("")
            lines.append("VERIFIED SOURCES (archived before cutoff):")
            for r in verified[:10]:  # Limit to 10
                title = r.title[:50] if r.title else "Unknown"
                lines.append(f"  - {title}: {r.reason}")

        lines.append("")
        lines.append("NOTE: SUSPICIOUS sources had no archive.org snapshot before the cutoff date.")
        lines.append("This is a STRONG indicator of potential data leakage.")
        lines.append("")

        return "\n".join(lines)

    def _build_wayback_issues(
        self,
        validation_results: List[ValidationResult],
    ) -> List[Issue]:
        """Build Issue objects for suspicious wayback results."""
        issues = []

        suspicious = [r for r in validation_results if r.is_suspicious]

        for r in suspicious:
            # Determine severity based on status
            if r.wayback_result and r.wayback_result.status == "not_found":
                severity = "major"  # No archive = strong leakage indicator
            else:
                severity = "minor"  # Error/timeout = less certain

            issues.append(Issue(
                type="leakage",
                severity=severity,
                description=f"Wayback validation: {r.reason} - URL: {r.url[:80]}",
                affected_claims=[],  # Will be filled by verifier
                recommendation="Verify this source's publication date manually",
            ))

        return issues

    def _run_iteration(
        self,
        example: Example,
        market_title: str,
        market_description: str,
        time_range: Tuple[str, str],
        research_cutoff: str,
        notable_moves: List[Dict],
        iteration: int,
        previous_feedback: Optional[str],
        seen_urls: Optional[Set[str]] = None,
    ) -> IterationResult:
        """Run a single pipeline iteration."""
        result = IterationResult(iteration=iteration)
        start_time = time.time()

        # 1. PLANNER
        if self.verbose:
            print("[PLANNER] Decomposing problem...")

        planner_out, planner_log = run_planner(
            client=self.client,
            market_title=market_title,
            market_description=market_description,
            time_range=time_range,
            research_cutoff=research_cutoff,
            notable_moves=notable_moves,
            query_budget=5,
            previous_feedback=previous_feedback,
            iteration=iteration,
        )

        result.planner_output = planner_out
        result.llm_calls.append(planner_log)

        if not planner_out or not planner_out.sub_questions:
            if self.verbose:
                print("[PLANNER] Failed - no sub-questions generated")
            return result

        if self.verbose:
            print(f"[PLANNER] Generated {len(planner_out.sub_questions)} sub-questions")

        # 2. PARALLEL PHASE (using asyncio for true concurrent execution)
        if self.verbose:
            print("[PARALLEL] Running Analyst + Advocates (async)...")

        # Run web agents in parallel using asyncio.gather()
        async def _run_parallel_agents():
            """Run analyst and advocates concurrently."""
            analyst_task = run_analyst_async(
                self.client, market_title, planner_out.sub_questions,
                time_range, research_cutoff, iteration, seen_urls
            )
            advocate_yes_task = run_advocate_async(
                self.client, market_title, planner_out.sub_questions,
                "YES", time_range, research_cutoff, iteration, seen_urls
            )
            advocate_no_task = run_advocate_async(
                self.client, market_title, planner_out.sub_questions,
                "NO", time_range, research_cutoff, iteration, seen_urls
            )

            return await asyncio.gather(
                analyst_task, advocate_yes_task, advocate_no_task
            )

        # Execute the async parallel phase
        (analyst_out, analyst_log), (advocate_yes_out, advocate_yes_log), (advocate_no_out, advocate_no_log) = asyncio.run(_run_parallel_agents())

        result.analyst_output = analyst_out
        result.advocate_yes_output = advocate_yes_out
        result.advocate_no_output = advocate_no_out
        result.llm_calls.extend([analyst_log, advocate_yes_log, advocate_no_log])

        if self.verbose:
            if analyst_out:
                print(f"[ANALYST] Confidence: {analyst_out.overall_confidence:.0%}")
            if advocate_yes_out:
                print(f"[ADVOCATE YES] Confidence: {advocate_yes_out.confidence:.0%}")
            if advocate_no_out:
                print(f"[ADVOCATE NO] Confidence: {advocate_no_out.confidence:.0%}")

        # 3. DATA_ANALYST
        if self.data_analyst_parallel:
            # Already ran in parallel conceptually, but let's run it now
            # (True parallel would require async, keeping it simple for MVP)
            web_context = None
        else:
            # Sequential: can use web agent findings
            web_context = {
                "analyst": analyst_out,
                "advocate_yes": advocate_yes_out,
                "advocate_no": advocate_no_out,
            }

        if self.verbose:
            mode = "parallel" if self.data_analyst_parallel else "sequential (with web context)"
            print(f"[DATA_ANALYST] Running in {mode} mode...")

        data_analyst_out, data_analyst_log = run_data_analyst(
            example=example,
            sub_questions=planner_out.sub_questions,
            search_index=self.search_index,
            api_key=self.api_key,
            model=self.model,
            max_iterations=5,
            web_context=web_context,
            verbose=self.verbose,
            iteration=iteration,
        )

        result.data_analyst_output = data_analyst_out
        result.llm_calls.append(data_analyst_log)

        if self.verbose and data_analyst_out:
            print(f"[DATA_ANALYST] Probability: {data_analyst_out.data_driven_probability:.0%}, "
                  f"Confidence: {data_analyst_out.confidence:.0%}")

        # Check for critical failures
        if not analyst_out:
            if self.verbose:
                print("[WARNING] Analyst failed")
            return result

        # 3.5. WAYBACK VALIDATION (between agent outputs and verifier)
        wayback_context = ""
        wayback_issues: List[Issue] = []

        if self.wayback_validator and self.wayback_validator.enabled:
            if self.verbose:
                print("[WAYBACK] Validating citation URLs against archive.org...")

            # Parse cutoff date
            try:
                cutoff_dt = datetime.strptime(research_cutoff, "%Y-%m-%d")
            except ValueError:
                cutoff_dt = datetime.now()  # Fallback

            # Collect all citations
            all_citations = self._collect_all_citations(
                analyst_out, advocate_yes_out, advocate_no_out
            )

            if all_citations:
                # Run validation (sync wrapper for async)
                validation_results = validate_citations_sync(
                    citations=all_citations,
                    cutoff=cutoff_dt,
                    enabled=True,
                    verbose=self.verbose,
                )

                # Annotate citations with validation results
                self._annotate_citations(
                    analyst_out, advocate_yes_out, advocate_no_out,
                    validation_results
                )

                # Build context for verifier
                wayback_context = self._build_wayback_context(validation_results)

                # Build issues for suspicious sources
                wayback_issues = self._build_wayback_issues(validation_results)

                if self.verbose:
                    suspicious_count = sum(1 for r in validation_results if r.is_suspicious)
                    print(f"[WAYBACK] Validated {len(all_citations)} URLs, "
                          f"{suspicious_count} suspicious")

        # 4. VERIFIER
        if self.verbose:
            print("[VERIFIER] Judging debate and verifying claims...")

        verifier_out, verifier_log = run_verifier(
            client=self.client,
            market_title=market_title,
            time_range=time_range,
            research_cutoff=research_cutoff,
            analyst_output=analyst_out,
            advocate_yes_output=advocate_yes_out,
            advocate_no_output=advocate_no_out,
            iteration=iteration,
            wayback_context=wayback_context,
        )

        result.verifier_output = verifier_out
        result.llm_calls.append(verifier_log)

        if self.verbose and verifier_out:
            print(f"[VERIFIER] Adjusted confidence: {verifier_out.overall_adjusted_confidence:.0%}")
            if verifier_out.debate_assessment:
                print(f"[VERIFIER] Debate winner: {verifier_out.debate_assessment.winner}")

        if not verifier_out:
            if self.verbose:
                print("[WARNING] Verifier failed")
            return result

        # 5. SYNTHESIZER
        if self.verbose:
            print("[SYNTHESIZER] Making decision...")

        synth_out, synth_log = run_synthesizer(
            client=self.client,
            market_title=market_title,
            time_range=time_range,
            research_cutoff=research_cutoff,
            analyst_output=analyst_out,
            verifier_output=verifier_out,
            iteration=iteration,
            max_iterations=self.max_iterations,
            confidence_threshold=self.confidence_threshold,
        )

        result.synthesizer_output = synth_out
        result.llm_calls.append(synth_log)

        if synth_out:
            result.decision = synth_out.decision
            result.confidence = synth_out.confidence

            if self.verbose:
                print(f"[SYNTHESIZER] Decision: {synth_out.decision}")
                print(f"[SYNTHESIZER] Prediction: {synth_out.prediction}")
                print(f"[SYNTHESIZER] Probability: {synth_out.probability:.0%}")

        # Calculate totals
        result.total_cost_usd = sum(log.cost_estimate_usd for log in result.llm_calls)
        result.total_latency_ms = int((time.time() - start_time) * 1000)

        return result

    def _fallback_result(
        self,
        example: Example,
        iteration_history: List[IterationResult],
        total_cost: float,
        total_latency: int,
    ) -> PipelineResult:
        """Generate fallback result using last price."""
        # Use last price as fallback
        probabilities = []
        for opt in example.options:
            if opt.history_belief:
                probabilities.append(opt.history_belief[-1])
            else:
                probabilities.append(1.0 / len(example.options))

        # Normalize
        total = sum(probabilities)
        if total > 0:
            probabilities = [p / total for p in probabilities]

        return PipelineResult(
            prediction="INSUFFICIENT_EVIDENCE",
            probability=probabilities[0] if probabilities else 0.5,
            confidence=0.3,
            probabilities=probabilities,
            iterations_used=len(iteration_history),
            reasoning="Pipeline failed to produce prediction, using last price fallback",
            caveats=["Fallback prediction", "Agent failures during pipeline"],
            total_cost_usd=total_cost,
            total_latency_ms=total_latency,
            iteration_history=iteration_history,
        )


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Pipeline Implementation:
#
# ORCHESTRATION:
# 1. asyncio.gather() for true parallel web agents (analyst, advocates)
#    - Uses asyncio.to_thread() to wrap blocking Gemini API calls
#    - Replaced ThreadPoolExecutor with asyncio.run(_run_parallel_agents())
# 2. DATA_ANALYST timing controlled by data_analyst_parallel flag
# 3. Sequential mode allows DATA_ANALYST to use web agent findings
#
# 2026-01-22 Async Refactoring:
# 1. Added async versions of agent runners in agents.py (run_*_async functions)
# 2. Pipeline parallel phase now uses asyncio.gather() instead of ThreadPoolExecutor
# 3. Each async function wraps sync call with asyncio.to_thread() for non-blocking I/O
# 4. asyncio.run() is called once per iteration to execute the parallel phase
# 5. This enables true concurrent execution of Gemini API calls
#
# ERROR HANDLING:
# 1. Each agent can fail independently - check outputs
# 2. Pipeline continues if non-critical agent fails
# 3. Fallback to last price if complete failure
#
# ITERATION LOOP:
# 1. PLANNER generates fresh sub-questions each iteration
# 2. Feedback from previous iteration guides new questions
# 3. SYNTHESIZER decides COMMIT vs RETRY based on confidence
#
# SIMPLIFICATIONS FROM TYPESCRIPT:
# 1. No cost tracking per call (simplified)
# 2. Sequential web agents instead of true async
#
# FUTURE IMPROVEMENTS:
# 1. Add async/await for true parallel execution [DONE - 2026-01-22]
# 2. Add per-call cost tracking
# 3. Add source deduplication across iterations [DONE - 2026-01-22]
#
# OBSERVABILITY (added 2026-01-21):
# 1. log_dir parameter enables JSONL logging compatible with RLM visualizer
# 2. Logs: metadata at start, each iteration, summary at end
# 3. Usage:
#    pipeline = FullRecursivePipeline(..., log_dir="logs/full_recursive")
#    result = pipeline.run(example)
#    # Log file created at logs/full_recursive/full_recursive_YYYY-MM-DD_HH-MM-SS_xxxx.jsonl
# 4. View logs: copy to external/rlm/visualizer/public/logs/ and run `npm run dev`
#
# WAYBACK VALIDATION (added 2026-01-21):
# 1. Uses archive.org CDX API to check if URLs were archived before cutoff
# 2. Runs between agent outputs and verifier (step 3.5)
# 3. Results passed to verifier as additional context
# 4. Citations annotated with wayback_validated, wayback_earliest, wayback_suspicious
# 5. Enable/disable via wayback_enabled parameter (default: True)
# 6. FAST_CONFIG preset disables wayback for speed
# 7. Typical latency: 2-4 seconds for 10 URLs (with deduplication and caching)
# 8. Skip patterns prevent wasted API calls on social media, APIs, etc.
#
# SOURCE DEDUPLICATION (2026-01-22):
# - self.seen_urls tracks URLs across iterations, passed to agent prompts
# - normalize_url() strips UTM params, trailing slashes, lowercases domain
# - Max 20 URLs in prompt to avoid bloat
#
