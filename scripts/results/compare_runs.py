#!/usr/bin/env python3
"""
Compare experiment runs side-by-side.

Usage examples:
    # Compare two specific runs
    uv run scripts/compare_runs.py --runs run_id_1 run_id_2
    
    # Compare all runs of a method
    uv run scripts/compare_runs.py --method last_price
    
    # Compare all methods on a task (latest run of each)
    uv run scripts/compare_runs.py --task resolve_binary --latest
    
    # Export comparison to CSV
    uv run scripts/compare_runs.py --task resolve_binary --output comparison.csv
"""

import sys
import argparse
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from runner.results_db import ResultsDatabase


def format_timestamp(timestamp: int) -> str:
    """Format Unix timestamp as readable date."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")


def format_metric(value: float) -> str:
    """Format metric value for display."""
    if value is None:
        return "N/A"
    return f"{value:.4f}"


def calculate_improvement(baseline: float, current: float) -> str:
    """Calculate percentage improvement (negative is better for loss metrics)."""
    if baseline is None or current is None or baseline == 0:
        return "N/A"
    
    improvement = ((baseline - current) / baseline) * 100
    if improvement > 0:
        return f"+{improvement:.1f}%"
    else:
        return f"{improvement:.1f}%"


def print_comparison_table(runs: List[Dict[str, Any]], metrics: List[str] = None):
    """Print comparison table for runs."""
    if not runs:
        print("No runs to compare.")
        return
    
    if metrics is None:
        metrics = ["test_brier", "bench_brier", "test_logloss", "bench_logloss"]
    
    # Filter out metrics that don't exist in any run
    available_metrics = [m for m in metrics if any(run.get(m) is not None for run in runs)]
    
    if not available_metrics:
        print("No metrics available for comparison.")
        return
    
    # Print header
    print()
    print("=" * 100)
    print("Run Comparison")
    print("=" * 100)
    print()
    
    # Column widths
    method_width = max(len(run["method"]) for run in runs) + 2
    method_width = max(method_width, 15)
    metric_width = 12
    
    # Build header row
    header = "Method".ljust(method_width)
    for metric in available_metrics:
        metric_display = metric.replace("_", " ").title()
        header += metric_display.ljust(metric_width)
    header += "Timestamp".ljust(18) + "Run Name"
    
    print(header)
    print("-" * len(header))
    
    # Print each run
    for run in runs:
        row = run["method"].ljust(method_width)
        for metric in available_metrics:
            row += format_metric(run.get(metric)).ljust(metric_width)
        row += format_timestamp(run["timestamp"]).ljust(18)
        row += run["run_name"]
        print(row)
    
    print()
    
    # If comparing multiple runs, show best performer for each metric
    if len(runs) > 1:
        print("Best Performers:")
        print("-" * 50)
        
        for metric in available_metrics:
            # Filter runs that have this metric
            runs_with_metric = [r for r in runs if r.get(metric) is not None]
            if runs_with_metric:
                best_run = min(runs_with_metric, key=lambda r: r[metric])
                print(f"  {metric}: {best_run['method']} ({format_metric(best_run[metric])})")
        
        print()


def print_method_comparison(runs: List[Dict[str, Any]], task: str = None):
    """Print method comparison with improvements vs baseline."""
    if not runs:
        print("No runs to compare.")
        return
    
    # Group by method (take latest for each if multiple)
    methods = {}
    for run in runs:
        method = run["method"]
        if method not in methods or run["timestamp"] > methods[method]["timestamp"]:
            methods[method] = run
    
    runs = sorted(methods.values(), key=lambda r: r.get("test_brier") or float("inf"))
    
    print()
    print("=" * 100)
    if task:
        print(f"Method Comparison (Task: {task})")
    else:
        print("Method Comparison")
    print("=" * 100)
    print()
    
    # Calculate column widths
    method_width = max(len(run["method"]) for run in runs) + 2
    method_width = max(method_width, 20)
    
    # Print header
    header = "Method".ljust(method_width)
    header += "Test Brier".ljust(14)
    header += "Bench Brier".ljust(14)
    header += "Test LogLoss".ljust(14)
    header += "Bench LogLoss".ljust(14)
    print(header)
    print("-" * len(header))
    
    # Print each method
    for run in runs:
        row = run["method"].ljust(method_width)
        row += format_metric(run.get("test_brier")).ljust(14)
        row += format_metric(run.get("bench_brier")).ljust(14)
        row += format_metric(run.get("test_logloss")).ljust(14)
        row += format_metric(run.get("bench_logloss")).ljust(14)
        print(row)
    
    print()
    
    # Show best performer and improvements
    if runs and runs[0].get("test_brier") is not None:
        best_run = runs[0]
        print(f"Best: {best_run['method']} (test_brier: {format_metric(best_run['test_brier'])})")
        print()
        
        # Show improvement vs other methods
        if len(runs) > 1:
            print("Improvements vs other methods (test_brier):")
            for i, run in enumerate(runs[1:], 1):
                if run.get("test_brier") is not None:
                    improvement = calculate_improvement(run["test_brier"], best_run["test_brier"])
                    print(f"  vs {run['method']}: {improvement}")
            print()


def export_to_csv(runs: List[Dict[str, Any]], output_path: str):
    """Export comparison to CSV file."""
    if not runs:
        print("No runs to export.")
        return
    
    # Define columns to export
    columns = [
        "run_id", "run_name", "method", "task", "timestamp",
        "test_brier", "test_logloss", "bench_brier", "bench_logloss",
        "seed", "dataset_path", "run_dir"
    ]
    
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        
        for run in runs:
            row = {col: run.get(col) for col in columns}
            writer.writerow(row)
    
    print(f"Exported {len(runs)} runs to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Compare experiment runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--runs", nargs="+", help="Specific run IDs to compare")
    parser.add_argument("--method", type=str, help="Compare all runs of a method")
    parser.add_argument("--task", type=str, help="Compare all methods on a task")
    parser.add_argument("--latest", action="store_true", help="Only use latest run of each method")
    parser.add_argument("--output", type=str, help="Export to CSV file")
    
    args = parser.parse_args()
    
    # Initialize database
    db = ResultsDatabase()
    
    # Check if database has any runs
    total_runs = db.count_runs()
    if total_runs == 0:
        print("No runs found in database.")
        print("Run some experiments to populate the database.")
        return
    
    # Determine what to compare
    runs = []
    
    if args.runs:
        # Compare specific runs
        runs = db.compare_runs(args.runs)
        if len(runs) != len(args.runs):
            found_ids = [r["run_id"] for r in runs]
            missing = [rid for rid in args.runs if rid not in found_ids]
            print(f"Warning: Could not find runs: {', '.join(missing)}")
    
    elif args.method:
        # Compare all runs of a method
        runs = db.get_method_runs(args.method)
        if not runs:
            print(f"No runs found for method: {args.method}")
            return
    
    elif args.task:
        # Compare methods on a task
        runs = db.get_methods_comparison(task=args.task, latest_only=args.latest)
        if not runs:
            print(f"No runs found for task: {args.task}")
            return
    
    else:
        # Default: compare all methods (latest only)
        runs = db.get_methods_comparison(latest_only=True)
    
    # Export to CSV if requested
    if args.output:
        export_to_csv(runs, args.output)
        if not args.runs and not args.method:
            # Don't print table if exporting and doing method comparison
            return
    
    # Print comparison
    if args.task or (not args.runs and not args.method):
        # Method comparison view
        print_method_comparison(runs, task=args.task)
    else:
        # Detailed comparison view
        print_comparison_table(runs)


if __name__ == "__main__":
    main()


# LESSONS LEARNED:
# 1. Separate views for method comparison (grouped by method) vs run comparison
#    (all runs) makes the tool more useful in different contexts.
# 2. Showing "best performer" and relative improvements helps quickly assess
#    which method is winning and by how much.
# 3. CSV export is valuable for further analysis in Excel/pandas/R.
# 4. Using "latest only" by default for method comparison avoids clutter when
#    multiple runs exist for the same method.
# 5. Handling missing metrics gracefully (N/A) is important since not all runs
#    may have all metrics computed.
