#!/usr/bin/env python3
"""
List and query experiment runs from the results database.

Usage examples:
    # List last 10 runs
    uv run scripts/list_runs.py
    
    # List runs for a specific method
    uv run scripts/list_runs.py --method last_price
    
    # List runs from last 7 days
    uv run scripts/list_runs.py --since 7
    
    # Show detailed view of a run
    uv run scripts/list_runs.py --run-id "last_price_abc123/run_1234567890_test"
    
    # List more runs
    uv run scripts/list_runs.py --limit 20
"""

import sys
import argparse
import json
from pathlib import Path
from datetime import datetime

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


def truncate(text: str, length: int = 20) -> str:
    """Truncate text to specified length."""
    if text is None:
        return "N/A"
    if len(text) <= length:
        return text
    return text[:length-3] + "..."


def print_runs_table(runs: list):
    """Print runs in a formatted table."""
    if not runs:
        print("No runs found.")
        return
    
    # Table headers
    headers = ["Timestamp", "Method", "Task", "Test Brier", "Bench Brier", "Run Name"]
    
    # Calculate column widths
    col_widths = [len(h) for h in headers]
    col_widths[0] = 16  # Timestamp
    col_widths[1] = 15  # Method
    col_widths[2] = 15  # Task
    col_widths[3] = 12  # Test Brier
    col_widths[4] = 12  # Bench Brier
    col_widths[5] = 20  # Run Name
    
    # Print header
    print()
    print("Recent Runs:")
    print("┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐")
    print("│ " + " │ ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " │")
    print("├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤")
    
    # Print rows
    for run in runs:
        row = [
            format_timestamp(run["timestamp"]),
            truncate(run["method"], 15),
            truncate(run["task"], 15),
            format_metric(run["test_brier"]),
            format_metric(run["bench_brier"]),
            truncate(run["run_name"], 20)
        ]
        print("│ " + " │ ".join(val.ljust(w) for val, w in zip(row, col_widths)) + " │")
    
    print("└" + "┴".join("─" * (w + 2) for w in col_widths) + "┘")
    print()


def print_run_details(run: dict):
    """Print detailed information about a single run."""
    print()
    print("=" * 80)
    print(f"Run Details: {run['run_id']}")
    print("=" * 80)
    print()
    
    print(f"Run Name:     {run['run_name']}")
    print(f"Method:       {run['method']}")
    print(f"Task:         {run['task']}")
    print(f"Timestamp:    {format_timestamp(run['timestamp'])}")
    print(f"Seed:         {run['seed']}")
    print(f"Dataset:      {run['dataset_path']}")
    print()
    
    print("Description:")
    print(f"  {run['description'] if run['description'] else 'N/A'}")
    print()
    
    print("Method Parameters:")
    params = json.loads(run['method_params'])
    if params:
        for key, val in params.items():
            print(f"  {key}: {val}")
    else:
        print("  (none)")
    print()
    
    print("Task Parameters:")
    task_params = json.loads(run['task_params'])
    if task_params:
        for key, val in task_params.items():
            print(f"  {key}: {val}")
    else:
        print("  (none)")
    print()
    
    print("Test Metrics:")
    print(f"  Brier Score: {format_metric(run['test_brier'])}")
    print(f"  Log Loss:    {format_metric(run['test_logloss'])}")
    print()
    
    print("Bench Metrics:")
    print(f"  Brier Score: {format_metric(run['bench_brier'])}")
    print(f"  Log Loss:    {format_metric(run['bench_logloss'])}")
    print()
    
    print("Paths:")
    print(f"  Run Dir:     {run['run_dir']}")
    print(f"  Model:       {run['model_path']}")
    print(f"  Metrics:     {run['metrics_path']}")
    print(f"  Spec:        {run['spec_path']}")
    print()
    print("=" * 80)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="List and query experiment runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--limit", type=int, default=10, help="Maximum number of runs to show")
    parser.add_argument("--method", type=str, help="Filter by method name")
    parser.add_argument("--task", type=str, help="Filter by task name")
    parser.add_argument("--since", type=int, help="Only show runs from the last N days")
    parser.add_argument("--run-id", type=str, help="Show detailed view of a specific run")
    parser.add_argument("--all", action="store_true", help="Show all runs (no limit)")
    
    args = parser.parse_args()
    
    # Initialize database
    db = ResultsDatabase()
    
    # Check if database has any runs
    total_runs = db.count_runs()
    if total_runs == 0:
        print("No runs found in database.")
        print("Run some experiments to populate the database, or use scripts/scan_existing_runs.py")
        return
    
    # Show detailed view of a specific run
    if args.run_id:
        run = db.get_run(args.run_id)
        if run:
            print_run_details(run)
        else:
            print(f"Run not found: {args.run_id}")
        return
    
    # List runs
    limit = None if args.all else args.limit
    runs = db.list_runs(
        limit=limit or 1000,  # Use a high limit if --all is specified
        method=args.method,
        task=args.task,
        since_days=args.since
    )
    
    print_runs_table(runs)
    
    # Show summary
    filters = []
    if args.method:
        filters.append(f"method={args.method}")
    if args.task:
        filters.append(f"task={args.task}")
    if args.since:
        filters.append(f"last {args.since} days")
    
    filter_str = f" (filtered: {', '.join(filters)})" if filters else ""
    print(f"Showing {len(runs)} of {total_runs} total runs{filter_str}")
    print()


if __name__ == "__main__":
    main()


# LESSONS LEARNED:
# 1. Using Unicode box drawing characters makes the table much more readable than
#    ASCII art. Modern terminals support these well.
# 2. Truncating long fields (task names, etc.) keeps the table width manageable.
#    Users can use --run-id to see full details if needed.
# 3. Formatting timestamps as readable dates is essential - Unix timestamps are
#    hard to interpret at a glance.
# 4. Separating table view (for lists) and detailed view (for single run) provides
#    both quick browsing and deep inspection capabilities.
# 5. Showing total run count helps users understand how much data they have.
