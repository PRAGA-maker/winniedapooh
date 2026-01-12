#!/usr/bin/env python3
"""
Visualize and plot experiment results.

Usage examples:
    # Plot all methods comparison
    uv run scripts/plot_results.py --task resolve_binary --metric test_brier
    
    # Plot method over time
    uv run scripts/plot_results.py --method mlp_nn --timeline
    
    # Plot multiple metrics
    uv run scripts/plot_results.py --task resolve_binary --metrics test_brier,test_logloss
    
    # Save plot to file
    uv run scripts/plot_results.py --task resolve_binary --output results.png
    
    # Test vs bench scatter
    uv run scripts/plot_results.py --task resolve_binary --scatter
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import matplotlib.pyplot as plt
import numpy as np

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from runner.results_db import ResultsDatabase


def plot_methods_comparison(
    runs: List[Dict[str, Any]],
    metric: str,
    task: Optional[str] = None,
    output_path: Optional[str] = None
):
    """
    Plot bar chart comparing methods on a single metric.
    
    Args:
        runs: List of run dictionaries
        metric: Metric to plot (e.g., "test_brier")
        task: Optional task name for title
        output_path: Optional path to save plot
    """
    # Group by method (take latest for each)
    methods = {}
    for run in runs:
        method = run["method"]
        if method not in methods or run["timestamp"] > methods[method]["timestamp"]:
            methods[method] = run
    
    # Extract data
    method_names = []
    values = []
    for method, run in sorted(methods.items()):
        if run.get(metric) is not None:
            method_names.append(method)
            values.append(run[metric])
    
    if not method_names:
        print(f"No data available for metric: {metric}")
        return
    
    # Create plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bars = ax.bar(method_names, values)
    
    # Color bars - gradient from best (green) to worst (red)
    colors = plt.cm.RdYlGn_r(np.linspace(0.3, 0.9, len(values)))
    sorted_indices = np.argsort(values)
    for i, bar in enumerate(bars):
        bar.set_color(colors[np.where(sorted_indices == i)[0][0]])
    
    # Formatting
    ax.set_ylabel(metric.replace("_", " ").title(), fontsize=12)
    ax.set_xlabel("Method", fontsize=12)
    
    title = f"Method Comparison: {metric.replace('_', ' ').title()}"
    if task:
        title += f" (Task: {task})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    
    # Rotate x labels if many methods
    if len(method_names) > 5:
        plt.xticks(rotation=45, ha="right")
    
    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, values)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.4f}',
                ha='center', va='bottom', fontsize=9)
    
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save or show
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_path}")
    else:
        plt.show()


def plot_multiple_metrics(
    runs: List[Dict[str, Any]],
    metrics: List[str],
    task: Optional[str] = None,
    output_path: Optional[str] = None
):
    """
    Plot grouped bar chart comparing methods across multiple metrics.
    
    Args:
        runs: List of run dictionaries
        metrics: List of metrics to plot
        task: Optional task name for title
        output_path: Optional path to save plot
    """
    # Group by method (take latest for each)
    methods = {}
    for run in runs:
        method = run["method"]
        if method not in methods or run["timestamp"] > methods[method]["timestamp"]:
            methods[method] = run
    
    # Extract data
    method_names = sorted(methods.keys())
    
    # Check which metrics are available
    available_metrics = [m for m in metrics if any(methods[mn].get(m) is not None for mn in method_names)]
    
    if not available_metrics:
        print("No data available for specified metrics.")
        return
    
    # Prepare data
    data = {metric: [] for metric in available_metrics}
    for method in method_names:
        for metric in available_metrics:
            data[metric].append(methods[method].get(metric, 0))
    
    # Create plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(method_names))
    width = 0.8 / len(available_metrics)
    
    # Plot bars for each metric
    for i, metric in enumerate(available_metrics):
        offset = (i - len(available_metrics)/2 + 0.5) * width
        ax.bar(x + offset, data[metric], width, label=metric.replace("_", " ").title())
    
    # Formatting
    ax.set_ylabel("Score", fontsize=12)
    ax.set_xlabel("Method", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(method_names)
    
    title = "Method Comparison: Multiple Metrics"
    if task:
        title += f" (Task: {task})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    
    # Rotate x labels if many methods
    if len(method_names) > 5:
        plt.xticks(rotation=45, ha="right")
    
    ax.legend(fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save or show
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_path}")
    else:
        plt.show()


def plot_timeline(
    runs: List[Dict[str, Any]],
    metric: str,
    method: Optional[str] = None,
    output_path: Optional[str] = None
):
    """
    Plot line chart showing metric over time.
    
    Args:
        runs: List of run dictionaries
        metric: Metric to plot
        method: Optional method name for title
        output_path: Optional path to save plot
    """
    # Sort by timestamp
    runs = sorted(runs, key=lambda r: r["timestamp"])
    
    # Extract data
    timestamps = []
    values = []
    for run in runs:
        if run.get(metric) is not None:
            timestamps.append(datetime.fromtimestamp(run["timestamp"]))
            values.append(run[metric])
    
    if not timestamps:
        print(f"No data available for metric: {metric}")
        return
    
    # Create plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(timestamps, values, marker='o', linewidth=2, markersize=8)
    
    # Formatting
    ax.set_ylabel(metric.replace("_", " ").title(), fontsize=12)
    ax.set_xlabel("Date", fontsize=12)
    
    title = f"Performance Over Time: {metric.replace('_', ' ').title()}"
    if method:
        title += f" (Method: {method})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    
    # Rotate x labels
    plt.xticks(rotation=45, ha="right")
    
    ax.grid(alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save or show
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_path}")
    else:
        plt.show()


def plot_test_vs_bench_scatter(
    runs: List[Dict[str, Any]],
    metric_base: str = "brier",
    task: Optional[str] = None,
    output_path: Optional[str] = None
):
    """
    Plot scatter plot of test vs bench performance.
    
    Args:
        runs: List of run dictionaries
        metric_base: Base metric name (e.g., "brier" for test_brier/bench_brier)
        task: Optional task name for title
        output_path: Optional path to save plot
    """
    test_metric = f"test_{metric_base}"
    bench_metric = f"bench_{metric_base}"
    
    # Group by method (take latest for each)
    methods = {}
    for run in runs:
        method = run["method"]
        if method not in methods or run["timestamp"] > methods[method]["timestamp"]:
            methods[method] = run
    
    # Extract data
    test_values = []
    bench_values = []
    labels = []
    
    for method, run in sorted(methods.items()):
        if run.get(test_metric) is not None and run.get(bench_metric) is not None:
            test_values.append(run[test_metric])
            bench_values.append(run[bench_metric])
            labels.append(method)
    
    if not test_values:
        print(f"No data available for {test_metric} vs {bench_metric}")
        return
    
    # Create plot
    fig, ax = plt.subplots(figsize=(10, 8))
    
    scatter = ax.scatter(test_values, bench_values, s=100, alpha=0.6)
    
    # Add labels for each point
    for i, label in enumerate(labels):
        ax.annotate(label, (test_values[i], bench_values[i]), 
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=9, alpha=0.8)
    
    # Add diagonal line (test = bench)
    min_val = min(min(test_values), min(bench_values))
    max_val = max(max(test_values), max(bench_values))
    ax.plot([min_val, max_val], [min_val, max_val], 
            'r--', alpha=0.5, linewidth=2, label='Test = Bench')
    
    # Formatting
    ax.set_xlabel(f"Test {metric_base.title()}", fontsize=12)
    ax.set_ylabel(f"Bench {metric_base.title()}", fontsize=12)
    
    title = f"Test vs Bench Performance: {metric_base.title()}"
    if task:
        title += f" (Task: {task})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3, linestyle='--')
    
    # Make axes equal scale
    ax.set_aspect('equal', adjustable='box')
    
    plt.tight_layout()
    
    # Save or show
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize experiment results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--task", type=str, help="Filter by task name")
    parser.add_argument("--method", type=str, help="Filter by method name")
    parser.add_argument("--metric", type=str, default="test_brier", help="Metric to plot")
    parser.add_argument("--metrics", type=str, help="Comma-separated list of metrics to plot")
    parser.add_argument("--timeline", action="store_true", help="Plot metric over time")
    parser.add_argument("--scatter", action="store_true", help="Plot test vs bench scatter")
    parser.add_argument("--output", type=str, help="Save plot to file (e.g., results.png)")
    
    args = parser.parse_args()
    
    # Initialize database
    db = ResultsDatabase()
    
    # Check if database has any runs
    total_runs = db.count_runs()
    if total_runs == 0:
        print("No runs found in database.")
        print("Run some experiments to populate the database.")
        return
    
    # Get runs
    if args.method:
        runs = db.get_method_runs(args.method)
        if not runs:
            print(f"No runs found for method: {args.method}")
            return
    elif args.task:
        runs = db.get_methods_comparison(task=args.task, latest_only=not args.timeline)
        if not runs:
            print(f"No runs found for task: {args.task}")
            return
    else:
        runs = db.get_methods_comparison(latest_only=not args.timeline)
    
    # Generate plot
    if args.scatter:
        # Test vs bench scatter plot
        metric_base = args.metric.replace("test_", "").replace("bench_", "")
        plot_test_vs_bench_scatter(runs, metric_base, args.task, args.output)
    
    elif args.timeline:
        # Timeline plot
        plot_timeline(runs, args.metric, args.method, args.output)
    
    elif args.metrics:
        # Multiple metrics comparison
        metrics = [m.strip() for m in args.metrics.split(",")]
        plot_multiple_metrics(runs, metrics, args.task, args.output)
    
    else:
        # Single metric bar chart
        plot_methods_comparison(runs, args.metric, args.task, args.output)


if __name__ == "__main__":
    main()


# LESSONS LEARNED:
# 1. Color coding bars by performance (gradient) makes it immediately obvious which
#    methods are better without having to read the values.
# 2. Adding value labels directly on bars saves the need to squint at the y-axis.
# 3. Test vs bench scatter with diagonal line quickly shows if methods generalize
#    well (points near diagonal) or overfit (points above diagonal).
# 4. DPI 300 is publication quality - good default for saving plots.
# 5. Tight layout prevents labels from getting cut off - essential with rotated labels.
# 6. Making axes equal scale for scatter plots prevents visual distortion of the data.
