# Results Visualization & Analysis Scripts

This directory contains tools for analyzing and visualizing experiment results from the Winnie Da Pooh forecasting pipeline.

## Quick Start

```bash
# List recent runs
uv run scripts/results/list_runs.py

# Compare methods
uv run scripts/results/compare_runs.py --task resolve_binary --latest

# Visualize results
uv run scripts/results/plot_results.py --task resolve_binary --metric test_brier --output results.png

# Index pre-existing runs
uv run scripts/results/scan_existing_runs.py
```

## Scripts

### `list_runs.py` - Browse and Query Runs

List experiment runs with filtering and detailed views.

```bash
# List last 10 runs
uv run scripts/results/list_runs.py

# Filter by method
uv run scripts/results/list_runs.py --method last_price

# Filter by task
uv run scripts/results/list_runs.py --task resolve_binary

# Show runs from last 7 days
uv run scripts/results/list_runs.py --since 7

# Show detailed view of a specific run
uv run scripts/results/list_runs.py --run-id "last_price_abc123/run_1234567890_test"

# Show all runs (no limit)
uv run scripts/results/list_runs.py --all
```

**Output**: Formatted table with timestamps, methods, tasks, and metrics.

### `compare_runs.py` - Compare Methods Side-by-Side

Compare performance across different forecasting methods.

```bash
# Compare all methods on a task (latest run of each)
uv run scripts/results/compare_runs.py --task resolve_binary --latest

# Compare two specific runs
uv run scripts/results/compare_runs.py --runs run_id_1 run_id_2

# Compare all runs of a specific method
uv run scripts/results/compare_runs.py --method last_price

# Export comparison to CSV
uv run scripts/results/compare_runs.py --task resolve_binary --output comparison.csv
```

**Output**: Performance table with metrics, best performer, and improvement percentages.

### `plot_results.py` - Visualize Results

Generate plots comparing methods and showing trends.

```bash
# Bar chart: compare methods on one metric
uv run scripts/results/plot_results.py --task resolve_binary --metric test_brier

# Grouped bar chart: multiple metrics
uv run scripts/results/plot_results.py --task resolve_binary --metrics test_brier,test_logloss

# Line chart: performance over time
uv run scripts/results/plot_results.py --method mlp_nn --timeline

# Scatter plot: test vs bench performance
uv run scripts/results/plot_results.py --task resolve_binary --scatter

# Save to file
uv run scripts/results/plot_results.py --task resolve_binary --output results.png
```

**Output**: matplotlib plots (displayed or saved to file).

### `scan_existing_runs.py` - Index Pre-existing Runs

Scan `data/outputs/` and index runs that aren't in the database yet.

```bash
# Scan and index all runs
uv run scripts/results/scan_existing_runs.py

# Dry run (preview what would be indexed)
uv run scripts/results/scan_existing_runs.py --dry-run

# Re-index everything (clears database first)
uv run scripts/results/scan_existing_runs.py --reindex
```

**Use Case**: Useful when runs exist before the database system was added, or after database corruption.

## Database

Results are indexed to `data/results.db` (SQLite database, auto-created, git-ignored).

**Schema**:
- Run metadata (method, task, timestamp, seed, etc.)
- Flattened metrics (test_brier, bench_brier, etc.)
- Paths to run outputs

**Indexing**: Automatic when running experiments via `runner/runner.py`.

## Workflow

```
1. Run experiments
   └─> runner/runner.py --method <method> --task <task>

2. Automatically indexed
   └─> data/results.db (SQLite)

3. Query/analyze
   ├─> scripts/results/list_runs.py
   ├─> scripts/results/compare_runs.py
   └─> scripts/results/plot_results.py

4. Export/share
   └─> PNG plots, CSV comparisons
```

## Design Principles

1. **Modular**: Each script focuses on one task (list, compare, visualize)
2. **Composable**: Scripts can be chained or used independently
3. **Fast**: Database queries are indexed for speed (<100ms typical)
4. **Scalable**: Handles hundreds/thousands of runs efficiently
5. **Extensible**: Easy to add new metrics, visualizations, or filters

## Adding New Metrics

When you add metrics to tasks:

1. Update database schema in `runner/results_db.py` (add columns)
2. Scripts automatically pick up new metrics
3. No other changes needed

## Adding New Visualizations

To add a new plot type:

1. Add function to `plot_results.py` (e.g., `plot_heatmap()`)
2. Add CLI flag (e.g., `--heatmap`)
3. Call function in `main()` based on flag

## Known Issues

- Unicode box characters may not render on some Windows terminals (use Windows Terminal)
- Large datasets (10K+ runs) may slow down `--all` queries
- Plots require matplotlib display (use `--output` for headless environments)

## Future Improvements

- Add filtering by date range, dataset version, seed
- Add aggregation views (average performance across seeds)
- Add regression detection (alert if performance degrades)
- Add interactive plots (plotly/bokeh)
- Add web dashboard (Flask/FastAPI)

## Questions?

See `runner/results_db.py` for database implementation details.
