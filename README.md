# Winnie Da Pooh - Forecasting Data Pipeline

Reproducible pipeline for fetching forecasting market data from Kalshi and Metaculus, normalizing it into a unified schema, and benchmarking forecasting methods.

## Prerequisites

- [uv](https://github.com/astral-sh/uv) for dependency management.
- Python 3.13+

## Setup

1. Clone the repository.
2. Create a `.env` file based on `.env.example` and add your API keys.
   - `KALSHI_API_KEY_ID`
   - `KALSHI_PRIVATE_KEY_PATH` (path to your PEM file)
   - `METACULUS_TOKEN`
3. Install dependencies:
   ```bash
   uv sync
   ```

## Usage

### Build Unified Dataset

To build the unified parquet dataset (Kalshi + Metaculus):
```bash
uv run scripts/build_db.py --limit 100
```
This will:
- Fetch settled/closed markets from Kalshi with trade history.
- Fetch posts and questions from Metaculus with community prediction history.
- Normalize and save to `data/datasets/vYYYYMMDD_HHMM_unified/`.

### Run Benchmarks

To run a benchmark experiment:
```bash
uv run runner/runner.py
```
This currently runs the `last_price` baseline on the latest unified dataset.

### Inspect Data

To see a summary of the latest dataset:
```bash
uv run scripts/inspect_parquet.py
```

## Architecture

- `src/common/`: Shared utilities (http, config, schema, parquet).
- `src/kalshi/`: Kalshi-specific ingestion and mapping logic.
- `src/metaculus/`: Metaculus-specific ingestion and mapping logic.
- `dataobject/`: Data models, dataset loading, and task definitions.
- `methods/`: Forecasting method implementations (baselines, models).
- `runner/`: Experiment orchestration and evaluation.
- `scripts/`: CLI entry points and smoke tests.
- `data/`: Raw data, cleaned parquets, and experiment runs.
