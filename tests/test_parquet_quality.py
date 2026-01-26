"""
Lightweight parquet quality test to ensure audit runs.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent))

import parquet_quality


def test_parquet_quality_audit_runs(latest_dataset_path, tmp_path):
    """Run the parquet quality audit on the latest dataset."""
    parquet_path = latest_dataset_path / "data.parquet"
    if not parquet_path.exists():
        pytest.skip("Parquet file not found")

    summary, _ = parquet_quality.run_quality_audit(
        parquet_path,
        tmp_path,
        sample_rows=2000,
    )

    assert summary["row_count"] > 0
    assert "columns" in summary


def test_parquet_quality_audit_runs_metaculus_source(latest_dataset_path, tmp_path):
    """Run the parquet quality audit for Metaculus only."""
    parquet_path = latest_dataset_path / "data.parquet"
    if not parquet_path.exists():
        pytest.skip("Parquet file not found")

    summary, _ = parquet_quality.run_quality_audit(
        parquet_path,
        tmp_path,
        sample_rows=2000,
        source="metaculus",
    )

    assert "validation" in summary
    assert summary["validation"]["source_filter"] == "metaculus"
    if summary["row_count"] == 0:
        assert "source_missing=metaculus" in summary["validation"]["errors"]
    else:
        assert "metaculus_options_json" in summary["validation"]

# --- LESSONS LEARNED ---
# 1. Keep the audit lightweight to avoid test slowdowns.
