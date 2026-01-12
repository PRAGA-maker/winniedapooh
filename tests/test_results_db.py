"""
Unit tests for the results database functionality.

Tests cover:
- Database initialization and schema creation
- Indexing runs
- Querying runs with filters
- Comparing runs
- Handling missing/malformed data
"""

import pytest
import tempfile
import json
from pathlib import Path
from runner.results_db import ResultsDatabase


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_results.db"
        db = ResultsDatabase(str(db_path))
        yield db


@pytest.fixture
def sample_run_data():
    """Create sample run data for testing."""
    return {
        "run_dir": Path("data/outputs/last_price_abc123/run_1234567890_test"),
        "spec": {
            "run_name": "test_run",
            "method": "last_price",
            "task": "resolve_binary",
            "dataset_path": "data/datasets/v1_unified",
            "seed": 42,
            "method_params": {"param1": "value1"},
            "task_params": {"relax_status": False},
            "description": "Test run"
        },
        "metrics": {
            "test": {
                "brier": 0.180,
                "logloss": 0.520
            },
            "bench": {
                "brier": 0.185,
                "logloss": 0.525
            }
        }
    }


def test_database_initialization(temp_db):
    """Test that database initializes correctly."""
    assert temp_db.db_path.exists()
    assert temp_db.count_runs() == 0


def test_index_run(temp_db, sample_run_data):
    """Test indexing a single run."""
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    assert temp_db.count_runs() == 1


def test_index_multiple_runs(temp_db, sample_run_data):
    """Test indexing multiple runs."""
    # Index first run
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    # Index second run with different method
    run_dir2 = Path("data/outputs/random_baseline_def456/run_1234567891_test2")
    spec2 = sample_run_data["spec"].copy()
    spec2["method"] = "random_baseline"
    spec2["run_name"] = "test_run_2"
    metrics2 = {
        "test": {"brier": 0.250, "logloss": 0.693},
        "bench": {"brier": 0.248, "logloss": 0.695}
    }
    
    temp_db.index_run(run_dir2, spec2, metrics2)
    
    assert temp_db.count_runs() == 2


def test_list_runs(temp_db, sample_run_data):
    """Test listing runs."""
    # Index a run
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    runs = temp_db.list_runs(limit=10)
    assert len(runs) == 1
    assert runs[0]["method"] == "last_price"
    assert runs[0]["test_brier"] == 0.180


def test_list_runs_with_filters(temp_db, sample_run_data):
    """Test listing runs with method filter."""
    # Index two runs with different methods
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    run_dir2 = Path("data/outputs/random_baseline_def456/run_1234567891_test2")
    spec2 = sample_run_data["spec"].copy()
    spec2["method"] = "random_baseline"
    metrics2 = {
        "test": {"brier": 0.250, "logloss": 0.693},
        "bench": {"brier": 0.248, "logloss": 0.695}
    }
    temp_db.index_run(run_dir2, spec2, metrics2)
    
    # Filter by method
    runs = temp_db.list_runs(method="last_price")
    assert len(runs) == 1
    assert runs[0]["method"] == "last_price"


def test_get_run(temp_db, sample_run_data):
    """Test getting a specific run."""
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    run_id = "last_price_abc123/run_1234567890_test"
    run = temp_db.get_run(run_id)
    
    assert run is not None
    assert run["method"] == "last_price"
    assert run["test_brier"] == 0.180


def test_get_nonexistent_run(temp_db):
    """Test getting a run that doesn't exist."""
    run = temp_db.get_run("nonexistent/run_id")
    assert run is None


def test_compare_runs(temp_db, sample_run_data):
    """Test comparing multiple runs."""
    # Index two runs
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    run_dir2 = Path("data/outputs/random_baseline_def456/run_1234567891_test2")
    spec2 = sample_run_data["spec"].copy()
    spec2["method"] = "random_baseline"
    metrics2 = {
        "test": {"brier": 0.250, "logloss": 0.693},
        "bench": {"brier": 0.248, "logloss": 0.695}
    }
    temp_db.index_run(run_dir2, spec2, metrics2)
    
    # Compare runs
    run_ids = ["last_price_abc123/run_1234567890_test", "random_baseline_def456/run_1234567891_test2"]
    runs = temp_db.compare_runs(run_ids)
    
    assert len(runs) == 2
    assert runs[0]["method"] == "last_price"
    assert runs[1]["method"] == "random_baseline"


def test_get_methods_comparison(temp_db, sample_run_data):
    """Test getting methods comparison."""
    # Index runs for different methods
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    run_dir2 = Path("data/outputs/random_baseline_def456/run_1234567891_test2")
    spec2 = sample_run_data["spec"].copy()
    spec2["method"] = "random_baseline"
    metrics2 = {
        "test": {"brier": 0.250, "logloss": 0.693},
        "bench": {"brier": 0.248, "logloss": 0.695}
    }
    temp_db.index_run(run_dir2, spec2, metrics2)
    
    # Get comparison
    runs = temp_db.get_methods_comparison(task="resolve_binary", latest_only=True)
    
    assert len(runs) == 2
    methods = [r["method"] for r in runs]
    assert "last_price" in methods
    assert "random_baseline" in methods


def test_get_method_runs(temp_db, sample_run_data):
    """Test getting all runs for a method."""
    # Index two runs for the same method
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    run_dir2 = Path("data/outputs/last_price_abc123/run_1234567892_test3")
    spec2 = sample_run_data["spec"].copy()
    spec2["run_name"] = "test_run_3"
    temp_db.index_run(run_dir2, spec2, sample_run_data["metrics"])
    
    # Get runs for method
    runs = temp_db.get_method_runs("last_price")
    
    assert len(runs) == 2
    assert all(r["method"] == "last_price" for r in runs)


def test_index_run_with_missing_metrics(temp_db, sample_run_data):
    """Test indexing a run with missing metrics."""
    # Remove metrics
    metrics_with_missing = {
        "test": {},
        "bench": {"brier": 0.185}
    }
    
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        metrics_with_missing
    )
    
    run_id = "last_price_abc123/run_1234567890_test"
    run = temp_db.get_run(run_id)
    
    assert run is not None
    assert run["test_brier"] is None
    assert run["bench_brier"] == 0.185


def test_replace_existing_run(temp_db, sample_run_data):
    """Test that re-indexing a run replaces the old data."""
    # Index initial run
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    # Re-index with updated metrics
    updated_metrics = {
        "test": {"brier": 0.150, "logloss": 0.450},
        "bench": {"brier": 0.155, "logloss": 0.455}
    }
    
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        updated_metrics
    )
    
    # Should still have only 1 run
    assert temp_db.count_runs() == 1
    
    # Check that metrics were updated
    run_id = "last_price_abc123/run_1234567890_test"
    run = temp_db.get_run(run_id)
    assert run["test_brier"] == 0.150


def test_get_metrics_summary(temp_db, sample_run_data):
    """Test getting aggregated metrics summary."""
    # Index multiple runs
    temp_db.index_run(
        sample_run_data["run_dir"],
        sample_run_data["spec"],
        sample_run_data["metrics"]
    )
    
    run_dir2 = Path("data/outputs/last_price_abc123/run_1234567892_test3")
    spec2 = sample_run_data["spec"].copy()
    spec2["run_name"] = "test_run_3"
    metrics2 = {
        "test": {"brier": 0.170, "logloss": 0.500},
        "bench": {"brier": 0.175, "logloss": 0.505}
    }
    temp_db.index_run(run_dir2, spec2, metrics2)
    
    # Get summary
    summary = temp_db.get_metrics_summary(method="last_price")
    
    assert len(summary) == 1
    assert summary[0]["method"] == "last_price"
    assert summary[0]["run_count"] == 2
    assert summary[0]["avg_test_brier"] == pytest.approx(0.175, abs=0.001)


# LESSONS LEARNED:
# 1. Using pytest fixtures for temp database ensures each test starts with a clean state.
# 2. Testing edge cases (missing metrics, nonexistent runs) is crucial for robustness.
# 3. Testing the "replace" behavior (INSERT OR REPLACE) ensures re-indexing works correctly.
# 4. Testing filters and queries separately helps isolate issues if queries break.
# 5. Using sample_run_data fixture reduces duplication and makes tests easier to maintain.
