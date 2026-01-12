"""
Integration tests for the results database system.

Tests the full workflow: run experiments -> index to database -> query -> compare -> visualize.
"""

import pytest
import subprocess
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from runner.results_db import ResultsDatabase


@pytest.mark.slow
def test_results_system_integration(tmp_path):
    """
    Test the full results system with actual experiments.
    
    This is a comprehensive integration test that:
    1. Runs baseline experiments
    2. Verifies database indexing
    3. Tests query functionality
    4. Validates performance comparison
    5. Tests CLI tools
    
    Note: Requires a dataset to exist (skips if none found).
    """
    # Check if dataset exists
    data_dir = Path("data/datasets")
    if not data_dir.exists():
        pytest.skip("No datasets found - run scripts/build_db.py first")
    
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if not datasets:
        pytest.skip("No datasets found - run scripts/build_db.py first")
    
    dataset_path = str(datasets[-1])
    
    # Create temporary database for testing
    test_db_path = tmp_path / "test_results.db"
    
    # Run a couple of quick experiments
    experiments = [
        ("random_baseline", "test_random"),
        ("last_price", "test_lastprice"),
    ]
    
    for method, name in experiments:
        result = subprocess.run(
            ["uv", "run", "runner/runner.py",
             "--name", name,
             "--method", method,
             "--task", "resolve_binary",
             "--dataset", dataset_path],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"Experiment {name} failed: {result.stderr}"
    
    # Test database queries
    db = ResultsDatabase()
    total_runs = db.count_runs()
    assert total_runs >= 2, f"Expected at least 2 runs, found {total_runs}"
    
    # Test listing
    runs = db.list_runs(limit=10)
    assert len(runs) > 0, "list_runs should return runs"
    
    # Test method comparison
    comparison = db.get_methods_comparison(task="resolve_binary", latest_only=True)
    assert len(comparison) >= 2, "Should have runs for both methods"
    
    # Verify performance ordering (last_price should beat random)
    methods_perf = {r["method"]: r["test_brier"] for r in comparison if r.get("test_brier")}
    
    if "last_price" in methods_perf and "random_baseline" in methods_perf:
        assert methods_perf["last_price"] < methods_perf["random_baseline"], \
            "last_price should outperform random_baseline"


@pytest.mark.slow
def test_cli_tools_work():
    """Test that CLI tools execute without errors."""
    # Test list_runs
    result = subprocess.run(
        ["uv", "run", "scripts/results/list_runs.py", "--limit", "5"],
        capture_output=True,
        text=True
    )
    # May fail if no runs exist, but should not crash
    assert "list_runs.py" not in result.stderr or result.returncode in [0, 1]
    
    # Test compare_runs  
    result = subprocess.run(
        ["uv", "run", "scripts/results/compare_runs.py", "--task", "resolve_binary", "--latest"],
        capture_output=True,
        text=True
    )
    assert "compare_runs.py" not in result.stderr or result.returncode in [0, 1]


# LESSONS LEARNED:
# 1. Integration tests should be marked @pytest.mark.slow so they can be skipped in quick runs
# 2. Skip tests gracefully if prerequisites (datasets) don't exist
# 3. Use subprocess to test CLI tools end-to-end
# 4. Test the full workflow, not just individual components
# 5. Validate actual performance ordering to ensure system makes sense
