# Winnie Da Pooh Test Suite

This directory contains comprehensive tests for the Winnie Da Pooh forecasting pipeline.

## Quick Start

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test category
uv run pytest tests/test_data_correctness.py -v
uv run pytest tests/test_edge_cases.py -v
uv run pytest tests/test_pipeline_idempotency.py -v

# Run with the test runner script
uv run python tests/run_tests.py
```

## Test Categories

### Data Correctness Tests (`test_data_correctness.py`)
**10 tests** validating data integrity:
- Schema validation (columns, types, ranges)
- Dataset statistics and completeness
- Belief list structure
- Random sampling (96%+ pass rate)
- History point validation
- Timestamp ordering
- Status and market type distributions

### Edge Case Tests (`test_edge_cases.py`)
**9 tests** for boundary conditions:
- Database initialization
- Checkpoint system
- Empty data handling
- Duplicate handling (markets and history points)
- Batch operations
- Market status retrieval

### Idempotency Tests (`test_pipeline_idempotency.py`)
**2 tests** for consistency:
- Parquet export determinism
- Checkpoint reprocessing prevention

## Test Utilities

### `conftest.py`
Pytest fixtures including:
- `test_date_range`: Small date range (Dec 30-31, 2024)
- `clean_test_db`: Fresh test database
- `latest_dataset_path`: Path to latest dataset
- `latest_parquet_df`: Loaded dataset DataFrame
- `sample_markets`: Random market sample

### `test_utils.py`
Helper functions:
- `compare_parquet_datasets()`: Compare two datasets
- `validate_data_types()`: Schema validation
- `sample_and_validate()`: Random sampling
- `check_completeness()`: Verify market presence
- `validate_history_points()`: Check individual points

### `code_quality_review.py`
Automated code review checking:
- Error handling patterns
- Documentation coverage
- Import organization

### `run_tests.py`
Convenience script to run full test suite with summary.

## Test Data

Tests use a small dataset (Dec 30-31, 2024) for fast iteration:
- **49,068 markets** from Kalshi
- **2 days** of history
- **~57 seconds** to generate
- **Location**: `data/datasets/v*_unified/`

## Results

All tests currently pass:
- **21/21 tests PASS**
- **100% pass rate**
- **No critical bugs found**

See `TESTING_REPORT.md` for detailed findings.

## Adding New Tests

### Example: Data Validation Test

```python
def test_my_validation(latest_parquet_df):
    """Test something about the dataset."""
    df = latest_parquet_df
    
    # Your test logic here
    assert len(df) > 0, "Dataset should not be empty"
```

### Example: Edge Case Test

```python
def test_my_edge_case(tmp_path):
    """Test an edge case."""
    from src.build_unified_parquet import CanonicalStore
    
    store = CanonicalStore(tmp_path / "test.db")
    # Your test logic here
```

## Continuous Integration

To add to CI pipeline:

```yaml
- name: Run tests
  run: uv run pytest tests/ -v --tb=short
```

## Dependencies

- `pytest`: Test framework
- `pytest-mock`: Mocking support
- `pandas`: Data manipulation
- `numpy`: Numerical operations
- `pyarrow`: Parquet support

## Coverage

To run with coverage:

```bash
uv run pytest tests/ --cov=src --cov=dataobject --cov=methods --cov=runner
```

## Known Issues

1. **Windows Unicode**: Test runner avoids emoji for Windows compatibility
2. **Pydantic Warning**: `TimeSeriesPoint` uses deprecated Config (non-critical)
3. **Pytest Mark Warning**: `@pytest.mark.slow` needs registration in `pytest.ini`

## Future Improvements

1. Add integration tests for full pipeline runs
2. Add performance benchmarking tests
3. Add regression tests with known-good baselines
4. Increase test coverage to 80%+

## Questions?

- See `TESTING_REPORT.md` for comprehensive findings
- See `TESTING_COMPLETE.md` for quick summary
- Check individual test files for inline documentation

---

**Test Suite Version**: 1.0  
**Last Updated**: January 11, 2026  
**Maintainer**: Winnie Da Pooh Team
