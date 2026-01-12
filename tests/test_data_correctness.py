"""
Core data integrity tests for the optimized pipeline.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import (
    validate_data_types, 
    sample_and_validate,
    validate_history_points
)


def test_dataset_exists(latest_dataset_path):
    """Test that a dataset exists."""
    assert latest_dataset_path.exists(), "Dataset directory not found"
    parquet_path = latest_dataset_path / "data.parquet"
    assert parquet_path.exists(), "Parquet file not found"


def test_schema_validation(latest_parquet_df):
    """Test that schema matches expected structure."""
    df = latest_parquet_df
    
    # Check required columns exist
    required_cols = ['source', 'market_id', 'title', 'description', 'status', 'market_type']
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"
    
    # Run full validation
    result = validate_data_types(df)
    
    # Print warnings for visibility
    if result["warnings"]:
        print("\nWarnings found:")
        for warning in result["warnings"]:
            print(f"  - {warning}")
    
    # Assert no errors
    if not result["valid"]:
        print("\nValidation errors:")
        for error in result["errors"]:
            print(f"  - {error}")
    
    assert result["valid"], f"Schema validation failed with {len(result['errors'])} errors"


def test_basic_statistics(latest_parquet_df):
    """Test basic statistics of the dataset."""
    df = latest_parquet_df
    
    print(f"\nDataset Statistics:")
    print(f"  Total rows: {len(df)}")
    print(f"  Total columns: {len(df.columns)}")
    print(f"  Sources: {df['source'].value_counts().to_dict()}")
    
    # Should have at least some data
    assert len(df) > 0, "Dataset is empty"
    
    # Should have both Kalshi data (Metaculus might be 0 if limit=0)
    sources = df['source'].value_counts().to_dict()
    assert 'kalshi' in sources, "No Kalshi data found"
    print(f"  Kalshi markets: {sources.get('kalshi', 0)}")
    print(f"  Metaculus markets: {sources.get('metaculus', 0)}")


def test_belief_lists_structure(latest_parquet_df):
    """Test that belief lists have proper structure."""
    df = latest_parquet_df
    
    if 'belief' not in df.columns or 'ts' not in df.columns:
        pytest.skip("Belief or ts column not found")
    
    # Check first 100 rows for structure
    issues = []
    for idx in range(min(100, len(df))):
        row = df.iloc[idx]
        
        if row['belief'] is None or row['ts'] is None:
            continue
        
        # Check they're lists
        if not isinstance(row['belief'], (list, np.ndarray)):
            issues.append(f"Row {idx}: belief is not a list (type: {type(row['belief'])})")
            continue
        
        if not isinstance(row['ts'], (list, np.ndarray)):
            issues.append(f"Row {idx}: ts is not a list (type: {type(row['ts'])})")
            continue
        
        # Check same length
        if len(row['belief']) != len(row['ts']):
            issues.append(f"Row {idx}: belief/ts length mismatch ({len(row['belief'])} vs {len(row['ts'])})")
    
    if issues:
        print("\nStructure issues found:")
        for issue in issues[:10]:  # Print first 10
            print(f"  - {issue}")
    
    assert len(issues) == 0, f"Found {len(issues)} structure issues"


def test_random_sampling(latest_parquet_df):
    """Test random sampling and validation."""
    df = latest_parquet_df
    
    result = sample_and_validate(df, n=min(50, len(df)))
    
    print(f"\nSampling Results:")
    print(f"  Sampled: {result['total_sampled']}")
    print(f"  Passed: {result['passed']}")
    print(f"  Failed: {result['failed']}")
    print(f"  Pass rate: {result['pass_rate']:.2%}")
    
    if result['failures']:
        print(f"\nFirst 5 failures:")
        for failure in result['failures'][:5]:
            print(f"  - {failure['market_id']} ({failure['source']}): {failure['issues']}")
    
    # Should have at least 90% pass rate
    assert result['pass_rate'] >= 0.90, f"Pass rate too low: {result['pass_rate']:.2%}"


def test_history_point_validation(latest_parquet_df):
    """Test validation of individual history points."""
    df = latest_parquet_df
    
    result = validate_history_points(df, sample_size=20)
    
    print(f"\nHistory Point Validation:")
    print(f"  Markets checked: {result['markets_checked']}")
    print(f"  Points checked: {result['points_checked']}")
    print(f"  Issues found: {result['issue_count']}")
    print(f"  Success rate: {result['success_rate']:.2%}")
    
    if result['issues']:
        print(f"\nFirst 5 issues:")
        for issue in result['issues'][:5]:
            print(f"  - {issue}")
    
    # Should have at least 95% success rate on point validation
    assert result['success_rate'] >= 0.95, f"Success rate too low: {result['success_rate']:.2%}"


def test_no_empty_belief_lists(latest_parquet_df):
    """Test that markets have non-empty belief lists."""
    df = latest_parquet_df
    
    if 'belief' not in df.columns:
        pytest.skip("Belief column not found")
    
    df['belief_len'] = df['belief'].apply(lambda x: len(x) if x is not None else 0)
    empty_count = (df['belief_len'] == 0).sum()
    total_count = len(df)
    
    print(f"\nBelief List Statistics:")
    print(f"  Total markets: {total_count}")
    print(f"  Markets with empty belief: {empty_count}")
    print(f"  Non-empty ratio: {1 - empty_count/total_count:.2%}")
    
    # Most markets should have history (allow up to 10% to be empty)
    assert empty_count / total_count < 0.10, f"Too many empty belief lists: {empty_count}/{total_count}"


def test_timestamp_ordering(latest_parquet_df):
    """Test that timestamps in history are sorted."""
    df = latest_parquet_df
    
    if 'ts' not in df.columns:
        pytest.skip("ts column not found")
    
    unsorted_count = 0
    checked_count = 0
    
    for idx in range(min(100, len(df))):
        row = df.iloc[idx]
        if row['ts'] is None or len(row['ts']) <= 1:
            continue
        
        checked_count += 1
        ts_list = row['ts']
        
        # Check if sorted
        is_sorted = all(ts_list[i] <= ts_list[i+1] for i in range(len(ts_list)-1))
        if not is_sorted:
            unsorted_count += 1
    
    print(f"\nTimestamp Ordering:")
    print(f"  Markets checked: {checked_count}")
    print(f"  Unsorted: {unsorted_count}")
    
    assert unsorted_count == 0, f"Found {unsorted_count} markets with unsorted timestamps"


def test_status_distribution(latest_parquet_df):
    """Test the distribution of market statuses."""
    df = latest_parquet_df
    
    if 'status' not in df.columns:
        pytest.skip("status column not found")
    
    status_counts = df['status'].value_counts().to_dict()
    
    print(f"\nStatus Distribution:")
    for status, count in status_counts.items():
        print(f"  {status}: {count} ({count/len(df):.1%})")
    
    # Should have valid status values
    valid_statuses = {'open', 'closed', 'resolved', 'unknown'}
    invalid = set(status_counts.keys()) - valid_statuses
    assert len(invalid) == 0, f"Invalid status values found: {invalid}"


def test_market_type_distribution(latest_parquet_df):
    """Test the distribution of market types."""
    df = latest_parquet_df
    
    if 'market_type' not in df.columns:
        pytest.skip("market_type column not found")
    
    type_counts = df['market_type'].value_counts().to_dict()
    
    print(f"\nMarket Type Distribution:")
    for mtype, count in type_counts.items():
        print(f"  {mtype}: {count} ({count/len(df):.1%})")
    
    # Should have valid market types
    valid_types = {'binary', 'multiple_choice', 'numeric', 'other'}
    invalid = set(type_counts.keys()) - valid_types
    assert len(invalid) == 0, f"Invalid market_type values found: {invalid}"
