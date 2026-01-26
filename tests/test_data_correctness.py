"""
Core data integrity tests for the optimized pipeline.
"""
import pytest
import pandas as pd
import numpy as np
import json
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
    required_cols = ['source', 'event_id', 'title', 'description', 'status', 'market_type', 'options_json']
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
    print(f"  Kalshi events: {sources.get('kalshi', 0)}")
    print(f"  Metaculus events: {sources.get('metaculus', 0)}")


def test_kalshi_enrichment_titles_and_descriptions(latest_parquet_df):
    """Ensure Kalshi metadata is enriched beyond S3 fallbacks."""
    df = latest_parquet_df
    if 'source' not in df.columns:
        pytest.skip("source column not found")

    kalshi_df = df[df['source'] == 'kalshi'].copy()
    if kalshi_df.empty:
        pytest.skip("No Kalshi rows to validate")

    kalshi_df["title"] = kalshi_df["title"].fillna("").astype(str)
    kalshi_df["description"] = kalshi_df["description"].fillna("").astype(str)

    id_col = "market_id" if "market_id" in kalshi_df.columns else "event_id"
    kalshi_df[id_col] = kalshi_df[id_col].fillna("").astype(str)

    ticker_title_mask = kalshi_df["title"].str.strip() == kalshi_df[id_col].str.strip()
    empty_desc_mask = kalshi_df["description"].str.strip() == ""
    short_desc_mask = kalshi_df["description"].str.len() < 50

    ticker_title_count = int(ticker_title_mask.sum())
    empty_desc_count = int(empty_desc_mask.sum())
    short_desc_count = int(short_desc_mask.sum())

    print("\nKalshi Enrichment Quality:")
    print(f"  Total Kalshi rows: {len(kalshi_df)}")
    print(f"  Ticker-only titles: {ticker_title_count}")
    print(f"  Empty descriptions: {empty_desc_count}")
    print(f"  Short descriptions (<50 chars): {short_desc_count}")

    assert ticker_title_count == 0, f"Ticker-only titles found: {ticker_title_count}"
    assert empty_desc_count == 0, f"Empty descriptions found: {empty_desc_count}"
    assert short_desc_count / len(kalshi_df) < 0.05, (
        f"Too many short descriptions: {short_desc_count}/{len(kalshi_df)}"
    )


def test_kalshi_url_quality(latest_parquet_df):
    """Validate Kalshi URLs follow expected format."""
    df = latest_parquet_df
    if 'source' not in df.columns or 'url' not in df.columns:
        pytest.skip("source/url column not found")

    kalshi_df = df[df['source'] == 'kalshi'].copy()
    if kalshi_df.empty:
        pytest.skip("No Kalshi rows to validate")

    kalshi_df["url"] = kalshi_df["url"].fillna("").astype(str)
    has_kalshi_url = kalshi_df["url"].str.contains("kalshi.com/markets/", na=False)
    valid_ratio = has_kalshi_url.sum() / len(kalshi_df)

    print("\nKalshi URL Quality:")
    print(f"  Total Kalshi rows: {len(kalshi_df)}")
    print(f"  Valid URL ratio: {valid_ratio:.2%}")

    assert valid_ratio > 0.95, (
        f"Only {has_kalshi_url.sum()} / {len(kalshi_df)} URLs look valid"
    )

def test_belief_lists_structure(latest_parquet_df):
    """Test that belief lists have proper structure."""
    df = latest_parquet_df
    
    if 'options_json' not in df.columns:
        pytest.skip("options_json column not found")

    issues = []
    for idx in range(min(100, len(df))):
        row = df.iloc[idx]
        raw_options = row.get("options_json")
        if not raw_options:
            continue
        try:
            options = json.loads(raw_options)
        except json.JSONDecodeError:
            issues.append(f"Row {idx}: options_json is invalid JSON")
            continue
        for option in options:
            ts_list = option.get("ts") or []
            belief_list = option.get("belief") or []
            if not isinstance(ts_list, (list, np.ndarray)):
                issues.append(f"Row {idx}: option ts is not a list")
                break
            if not isinstance(belief_list, (list, np.ndarray)):
                issues.append(f"Row {idx}: option belief is not a list")
                break
            if len(ts_list) != len(belief_list):
                issues.append(f"Row {idx}: option ts/belief length mismatch")
                break
    
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
            print(f"  - {failure.get('event_id')} ({failure['source']}): {failure['issues']}")
    
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
    
    if 'options_json' not in df.columns:
        pytest.skip("options_json column not found")

    def has_empty_option(raw):
        if not raw:
            return True
        try:
            options = json.loads(raw)
        except json.JSONDecodeError:
            return True
        return any(len(option.get("belief") or []) == 0 for option in options)

    df['has_empty_option'] = df['options_json'].apply(has_empty_option)
    empty_count = df['has_empty_option'].sum()
    total_count = len(df)

    print(f"\nOption Belief Statistics:")
    print(f"  Total events: {total_count}")
    print(f"  Events with empty option belief: {empty_count}")
    print(f"  Non-empty ratio: {1 - empty_count/total_count:.2%}")

    assert empty_count / total_count < 0.10, f"Too many events with empty option belief: {empty_count}/{total_count}"


def test_timestamp_ordering(latest_parquet_df):
    """Test that timestamps in history are sorted."""
    df = latest_parquet_df
    
    if 'options_json' not in df.columns:
        pytest.skip("options_json column not found")
    
    unsorted_count = 0
    checked_count = 0
    
    for idx in range(min(100, len(df))):
        row = df.iloc[idx]
        raw_options = row.get("options_json")
        if not raw_options:
            continue
        try:
            options = json.loads(raw_options)
        except json.JSONDecodeError:
            continue
        for option in options:
            ts_list = option.get("ts") or []
            if len(ts_list) <= 1:
                continue
            checked_count += 1
            cleaned = [ts for ts in ts_list if ts is not None]
            if len(cleaned) <= 1:
                continue
            is_sorted = all(cleaned[i] <= cleaned[i+1] for i in range(len(cleaned)-1))
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
    valid_types = {'event'}
    invalid = set(type_counts.keys()) - valid_types
    assert len(invalid) == 0, f"Invalid market_type values found: {invalid}"
