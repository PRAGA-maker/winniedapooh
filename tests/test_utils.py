"""
Testing utilities for data validation and comparison.
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime


def compare_parquet_datasets(df1: pd.DataFrame, df2: pd.DataFrame, tolerance: float = 1e-6) -> Dict[str, Any]:
    """
    Compare two Parquet datasets for equivalence.
    
    Returns a dict with comparison results:
    - identical: bool
    - differences: list of differences found
    - metrics: dict of comparison metrics
    """
    differences = []
    
    # Check row counts
    if len(df1) != len(df2):
        differences.append(f"Row count mismatch: {len(df1)} vs {len(df2)}")
    
    # Check column names
    cols1, cols2 = set(df1.columns), set(df2.columns)
    if cols1 != cols2:
        missing_in_df2 = cols1 - cols2
        missing_in_df1 = cols2 - cols1
        if missing_in_df2:
            differences.append(f"Columns in df1 but not df2: {missing_in_df2}")
        if missing_in_df1:
            differences.append(f"Columns in df2 but not df1: {missing_in_df1}")
    
    # Check market_ids match (if both have the column)
    if 'market_id' in df1.columns and 'market_id' in df2.columns:
        ids1 = set(df1['market_id'])
        ids2 = set(df2['market_id'])
        if ids1 != ids2:
            missing_in_df2 = len(ids1 - ids2)
            missing_in_df1 = len(ids2 - ids1)
            differences.append(f"Market IDs differ: {missing_in_df2} missing in df2, {missing_in_df1} missing in df1")
    
    metrics = {
        "df1_rows": len(df1),
        "df2_rows": len(df2),
        "df1_cols": len(df1.columns),
        "df2_cols": len(df2.columns),
        "identical": len(differences) == 0
    }
    
    return {
        "identical": len(differences) == 0,
        "differences": differences,
        "metrics": metrics
    }


def validate_data_types(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate data types in the dataset match expected schema.
    
    Returns dict with:
    - valid: bool
    - errors: list of validation errors
    - warnings: list of warnings
    """
    errors = []
    warnings = []
    
    # Check required columns exist
    required_cols = ['source', 'market_id', 'title', 'status', 'market_type']
    for col in required_cols:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    
    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}
    
    # Check timestamp columns are datetime
    timestamp_cols = ['end_time', 'created_time']
    for col in timestamp_cols:
        if col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                errors.append(f"Column {col} should be datetime but is {df[col].dtype}")
    
    # Check 'ts' in nested lists are datetime
    if 'ts' in df.columns:
        sample_ts = df['ts'].iloc[0] if len(df) > 0 else None
        if sample_ts is not None and len(sample_ts) > 0:
            # Accept pd.Timestamp, datetime, and numpy.datetime64
            if not isinstance(sample_ts[0], (pd.Timestamp, datetime, np.datetime64)):
                warnings.append(f"'ts' list elements should be datetime, found {type(sample_ts[0])}")
    
    # Check belief values are in valid range [0, 1]
    if 'belief' in df.columns:
        for idx, belief_list in enumerate(df['belief'].head(100)):  # Check first 100
            if belief_list is not None and len(belief_list) > 0:
                valid_beliefs = [b for b in belief_list if b is not None and not (isinstance(b, float) and np.isnan(b))]
                if valid_beliefs:
                    min_b, max_b = min(valid_beliefs), max(valid_beliefs)
                    if min_b < 0 or max_b > 1:
                        errors.append(f"Belief values out of range [0,1] at index {idx}: min={min_b}, max={max_b}")
                        break
    
    # Check status values are valid
    valid_statuses = {'open', 'closed', 'resolved', 'unknown'}
    if 'status' in df.columns:
        actual_statuses = set(df['status'].unique())
        invalid = actual_statuses - valid_statuses
        if invalid:
            errors.append(f"Invalid status values found: {invalid}")
    
    # Check market_type values are valid
    valid_types = {'binary', 'multiple_choice', 'numeric', 'other'}
    if 'market_type' in df.columns:
        actual_types = set(df['market_type'].unique())
        invalid = actual_types - valid_types
        if invalid:
            errors.append(f"Invalid market_type values found: {invalid}")
    
    # Check JSON fields parse correctly
    json_fields = ['answer_options_json', 'resolved_value_json', 'metadata_json']
    for col in json_fields:
        if col in df.columns:
            for idx, val in enumerate(df[col].head(20)):  # Check first 20
                if val and not pd.isna(val):
                    try:
                        json.loads(val)
                    except json.JSONDecodeError:
                        errors.append(f"Invalid JSON in {col} at index {idx}: {val[:100]}")
                        break
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


def sample_and_validate(df: pd.DataFrame, n: int = 50, seed: int = 42) -> Dict[str, Any]:
    """
    Random sample and validate markets for spot checks.
    
    Returns dict with validation results for each sampled market.
    """
    if len(df) < n:
        sample = df
    else:
        sample = df.sample(n=n, random_state=seed)
    
    results = {
        "total_sampled": len(sample),
        "passed": 0,
        "failed": 0,
        "failures": []
    }
    
    for idx, row in sample.iterrows():
        issues = []
        
        # Check title not empty
        if not row.get('title') or (isinstance(row.get('title'), str) and len(row['title'].strip()) == 0):
            issues.append("Empty title")
        
        # Check description not empty
        if not row.get('description') or (isinstance(row.get('description'), str) and len(row['description'].strip()) == 0):
            issues.append("Empty description")
        
        # Check history length
        if 'belief' in row and row['belief'] is not None:
            if len(row['belief']) < 0:
                issues.append(f"Negative history length: {len(row['belief'])}")
        
        # Check ts and belief lists have same length
        if 'ts' in row and 'belief' in row:
            if row['ts'] is not None and row['belief'] is not None:
                if len(row['ts']) != len(row['belief']):
                    issues.append(f"ts/belief length mismatch: {len(row['ts'])} vs {len(row['belief'])}")
        
        # Check timestamps are sorted
        if 'ts' in row and row['ts'] is not None and len(row['ts']) > 1:
            ts_list = row['ts']
            if not all(ts_list[i] <= ts_list[i+1] for i in range(len(ts_list)-1)):
                issues.append("Timestamps not sorted")
        
        if issues:
            results["failed"] += 1
            results["failures"].append({
                "market_id": row.get('market_id'),
                "source": row.get('source'),
                "issues": issues
            })
        else:
            results["passed"] += 1
    
    results["pass_rate"] = results["passed"] / results["total_sampled"] if results["total_sampled"] > 0 else 0
    
    return results


def check_completeness(df: pd.DataFrame, expected_tickers: List[str]) -> Dict[str, Any]:
    """
    Check if all expected markets are present in the dataset.
    
    Args:
        df: The dataset DataFrame
        expected_tickers: List of expected market IDs
    
    Returns dict with completeness metrics.
    """
    actual_ids = set(df['market_id']) if 'market_id' in df.columns else set()
    expected_ids = set(expected_tickers)
    
    missing = expected_ids - actual_ids
    unexpected = actual_ids - expected_ids
    
    completeness = len(actual_ids & expected_ids) / len(expected_ids) if expected_ids else 0
    
    return {
        "expected_count": len(expected_ids),
        "actual_count": len(actual_ids),
        "matched_count": len(actual_ids & expected_ids),
        "missing_count": len(missing),
        "unexpected_count": len(unexpected),
        "completeness": completeness,
        "missing_ids": list(missing)[:20],  # First 20 for debugging
        "unexpected_ids": list(unexpected)[:20]
    }


def validate_history_points(df: pd.DataFrame, sample_size: int = 10) -> Dict[str, Any]:
    """
    Validate random history points for reasonableness.
    
    Returns dict with validation results.
    """
    results = {
        "markets_checked": 0,
        "points_checked": 0,
        "issues": []
    }
    
    sample = df.sample(n=min(sample_size, len(df)), random_state=42) if len(df) > 0 else df
    
    for idx, row in sample.iterrows():
        if 'ts' not in row or 'belief' not in row:
            continue
        
        if row['ts'] is None or row['belief'] is None:
            continue
        
        results["markets_checked"] += 1
        
        ts_list = row['ts']
        belief_list = row['belief']
        
        # Sample 5 random points from this market
        if len(ts_list) > 5:
            indices = np.random.choice(len(ts_list), size=min(5, len(ts_list)), replace=False)
        else:
            indices = range(len(ts_list))
        
        for i in indices:
            results["points_checked"] += 1
            
            ts = ts_list[i]
            belief = belief_list[i]
            
            # Check timestamp is valid (accept pd.Timestamp, datetime, and numpy.datetime64)
            if not isinstance(ts, (pd.Timestamp, datetime, np.datetime64)):
                results["issues"].append({
                    "market_id": row.get('market_id'),
                    "issue": f"Invalid timestamp type at index {i}: {type(ts)}"
                })
            
            # Check belief is reasonable
            if belief is not None and not np.isnan(belief):
                if belief < 0 or belief > 1:
                    results["issues"].append({
                        "market_id": row.get('market_id'),
                        "issue": f"Belief out of range at index {i}: {belief}"
                    })
    
    results["issue_count"] = len(results["issues"])
    results["success_rate"] = 1 - (results["issue_count"] / results["points_checked"]) if results["points_checked"] > 0 else 1
    
    return results
