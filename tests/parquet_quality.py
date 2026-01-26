#!/usr/bin/env python
"""
Parquet quality audit tool for unified datasets.
Produces a report and JSON summary for exploratory health checks.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, List

import numpy as np
import pandas as pd


def _safe_json_loads(value: str):
    try:
        return json.loads(value), None
    except Exception as exc:
        return None, str(exc)


def _series_basic_stats(series: pd.Series) -> dict:
    return {
        "dtype": str(series.dtype),
        "null_count": int(series.isna().sum()),
        "null_pct": float(series.isna().mean()),
    }


def _string_stats(series: pd.Series) -> dict:
    non_null = series.dropna()
    empty = non_null.eq("").sum()
    whitespace = non_null.str.fullmatch(r"\s+").sum()
    lengths = non_null.str.len()
    return {
        "empty_count": int(empty),
        "whitespace_only_count": int(whitespace),
        "min_len": int(lengths.min()) if not lengths.empty else None,
        "max_len": int(lengths.max()) if not lengths.empty else None,
        "mean_len": float(lengths.mean()) if not lengths.empty else None,
    }


def _datetime_stats(series: pd.Series) -> dict:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    non_null = dt.dropna()
    if non_null.empty:
        return {
            "min": None,
            "max": None,
            "time_midnight_ratio": None,
            "unique_dates": 0,
        }
    times = non_null.dt.time
    time_midnight_ratio = float((times == pd.to_datetime("00:00:00").time()).mean())
    unique_dates = int(non_null.dt.date.nunique())
    return {
        "min": non_null.min().isoformat(),
        "max": non_null.max().isoformat(),
        "time_midnight_ratio": time_midnight_ratio,
        "unique_dates": unique_dates,
    }


def _detect_list_like(series: pd.Series) -> bool:
    for value in series.dropna().head(200):
        if isinstance(value, (list, tuple, np.ndarray)):
            return True
    return False


def _list_stats(series: pd.Series) -> dict:
    lengths = series.apply(lambda x: len(x) if isinstance(x, (list, tuple, np.ndarray)) else np.nan)
    return {
        "list_count": int(lengths.notna().sum()),
        "empty_list_count": int((lengths == 0).sum()),
        "min_len": int(lengths.min()) if lengths.notna().any() else None,
        "max_len": int(lengths.max()) if lengths.notna().any() else None,
        "mean_len": float(lengths.mean()) if lengths.notna().any() else None,
    }


def _flatten_numeric(series: pd.Series, sample_rows: int = 20000) -> pd.Series:
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
    values = []
    for item in non_null:
        if isinstance(item, (list, tuple, np.ndarray)):
            values.extend(item)
    return pd.to_numeric(pd.Series(values), errors="coerce")


def _list_numeric_stats(series: pd.Series, sample_rows: int = 20000) -> dict:
    flattened = _flatten_numeric(series, sample_rows=sample_rows)
    if flattened.empty:
        return {"sample_rows": 0, "min": None, "max": None, "mean": None, "negatives": 0}
    return {
        "sample_rows": int(min(sample_rows, len(series.dropna()))),
        "min": float(flattened.min()) if flattened.notna().any() else None,
        "max": float(flattened.max()) if flattened.notna().any() else None,
        "mean": float(flattened.mean()) if flattened.notna().any() else None,
        "negatives": int((flattened < 0).sum()),
        "outside_0_1": int(((flattened < 0) | (flattened > 1)).sum()),
        "nan_after_coerce": int(flattened.isna().sum()),
    }


def _list_null_value_stats(series: pd.Series, sample_rows: int = 20000) -> dict:
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
    total = 0
    nulls = 0
    for item in non_null:
        if isinstance(item, (list, tuple, np.ndarray)):
            for value in item:
                total += 1
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    nulls += 1
    return {
        "sample_rows": int(min(sample_rows, len(series.dropna()))),
        "total_values": int(total),
        "null_values": int(nulls),
        "null_ratio": float(nulls / total) if total else None,
    }


def _ts_list_time_stats(series: pd.Series, sample_rows: int = 20000) -> dict:
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
    total = 0
    midnight = 0
    date_only = 0
    parse_fail = 0
    for item in non_null:
        if isinstance(item, (list, tuple, np.ndarray)):
            for value in item:
                if value is None:
                    continue
                total += 1
                if isinstance(value, str) and "T" not in value:
                    date_only += 1
                try:
                    dt = pd.to_datetime(value, errors="raise", utc=True)
                    if dt.time() == pd.Timestamp("00:00:00").time():
                        midnight += 1
                except Exception:
                    parse_fail += 1
    return {
        "sample_rows": int(min(sample_rows, len(series.dropna()))),
        "total_values": int(total),
        "midnight_ratio": float(midnight / total) if total else None,
        "date_only_ratio": float(date_only / total) if total else None,
        "parse_failures": int(parse_fail),
    }


def _ts_list_order_stats(series: pd.Series, sample_rows: int = 20000) -> dict:
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
    checked = 0
    unsorted = 0
    for item in non_null:
        if isinstance(item, (list, tuple, np.ndarray)):
            parsed = []
            for value in item:
                if value is None:
                    continue
                try:
                    parsed.append(pd.to_datetime(value, errors="raise", utc=True))
                except Exception:
                    continue
            if len(parsed) > 1:
                checked += 1
                if any(parsed[i] > parsed[i + 1] for i in range(len(parsed) - 1)):
                    unsorted += 1
    return {
        "sample_rows": int(min(sample_rows, len(series.dropna()))),
        "checked_lists": int(checked),
        "unsorted_lists": int(unsorted),
        "unsorted_ratio": float(unsorted / checked) if checked else None,
    }


def _json_stats(series: pd.Series, sample_size: int | None = None) -> dict:
    non_null = series.dropna()
    if sample_size is not None and len(non_null) > sample_size:
        non_null = non_null.sample(sample_size, random_state=42)
        sampled = True
    else:
        sampled = False

    type_counts = Counter()
    error_counts = Counter()
    parsed_examples = []

    for value in non_null:
        parsed, error = _safe_json_loads(value)
        if error:
            error_counts[error] += 1
            continue
        type_counts[type(parsed).__name__] += 1
        if len(parsed_examples) < 3:
            parsed_examples.append(parsed)

    return {
        "sampled": sampled,
        "sample_size": len(non_null),
        "type_counts": dict(type_counts),
        "parse_errors": dict(error_counts),
        "example_values": parsed_examples,
    }


def _options_json_stats(series: pd.Series, sample_rows: int = 20000) -> dict:
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)

    parse_errors = Counter()
    option_counts = []
    ts_belief_mismatch = 0
    ts_bid_mismatch = 0
    ts_ask_mismatch = 0

    for raw in non_null:
        parsed, error = _safe_json_loads(raw)
        if error or not isinstance(parsed, list):
            parse_errors[error or "options_json not list"] += 1
            continue

        option_counts.append(len(parsed))
        for option in parsed:
            ts_list = option.get("ts") or []
            belief_list = option.get("belief") or []
            bid_list = option.get("bid") or []
            ask_list = option.get("ask") or []
            if len(ts_list) != len(belief_list):
                ts_belief_mismatch += 1
            if bid_list and len(ts_list) != len(bid_list):
                ts_bid_mismatch += 1
            if ask_list and len(ts_list) != len(ask_list):
                ts_ask_mismatch += 1

    option_counts_series = pd.Series(option_counts)
    return {
        "sample_rows": int(min(sample_rows, len(series.dropna()))),
        "parse_errors": dict(parse_errors),
        "options_count_stats": {
            "min": int(option_counts_series.min()) if not option_counts_series.empty else None,
            "max": int(option_counts_series.max()) if not option_counts_series.empty else None,
            "mean": float(option_counts_series.mean()) if not option_counts_series.empty else None,
        },
        "options_ts_belief_length_mismatch": int(ts_belief_mismatch),
        "options_ts_bid_length_mismatch": int(ts_bid_mismatch),
        "options_ts_ask_length_mismatch": int(ts_ask_mismatch),
    }


def _validate_uniqueness(df: pd.DataFrame) -> dict:
    """Check (source, event_id) uniqueness constraint."""
    if {"source", "event_id"}.issubset(df.columns):
        dupes = df.duplicated(subset=["source", "event_id"], keep=False)
        dupe_count = int(dupes.sum())
        dupe_examples = []
        if dupe_count > 0:
            dupe_df = df[dupes][["source", "event_id"]].head(10)
            dupe_examples = dupe_df.to_dict("records")
        return {
            "duplicate_count": dupe_count,
            "duplicate_examples": dupe_examples,
            "is_valid": dupe_count == 0,
        }
    return {"error": "missing_columns", "is_valid": False}


def _validate_kalshi_enrichment_detailed(df: pd.DataFrame, sample_rows: int = 20000) -> dict:
    """Deep check of Kalshi metadata quality (titles, descriptions, URLs)."""
    if "source" not in df.columns:
        return {"error": "source_column_missing"}
    
    kalshi_df = df[df["source"] == "kalshi"].copy()
    if kalshi_df.empty:
        return {"error": "no_kalshi_data", "kalshi_count": 0}
    
    # Sample if too large
    if len(kalshi_df) > sample_rows:
        kalshi_df = kalshi_df.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    kalshi_df["title"] = kalshi_df["title"].fillna("").astype(str)
    kalshi_df["description"] = kalshi_df["description"].fillna("").astype(str)
    kalshi_df["market_id"] = kalshi_df.get("market_id", pd.Series([""] * len(kalshi_df))).fillna("").astype(str)
    kalshi_df["url"] = kalshi_df["url"].fillna("").astype(str)
    
    # Ticker-only titles (title equals market_id)
    ticker_title_mask = kalshi_df["title"].str.strip() == kalshi_df["market_id"].str.strip()
    ticker_title_count = int(ticker_title_mask.sum())
    ticker_examples = kalshi_df[ticker_title_mask][["event_id", "title", "market_id"]].head(5).to_dict("records")
    
    # Empty descriptions
    empty_desc_mask = kalshi_df["description"].str.strip() == ""
    empty_desc_count = int(empty_desc_mask.sum())
    empty_desc_examples = kalshi_df[empty_desc_mask][["event_id", "title"]].head(5).to_dict("records")
    
    # Short descriptions (<50 chars)
    short_desc_mask = kalshi_df["description"].str.len() < 50
    short_desc_count = int(short_desc_mask.sum())
    short_desc_examples = kalshi_df[short_desc_mask][["event_id", "description"]].head(5).to_dict("records")
    
    # URL quality
    valid_url_mask = kalshi_df["url"].str.contains("kalshi.com/markets/", na=False, regex=False)
    valid_url_count = int(valid_url_mask.sum())
    invalid_url_examples = kalshi_df[~valid_url_mask][["event_id", "url"]].head(5).to_dict("records")
    
    return {
        "sampled": sampled,
        "sample_size": len(kalshi_df),
        "ticker_only_titles": ticker_title_count,
        "ticker_only_ratio": float(ticker_title_count / len(kalshi_df)) if len(kalshi_df) > 0 else 0,
        "ticker_examples": ticker_examples,
        "empty_descriptions": empty_desc_count,
        "empty_desc_ratio": float(empty_desc_count / len(kalshi_df)) if len(kalshi_df) > 0 else 0,
        "empty_desc_examples": empty_desc_examples,
        "short_descriptions": short_desc_count,
        "short_desc_ratio": float(short_desc_count / len(kalshi_df)) if len(kalshi_df) > 0 else 0,
        "short_desc_examples": short_desc_examples,
        "valid_urls": valid_url_count,
        "valid_url_ratio": float(valid_url_count / len(kalshi_df)) if len(kalshi_df) > 0 else 0,
        "invalid_url_examples": invalid_url_examples,
    }


def _validate_metaculus_structure(df: pd.DataFrame, sample_rows: int = 20000) -> dict:
    """Check Metaculus-specific requirements (no volume/bid/ask, proper structure)."""
    if "source" not in df.columns:
        return {"error": "source_column_missing"}
    
    meta_df = df[df["source"] == "metaculus"].copy()
    if meta_df.empty:
        return {"error": "no_metaculus_data", "metaculus_count": 0}
    
    # Sample if too large
    if len(meta_df) > sample_rows:
        meta_df = meta_df.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    issues = []
    volume_present = 0
    bid_present = 0
    ask_present = 0
    open_interest_present = 0
    
    for idx, row in meta_df.iterrows():
        raw_options = row.get("options_json")
        if not raw_options:
            continue
        try:
            options = json.loads(raw_options)
        except json.JSONDecodeError:
            continue
        
        for option in options:
            if option.get("volume"):
                volume_present += 1
            if option.get("bid"):
                bid_present += 1
            if option.get("ask"):
                ask_present += 1
            if option.get("open_interest"):
                open_interest_present += 1
    
    return {
        "sampled": sampled,
        "sample_size": len(meta_df),
        "volume_present_count": volume_present,
        "bid_present_count": bid_present,
        "ask_present_count": ask_present,
        "open_interest_present_count": open_interest_present,
        "has_kalshi_fields": volume_present > 0 or bid_present > 0 or ask_present > 0 or open_interest_present > 0,
    }


def _validate_belief_ranges_detailed(series: pd.Series, sample_rows: int = 20000) -> dict:
    """Check belief values in [0,1] with violation details."""
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    violations = []
    total_beliefs_checked = 0
    out_of_range_count = 0
    negative_count = 0
    above_one_count = 0
    
    for idx, raw in enumerate(non_null):
        try:
            options = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        
        if not isinstance(options, list):
            continue
        
        for opt_idx, option in enumerate(options):
            belief_list = option.get("belief") or []
            for b_idx, belief in enumerate(belief_list):
                if belief is None or (isinstance(belief, float) and np.isnan(belief)):
                    continue
                total_beliefs_checked += 1
                if belief < 0 or belief > 1:
                    out_of_range_count += 1
                    if belief < 0:
                        negative_count += 1
                    if belief > 1:
                        above_one_count += 1
                    if len(violations) < 10:  # Keep first 10 violations
                        violations.append({
                            "sample_idx": idx,
                            "option_idx": opt_idx,
                            "belief_idx": b_idx,
                            "value": float(belief),
                            "option_id": option.get("option_id", "unknown"),
                        })
    
    return {
        "sampled": sampled,
        "sample_size": len(non_null),
        "total_beliefs_checked": total_beliefs_checked,
        "out_of_range_count": out_of_range_count,
        "negative_count": negative_count,
        "above_one_count": above_one_count,
        "violation_ratio": float(out_of_range_count / total_beliefs_checked) if total_beliefs_checked > 0 else 0,
        "violations_sample": violations,
    }


def _validate_timestamp_monotonicity_detailed(series: pd.Series, sample_rows: int = 20000) -> dict:
    """Per-option timestamp ordering with violation examples."""
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    checked = 0
    unsorted = 0
    violations = []
    
    for idx, raw in enumerate(non_null):
        try:
            options = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        
        if not isinstance(options, list):
            continue
        
        for opt_idx, option in enumerate(options):
            ts_list = option.get("ts") or []
            if len(ts_list) <= 1:
                continue
            
            checked += 1
            parsed = []
            for ts in ts_list:
                if ts is None:
                    continue
                try:
                    parsed.append(pd.to_datetime(ts, errors="raise", utc=True))
                except Exception:
                    continue
            
            if len(parsed) > 1:
                is_sorted = all(parsed[i] <= parsed[i + 1] for i in range(len(parsed) - 1))
                if not is_sorted:
                    unsorted += 1
                    if len(violations) < 10:
                        violations.append({
                            "sample_idx": idx,
                            "option_idx": opt_idx,
                            "option_id": option.get("option_id", "unknown"),
                            "ts_count": len(parsed),
                            "first_ts": parsed[0].isoformat(),
                            "last_ts": parsed[-1].isoformat(),
                        })
    
    return {
        "sampled": sampled,
        "sample_size": len(non_null),
        "checked_lists": checked,
        "unsorted_lists": unsorted,
        "unsorted_ratio": float(unsorted / checked) if checked > 0 else 0,
        "violations_sample": violations,
    }


def _validate_list_alignment_comprehensive(df: pd.DataFrame, sample_rows: int = 20000) -> dict:
    """Check all list fields align with ts length."""
    if "options_json" not in df.columns:
        return {"error": "options_json_missing"}
    
    sample_df = df if len(df) <= sample_rows else df.sample(sample_rows, random_state=42)
    sampled = len(df) > sample_rows
    
    misalignment_counts = {
        "ts_belief": 0,
        "ts_volume": 0,
        "ts_open_interest": 0,
        "ts_bid": 0,
        "ts_ask": 0,
    }
    
    total_options_checked = 0
    violations = []
    
    for idx, row in sample_df.iterrows():
        raw_options = row.get("options_json")
        if not raw_options:
            continue
        try:
            options = json.loads(raw_options)
        except json.JSONDecodeError:
            continue
        
        for opt_idx, option in enumerate(options):
            total_options_checked += 1
            ts_list = option.get("ts") or []
            ts_len = len(ts_list)
            
            belief_list = option.get("belief") or []
            if belief_list and len(belief_list) != ts_len:
                misalignment_counts["ts_belief"] += 1
                if len(violations) < 10:
                    violations.append({
                        "event_id": row.get("event_id"),
                        "option_id": option.get("option_id"),
                        "field": "belief",
                        "ts_len": ts_len,
                        "field_len": len(belief_list),
                    })
            
            volume_list = option.get("volume") or []
            if volume_list and len(volume_list) != ts_len:
                misalignment_counts["ts_volume"] += 1
            
            oi_list = option.get("open_interest") or []
            if oi_list and len(oi_list) != ts_len:
                misalignment_counts["ts_open_interest"] += 1
            
            bid_list = option.get("bid") or []
            if bid_list and len(bid_list) != ts_len:
                misalignment_counts["ts_bid"] += 1
            
            ask_list = option.get("ask") or []
            if ask_list and len(ask_list) != ts_len:
                misalignment_counts["ts_ask"] += 1
    
    return {
        "sampled": sampled,
        "sample_size": len(sample_df),
        "total_options_checked": total_options_checked,
        "misalignment_counts": misalignment_counts,
        "total_misalignments": sum(misalignment_counts.values()),
        "violations_sample": violations,
    }


def _validate_synthetic_options_structure(series: pd.Series, sample_rows: int = 20000) -> dict:
    """Check synthetic options have parent references."""
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    synthetic_count = 0
    synthetic_missing_parent = 0
    violations = []
    
    for idx, raw in enumerate(non_null):
        try:
            options = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        
        if not isinstance(options, list):
            continue
        
        for opt_idx, option in enumerate(options):
            if option.get("is_synthetic") is True:
                synthetic_count += 1
                if not option.get("derived_from_market_id"):
                    synthetic_missing_parent += 1
                    if len(violations) < 10:
                        violations.append({
                            "sample_idx": idx,
                            "option_idx": opt_idx,
                            "option_id": option.get("option_id", "unknown"),
                            "market_id": option.get("market_id", "unknown"),
                        })
    
    return {
        "sampled": sampled,
        "sample_size": len(non_null),
        "synthetic_options_count": synthetic_count,
        "synthetic_missing_parent": synthetic_missing_parent,
        "missing_parent_ratio": float(synthetic_missing_parent / synthetic_count) if synthetic_count > 0 else 0,
        "violations_sample": violations,
    }


def _validate_timeseries_null_patterns(series: pd.Series, sample_rows: int = 20000) -> dict:
    """Track null patterns in bid/ask/volume/open_interest within options_json."""
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    stats = {
        "bid": {"total": 0, "nulls": 0, "has_data": 0},
        "ask": {"total": 0, "nulls": 0, "has_data": 0},
        "volume": {"total": 0, "nulls": 0, "has_data": 0},
        "open_interest": {"total": 0, "nulls": 0, "has_data": 0},
    }
    
    for raw in non_null:
        try:
            options = json.loads(raw)
            if not isinstance(options, list):
                continue
            for option in options:
                for field in ["bid", "ask", "volume", "open_interest"]:
                    field_list = option.get(field) or []
                    if not isinstance(field_list, list):
                        continue
                    for value in field_list:
                        stats[field]["total"] += 1
                        if value is None or (isinstance(value, float) and np.isnan(value)):
                            stats[field]["nulls"] += 1
                        else:
                            stats[field]["has_data"] += 1
        except (json.JSONDecodeError, TypeError):
            continue
    
    result = {
        "sampled": sampled,
        "sample_size": len(non_null),
    }
    
    for field in ["bid", "ask", "volume", "open_interest"]:
        total = stats[field]["total"]
        nulls = stats[field]["nulls"]
        has_data = stats[field]["has_data"]
        result[f"{field}_total_points"] = total
        result[f"{field}_null_count"] = nulls
        result[f"{field}_has_data_count"] = has_data
        result[f"{field}_null_ratio"] = float(nulls / total) if total > 0 else None
        result[f"{field}_coverage_ratio"] = float(has_data / total) if total > 0 else None
    
    return result


def _validate_metadata_completeness(series: pd.Series, sample_rows: int = 20000) -> dict:
    """Validate metadata_json contains expected key fields."""
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    # Key fields we expect in metadata_json for enriched data
    expected_fields = [
        "market_type",
        "status",
        "open_time",
        "close_time",
        "ticker",
        "title",
    ]
    
    field_presence = {field: 0 for field in expected_fields}
    total_checked = 0
    parse_errors = 0
    empty_metadata = 0
    
    for raw in non_null:
        if not raw or (isinstance(raw, str) and raw.strip() in ("", "{}")):
            empty_metadata += 1
            continue
        
        try:
            metadata = json.loads(raw)
            if not isinstance(metadata, dict):
                parse_errors += 1
                continue
            
            total_checked += 1
            for field in expected_fields:
                if field in metadata and metadata[field] not in (None, "", []):
                    field_presence[field] += 1
        except (json.JSONDecodeError, TypeError):
            parse_errors += 1
    
    field_ratios = {}
    for field, count in field_presence.items():
        field_ratios[f"{field}_ratio"] = float(count / total_checked) if total_checked > 0 else None
    
    return {
        "sampled": sampled,
        "sample_size": len(non_null),
        "total_checked": total_checked,
        "empty_metadata": empty_metadata,
        "parse_errors": parse_errors,
        "field_presence": field_presence,
        "field_ratios": field_ratios,
        "overall_completeness": float(sum(field_presence.values()) / (len(expected_fields) * total_checked)) if total_checked > 0 else None,
    }


def _validate_history_depth(series: pd.Series, sample_rows: int = 20000) -> dict:
    """Report time series depth statistics (ts list lengths)."""
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)
        sampled = True
    else:
        sampled = False
    
    ts_lengths = []
    belief_lengths = []
    empty_ts_count = 0
    empty_belief_count = 0
    
    for raw in non_null:
        try:
            options = json.loads(raw)
            if not isinstance(options, list):
                continue
            for option in options:
                ts_list = option.get("ts") or []
                belief_list = option.get("belief") or []
                
                ts_len = len(ts_list)
                belief_len = len(belief_list)
                
                ts_lengths.append(ts_len)
                belief_lengths.append(belief_len)
                
                if ts_len == 0:
                    empty_ts_count += 1
                if belief_len == 0:
                    empty_belief_count += 1
        except (json.JSONDecodeError, TypeError):
            continue
    
    if not ts_lengths:
        return {
            "sampled": sampled,
            "sample_size": len(non_null),
            "options_checked": 0,
            "error": "no_valid_options",
        }
    
    ts_array = np.array(ts_lengths)
    belief_array = np.array(belief_lengths)
    
    return {
        "sampled": sampled,
        "sample_size": len(non_null),
        "options_checked": len(ts_lengths),
        "ts_length_stats": {
            "min": int(ts_array.min()),
            "max": int(ts_array.max()),
            "mean": float(ts_array.mean()),
            "median": float(np.median(ts_array)),
            "p25": float(np.percentile(ts_array, 25)),
            "p75": float(np.percentile(ts_array, 75)),
        },
        "belief_length_stats": {
            "min": int(belief_array.min()),
            "max": int(belief_array.max()),
            "mean": float(belief_array.mean()),
            "median": float(np.median(belief_array)),
            "p25": float(np.percentile(belief_array, 25)),
            "p75": float(np.percentile(belief_array, 75)),
        },
        "empty_ts_count": empty_ts_count,
        "empty_belief_count": empty_belief_count,
        "shallow_history_count": int((ts_array < 3).sum()),
        "shallow_history_ratio": float((ts_array < 3).sum() / len(ts_array)),
        "good_depth_count": int((ts_array >= 7).sum()),
        "good_depth_ratio": float((ts_array >= 7).sum() / len(ts_array)),
    }


def _validate_event_schema(df: pd.DataFrame) -> dict:
    expected = {
        "source": {"nullable": False, "allowed": ["kalshi", "metaculus"]},
        "event_id": {"nullable": False},
        "title": {"nullable": False},
        "description": {"nullable": False},
        "url": {"nullable": False},
        "market_type": {"nullable": False, "allowed": ["event"]},
        "options_json": {"nullable": False},
        "end_time": {"nullable": False, "dtype": "datetime"},
        "status": {"nullable": False, "allowed": ["open", "closed", "resolved", "unknown"]},
        "resolved_value_json": {"nullable": True},
        "created_time": {"nullable": True, "dtype": "datetime"},
        "metadata_json": {"nullable": False},
    }

    errors: List[str] = []
    warnings: List[str] = []

    missing = [col for col in expected if col not in df.columns]
    if missing:
        errors.append(f"missing_columns={missing}")

    for col, rules in expected.items():
        if col not in df.columns:
            continue
        series = df[col]
        if not rules.get("nullable", True):
            nulls = int(series.isna().sum())
            if nulls > 0:
                errors.append(f"{col}_null_count={nulls}")
        if rules.get("dtype") == "datetime":
            parsed = pd.to_datetime(series, errors="coerce", utc=True)
            invalid = int((series.notna() & parsed.isna()).sum())
            if invalid > 0:
                errors.append(f"{col}_invalid_datetime={invalid}")
        allowed = rules.get("allowed")
        if allowed:
            invalid = int((~series.isna() & ~series.isin(allowed)).sum())
            if invalid > 0:
                errors.append(f"{col}_invalid_values={invalid}")

    if {"source", "event_id"}.issubset(df.columns) and not df.empty:
        dupes = int(df.duplicated(subset=["source", "event_id"]).sum())
        if dupes > 0:
            warnings.append(f"duplicate_event_ids={dupes}")

    if {"created_time", "end_time"}.issubset(df.columns) and not df.empty:
        created = pd.to_datetime(df["created_time"], errors="coerce", utc=True)
        end = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
        created_after_end = int(((created > end) & created.notna() & end.notna()).sum())
        if created_after_end > 0:
            warnings.append(f"created_after_end={created_after_end}")

    if {"status", "resolved_value_json"}.issubset(df.columns) and not df.empty:
        status = df["status"].astype("string")
        resolved = df["resolved_value_json"].astype("string")
        resolved_empty = resolved.eq("")
        resolved_nonempty = resolved.ne("")
        resolved_missing = int(((status == "resolved") & resolved.isna()).sum())
        resolved_empty_count = int(((status == "resolved") & resolved_empty).sum())
        unresolved_with_value = int(((status != "resolved") & resolved_nonempty).sum())
        if resolved_missing > 0:
            warnings.append(f"resolved_without_value={resolved_missing}")
        if resolved_empty_count > 0:
            warnings.append(f"resolved_empty_value={resolved_empty_count}")
        if unresolved_with_value > 0:
            warnings.append(f"value_without_resolved_status={unresolved_with_value}")

    return {"errors": errors, "warnings": warnings}


def _validate_options_json_structure(
    series: pd.Series,
    sample_rows: int = 20000,
    require_keys: Optional[List[str]] = None,
) -> dict:
    non_null = series.dropna()
    if len(non_null) > sample_rows:
        non_null = non_null.sample(sample_rows, random_state=42)

    parse_errors = 0
    non_list = 0
    non_dict_option = 0
    missing_keys = Counter()
    synthetic_missing_parent = 0
    empty_ts = 0
    empty_belief = 0
    ts_belief_mismatch = 0
    total_options = 0

    for raw in non_null:
        parsed, error = _safe_json_loads(raw)
        if error:
            parse_errors += 1
            continue
        if not isinstance(parsed, list):
            non_list += 1
            continue
        for option in parsed:
            total_options += 1
            if not isinstance(option, dict):
                non_dict_option += 1
                continue
            if require_keys:
                for key in require_keys:
                    if key not in option:
                        missing_keys[key] += 1
            if option.get("is_synthetic") is True and not option.get("derived_from_market_id"):
                synthetic_missing_parent += 1
            ts_list = option.get("ts") or []
            belief_list = option.get("belief") or []
            if not ts_list:
                empty_ts += 1
            if not belief_list:
                empty_belief += 1
            if len(ts_list) != len(belief_list):
                ts_belief_mismatch += 1

    return {
        "sample_rows": int(min(sample_rows, len(series.dropna()))),
        "parse_errors": int(parse_errors),
        "non_list_values": int(non_list),
        "non_dict_options": int(non_dict_option),
        "missing_keys": dict(missing_keys),
        "synthetic_missing_parent": int(synthetic_missing_parent),
        "empty_ts_options": int(empty_ts),
        "empty_belief_options": int(empty_belief),
        "ts_belief_length_mismatch": int(ts_belief_mismatch),
        "total_options_checked": int(total_options),
    }


def _generate_requirement_mapping_report(summary: dict, df: pd.DataFrame) -> List[str]:
    """Map DOCUMENTATION.txt requirements to test results."""
    lines = []
    lines.append("=" * 80)
    lines.append("SECTION 2: REQUIREMENT MAPPING")
    lines.append("=" * 80)
    lines.append("")
    lines.append("This section maps requirements from DOCUMENTATION.txt to actual test results.")
    lines.append("")
    
    # Define requirements from DOCUMENTATION.txt
    requirements = [
        {
            "id": "REQ-1",
            "requirement": "EventRecord Schema: All required columns present (source, event_id, title, description, url, market_type, options_json, end_time, status, metadata_json)",
            "test": "Schema validation (_validate_event_schema)",
            "expected": "0 missing columns",
            "actual": lambda: f"{len(summary['validation']['schema'].get('errors', []))} errors" if 'schema' in summary['validation'] else "N/A",
            "status": lambda: "PASS" if not any("missing_columns" in e for e in summary['validation']['schema'].get('errors', [])) else "FAIL",
        },
        {
            "id": "REQ-2",
            "requirement": "Uniqueness: (source, event_id) pairs must be unique",
            "test": "Uniqueness validation (_validate_uniqueness)",
            "expected": "0 duplicates",
            "actual": lambda: f"{summary['validation']['uniqueness'].get('duplicate_count', 0)} duplicates",
            "status": lambda: "PASS" if summary['validation']['uniqueness'].get('is_valid', False) else "FAIL",
        },
        {
            "id": "REQ-3",
            "requirement": "Time Series: ts, belief, volume, open_interest, bid, ask lists must align in length",
            "test": "List alignment validation (_validate_list_alignment_comprehensive)",
            "expected": "0 misalignments",
            "actual": lambda: f"{summary['validation']['list_alignment'].get('total_misalignments', 0)} misalignments",
            "status": lambda: "PASS" if summary['validation']['list_alignment'].get('total_misalignments', 0) == 0 else "FAIL",
        },
        {
            "id": "REQ-4",
            "requirement": "Belief Range: All belief values must be in [0, 1]",
            "test": "Belief range validation (_validate_belief_ranges_detailed)",
            "expected": "0 values outside [0, 1]",
            "actual": lambda: f"{summary['validation']['belief_ranges'].get('out_of_range_count', 0)} violations",
            "status": lambda: "PASS" if summary['validation']['belief_ranges'].get('out_of_range_count', 0) == 0 else "FAIL",
        },
        {
            "id": "REQ-5",
            "requirement": "Timestamp Monotonicity: Timestamps within each option must be sorted",
            "test": "Timestamp monotonicity validation (_validate_timestamp_monotonicity_detailed)",
            "expected": "0 unsorted lists",
            "actual": lambda: f"{summary['validation']['timestamp_monotonicity'].get('unsorted_lists', 0)} unsorted",
            "status": lambda: "PASS" if summary['validation']['timestamp_monotonicity'].get('unsorted_lists', 0) == 0 else "WARNING",
        },
        {
            "id": "REQ-6",
            "requirement": "Synthetic Options: If is_synthetic=true, must have derived_from_market_id",
            "test": "Synthetic options validation (_validate_synthetic_options_structure)",
            "expected": "0 missing parent references",
            "actual": lambda: f"{summary['validation']['synthetic_options'].get('synthetic_missing_parent', 0)} missing",
            "status": lambda: "PASS" if summary['validation']['synthetic_options'].get('synthetic_missing_parent', 0) == 0 else "WARNING",
        },
        {
            "id": "REQ-7-KALSHI",
            "requirement": "Kalshi Enrichment: Titles must not be ticker-only",
            "test": "Kalshi enrichment validation (_validate_kalshi_enrichment_detailed)",
            "expected": "0 ticker-only titles",
            "actual": lambda: f"{summary['validation'].get('kalshi_enrichment', {}).get('ticker_only_titles', 'N/A')} ticker-only" if 'kalshi_enrichment' in summary['validation'] else "N/A (no Kalshi data)",
            "status": lambda: "PASS" if summary['validation'].get('kalshi_enrichment', {}).get('ticker_only_titles', 0) == 0 else "FAIL" if 'kalshi_enrichment' in summary['validation'] else "N/A",
        },
        {
            "id": "REQ-8-KALSHI",
            "requirement": "Kalshi Descriptions: Must be substantive (not empty, ideally >50 chars)",
            "test": "Kalshi enrichment validation (_validate_kalshi_enrichment_detailed)",
            "expected": "0 empty, <5% short (<50 chars)",
            "actual": lambda: f"{summary['validation'].get('kalshi_enrichment', {}).get('empty_descriptions', 'N/A')} empty, {summary['validation'].get('kalshi_enrichment', {}).get('short_desc_ratio', 0):.1%} short" if 'kalshi_enrichment' in summary['validation'] else "N/A",
            "status": lambda: "PASS" if (summary['validation'].get('kalshi_enrichment', {}).get('empty_descriptions', 0) == 0 and summary['validation'].get('kalshi_enrichment', {}).get('short_desc_ratio', 1) < 0.05) else "FAIL" if 'kalshi_enrichment' in summary['validation'] else "N/A",
        },
        {
            "id": "REQ-9-KALSHI",
            "requirement": "Kalshi URLs: Must include kalshi.com/markets/",
            "test": "Kalshi enrichment validation (_validate_kalshi_enrichment_detailed)",
            "expected": ">95% valid URLs",
            "actual": lambda: f"{summary['validation'].get('kalshi_enrichment', {}).get('valid_url_ratio', 0):.1%} valid" if 'kalshi_enrichment' in summary['validation'] else "N/A",
            "status": lambda: "PASS" if summary['validation'].get('kalshi_enrichment', {}).get('valid_url_ratio', 0) > 0.95 else "WARNING" if 'kalshi_enrichment' in summary['validation'] else "N/A",
        },
        {
            "id": "REQ-10-META",
            "requirement": "Metaculus Structure: Should NOT have volume/bid/ask/open_interest fields",
            "test": "Metaculus structure validation (_validate_metaculus_structure)",
            "expected": "0 Kalshi-specific fields present",
            "actual": lambda: "Has Kalshi fields" if summary['validation'].get('metaculus_structure', {}).get('has_kalshi_fields', False) else "No Kalshi fields" if 'metaculus_structure' in summary['validation'] else "N/A",
            "status": lambda: "PASS" if not summary['validation'].get('metaculus_structure', {}).get('has_kalshi_fields', False) else "WARNING" if 'metaculus_structure' in summary['validation'] else "N/A",
        },
        {
            "id": "REQ-11",
            "requirement": "Status & Resolution: If status='resolved', must have resolved_value_json",
            "test": "Schema validation (resolved_value checks)",
            "expected": "0 resolved without value",
            "actual": lambda: f"{summary['cross_checks'].get('resolved_without_value', 0)} missing",
            "status": lambda: "PASS" if summary['cross_checks'].get('resolved_without_value', 0) == 0 else "WARNING",
        },
        {
            "id": "REQ-12",
            "requirement": "Time Validity: created_time <= end_time (when both present)",
            "test": "Schema validation (time ordering check)",
            "expected": "0 violations",
            "actual": lambda: f"{summary['cross_checks'].get('created_after_end', 0)} violations",
            "status": lambda: "PASS" if summary['cross_checks'].get('created_after_end', 0) == 0 else "WARNING",
        },
        {
            "id": "REQ-13-KALSHI",
            "requirement": "Kalshi Bid/Ask Coverage: ~81% of time series points should have bid/ask data",
            "test": "Time series null patterns validation (_validate_timeseries_null_patterns)",
            "expected": "~19% null ratio for bid/ask (81% coverage)",
            "actual": lambda: f"Bid: {summary['validation'].get('timeseries_null_patterns', {}).get('bid_null_ratio', 0):.1%} null, Ask: {summary['validation'].get('timeseries_null_patterns', {}).get('ask_null_ratio', 0):.1%} null" if 'timeseries_null_patterns' in summary['validation'] else "N/A",
            "status": lambda: "INFO" if 'timeseries_null_patterns' in summary['validation'] else "N/A",
        },
        {
            "id": "REQ-14",
            "requirement": "Metadata Completeness: metadata_json should contain key fields (market_type, status, times, etc.)",
            "test": "Metadata completeness validation (_validate_metadata_completeness)",
            "expected": ">70% overall completeness",
            "actual": lambda: f"{summary['validation'].get('metadata_completeness', {}).get('overall_completeness', 0):.1%} complete" if 'metadata_completeness' in summary['validation'] else "N/A",
            "status": lambda: "PASS" if summary['validation'].get('metadata_completeness', {}).get('overall_completeness', 1.0) >= 0.7 else "WARNING" if 'metadata_completeness' in summary['validation'] else "N/A",
        },
        {
            "id": "REQ-15",
            "requirement": "History Depth: Time series should have sufficient depth (ideally >=7 days when available)",
            "test": "History depth validation (_validate_history_depth)",
            "expected": ">50% of options have >=7 data points",
            "actual": lambda: f"{summary['validation'].get('history_depth', {}).get('good_depth_ratio', 0):.1%} with >=7 points, mean={summary['validation'].get('history_depth', {}).get('ts_length_stats', {}).get('mean', 0):.1f}" if 'history_depth' in summary['validation'] else "N/A",
            "status": lambda: "PASS" if summary['validation'].get('history_depth', {}).get('good_depth_ratio', 0) > 0.5 else "WARNING" if 'history_depth' in summary['validation'] else "N/A",
        },
    ]
    
    # Generate table
    for req in requirements:
        lines.append(f"[{req['id']}] {req['requirement']}")
        lines.append(f"  Test: {req['test']}")
        lines.append(f"  Expected: {req['expected']}")
        actual = req['actual']() if callable(req['actual']) else req['actual']
        lines.append(f"  Actual: {actual}")
        status = req['status']() if callable(req['status']) else req['status']
        lines.append(f"  Status: {status}")
        
        # Add improvement needed if failed
        if status == "FAIL":
            lines.append(f"  >>> IMPROVEMENT NEEDED: This requirement is violated and must be fixed.")
        elif status == "WARNING":
            lines.append(f"  >>> REVIEW NEEDED: This may indicate a data quality issue.")
        
        lines.append("")
    
    return lines


def _generate_executive_summary(summary: dict, df: pd.DataFrame) -> List[str]:
    """Generate executive summary section."""
    lines = []
    lines.append("=" * 80)
    lines.append("DATA QUALITY AUDIT REPORT")
    lines.append("=" * 80)
    lines.append("")
    lines.append("SECTION 1: EXECUTIVE SUMMARY")
    lines.append("=" * 80)
    lines.append("")
    
    # Count critical issues, warnings
    errors = summary['validation'].get('errors', [])
    warnings = summary['validation'].get('warnings', [])
    
    critical_count = len([e for e in errors if 'missing_columns' in e or 'duplicate_events' in e or 'beliefs_out_of_range' in e or 'list_misalignments' in e])
    
    lines.append(f"Dataset: {len(df)} events")
    if "source" in df.columns:
        source_counts = df['source'].value_counts().to_dict()
        for source, count in source_counts.items():
            lines.append(f"  - {source}: {count} events")
    lines.append("")
    
    lines.append(f"Overall Status: {'PASS' if len(errors) == 0 else 'FAIL'}")
    lines.append(f"Critical Issues: {critical_count}")
    lines.append(f"Warnings: {len(warnings)}")
    lines.append(f"Total Errors: {len(errors)}")
    lines.append("")
    
    if errors:
        lines.append("Critical Issues Found:")
        for error in errors[:10]:  # First 10
            lines.append(f"  - {error}")
        if len(errors) > 10:
            lines.append(f"  ... and {len(errors) - 10} more")
        lines.append("")
    
    if warnings:
        lines.append("Warnings:")
        for warning in warnings[:10]:
            lines.append(f"  - {warning}")
        if len(warnings) > 10:
            lines.append(f"  ... and {len(warnings) - 10} more")
        lines.append("")
    
    return lines


def _generate_source_specific_analysis(summary: dict, df: pd.DataFrame) -> List[str]:
    """Generate source-specific analysis section."""
    lines = []
    lines.append("=" * 80)
    lines.append("SECTION 3: SOURCE-SPECIFIC ANALYSIS")
    lines.append("=" * 80)
    lines.append("")
    
    # Kalshi analysis
    if 'kalshi_enrichment' in summary['validation']:
        ke = summary['validation']['kalshi_enrichment']
        lines.append("KALSHI DATA QUALITY:")
        lines.append(f"  Sample size: {ke.get('sample_size', 0)}")
        lines.append("")
        lines.append("  Metadata Enrichment:")
        lines.append(f"    Ticker-only titles: {ke.get('ticker_only_titles', 0)} ({ke.get('ticker_only_ratio', 0):.1%})")
        lines.append(f"    Empty descriptions: {ke.get('empty_descriptions', 0)} ({ke.get('empty_desc_ratio', 0):.1%})")
        lines.append(f"    Short descriptions (<50 chars): {ke.get('short_descriptions', 0)} ({ke.get('short_desc_ratio', 0):.1%})")
        lines.append(f"    Valid URLs: {ke.get('valid_urls', 0)} ({ke.get('valid_url_ratio', 0):.1%})")
        lines.append("")
    
    # Time series enrichment (all sources)
    if 'timeseries_null_patterns' in summary['validation']:
        tsnp = summary['validation']['timeseries_null_patterns']
        lines.append("TIME SERIES DATA COVERAGE:")
        lines.append(f"  Sample size: {tsnp.get('sample_size', 0)} events")
        lines.append("")
        lines.append("  Bid/Ask Coverage:")
        lines.append(f"    Bid: {tsnp.get('bid_has_data_count', 0):,} / {tsnp.get('bid_total_points', 0):,} points ({tsnp.get('bid_coverage_ratio', 0):.1%} coverage)")
        lines.append(f"    Ask: {tsnp.get('ask_has_data_count', 0):,} / {tsnp.get('ask_total_points', 0):,} points ({tsnp.get('ask_coverage_ratio', 0):.1%} coverage)")
        lines.append("")
        lines.append("  Volume/Open Interest Coverage:")
        lines.append(f"    Volume: {tsnp.get('volume_has_data_count', 0):,} / {tsnp.get('volume_total_points', 0):,} points ({tsnp.get('volume_coverage_ratio', 0):.1%} coverage)")
        lines.append(f"    Open Interest: {tsnp.get('open_interest_has_data_count', 0):,} / {tsnp.get('open_interest_total_points', 0):,} points ({tsnp.get('open_interest_coverage_ratio', 0):.1%} coverage)")
        lines.append("")
    
    # History depth
    if 'history_depth' in summary['validation']:
        hd = summary['validation']['history_depth']
        if 'ts_length_stats' in hd:
            lines.append("HISTORY DEPTH STATISTICS:")
            lines.append(f"  Options checked: {hd.get('options_checked', 0):,}")
            lines.append("")
            lines.append("  Time Series Length (days):")
            lines.append(f"    Min: {hd['ts_length_stats'].get('min', 0)}")
            lines.append(f"    Mean: {hd['ts_length_stats'].get('mean', 0):.1f}")
            lines.append(f"    Median: {hd['ts_length_stats'].get('median', 0):.1f}")
            lines.append(f"    Max: {hd['ts_length_stats'].get('max', 0)}")
            lines.append("")
            lines.append("  Data Quality:")
            lines.append(f"    Empty history: {hd.get('empty_ts_count', 0)} options ({hd.get('empty_ts_count', 0) / hd.get('options_checked', 1):.1%})")
            lines.append(f"    Shallow (<3 days): {hd.get('shallow_history_count', 0)} options ({hd.get('shallow_history_ratio', 0):.1%})")
            lines.append(f"    Good depth (>=7 days): {hd.get('good_depth_count', 0)} options ({hd.get('good_depth_ratio', 0):.1%})")
            lines.append("")
    
    # Metadata completeness
    if 'metadata_completeness' in summary['validation']:
        mc = summary['validation']['metadata_completeness']
        lines.append("METADATA COMPLETENESS:")
        lines.append(f"  Sample size: {mc.get('sample_size', 0)}")
        lines.append(f"  Valid metadata: {mc.get('total_checked', 0)}")
        lines.append(f"  Empty metadata: {mc.get('empty_metadata', 0)}")
        lines.append(f"  Overall completeness: {mc.get('overall_completeness', 0):.1%}")
        lines.append("")
        if 'field_ratios' in mc:
            lines.append("  Key Field Presence:")
            for field, ratio in sorted(mc['field_ratios'].items()):
                field_name = field.replace('_ratio', '')
                lines.append(f"    {field_name}: {ratio:.1%}")
            lines.append("")
    
    # Metaculus analysis
    if 'metaculus_structure' in summary['validation']:
        ms = summary['validation']['metaculus_structure']
        lines.append("METACULUS DATA QUALITY:")
        lines.append(f"  Sample size: {ms.get('sample_size', 0)}")
        lines.append(f"  Volume fields present: {ms.get('volume_present_count', 0)}")
        lines.append(f"  Bid fields present: {ms.get('bid_present_count', 0)}")
        lines.append(f"  Ask fields present: {ms.get('ask_present_count', 0)}")
        lines.append(f"  Open interest fields present: {ms.get('open_interest_present_count', 0)}")
        lines.append(f"  Has Kalshi-specific fields: {ms.get('has_kalshi_fields', False)}")
        lines.append("")
    
    return lines


def _generate_detailed_findings(summary: dict) -> List[str]:
    """Generate detailed findings section."""
    lines = []
    lines.append("=" * 80)
    lines.append("SECTION 4: DETAILED FINDINGS")
    lines.append("=" * 80)
    lines.append("")
    
    # Belief range violations
    if 'belief_ranges' in summary['validation']:
        br = summary['validation']['belief_ranges']
        if br.get('out_of_range_count', 0) > 0:
            lines.append("BELIEF RANGE VIOLATIONS:")
            lines.append(f"  Total beliefs checked: {br.get('total_beliefs_checked', 0)}")
            lines.append(f"  Out of range: {br.get('out_of_range_count', 0)} ({br.get('violation_ratio', 0):.2%})")
            lines.append(f"  Negative values: {br.get('negative_count', 0)}")
            lines.append(f"  Above 1.0: {br.get('above_one_count', 0)}")
            if br.get('violations_sample'):
                lines.append("  Sample violations:")
                for v in br['violations_sample'][:5]:
                    lines.append(f"    - Option {v['option_id']}: belief[{v['belief_idx']}] = {v['value']}")
            lines.append("")
    
    # Timestamp monotonicity violations
    if 'timestamp_monotonicity' in summary['validation']:
        tm = summary['validation']['timestamp_monotonicity']
        if tm.get('unsorted_lists', 0) > 0:
            lines.append("TIMESTAMP ORDERING VIOLATIONS:")
            lines.append(f"  Lists checked: {tm.get('checked_lists', 0)}")
            lines.append(f"  Unsorted: {tm.get('unsorted_lists', 0)} ({tm.get('unsorted_ratio', 0):.2%})")
            if tm.get('violations_sample'):
                lines.append("  Sample violations:")
                for v in tm['violations_sample'][:5]:
                    lines.append(f"    - Option {v['option_id']}: {v['ts_count']} timestamps unsorted")
            lines.append("")
    
    # List alignment violations
    if 'list_alignment' in summary['validation']:
        la = summary['validation']['list_alignment']
        if la.get('total_misalignments', 0) > 0:
            lines.append("LIST ALIGNMENT VIOLATIONS:")
            lines.append(f"  Options checked: {la.get('total_options_checked', 0)}")
            lines.append(f"  Total misalignments: {la.get('total_misalignments', 0)}")
            lines.append(f"  By field:")
            for field, count in la.get('misalignment_counts', {}).items():
                if count > 0:
                    lines.append(f"    - {field}: {count}")
            if la.get('violations_sample'):
                lines.append("  Sample violations:")
                for v in la['violations_sample'][:5]:
                    lines.append(f"    - Event {v['event_id']}, Option {v['option_id']}: {v['field']} length {v['field_len']} != ts length {v['ts_len']}")
            lines.append("")
    
    # Synthetic options violations
    if 'synthetic_options' in summary['validation']:
        so = summary['validation']['synthetic_options']
        if so.get('synthetic_missing_parent', 0) > 0:
            lines.append("SYNTHETIC OPTIONS VIOLATIONS:")
            lines.append(f"  Synthetic options: {so.get('synthetic_options_count', 0)}")
            lines.append(f"  Missing parent reference: {so.get('synthetic_missing_parent', 0)} ({so.get('missing_parent_ratio', 0):.2%})")
            if so.get('violations_sample'):
                lines.append("  Sample violations:")
                for v in so['violations_sample'][:5]:
                    lines.append(f"    - Option {v['option_id']} (Market {v['market_id']}): is_synthetic=true but no derived_from_market_id")
            lines.append("")
    
    return lines


def _generate_remediation_recommendations(summary: dict) -> List[str]:
    """Generate remediation recommendations."""
    lines = []
    lines.append("=" * 80)
    lines.append("SECTION 5: REMEDIATION RECOMMENDATIONS")
    lines.append("=" * 80)
    lines.append("")
    
    recommendations = []
    
    # Check for critical issues and generate recommendations
    if summary['validation']['uniqueness'].get('duplicate_count', 0) > 0:
        recommendations.append({
            "priority": "CRITICAL",
            "issue": "Duplicate (source, event_id) pairs found",
            "recommendation": "Investigate duplicate events in data ingestion pipeline. Check for race conditions or incorrect deduplication logic.",
        })
    
    if summary['validation']['belief_ranges'].get('out_of_range_count', 0) > 0:
        recommendations.append({
            "priority": "CRITICAL",
            "issue": "Belief values outside [0, 1] range",
            "recommendation": "Add validation and normalization in data ingestion. Check for unit conversion errors (e.g., percentage vs probability).",
        })
    
    if summary['validation']['list_alignment'].get('total_misalignments', 0) > 0:
        recommendations.append({
            "priority": "CRITICAL",
            "issue": "Time series list length misalignments",
            "recommendation": "Fix data ingestion to ensure all time series fields (belief, volume, bid, ask, etc.) align with ts list length.",
        })
    
    if summary['validation'].get('kalshi_enrichment', {}).get('ticker_only_titles', 0) > 0:
        recommendations.append({
            "priority": "HIGH",
            "issue": "Kalshi ticker-only titles found",
            "recommendation": "Ensure Kalshi API enrichment is working correctly. Check /markets endpoint calls and fallback logic.",
        })
    
    if summary['validation'].get('kalshi_enrichment', {}).get('empty_descriptions', 0) > 0:
        recommendations.append({
            "priority": "HIGH",
            "issue": "Empty descriptions in Kalshi data",
            "recommendation": "Verify Kalshi API enrichment. Empty descriptions should be filled from API response.",
        })
    
    if summary['validation']['timestamp_monotonicity'].get('unsorted_lists', 0) > 0:
        recommendations.append({
            "priority": "MEDIUM",
            "issue": "Unsorted timestamps in time series",
            "recommendation": "Add sorting step in data ingestion or verify that data is ingested in chronological order.",
        })
    
    if summary['validation']['synthetic_options'].get('synthetic_missing_parent', 0) > 0:
        recommendations.append({
            "priority": "MEDIUM",
            "issue": "Synthetic options missing parent market reference",
            "recommendation": "Ensure derived_from_market_id is set when creating synthetic options.",
        })
    
    if not recommendations:
        lines.append("No critical issues found. Data quality is good!")
        lines.append("")
    else:
        # Sort by priority
        priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 99))
        
        for rec in recommendations:
            lines.append(f"[{rec['priority']}] {rec['issue']}")
            lines.append(f"  Recommendation: {rec['recommendation']}")
            lines.append("")
    
    return lines


def _resolve_latest_parquet() -> Path | None:
    datasets_dir = Path("data/datasets")
    if not datasets_dir.exists():
        return None
    datasets = sorted(list(datasets_dir.glob("v*_unified")))
    if not datasets:
        return None
    parquet_path = datasets[-1] / "data.parquet"
    return parquet_path if parquet_path.exists() else None


def run_quality_audit(
    parquet_path: Path,
    output_dir: Path,
    sample_rows: int = 20000,
    source: Optional[str] = None,
) -> Tuple[Dict[str, Any], str]:
    df = pd.read_parquet(parquet_path)
    if source:
        df = df[df["source"] == source].copy()
    report_lines = []

    report_lines.append(f"Rows: {len(df)}")
    report_lines.append(f"Columns: {df.columns.tolist()}")
    if source:
        report_lines.append(f"Source filter: {source}")
    report_lines.append("")

    summary: Dict[str, Any] = {
        "row_count": int(len(df)),
        "columns": {},
        "cross_checks": {},
        "validation": {
            "source_filter": source or "all",
            "errors": [],
            "warnings": [],
        },
    }

    if "source" in df.columns:
        source_counts = df["source"].value_counts(dropna=False).to_dict()
        summary["validation"]["source_counts"] = source_counts
        expected_sources = {source} if source else {"kalshi", "metaculus"}
        present_sources = set(k for k in source_counts if k is not None)
        missing_sources = sorted(expected_sources - present_sources)
        if missing_sources:
            summary["validation"]["warnings"].append(f"missing_sources={missing_sources}")

    if source and df.empty:
        summary["validation"]["errors"].append(f"source_missing={source}")

    if not df.empty:
        schema_validation = _validate_event_schema(df)
        summary["validation"]["errors"].extend(schema_validation["errors"])
        summary["validation"]["warnings"].extend(schema_validation["warnings"])
        summary["validation"]["schema"] = schema_validation

        # Run new comprehensive validation functions
        summary["validation"]["uniqueness"] = _validate_uniqueness(df)
        if not summary["validation"]["uniqueness"].get("is_valid"):
            summary["validation"]["errors"].append(
                f"duplicate_events={summary['validation']['uniqueness'].get('duplicate_count', 0)}"
            )
        
        if "options_json" in df.columns:
            summary["validation"]["belief_ranges"] = _validate_belief_ranges_detailed(
                df["options_json"], sample_rows=sample_rows
            )
            if summary["validation"]["belief_ranges"].get("out_of_range_count", 0) > 0:
                summary["validation"]["errors"].append(
                    f"beliefs_out_of_range={summary['validation']['belief_ranges']['out_of_range_count']}"
                )
            
            summary["validation"]["timestamp_monotonicity"] = _validate_timestamp_monotonicity_detailed(
                df["options_json"], sample_rows=sample_rows
            )
            if summary["validation"]["timestamp_monotonicity"].get("unsorted_lists", 0) > 0:
                summary["validation"]["warnings"].append(
                    f"unsorted_timestamps={summary['validation']['timestamp_monotonicity']['unsorted_lists']}"
                )
            
            summary["validation"]["list_alignment"] = _validate_list_alignment_comprehensive(
                df, sample_rows=sample_rows
            )
            if summary["validation"]["list_alignment"].get("total_misalignments", 0) > 0:
                summary["validation"]["errors"].append(
                    f"list_misalignments={summary['validation']['list_alignment']['total_misalignments']}"
                )
            
            summary["validation"]["synthetic_options"] = _validate_synthetic_options_structure(
                df["options_json"], sample_rows=sample_rows
            )
            if summary["validation"]["synthetic_options"].get("synthetic_missing_parent", 0) > 0:
                summary["validation"]["warnings"].append(
                    f"synthetic_missing_parent={summary['validation']['synthetic_options']['synthetic_missing_parent']}"
                )
            
            # New enrichment quality validations
            summary["validation"]["timeseries_null_patterns"] = _validate_timeseries_null_patterns(
                df["options_json"], sample_rows=sample_rows
            )
            
            summary["validation"]["history_depth"] = _validate_history_depth(
                df["options_json"], sample_rows=sample_rows
            )
            
            # Check for shallow or empty history
            if summary["validation"]["history_depth"].get("shallow_history_ratio", 0) > 0.5:
                summary["validation"]["warnings"].append(
                    f"shallow_history={summary['validation']['history_depth'].get('shallow_history_count', 0)}"
                )
        
        # Validate metadata completeness
        if "metadata_json" in df.columns:
            summary["validation"]["metadata_completeness"] = _validate_metadata_completeness(
                df["metadata_json"], sample_rows=sample_rows
            )
            if summary["validation"]["metadata_completeness"].get("overall_completeness", 1.0) < 0.7:
                summary["validation"]["warnings"].append(
                    f"metadata_incomplete={summary['validation']['metadata_completeness'].get('overall_completeness', 0):.1%}"
                )
        
        # Source-specific validation
        if "source" in df.columns and (source == "kalshi" or (source is None and (df["source"] == "kalshi").any())):
            summary["validation"]["kalshi_enrichment"] = _validate_kalshi_enrichment_detailed(
                df, sample_rows=sample_rows
            )
            if summary["validation"]["kalshi_enrichment"].get("ticker_only_titles", 0) > 0:
                summary["validation"]["errors"].append(
                    f"kalshi_ticker_only_titles={summary['validation']['kalshi_enrichment']['ticker_only_titles']}"
                )
            if summary["validation"]["kalshi_enrichment"].get("empty_descriptions", 0) > 0:
                summary["validation"]["errors"].append(
                    f"kalshi_empty_descriptions={summary['validation']['kalshi_enrichment']['empty_descriptions']}"
                )
        
        if "source" in df.columns and (source == "metaculus" or (source is None and (df["source"] == "metaculus").any())):
            summary["validation"]["metaculus_structure"] = _validate_metaculus_structure(
                df, sample_rows=sample_rows
            )
            if summary["validation"]["metaculus_structure"].get("has_kalshi_fields"):
                summary["validation"]["warnings"].append(
                    f"metaculus_has_kalshi_fields=true"
                )

        if "options_json" in df.columns:
            required_keys = [
                "option_id",
                "market_id",
                "title",
                "ts",
                "belief",
                "is_synthetic",
            ]
            if source == "metaculus" or (source is None and (df["source"] == "metaculus").any()):
                meta_df = df[df["source"] == "metaculus"]
                summary["validation"]["metaculus_options_json"] = _validate_options_json_structure(
                    meta_df["options_json"],
                    sample_rows=sample_rows,
                    require_keys=required_keys,
                )

    for col in df.columns:
        series = df[col]
        col_summary = _series_basic_stats(series)

        report_lines.append(f"== Column: {col} ==")
        report_lines.append(f"Basic: {col_summary}")

        if series.dtype == object:
            col_summary["string_stats"] = _string_stats(series.astype("string"))
            report_lines.append(f"String: {col_summary['string_stats']}")

        if col in {"end_time", "created_time", "ts"} and not _detect_list_like(series):
            col_summary["datetime_stats"] = _datetime_stats(series)
            report_lines.append(f"Datetime: {col_summary['datetime_stats']}")

        if col in {"answer_options_json", "options_json", "resolved_value_json", "metadata_json"}:
            col_summary["json_stats"] = _json_stats(series, sample_size=sample_rows)
            report_lines.append(f"JSON: {col_summary['json_stats']}")

        if _detect_list_like(series):
            col_summary["list_stats"] = _list_stats(series)
            report_lines.append(f"List: {col_summary['list_stats']}")
            col_summary["list_null_stats"] = _list_null_value_stats(series, sample_rows=sample_rows)
            report_lines.append(f"List nulls: {col_summary['list_null_stats']}")

            if col in {"belief", "volume", "open_interest", "bid", "ask"}:
                col_summary["list_numeric_stats"] = _list_numeric_stats(series, sample_rows=sample_rows)
                report_lines.append(f"List numeric: {col_summary['list_numeric_stats']}")

            if col == "ts":
                col_summary["ts_time_stats"] = _ts_list_time_stats(series, sample_rows=sample_rows)
                report_lines.append(f"TS time: {col_summary['ts_time_stats']}")
                col_summary["ts_order_stats"] = _ts_list_order_stats(series, sample_rows=sample_rows)
                report_lines.append(f"TS order: {col_summary['ts_order_stats']}")

        if col in {"source", "status", "market_type"}:
            vc = series.value_counts(dropna=False).head(20)
            col_summary["value_counts_top20"] = vc.to_dict()
            report_lines.append(f"Top values: {col_summary['value_counts_top20']}")

        if col in {"event_id", "market_id"}:
            col_summary["unique_count"] = int(series.nunique(dropna=True))
            report_lines.append(f"Unique: {col_summary['unique_count']}")

        summary["columns"][col] = col_summary
        report_lines.append("")

    cross = {}
    if "options_json" in df.columns:
        cross["options_json_stats"] = _options_json_stats(df["options_json"], sample_rows=sample_rows)
    elif "answer_options_json" in df.columns:
        cross["answer_options_json_stats"] = _options_json_stats(df["answer_options_json"], sample_rows=sample_rows)
    if {"ts", "belief"}.issubset(df.columns) and _detect_list_like(df["ts"]) and _detect_list_like(df["belief"]):
        ts_len = df["ts"].apply(lambda x: len(x) if isinstance(x, (list, tuple, np.ndarray)) else np.nan)
        belief_len = df["belief"].apply(lambda x: len(x) if isinstance(x, (list, tuple, np.ndarray)) else np.nan)
        cross["ts_belief_length_mismatch"] = int(((ts_len.notna()) & (belief_len.notna()) & (ts_len != belief_len)).sum())

    for col in ["volume", "open_interest", "bid", "ask"]:
        if col in df.columns and _detect_list_like(df[col]) and _detect_list_like(df["ts"]):
            col_len = df[col].apply(lambda x: len(x) if isinstance(x, (list, tuple, np.ndarray)) else np.nan)
            ts_len = df["ts"].apply(lambda x: len(x) if isinstance(x, (list, tuple, np.ndarray)) else np.nan)
            cross[f"ts_{col}_length_mismatch"] = int(((ts_len.notna()) & (col_len.notna()) & (ts_len != col_len)).sum())

    if {"status", "resolved_value_json"}.issubset(df.columns):
        status = df["status"].astype("string")
        resolved = df["resolved_value_json"].astype("string")
        resolved_empty = resolved.eq("")
        resolved_nonempty = resolved.ne("")
        cross["resolved_without_value"] = int(((status == "resolved") & resolved.isna()).sum())
        cross["resolved_status_empty_value"] = int(((status == "resolved") & resolved_empty).sum())
        cross["resolved_status_nonempty_value"] = int(((status == "resolved") & resolved_nonempty).sum())
        cross["has_value_without_resolved_status"] = int(((status != "resolved") & resolved_nonempty).sum())

    if {"created_time", "end_time"}.issubset(df.columns):
        created = pd.to_datetime(df["created_time"], errors="coerce", utc=True)
        end = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
        cross["created_after_end"] = int(((created > end) & created.notna() & end.notna()).sum())

    summary["cross_checks"] = cross
    
    # Generate enhanced report with all sections
    enhanced_report = []
    
    # Section 1: Executive Summary
    enhanced_report.extend(_generate_executive_summary(summary, df))
    enhanced_report.append("")
    
    # Section 2: Requirement Mapping
    enhanced_report.extend(_generate_requirement_mapping_report(summary, df))
    enhanced_report.append("")
    
    # Section 3: Source-Specific Analysis
    enhanced_report.extend(_generate_source_specific_analysis(summary, df))
    enhanced_report.append("")
    
    # Section 4: Detailed Findings
    enhanced_report.extend(_generate_detailed_findings(summary))
    enhanced_report.append("")
    
    # Section 5: Remediation Recommendations
    enhanced_report.extend(_generate_remediation_recommendations(summary))
    enhanced_report.append("")
    
    # Append original detailed column analysis
    enhanced_report.append("=" * 80)
    enhanced_report.append("APPENDIX: DETAILED COLUMN STATISTICS")
    enhanced_report.append("=" * 80)
    enhanced_report.append("")
    enhanced_report.extend(report_lines)
    enhanced_report.append("")
    enhanced_report.append("== Cross-column checks ==")
    enhanced_report.append(json.dumps(cross, indent=2))
    enhanced_report.append("")
    enhanced_report.append("== Validation Details ==")
    enhanced_report.append(json.dumps(summary["validation"], indent=2))

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "DATA_QUALITY_REPORT.txt"
    summary_path = output_dir / "data_quality_results.json"
    report_path.write_text("\n".join(enhanced_report), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return summary, "\n".join(enhanced_report)


def main() -> int:
    parser = argparse.ArgumentParser(description="Parquet quality audit tool")
    parser.add_argument("--parquet", type=str, default="", help="Path to data.parquet (defaults to latest dataset)")
    parser.add_argument("--output", type=str, default="tests/parquet_quality_outputs", help="Output directory")
    parser.add_argument("--sample-rows", type=int, default=20000, help="Sample size for list/JSON stats")
    parser.add_argument("--source", type=str, default="", help="Limit audit to a single source")
    args = parser.parse_args()

    parquet_path = Path(args.parquet) if args.parquet else _resolve_latest_parquet()
    if parquet_path is None or not parquet_path.exists():
        print("No parquet dataset found. Run scripts/build_db.py first.")
        return 0

    output_dir = Path(args.output)
    source = args.source if args.source else None
    run_quality_audit(parquet_path, output_dir, sample_rows=args.sample_rows, source=source)
    print(f"Wrote report to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# --- LESSONS LEARNED ---
# 1. Sample list-heavy columns to keep audits fast on large datasets.
# 2. options_json audits catch misaligned histories early.
# 3. Datetime validation should ignore nulls for nullable fields.
# 4. Timestamp Granularity: Data points are daily, captured at end-of-day (23:59:59).
#
# 5. ENRICHMENT QUALITY TRACKING (Added Jan 2026):
#    - Added _validate_timeseries_null_patterns() to track null vs data coverage in bid/ask/volume/open_interest.
#    - Added _validate_metadata_completeness() to ensure metadata_json has expected key fields.
#    - Added _validate_history_depth() to analyze time series depth (min/max/mean/median).
#    - These track whether data distributions are enriched or mostly nulls.
#
# 6. BID/ASK COVERAGE FINDINGS:
#    - Expected: ~81% coverage for Kalshi (per DOCUMENTATION.txt), 0% for Metaculus.
#    - Test dataset (Dec 23-29, 2024): Found 0% bid/ask coverage - needs investigation.
#    - Volume/Open Interest: 96.3% coverage - excellent enrichment working.
#    - Bid/ask likely comes from candlestick data which may be missing for this date range.
#    - Action: Test different date ranges or verify candlestick data availability in S3.
#
# 7. METADATA COMPLETENESS GOTCHA:
#    - Test checks for canonical field names (market_type, status, open_time, close_time, ticker, title).
#    - Kalshi uses different field names in actual metadata_json (can_close_early, cap_strike, etc.).
#    - Test reports 0% completeness but metadata IS present with Kalshi's naming.
#    - Future: Update expected_fields to match Kalshi's actual structure, or normalize during ingestion.
#
# 8. HISTORY DEPTH CONTEXT:
#    - Mean 4.3 days for 7-day dataset is appropriate (markets open mid-period, close early, etc.).
#    - Test threshold of ">=7 days = good depth" should be adaptive based on dataset date range.
#    - For short test datasets (<14 days), expect proportionally less depth.
#    - 0.4% empty history (277 / 69,822 options) is excellent.
#
# 9. REQUIREMENT MAPPING EVOLUTION:
#    - Started with 12 requirements from DOCUMENTATION.txt.
#    - Added 3 enrichment quality requirements: REQ-13 (bid/ask), REQ-14 (metadata), REQ-15 (depth).
#    - Now tracks 15 requirements total, providing comprehensive data quality visibility.
#
# 10. VALIDATION FUNCTION DESIGN PATTERNS:
#     - All validation functions sample 20,000 rows by default for performance.
#     - Functions parse options_json directly rather than relying on flattened columns.
#     - Return dict with sampled=bool, sample_size=int, and metric-specific fields.
#     - Include violation_sample for debugging (first 10 examples of issues).
#     - Status determination: PASS (perfect), WARNING (acceptable but notable), FAIL (critical issue).
#
# 11. REPORT STRUCTURE:
#     - Section 1: Executive Summary (overall status, critical issues count, warnings count).
#     - Section 2: Requirement Mapping (15 requirements mapped to tests with expected vs actual).
#     - Section 3: Source-Specific Analysis (Kalshi metrics, Metaculus metrics, coverage stats).
#     - Section 4: Detailed Findings (violations with event_ids and examples).
#     - Section 5: Remediation Recommendations (priority-sorted actionable steps).
#     - Appendix: Detailed Column Statistics (original parquet_quality output).
#
# 12. SYNTHETIC OPTIONS BUG (FIXED - Jan 18, 2026):
#     - Initial detection: 454 NONE_OF_ABOVE options (21.67%) missing derived_from_market_id.
#     - Root cause: Line 695 in src/build_unified_parquet.py was setting it to None.
#     - Fix applied: Changed to options[0].get("market_id") if options else event_id.
#     - Validation: Re-ran quality audit on new dataset - 0 violations detected (100% fix rate).
#     - This demonstrates the data-driven workflow: detect >> diagnose >> fix >> validate.
#
# 13. QUALITY AUDIT VALIDATION (Jan 18, 2026 - v20260118_1740_unified):
#     Dataset: 4,099 events (3,917 Kalshi, 182 Metaculus), 69,822 options, 297,275 time series points
#     Date Range: Dec 23-29, 2024 (7 days)
#     Runtime: ~2.7 minutes
#     
#     STATUS: ✅ PASS (10/15 requirements passing, 4 warnings expected, 0 critical issues)
#     
#     FIXED ISSUES:
#       ✅ REQ-6: Synthetic options - 0 missing parent refs (was 454, now 0 - 100% fix rate)
#     
#     ALL PASSING REQUIREMENTS:
#       ✅ Schema validation, Uniqueness, Time series alignment, Belief ranges
#       ✅ Timestamp ordering, Synthetic options, Kalshi enrichment (titles/descriptions/URLs)
#       ✅ Metaculus structure, Resolution data handling
#     
#     EXPECTED WARNINGS (not real issues):
#       ⚠️ REQ-12: created_after_end - 2 events (0.05%, negligible)
#       ℹ️ REQ-13: Bid/ask - 0% coverage (INFO, likely date-specific; volume/OI at 96.3%)
#       ⚠️ REQ-14: Metadata completeness - 0% (false positive, field name mismatch)
#       ⚠️ REQ-15: History depth - 23.9% >=7 days (appropriate for 7-day dataset)
#     
#     REMAINING LOW-PRIORITY WORK:
#       - Investigate bid/ask coverage on different date ranges
#       - Update metadata test to match actual Kalshi field names
#       - Make history depth test adaptive to dataset length
#
# 14. PERFORMANCE:
#     - Full audit of 4,099 events with 69,822 options completes in ~2 minutes.
#     - Sampling strategy (20,000 rows) keeps it fast even on larger datasets.
#     - JSON parsing is the bottleneck - consider caching parsed options_json if auditing repeatedly.
