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
    report_lines.append("== Cross-column checks ==")
    report_lines.append(json.dumps(cross, indent=2))
    report_lines.append("")
    report_lines.append("== Validation ==")
    report_lines.append(json.dumps(summary["validation"], indent=2))

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "parquet_quality_report.txt"
    summary_path = output_dir / "parquet_quality_summary.json"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return summary, "\n".join(report_lines)


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
