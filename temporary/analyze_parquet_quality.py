import json
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


REPORT_PATH = Path("temporary/parquet_quality_report.txt")
SUMMARY_PATH = Path("temporary/parquet_quality_summary.json")


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


def _numeric_stats(series: pd.Series) -> dict:
    non_null = pd.to_numeric(series, errors="coerce")
    return {
        "min": float(non_null.min()) if non_null.notna().any() else None,
        "max": float(non_null.max()) if non_null.notna().any() else None,
        "mean": float(non_null.mean()) if non_null.notna().any() else None,
        "zeros": int((non_null == 0).sum()),
        "negatives": int((non_null < 0).sum()),
        "nan_after_coerce": int(non_null.isna().sum()),
        "p01": float(non_null.quantile(0.01)) if non_null.notna().any() else None,
        "p99": float(non_null.quantile(0.99)) if non_null.notna().any() else None,
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


def main() -> None:
    parquet_path = Path("temporary/hf_dataset/data.parquet")
    if not parquet_path.exists():
        raise SystemExit(f"Parquet not found at {parquet_path}")

    df = pd.read_parquet(parquet_path)
    report_lines = []

    report_lines.append(f"Rows: {len(df)}")
    report_lines.append(f"Columns: {df.columns.tolist()}")
    report_lines.append("")

    summary = {
        "row_count": int(len(df)),
        "columns": {},
        "cross_checks": {},
    }

    for col in df.columns:
        print(f"Analyzing column: {col}")
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

        if col in {"answer_options_json", "resolved_value_json", "metadata_json"}:
            col_summary["json_stats"] = _json_stats(series, sample_size=20000)
            report_lines.append(f"JSON: {col_summary['json_stats']}")

        if _detect_list_like(series):
            col_summary["list_stats"] = _list_stats(series)
            report_lines.append(f"List: {col_summary['list_stats']}")
            col_summary["list_null_stats"] = _list_null_value_stats(series)
            report_lines.append(f"List nulls: {col_summary['list_null_stats']}")

            if col in {"belief", "volume", "open_interest", "bid", "ask"}:
                col_summary["list_numeric_stats"] = _list_numeric_stats(series)
                report_lines.append(f"List numeric: {col_summary['list_numeric_stats']}")

            if col == "ts":
                col_summary["ts_time_stats"] = _ts_list_time_stats(series)
                report_lines.append(f"TS time: {col_summary['ts_time_stats']}")
                col_summary["ts_order_stats"] = _ts_list_order_stats(series)
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

    # Cross-column checks
    cross = {}
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

    REPORT_PATH.write_text("\n".join(report_lines), encoding="utf-8")
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote report: {REPORT_PATH}")
    print(f"Wrote summary: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
