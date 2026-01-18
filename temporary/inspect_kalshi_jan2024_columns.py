import json
from collections import Counter
from datetime import time
from pathlib import Path

import pandas as pd


DATASET_PATH = Path(
    "data/datasets/v20260118_0258_kalshi_jan2024_v2_20260118_unified/data.parquet"
)


def _safe_json(value):
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def _parse_ts(value):
    if not value:
        return None
    try:
        return pd.to_datetime(value, errors="coerce", utc=True)
    except Exception:
        return None


def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"Missing dataset: {DATASET_PATH}")

    df = pd.read_parquet(DATASET_PATH)
    print(f"Rows: {len(df)}")

    end_time = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    midnight_ratio = (end_time.dt.time == time(0, 0)).mean()
    print(f"\nend_time midnight ratio: {midnight_ratio:.3f}")

    option_exp_times = []
    option_expected_exp_times = []
    option_close_times = []
    for raw in df["options_json"]:
        options = _safe_json(raw) or []
        for option in options:
            meta = _safe_json(option.get("metadata_json")) or {}
            option_exp_times.append(_parse_ts(meta.get("expiration_time")))
            option_expected_exp_times.append(_parse_ts(meta.get("expected_expiration_time")))
            option_close_times.append(_parse_ts(meta.get("close_time")))

    def _ratio_midnight(values):
        series = pd.Series([v for v in values if v is not None and not pd.isna(v)])
        if series.empty:
            return None
        return (series.dt.time == time(0, 0)).mean()

    print("option expiration_time midnight ratio:", _ratio_midnight(option_exp_times))
    print("option expected_expiration_time midnight ratio:", _ratio_midnight(option_expected_exp_times))
    print("option close_time midnight ratio:", _ratio_midnight(option_close_times))

    bid_non_null = 0
    ask_non_null = 0
    volume_null_synth = 0
    volume_null_non_synth = 0
    oi_null_synth = 0
    oi_null_non_synth = 0
    synth_count = 0
    non_synth_count = 0

    for raw in df["options_json"]:
        options = _safe_json(raw) or []
        for option in options:
            is_synth = bool(option.get("is_synthetic"))
            if is_synth:
                synth_count += 1
            else:
                non_synth_count += 1

            for v in option.get("bid") or []:
                if v is not None:
                    bid_non_null += 1
            for v in option.get("ask") or []:
                if v is not None:
                    ask_non_null += 1

            vol = option.get("volume") or []
            oi = option.get("open_interest") or []
            if is_synth:
                volume_null_synth += sum(v is None for v in vol)
                oi_null_synth += sum(v is None for v in oi)
            else:
                volume_null_non_synth += sum(v is None for v in vol)
                oi_null_non_synth += sum(v is None for v in oi)

    print(f"\nBid non-null count across lists: {bid_non_null}")
    print(f"Ask non-null count across lists: {ask_non_null}")
    print(f"Synthetic options: {synth_count}, non-synthetic: {non_synth_count}")
    print(f"Volume nulls (synthetic): {volume_null_synth}")
    print(f"Volume nulls (non-synthetic): {volume_null_non_synth}")
    print(f"Open interest nulls (synthetic): {oi_null_synth}")
    print(f"Open interest nulls (non-synthetic): {oi_null_non_synth}")

    status_counts = Counter(df["status"].fillna("null").tolist())
    print("\nEvent status counts:", dict(status_counts))


if __name__ == "__main__":
    main()
