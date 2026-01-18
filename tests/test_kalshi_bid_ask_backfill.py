import json
import math
import os
from pathlib import Path

import pandas as pd
import pytest


DATASET_ENV = "KALSHI_BIDASK_DATASET"


def _non_null_ratio(values: list[object]) -> float:
    total = 0
    non_null = 0
    for value in values:
        total += 1
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        non_null += 1
    return (non_null / total) if total else 0.0


def _collect_option_values(df: pd.DataFrame, key: str) -> list[object]:
    values = []
    for raw in df["options_json"].dropna():
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if not isinstance(parsed, list):
            continue
        for option in parsed:
            values.extend(option.get(key) or [])
    return values


def test_kalshi_bid_ask_backfill_ratio():
    dataset_path = os.getenv(DATASET_ENV, "")
    if not dataset_path:
        pytest.skip(f"{DATASET_ENV} not set")
    path = Path(dataset_path)
    if not path.exists():
        pytest.skip(f"{DATASET_ENV} path not found: {path}")

    df = pd.read_parquet(path)
    df = df[df["source"] == "kalshi"].copy()
    if df.empty:
        pytest.skip("No Kalshi rows in dataset")

    bid_values = _collect_option_values(df, "bid")
    ask_values = _collect_option_values(df, "ask")

    bid_ratio = _non_null_ratio(bid_values)
    ask_ratio = _non_null_ratio(ask_values)

    assert bid_ratio > 0.0, f"Expected bid ratio > 0, got {bid_ratio:.6f}"
    assert ask_ratio > 0.0, f"Expected ask ratio > 0, got {ask_ratio:.6f}"
