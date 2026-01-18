#!/usr/bin/env python
"""
Metaculus data quality analyzer - detailed investigation of history presence and data completeness.

Usage:
    uv run python temporary/analyze_metaculus_quality.py --dataset <path_to_dataset_dir>
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from datetime import datetime

import pandas as pd


def analyze_metaculus_quality(dataset_path: str) -> dict:
    """Analyze Metaculus data quality in a unified dataset."""
    parquet_path = Path(dataset_path) / "data.parquet"
    
    if not parquet_path.exists():
        print(f"Error: Dataset not found at {parquet_path}")
        sys.exit(1)
    
    print(f"Loading dataset from {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # Filter to Metaculus only
    metaculus_df = df[df["source"] == "metaculus"].copy()
    total_events = len(metaculus_df)
    
    print(f"\n{'=' * 80}")
    print(f"METACULUS DATA QUALITY ANALYSIS")
    print(f"{'=' * 80}\n")
    print(f"Total Metaculus events: {total_events}")
    
    if total_events == 0:
        print("No Metaculus events found in dataset!")
        return {}
    
    # Status distribution
    status_counts = metaculus_df["status"].value_counts()
    print(f"\nStatus distribution:")
    for status, count in status_counts.items():
        pct = (count / total_events) * 100
        print(f"  {status}: {count} ({pct:.1f}%)")
    
    # Description completeness
    empty_descriptions = metaculus_df["description"].isna().sum() + (metaculus_df["description"] == "").sum()
    desc_completeness_pct = ((total_events - empty_descriptions) / total_events) * 100
    print(f"\nDescription completeness: {total_events - empty_descriptions}/{total_events} ({desc_completeness_pct:.1f}%)")
    print(f"  Empty descriptions: {empty_descriptions}")
    
    # Analyze options and history presence
    print(f"\n{'=' * 80}")
    print("HISTORY ANALYSIS")
    print(f"{'=' * 80}\n")
    
    events_with_history = 0
    events_without_history = 0
    total_options = 0
    options_with_history = 0
    total_history_points = 0
    history_lengths = []
    
    events_with_aggregations = 0
    
    for idx, row in metaculus_df.iterrows():
        try:
            options = json.loads(row["options_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        
        event_has_history = False
        for option in options:
            total_options += 1
            ts_list = option.get("ts", [])
            belief_list = option.get("belief", [])
            
            if ts_list and belief_list and len(ts_list) > 0 and len(belief_list) > 0:
                options_with_history += 1
                event_has_history = True
                history_length = len(ts_list)
                history_lengths.append(history_length)
                total_history_points += history_length
        
        if event_has_history:
            events_with_history += 1
        else:
            events_without_history += 1
    
    event_history_pct = (events_with_history / total_events) * 100 if total_events > 0 else 0
    option_history_pct = (options_with_history / total_options) * 100 if total_options > 0 else 0
    avg_history_length = (total_history_points / options_with_history) if options_with_history > 0 else 0
    avg_history_per_event = (total_history_points / total_events) if total_events > 0 else 0
    
    print(f"Events with at least one non-empty history: {events_with_history}/{total_events} ({event_history_pct:.1f}%)")
    print(f"Events with NO history: {events_without_history}/{total_events} ({(events_without_history/total_events)*100:.1f}%)")
    print(f"\nTotal options: {total_options}")
    print(f"Options with non-empty history: {options_with_history}/{total_options} ({option_history_pct:.1f}%)")
    print(f"Options with empty history: {total_options - options_with_history}/{total_options} ({((total_options - options_with_history)/total_options)*100:.1f}%)")
    print(f"\nTotal history points across all options: {total_history_points}")
    print(f"Average history length per option (with history): {avg_history_length:.1f} points")
    print(f"Average history points per event: {avg_history_per_event:.1f} points")
    
    if history_lengths:
        print(f"\nHistory length distribution:")
        print(f"  Min: {min(history_lengths)}")
        print(f"  Max: {max(history_lengths)}")
        print(f"  Median: {sorted(history_lengths)[len(history_lengths)//2]}")
    
    # Date range analysis
    print(f"\n{'=' * 80}")
    print("DATE RANGE COVERAGE")
    print(f"{'=' * 80}\n")
    
    end_times = pd.to_datetime(metaculus_df["end_time"], errors="coerce", utc=True)
    end_times_valid = end_times.dropna()
    
    if len(end_times_valid) > 0:
        print(f"Event end_time range:")
        print(f"  Earliest: {end_times_valid.min()}")
        print(f"  Latest: {end_times_valid.max()}")
        print(f"  Span: {(end_times_valid.max() - end_times_valid.min()).days} days")
    
    # Sample events without history
    print(f"\n{'=' * 80}")
    print("SAMPLE EVENTS WITHOUT HISTORY (first 5)")
    print(f"{'=' * 80}\n")
    
    no_history_events = []
    for idx, row in metaculus_df.iterrows():
        try:
            options = json.loads(row["options_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        
        has_history = any(
            len(opt.get("ts", [])) > 0 and len(opt.get("belief", [])) > 0
            for opt in options
        )
        
        if not has_history:
            no_history_events.append({
                "event_id": row["event_id"],
                "title": row["title"][:80],
                "status": row["status"],
                "num_options": len(options)
            })
            
            if len(no_history_events) >= 5:
                break
    
    for i, event in enumerate(no_history_events, 1):
        print(f"{i}. Event {event['event_id']}")
        print(f"   Title: {event['title']}")
        print(f"   Status: {event['status']}")
        print(f"   Options: {event['num_options']}")
        print()
    
    # Summary metrics
    metrics = {
        "total_events": int(total_events),
        "events_with_history": int(events_with_history),
        "events_with_history_pct": float(event_history_pct),
        "events_without_history": int(events_without_history),
        "total_options": int(total_options),
        "options_with_history": int(options_with_history),
        "option_history_pct": float(option_history_pct),
        "total_history_points": int(total_history_points),
        "avg_history_length": float(avg_history_length),
        "avg_history_per_event": float(avg_history_per_event),
        "description_completeness_pct": float(desc_completeness_pct),
        "status_distribution": {k: int(v) for k, v in status_counts.items()}
    }
    
    print(f"\n{'=' * 80}")
    print("SUCCESS CRITERIA CHECK")
    print(f"{'=' * 80}\n")
    
    print(f"Target: >90% non-empty histories")
    print(f"Actual: {option_history_pct:.1f}%")
    if option_history_pct >= 90:
        print("[PASS]")
    else:
        print(f"[FAIL] (shortfall: {90 - option_history_pct:.1f}%)")
    
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Analyze Metaculus data quality in a unified dataset")
    parser.add_argument("--dataset", type=str, required=True, help="Path to dataset directory (containing data.parquet)")
    args = parser.parse_args()
    
    metrics = analyze_metaculus_quality(args.dataset)
    
    # Save metrics to JSON
    output_path = Path(args.dataset) / "metaculus_quality_metrics.json"
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nMetrics saved to: {output_path}")


if __name__ == "__main__":
    main()


# --- LESSONS LEARNED ---
# 1. Jan 10-17 2026 Verification Results: Only 19.6% of Metaculus options had non-empty
#    histories (55/281 options), far below 90% target. 83.9% of events (156/186) had NO
#    history at all. This resulted in only 5.4% rig example generation rate vs 30% target.
# 2. Quality of Existing Histories: The 55 options that DID have history had excellent
#    quality - average 98.7 points per option, range 2-252, median 97. This suggests the
#    pipeline correctly processes history when present, but upstream API issue prevents
#    history from being fetched in the first place.
# 3. Root Cause: Diagnostic investigation revealed Metaculus API returned empty aggregation
#    history arrays during build time despite: (a) history points existing before build,
#    (b) ~50% falling within date window, (c) include_cp_history=true parameter used,
#    (d) manual API calls returning history correctly. Issue is upstream API behavior, not
#    pipeline logic.
# 4. Date Window Not the Issue: Initial hypothesis was date filtering removed points, but
#    diagnostics proved points existed within window. The empty histories come directly from
#    API response, not from filtering logic.
# 5. Description Completeness: 100% of Metaculus events had non-empty descriptions (186/186),
#    showing metadata mapping works correctly even when history is missing.
