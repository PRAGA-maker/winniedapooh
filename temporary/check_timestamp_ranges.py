#!/usr/bin/env python
"""Check the actual timestamp ranges of Metaculus aggregation history."""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber


def check_timestamps():
    grabber = MetaculusGrabber()
    
    # Check post 41339
    post_id = 41339
    print(f"Fetching post {post_id}...")
    
    detail = grabber.fetch_post_detail(post_id, use_cache=False)
    q = detail.get("question")
    
    if not q or "aggregations" not in q:
        print("No question or aggregations found")
        return
    
    agg = q["aggregations"].get("recency_weighted", {})
    hist = agg.get("history", [])
    
    print(f"\nPost {post_id} aggregation history:")
    print(f"  Total points: {len(hist)}")
    
    if not hist:
        print("  No history points!")
        return
    
    # Check first point structure
    print(f"\n  First point keys: {list(hist[0].keys())}")
    print(f"  First point sample: {hist[0]}")
    
    # Parse timestamps
    timestamps = []
    for p in hist:
        ts_val = p.get("start_time")
        if ts_val:
            try:
                if isinstance(ts_val, (int, float)):
                    dt = datetime.fromtimestamp(ts_val)
                else:
                    dt = datetime.fromisoformat(str(ts_val).replace("Z", "+00:00"))
                timestamps.append(dt)
            except Exception as e:
                print(f"  Failed to parse timestamp: {ts_val}, error: {e}")
    
    if timestamps:
        timestamps.sort()
        print(f"\n  Timestamp range:")
        print(f"    Earliest: {timestamps[0]}")
        print(f"    Latest: {timestamps[-1]}")
        print(f"    Span: {(timestamps[-1] - timestamps[0]).days} days")
        
        # Check against build window
        build_start = datetime(2026, 1, 10).date()
        build_end = datetime(2026, 1, 17).date()
        
        print(f"\n  Build window: {build_start} to {build_end}")
        
        in_window = [ts for ts in timestamps if build_start <= ts.date() <= build_end]
        print(f"  Points in window: {len(in_window)}/{len(timestamps)}")
        
        if in_window:
            print(f"    In-window timestamps:")
            for ts in in_window[:5]:
                print(f"      {ts}")
        else:
            print(f"    [ISSUE] ALL timestamps are OUTSIDE the build window!")
            print(f"    First few timestamps:")
            for ts in timestamps[:5]:
                print(f"      {ts}")


if __name__ == "__main__":
    check_timestamps()
