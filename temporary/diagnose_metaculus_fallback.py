#!/usr/bin/env python
"""
Diagnose why the download-data fallback isn't being triggered for Metaculus posts.

Checks:
1. Do posts have aggregation blocks (even if empty)?
2. Do posts have forecasts_count > 0?
3. Does the download-data endpoint return data?
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber


def diagnose_fallback():
    """Check why fallback isn't working for a sample post."""
    grabber = MetaculusGrabber()
    
    # Test with post 41339 (which we know has empty history)
    post_id = 41339
    
    print(f"\n{'=' * 80}")
    print(f"DIAGNOSING POST {post_id}")
    print(f"{'=' * 80}\n")
    
    # 1. Fetch post detail
    print("Step 1: Fetching post detail...")
    try:
        detail = grabber.fetch_post_detail(post_id, use_cache=False)
        print(f"  Post ID: {detail.get('id')}")
        print(f"  Title: {detail.get('title', 'N/A')[:80]}")
        print(f"  forecasts_count: {detail.get('forecasts_count')}")
        print(f"  published_at: {detail.get('published_at')}")
        
        # Check questions
        sub_qs = []
        if "question" in detail and detail["question"]:
            sub_qs.append(detail["question"])
        if "group_questions" in detail and detail["group_questions"]:
            sub_qs.extend(detail["group_questions"])
        
        print(f"\n  Found {len(sub_qs)} question(s)")
        
        for i, q in enumerate(sub_qs, 1):
            q_id = q.get("id")
            print(f"\n  Question {i} (ID: {q_id}):")
            print(f"    Type: {q.get('type')}")
            poss = q.get('possibilities')
            if poss and isinstance(poss, dict):
                print(f"    Possibilities type: {poss.get('type')}")
            else:
                print(f"    Possibilities: {poss}")
            
            # Check aggregations
            if "aggregations" in q:
                aggs = q["aggregations"]
                print(f"    Aggregations present: {list(aggs.keys())}")
                
                for agg_key in ["recency_weighted", "unweighted", "weighted"]:
                    if agg_key in aggs:
                        agg_block = aggs[agg_key]
                        history = agg_block.get("history", [])
                        latest = agg_block.get("latest")
                        print(f"      {agg_key}:")
                        print(f"        history: {len(history)} points" if history else "        history: None or empty")
                        print(f"        latest: {'present' if latest else 'None'}")
            else:
                print(f"    Aggregations: NOT PRESENT")
        
        # 2. Test download-data fallback
        print(f"\n{'=' * 80}")
        print("Step 2: Testing download-data fallback")
        print(f"{'=' * 80}\n")
        
        forecasts_count = detail.get("forecasts_count") or 0
        print(f"  forecasts_count check: {forecasts_count}")
        
        if forecasts_count == 0:
            print("  [SKIP] forecasts_count is 0, fallback won't trigger")
            return
        
        print(f"  Calling extract_aggregate_history_from_download...")
        try:
            download_data = grabber.extract_aggregate_history_from_download(
                post_id,
                aggregation_priority=["recency_weighted", "unweighted"]
            )
            
            print(f"  Returned {len(download_data)} question(s) with history")
            for q_id, points in download_data.items():
                print(f"    Question {q_id}: {len(points)} history points")
                if points:
                    sample = points[0]
                    print(f"      Sample point: {sample}")
            
            if not download_data:
                print("  [ISSUE] Download-data returned empty dict!")
        except Exception as e:
            print(f"  [ERROR] Failed to fetch download-data: {e}")
            import traceback
            traceback.print_exc()
    
    except Exception as e:
        print(f"[ERROR] Failed to fetch post detail: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    diagnose_fallback()
