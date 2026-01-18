#!/usr/bin/env python
"""
Debug Metaculus fallback logic to understand why histories are still empty.
Tests the actual behavior with a known problematic post.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber
from src.metaculus.map_to_canonical import map_metaculus_history_point
import json

def test_metaculus_fallback():
    """Test the fallback logic with post 41339 (known to have history issues)."""
    print("Testing Metaculus Fallback Logic")
    print("=" * 80)
    
    grabber = MetaculusGrabber()
    
    # Test with post 41339 from the report
    post_id = 41339
    print(f"\nFetching post {post_id}...")
    
    try:
        detail = grabber.fetch_post_detail(post_id, use_cache=False)
        
        if not detail:
            print(f"[FAIL] Failed to fetch post {post_id}")
            return
        
        print(f"[OK] Fetched post {post_id}")
        print(f"  Title: {detail.get('title', 'N/A')[:80]}")
        print(f"  Forecasts count: {detail.get('forecasts_count', 0)}")
        
        # Extract questions
        sub_qs = []
        if "question" in detail and detail["question"]:
            sub_qs.append(detail["question"])
        if "group_questions" in detail and detail["group_questions"]:
            sub_qs.extend(detail["group_questions"])
        
        print(f"  Sub-questions found: {len(sub_qs)}")
        
        for i, q in enumerate(sub_qs):
            q_id = str(q["id"])
            q_type = q.get("type", "unknown")
            print(f"\n  Question {i+1}: ID={q_id}, Type={q_type}")
            
            # Simulate the pipeline logic
            points = []
            if "aggregations" in q:
                aggs = q["aggregations"]
                print(f"    [OK] Has aggregations: {list(aggs.keys())}")
                
                for agg_key in ["recency_weighted", "unweighted", "weighted"]:
                    if agg_key in aggs:
                        agg_block = aggs[agg_key]
                        history = agg_block.get("history", [])
                        latest = agg_block.get("latest")
                        
                        print(f"    - {agg_key}: history={len(history) if isinstance(history, list) else 'not a list'}, latest={'present' if latest else 'absent'}")
                        
                        points = history
                        if not points and latest:
                            points = [latest]
                        if points:
                            print(f"      => Using {len(points)} points from {agg_key}")
                            break
            else:
                print(f"    [FAIL] No aggregations key in question")
            
            # Check fallback trigger
            print(f"\n    Fallback check:")
            print(f"      not points: {not points}")
            print(f"      len(points): {len(points) if isinstance(points, list) else 'N/A'}")
            print(f"      forecasts_count: {detail.get('forecasts_count', 0)}")
            
            should_fallback = (not points or len(points) == 0) and detail.get('forecasts_count', 0) > 0
            print(f"      => Should trigger fallback: {should_fallback}")
            
            if should_fallback:
                print(f"\n    Testing download-data fallback...")
                try:
                    download_points = grabber.extract_aggregate_history_from_download(
                        post_id,
                        aggregation_priority=["recency_weighted", "unweighted"],
                    )
                    
                    if q_id in download_points:
                        fallback_count = len(download_points[q_id])
                        print(f"      [OK] Download-data returned {fallback_count} points for q_id={q_id}")
                        
                        # Show first few points
                        if fallback_count > 0:
                            sample = download_points[q_id][:3]
                            print(f"      Sample points:")
                            for pt in sample:
                                try:
                                    ts_pt = map_metaculus_history_point(q_id, pt)
                                    print(f"        - ts={ts_pt.ts}, belief={ts_pt.belief_scalar}")
                                except Exception as e:
                                    print(f"        - Error mapping point: {e}")
                    else:
                        print(f"      [FAIL] Download-data did not return data for q_id={q_id}")
                        print(f"      Available q_ids: {list(download_points.keys())}")
                        
                except Exception as e:
                    print(f"      [FAIL] Download-data failed: {e}")
            else:
                print(f"    => Fallback not triggered (already have {len(points) if isinstance(points, list) else 0} points)")
    
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_metaculus_fallback()
