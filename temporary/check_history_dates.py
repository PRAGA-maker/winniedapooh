#!/usr/bin/env python3
"""Check the actual dates of history points for a post."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from src.metaculus.grabber import MetaculusGrabber

def check_history_dates(post_id: int):
    """Check history point dates for a post."""
    grabber = MetaculusGrabber()
    
    print(f"\n{'='*80}")
    print(f"CHECKING HISTORY DATES FOR POST {post_id}")
    print(f"{'='*80}\n")
    
    # Fetch post detail
    print(f"Fetching post detail...")
    detail = grabber.fetch_post_detail(post_id, use_cache=False)
    
    post_title = detail.get("title", "Unknown")[:80]
    print(f"Post: {post_title}")
    
    # Get questions
    questions = detail.get("question", {}).get("sub_questions", [])
    if not questions:
        questions = [detail.get("question", {})]
    
    print(f"Found {len(questions)} question(s)\n")
    
    for i, q in enumerate(questions, 1):
        q_id = q.get("id")
        q_type = q.get("type", "unknown")
        print(f"Question {i} (ID: {q_id}, type: {q_type}):")
        
        # Check aggregations
        if "aggregations" in q:
            aggs = q["aggregations"]
            for agg_key in ["recency_weighted", "unweighted", "weighted"]:
                if agg_key in aggs:
                    agg_block = aggs[agg_key]
                    history = agg_block.get("history", [])
                    print(f"  {agg_key}: {len(history)} history points")
                    
                    if history:
                        # Check date range
                        dates = []
                        for pt in history:
                            if "start_time" in pt:
                                start_time = pt["start_time"]
                                if isinstance(start_time, (int, float)):
                                    # Unix timestamp
                                    ts = datetime.fromtimestamp(start_time, tz=timezone.utc)
                                else:
                                    # ISO string
                                    ts = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
                                dates.append(ts)
                        
                        if dates:
                            dates.sort()
                            print(f"    Date range: {dates[0].date()} to {dates[-1].date()}")
                            
                            # Count points in Jan 10-17 window
                            start_date = datetime(2026, 1, 10, tzinfo=timezone.utc).date()
                            end_date = datetime(2026, 1, 17, tzinfo=timezone.utc).date()
                            in_window = sum(1 for d in dates if start_date <= d.date() <= end_date)
                            print(f"    Points in Jan 10-17 window: {in_window}/{len(dates)}")
                        break
        else:
            print("  No aggregations found")
        print()

if __name__ == "__main__":
    # Check several posts that showed no history
    test_posts = [41339, 41316, 41299, 41298, 41297]
    
    for post_id in test_posts[:3]:  # Check first 3
        try:
            check_history_dates(post_id)
        except Exception as e:
            print(f"Error checking post {post_id}: {e}\n")
