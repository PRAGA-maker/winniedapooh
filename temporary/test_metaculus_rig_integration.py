#!/usr/bin/env python
"""
Test Metaculus data compatibility with the forecasting rig.

Validates that Metaculus events can generate training examples using ResolveEventTask.

Usage:
    uv run python temporary/test_metaculus_rig_integration.py --dataset <path_to_dataset_dir>
"""

import argparse
import sys
import random
from pathlib import Path
from collections import Counter

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataobject.dataset import EventDataset
from dataobject.tasks.resolve_binary import ResolveEventTask


def test_rig_integration(dataset_path: str, relax_status: bool = True, min_history_points: int = 5):
    """Test how many Metaculus events can generate rig examples."""
    
    print(f"Loading dataset from {dataset_path}...")
    dataset = EventDataset.load(dataset_path)
    
    # Filter to Metaculus only
    metaculus_df = dataset.df[dataset.df["source"] == "metaculus"].copy()
    total_metaculus = len(metaculus_df)
    
    print(f"\n{'=' * 80}")
    print(f"METACULUS RIG INTEGRATION TEST")
    print(f"{'=' * 80}\n")
    print(f"Total Metaculus events: {total_metaculus}")
    print(f"Task: ResolveEventTask(relax_status={relax_status}, min_history_points={min_history_points})")
    print()
    
    if total_metaculus == 0:
        print("No Metaculus events found in dataset!")
        return
    
    # Create task
    task = ResolveEventTask(relax_status=relax_status, min_history_points=min_history_points)
    rng = random.Random(42)
    
    # Test example generation
    success_count = 0
    failure_reasons = Counter()
    events_tested = 0
    
    print("Testing example generation...")
    for idx, row in metaculus_df.iterrows():
        events_tested += 1
        from dataobject.dataset import EventRecordWrapper
        record = EventRecordWrapper(row.to_dict())
        
        try:
            examples = task.make_examples(record, rng)
            if examples:
                success_count += 1
            else:
                # Try to determine why it failed
                if not record.options:
                    failure_reasons["no_options"] += 1
                elif record.status not in ["resolved", "closed"]:
                    failure_reasons["wrong_status"] += 1
                else:
                    # Check history
                    has_history = False
                    min_history_len = float('inf')
                    for option in record.options:
                        ts_list = option.get("ts") or []
                        belief_list = option.get("belief") or []
                        if ts_list and belief_list:
                            has_history = True
                            min_history_len = min(min_history_len, len(ts_list))
                    
                    if not has_history:
                        failure_reasons["empty_history"] += 1
                    elif min_history_len < min_history_points:
                        failure_reasons[f"history_too_short_(<{min_history_points})"] += 1
                    else:
                        failure_reasons["other"] += 1
        except Exception as e:
            failure_reasons[f"exception_{type(e).__name__}"] += 1
    
    success_rate = (success_count / events_tested) * 100 if events_tested > 0 else 0
    failure_count = events_tested - success_count
    
    print(f"\n{'=' * 80}")
    print("RESULTS")
    print(f"{'=' * 80}\n")
    print(f"Events tested: {events_tested}")
    print(f"Successfully generated examples: {success_count} ({success_rate:.1f}%)")
    print(f"Failed to generate examples: {failure_count} ({(failure_count/events_tested)*100:.1f}%)")
    
    if failure_reasons:
        print(f"\nFailure reason breakdown:")
        for reason, count in failure_reasons.most_common():
            pct = (count / failure_count) * 100 if failure_count > 0 else 0
            print(f"  {reason}: {count} ({pct:.1f}%)")
    
    print(f"\n{'=' * 80}")
    print("SUCCESS CRITERIA CHECK")
    print(f"{'=' * 80}\n")
    
    target_rate = 30.0
    print(f"Target: >={target_rate}% example generation rate")
    print(f"Actual: {success_rate:.1f}%")
    
    if success_rate >= target_rate:
        print("[PASS]")
    else:
        print(f"[FAIL] (shortfall: {target_rate - success_rate:.1f}%)")
    
    # Additional insights
    print(f"\n{'=' * 80}")
    print("INSIGHTS")
    print(f"{'=' * 80}\n")
    
    if "empty_history" in failure_reasons:
        empty_history_pct = (failure_reasons["empty_history"] / failure_count) * 100 if failure_count > 0 else 0
        print(f"[WARNING] {failure_reasons['empty_history']} events ({empty_history_pct:.1f}% of failures) have EMPTY histories")
        print(f"  -> This suggests the download-data fallback is not populating histories correctly")
    
    if any("history_too_short" in reason for reason in failure_reasons):
        short_history_count = sum(count for reason, count in failure_reasons.items() if "history_too_short" in reason)
        short_history_pct = (short_history_count / failure_count) * 100 if failure_count > 0 else 0
        print(f"[WARNING] {short_history_count} events ({short_history_pct:.1f}% of failures) have histories < {min_history_points} points")
        print(f"  -> Consider lowering min_history_points or checking date window filtering")
    
    if "wrong_status" in failure_reasons:
        wrong_status_pct = (failure_reasons["wrong_status"] / failure_count) * 100 if failure_count > 0 else 0
        print(f"[WARNING] {failure_reasons['wrong_status']} events ({wrong_status_pct:.1f}% of failures) have wrong status")
        if not relax_status:
            print(f"  -> Try using relax_status=True to include 'closed' events")
    
    return {
        "total_events": events_tested,
        "success_count": success_count,
        "success_rate": success_rate,
        "failure_count": failure_count,
        "failure_reasons": dict(failure_reasons),
        "target_rate": target_rate,
        "passed": success_rate >= target_rate
    }


def main():
    parser = argparse.ArgumentParser(description="Test Metaculus rig integration")
    parser.add_argument("--dataset", type=str, required=True, help="Path to dataset directory")
    parser.add_argument("--relax-status", action="store_true", default=True, help="Allow closed events (default: True)")
    parser.add_argument("--min-history", type=int, default=5, help="Minimum history points required (default: 5)")
    args = parser.parse_args()
    
    test_rig_integration(args.dataset, relax_status=args.relax_status, min_history_points=args.min_history)


if __name__ == "__main__":
    main()


# --- LESSONS LEARNED ---
# 1. Empty History Issue: Initial testing on Jan 10-17, 2026 window showed ~80% of Metaculus 
#    options had empty histories despite download-data fallback implementation. Root cause TBD.
# 2. Rig Compatibility: ResolveEventTask requires min_history_points=5 by default. With empty 
#    histories, most Metaculus events fail to generate examples even with relax_status=True.
# 3. Investigation Needed: Need to verify download-data fallback is actually being called and 
#    parsing CSV correctly. Check build logs for fallback trigger messages.
