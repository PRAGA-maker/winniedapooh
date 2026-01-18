#!/usr/bin/env python3
"""Test that the timezone fix works correctly."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, date
from src.metaculus.map_to_canonical import map_metaculus_history_point

# Simulate a history point from Metaculus API
# This timestamp should be 2026-01-12 12:00:00 UTC
test_timestamp = 1736683200  # 2026-01-12 12:00:00 UTC

point = {
    "start_time": test_timestamp,
    "centers": [0.5]
}

# Map the point
ts_point = map_metaculus_history_point("test_q_id", point)

print(f"Timestamp: {test_timestamp}")
print(f"DateTime (UTC): {datetime.fromtimestamp(test_timestamp, tz=timezone.utc)}")
print(f"DateTime (local): {datetime.fromtimestamp(test_timestamp)}")
print(f"ts_point.ts: {ts_point.ts}")
print(f"ts_point.ts.date(): {ts_point.ts.date()}")
print(f"ts_point.ts.tzinfo: {ts_point.ts.tzinfo}")

# Test date filtering
start_date = date(2026, 1, 10)
end_date = date(2026, 1, 17)
point_date = ts_point.ts.date()

print(f"\nDate filtering test:")
print(f"start_date: {start_date}")
print(f"end_date: {end_date}")
print(f"point_date: {point_date}")
print(f"point_date >= start_date: {point_date >= start_date}")
print(f"point_date <= end_date: {point_date <= end_date}")
print(f"Would be INCLUDED: {point_date >= start_date and point_date <= end_date}")
