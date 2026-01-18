#!/usr/bin/env python3
"""Check the raw parquet data."""

import pyarrow.parquet as pq
import json

parquet_path = "data/datasets/v20260118_1506_metaculus_verify_final_unified/data.parquet"

# Read parquet
table = pq.read_table(parquet_path)
df = table.to_pandas()

# Find event 41339
event_row = df[df['event_id'] == '41339']

if len(event_row) == 0:
    print("Event 41339 not found in parquet!")
else:
    print(f"Event 41339 found!")
    row = event_row.iloc[0]
    
    print(f"Title: {row['title'][:60]}")
    print(f"Status: {row['status']}")
    
    # Check options_json
    options = json.loads(row['options_json'])
    print(f"\nOptions: {len(options)}")
    
    for opt in options:
        opt_id = opt['option_id']
        ts_list = opt.get('ts', [])
        belief_list = opt.get('belief', [])
        print(f"  Option {opt_id}:")
        print(f"    ts length: {len(ts_list) if ts_list else 0}")
        print(f"    belief length: {len(belief_list) if belief_list else 0}")
        if ts_list and belief_list:
            print(f"    Sample: ts={ts_list[0]}, belief={belief_list[0]}")

print(f"\n{'='*80}")
print("Checking overall parquet structure...")
print(f"{'='*80}\n")

# Check schema
print(f"Parquet columns: {df.columns.tolist()}")
print(f"\nTotal events: {len(df)}")

# Check if there are any events with history
events_with_hist = 0
for _, row in df.iterrows():
    options = json.loads(row['options_json'])
    for opt in options:
        ts_list = opt.get('ts', [])
        belief_list = opt.get('belief', [])
        if ts_list and belief_list and len(ts_list) > 0 and len(belief_list) > 0:
            events_with_hist += 1
            break

print(f"Events with history in parquet: {events_with_hist}/{len(df)}")
