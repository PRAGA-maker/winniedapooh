#!/usr/bin/env python3
"""Check history in parquet for specific events."""

import json
import pyarrow.parquet as pq

import sys

if len(sys.argv) > 1:
    dataset_dir = sys.argv[1]
    parquet_path = f"{dataset_dir}/data.parquet"
else:
    parquet_path = "data/datasets/v20260118_1454_meta_fix_test2_unified/data.parquet"

df = pq.read_table(parquet_path).to_pandas()

# Check event 41339
row = df[df['event_id'] == '41339'].iloc[0]
options = json.loads(row['options_json'])

print(f"\nEvent 41339: {row['title'][:60]}...")
print(f"  Options: {len(options)}")

for opt in options:
    option_id = opt['option_id']
    history = opt.get('history', [])
    print(f"  Option {option_id}:")
    print(f"    History points: {len(history)}")
    if history:
        print(f"    Sample: {history[0] if history else 'N/A'}")

# Check overall coverage
print(f"\n{'='*80}")
print("Overall Coverage Check:")
print(f"{'='*80}\n")

events_with_history = 0
total_events = len(df)
total_options = 0
options_with_history = 0

for _, row in df.iterrows():
    options = json.loads(row['options_json'])
    event_has_history = False
    
    for opt in options:
        total_options += 1
        history = opt.get('history', [])
        if history:
            options_with_history += 1
            event_has_history = True
    
    if event_has_history:
        events_with_history += 1

print(f"Events with history: {events_with_history}/{total_events} ({100*events_with_history/total_events:.1f}%)")
print(f"Options with history: {options_with_history}/{total_options} ({100*options_with_history/total_options:.1f}%)")
