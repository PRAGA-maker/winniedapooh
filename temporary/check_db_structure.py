#!/usr/bin/env python3
"""Check SQLite database structure and history data."""

import sqlite3
import json

import sys

if len(sys.argv) > 1:
    dataset_dir = sys.argv[1]
    db_path = f"{dataset_dir}/unified.db"
else:
    db_path = "data/datasets/v20260118_1454_meta_fix_test2_unified/unified.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# List tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall()]
print(f"Tables in database: {tables}\n")

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table}: {count} rows")

# Check metaculus table
if 'metaculus' in tables:
    print(f"\n{'='*80}")
    print("Checking metaculus table...")
    print(f"{'='*80}\n")
    
    cursor.execute("SELECT id, event_id, title FROM metaculus WHERE event_id = '41339'")
    row = cursor.fetchone()
    if row:
        print(f"Found event 41339:")
        print(f"  ID: {row[0]}")
        print(f"  Event ID: {row[1]}")
        print(f"  Title: {row[2][:60]}...")

# Check timeseries table
if 'timeseries' in tables:
    print(f"\n{'='*80}")
    print("Checking timeseries table...")
    print(f"{'='*80}\n")
    
    cursor.execute("SELECT COUNT(*) FROM timeseries WHERE market_id = '41039'")
    count = cursor.fetchone()[0]
    print(f"Timeseries points for market 41039: {count}")
    
    cursor.execute("SELECT COUNT(*) FROM timeseries WHERE source = 'metaculus'")
    meta_count = cursor.fetchone()[0]
    print(f"Total Metaculus timeseries points: {meta_count}")
    
    if count > 0:
        cursor.execute("SELECT * FROM timeseries WHERE market_id = '41039' LIMIT 3")
        print("\nSample timeseries points:")
        for row in cursor.fetchall():
            print(f"  {row}")

conn.close()
