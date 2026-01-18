#!/usr/bin/env python3
"""Check the canonical database for history data."""

import sqlite3

db_path = "data/clean/canonical_meta_fix_test2.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# List tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall()]
print(f"Tables: {tables}\n")

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table}: {count} rows")

# Check history for market 41039
print(f"\n{'='*80}")
print("Checking history for market 41039...")
print(f"{'='*80}\n")

cursor.execute("SELECT COUNT(*) FROM history WHERE market_id = '41039'")
count = cursor.fetchone()[0]
print(f"History points for market 41039: {count}")

cursor.execute("SELECT COUNT(*) FROM history WHERE source = 'metaculus'")
meta_count = cursor.fetchone()[0]
print(f"Total Metaculus history points: {meta_count}")

if count > 0:
    cursor.execute("SELECT ts, belief_scalar FROM history WHERE market_id = '41039' ORDER BY ts LIMIT 5")
    print("\nSample history points:")
    for row in cursor.fetchall():
        print(f"  {row}")

conn.close()
