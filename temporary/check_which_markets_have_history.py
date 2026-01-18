#!/usr/bin/env python3
"""Check which markets have history in the canonical database."""

import sqlite3

db_path = "data/clean/canonical_meta_fix_test2.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Count markets with history
cursor.execute("""
    SELECT m.market_id, m.event_id, COUNT(h.ts) as hist_count
    FROM markets m
    LEFT JOIN history h ON m.market_id = h.market_id AND m.source = h.source
    WHERE m.source = 'metaculus'
    GROUP BY m.market_id, m.event_id
    ORDER BY hist_count DESC
""")

print("Markets and their history counts:")
print(f"{'Market ID':<15} {'Event ID':<15} {'History Points'}")
print("="*50)

total_markets = 0
markets_with_history = 0

for row in cursor.fetchall():
    market_id, event_id, hist_count = row
    total_markets += 1
    if hist_count > 0:
        markets_with_history += 1
        event_id_str = str(event_id) if event_id else "N/A"
        print(f"{market_id:<15} {event_id_str:<15} {hist_count}")

print(f"\n{'='*50}")
print(f"Markets with history: {markets_with_history}/{total_markets} ({100*markets_with_history/total_markets:.1f}%)")

# Check event 41339 specifically
print(f"\n{'='*50}")
print("Checking event 41339 (which should have history)...")
print(f"{'='*50}\n")

cursor.execute("""
    SELECT m.market_id, m.event_id, COUNT(h.ts) as hist_count
    FROM markets m
    LEFT JOIN history h ON m.market_id = h.market_id AND m.source = h.source
    WHERE m.source = 'metaculus' AND m.event_id = '41339'
    GROUP BY m.market_id, m.event_id
""")

for row in cursor.fetchall():
    market_id, event_id, hist_count = row
    print(f"Market {market_id} (Event {event_id}): {hist_count} history points")

conn.close()
