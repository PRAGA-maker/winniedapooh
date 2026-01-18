#!/usr/bin/env python3
"""Check details of event 41339."""

import sqlite3

db_path = "data/clean/canonical_meta_fix_test2.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Find market(s) with event_id = 41339
cursor.execute("""
    SELECT source, market_id, event_id, title
    FROM markets
    WHERE event_id = '41339'
""")

print("Markets for event 41339:")
for row in cursor.fetchall():
    source, market_id, event_id, title = row
    print(f"  Source: {source}")
    print(f"  Market ID: {market_id}")
    print(f"  Event ID: {event_id}")
    print(f"  Title: {title[:80]}")
    
    # Check history
    cursor.execute("SELECT COUNT(*) FROM history WHERE market_id = ? AND source = ?", (market_id, source))
    hist_count = cursor.fetchone()[0]
    print(f"  History points: {hist_count}")
    print()

# Also check if there's a market with market_id = 41039 (the question ID from the API)
print(f"\n{'='*80}")
print("Checking for market_id = 41039...")
print(f"{'='*80}\n")

cursor.execute("""
    SELECT source, market_id, event_id, title
    FROM markets
    WHERE market_id = '41039'
""")

for row in cursor.fetchall():
    source, market_id, event_id, title = row
    print(f"  Source: {source}")
    print(f"  Market ID: {market_id}")
    print(f"  Event ID: {event_id}")
    print(f"  Title: {title[:80] if title else 'N/A'}")
    
    # Check history
    cursor.execute("SELECT COUNT(*) FROM history WHERE market_id = ? AND source = ?", (market_id, source))
    hist_count = cursor.fetchone()[0]
    print(f"  History points: {hist_count}")

conn.close()
