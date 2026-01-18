#!/usr/bin/env python3
"""Check if event 41339 is in the final database."""

import sqlite3

db_path = "data/clean/canonical_metaculus_verify_final.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM markets WHERE event_id='41339'")
count = cursor.fetchone()[0]
print(f"Event 41339 in DB: {count} markets")

if count > 0:
    cursor.execute("SELECT market_id, title FROM markets WHERE event_id='41339'")
    for row in cursor.fetchall():
        market_id, title = row
        print(f"  Market {market_id}: {title[:60] if title else 'N/A'}")
        
        # Check history for this market
        cursor.execute("SELECT COUNT(*) FROM history WHERE market_id=? AND source='metaculus'", (market_id,))
        hist_count = cursor.fetchone()[0]
        print(f"    History points: {hist_count}")
else:
    print("\nEvent 41339 NOT FOUND in database!")
    print("\nLet me check which events ARE in the database...")
    cursor.execute("SELECT DISTINCT event_id FROM markets WHERE source='metaculus' ORDER BY event_id DESC LIMIT 20")
    print("\nRecent event IDs in database:")
    for row in cursor.fetchall():
        print(f"  {row[0]}")

conn.close()
