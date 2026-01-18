#!/usr/bin/env python3
"""Check the final canonical database."""

import sqlite3

db_path = "data/clean/canonical_metaculus_verify_final.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM history WHERE source='metaculus'")
total_hist = cursor.fetchone()[0]
print(f"Total Metaculus history points in DB: {total_hist}")

cursor.execute("SELECT COUNT(DISTINCT market_id) FROM history WHERE source='metaculus'")
markets_with_hist = cursor.fetchone()[0]
print(f"Markets with history in DB: {markets_with_hist}")

cursor.execute("SELECT COUNT(*) FROM markets WHERE source='metaculus'")
total_markets = cursor.fetchone()[0]
print(f"Total Metaculus markets in DB: {total_markets}")

print(f"\nCoverage in DB: {markets_with_hist}/{total_markets} ({100*markets_with_hist/total_markets:.1f}%)")

# Check specific market
cursor.execute("SELECT COUNT(*) FROM history WHERE market_id='41039' AND source='metaculus'")
market_41039_hist = cursor.fetchone()[0]
print(f"\nMarket 41039 history points in DB: {market_41039_hist}")

conn.close()
