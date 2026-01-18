#!/usr/bin/env python3
"""List event IDs in the database."""

import sqlite3

db_path = "data/clean/canonical_meta_fix_test2.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT DISTINCT event_id FROM markets WHERE source='metaculus' ORDER BY event_id")
event_ids = [row[0] for row in cursor.fetchall()]

print(f"Total Metaculus events in database: {len(event_ids)}")
print(f"\nEvent IDs (first 20):")
for eid in event_ids[:20]:
    print(f"  {eid}")

# Check if 41339 is in the list
if '41339' in event_ids:
    print("\nEvent 41339 IS in the database")
else:
    print("\nEvent 41339 is NOT in the database")

# Show the post IDs from the build output
expected_posts = [41593, 41588, 41577, 41568, 41567, 41563, 41562, 41561, 41548, 41544,
                  41543, 41540, 41538, 41537, 41527, 41526, 41524, 41523, 41521, 41520,
                  41519, 41518, 41517, 41516, 41514, 41513, 41510, 41509, 41508, 41507,
                  41505, 41502, 41499, 41497, 41495, 41490, 41489, 41487, 41485, 41481,
                  41480, 41478, 41477, 41472, 41468, 41462, 41458, 41454, 41451, 41440]

print(f"\nExpected posts from build log: {len(expected_posts)}")
print("Checking which expected posts are in database:")
in_db = [str(p) in event_ids for p in expected_posts]
print(f"  {sum(in_db)}/{len(expected_posts)} expected posts are in the database")

conn.close()
