from __future__ import annotations

import sqlite3
from pathlib import Path


def main() -> None:
    db_path = Path("data/clean/canonical_metaculus_5day_check_v4.db")
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        markets = cur.execute(
            "SELECT COUNT(*) FROM markets WHERE source = 'metaculus'"
        ).fetchone()[0]
        history = cur.execute(
            "SELECT COUNT(*) FROM history WHERE source = 'metaculus'"
        ).fetchone()[0]
    print(f"Metaculus markets: {markets}")
    print(f"Metaculus history rows: {history}")


if __name__ == "__main__":
    main()
