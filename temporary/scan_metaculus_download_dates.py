from __future__ import annotations

import csv
import io
from collections import Counter
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.common.http import get_metaculus_client
from src.metaculus.grabber import MetaculusGrabber


def extract_forecast_csv(content: bytes) -> str | None:
    marker = b"forecast_data.csvQuestion ID"
    marker_index = content.find(marker)
    if marker_index == -1:
        return None
    data_start = marker_index + len(b"forecast_data.csv")
    next_local = content.find(b"PK\x03\x04", data_start)
    next_central = content.find(b"PK\x01\x02", data_start)
    candidates = [pos for pos in [next_local, next_central] if pos != -1]
    data_end = min(candidates) if candidates else len(content)
    csv_bytes = content[data_start:data_end]
    csv_text = csv_bytes.decode("utf-8", errors="replace")
    if "Question ID" not in csv_text:
        return None
    return csv_text


def main() -> None:
    grabber = MetaculusGrabber()
    client = get_metaculus_client()
    posts = grabber.fetch_posts(limit=30)
    date_counts = Counter()
    scanned = 0

    for post in posts:
        if (post.get("forecasts_count") or 0) <= 0:
            continue
        post_id = post["id"]
        response = client.get(
            f"/api/posts/{post_id}/download-data/",
            params={"aggregation_methods": ["recency_weighted", "unweighted"]},
        )
        response.raise_for_status()
        csv_text = extract_forecast_csv(response.content)
        if not csv_text:
            continue
        scanned += 1
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            method = str(row.get("Forecaster Username") or "").strip().lower()
            if method not in {"recency_weighted", "unweighted"}:
                continue
            start_time = str(row.get("Start Time") or "").strip()
            if not start_time:
                continue
            try:
                ts = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            except ValueError:
                continue
            date_counts[ts.date().isoformat()] += 1

    print(f"Posts scanned with forecasts: {scanned}")
    if not date_counts:
        print("No forecast rows found.")
        return
    print("Top 10 dates by aggregate forecasts:")
    for day, count in date_counts.most_common(10):
        print(f"  {day}: {count}")


if __name__ == "__main__":
    main()
