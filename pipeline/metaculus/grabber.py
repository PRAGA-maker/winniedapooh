import csv
import io
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from pipeline.common.http import get_metaculus_client
from pipeline.common.config import config
from pipeline.common.logging import logger

class MetaculusGrabber:
    def __init__(self):
        self.client = get_metaculus_client()

    def fetch_posts(self, limit: int = 1000, offset: int = 0, status: str = None, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch list of posts (which contain questions)."""
        logger.info(f"Fetching Metaculus posts (limit={limit}, offset={offset}, status={status})...")
        posts = []
        current_offset = offset
        
        while len(posts) < limit:
            params = {
                "limit": min(100, limit - len(posts)),
                "offset": current_offset,
                "include_cp_history": "true",
                "include_descriptions": "true",
            }
            if status:
                params["status"] = status
            
            # Using /api/posts/ as it seems more robust for detail
            response = self.client.get("/api/posts/", params=params)
            data = response.json()
            
            batch = data.get("results", [])
            posts.extend(batch)
            
            if not data.get("next") or not batch:
                break
            current_offset += len(batch)
                
        return posts[:limit]

    def fetch_post_detail(self, post_id: int, use_cache: bool = True) -> Dict[str, Any]:
        """Fetch detailed info for a single post with history."""
        logger.info(f"Fetching Metaculus post details for {post_id}...")
        params = {"include_cp_history": "true"}
        response = self.client.get(f"/api/posts/{post_id}/", params=params)
        data = response.json()
        return data

    def fetch_prediction_history(self, q_id: int, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch prediction history for a single question."""
        logger.info(f"Fetching Metaculus prediction history for {q_id}...")
        response = self.client.get(f"/api2/questions/{q_id}/prediction-history/")
        data = response.json()
        return data

    def fetch_post_download_data(self, post_id: int, aggregation_methods: Optional[List[str]] = None) -> bytes:
        """Fetch a post's download-data zip as raw bytes."""
        params = {}
        if aggregation_methods:
            params["aggregation_methods"] = aggregation_methods
        response = self.client.get(f"/api/posts/{post_id}/download-data/", params=params)
        response.raise_for_status()
        return response.content

    @staticmethod
    def _extract_csv_section(content: bytes, marker: bytes) -> Optional[str]:
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

    def extract_aggregate_history_from_download(
        self,
        post_id: int,
        aggregation_priority: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Extract per-question aggregate history from a post's download-data zip."""
        priority = aggregation_priority or ["recency_weighted", "unweighted"]
        try:
            content = self.fetch_post_download_data(post_id, aggregation_methods=priority)
        except Exception as exc:
            logger.warning(f"Metaculus download-data failed for post {post_id}: {exc}")
            return {}

        csv_text = self._extract_csv_section(
            content,
            marker=b"forecast_data.csvQuestion ID",
        )
        if not csv_text:
            logger.warning(f"Metaculus download-data missing forecast_data.csv for post {post_id}")
            return {}

        per_question = {}
        method_buckets: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
            method: {} for method in priority
        }

        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            q_id = str(row.get("Question ID") or "").strip()
            if not q_id:
                continue
            method = str(row.get("Forecaster Username") or "").strip().lower()
            if method not in method_buckets:
                continue
            start_time = str(row.get("Start Time") or "").strip()
            prob_yes = row.get("Probability Yes")
            if not start_time or prob_yes in (None, ""):
                continue
            try:
                ts = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            except ValueError:
                continue
            try:
                prob_val = float(prob_yes)
            except (TypeError, ValueError):
                continue
            point = {"start_time": ts.timestamp(), "centers": [prob_val]}
            method_buckets[method].setdefault(q_id, []).append(point)

        for q_id in {qid for bucket in method_buckets.values() for qid in bucket.keys()}:
            for method in priority:
                points = method_buckets[method].get(q_id)
                if points:
                    points.sort(key=lambda p: p["start_time"])
                    per_question[q_id] = points
                    break

        return per_question

# --- NOTES ---
# Key Rotation: This grabber uses get_metaculus_client() which automatically handles API key rotation.
# Multiple keys can be configured via METACULUS_TOKEN_1, METACULUS_TOKEN_2, etc. in .env.
# The rotation system (in http.py) automatically switches keys on rate limits (429) and preemptively
# rotates at 90% usage threshold. With 2 keys, expect ~2x speedup (99.7% efficiency).
# See http.py LESSONS LEARNED section for detailed implementation notes.
#
# METACULUS API NOTES:
# 1. Rate Limiting: Metaculus endpoints are rate-limited and can fail after retries. 
#    If you only need Kalshi for a build, set `--metaculus-limit 0` to skip Metaculus 
#    collection and still produce a valid unified dataset.
# 2. Failure Handling: If Metaculus requests fail after retries during a build, 
#    the pipeline logs a warning and automatically skips Metaculus while continuing 
#    with Kalshi-only export.
# 3. History Persistence: Date-window builds can yield Metaculus markets with empty 
#    histories; the pipeline still persists metadata so source coverage is visible 
#    even when no in-window points exist.
# 4. Window Filtering: Window filtering is day-based (matching Kalshi): if 
#    --start 2025-01-01 --end 2025-01-05, all Metaculus aggregation points with 
#    dates in [2025-01-01, 2025-01-05] are included, regardless of exact timestamp 
#    within those days. This ensures consistent edge case handling across sources.
# 5. Aggregation Fallback: If post aggregations omit history, fallback to the
#    download-data endpoint and parse aggregate forecast rows (recency_weighted,
#    unweighted) for time series.
# 6. History Omission: Post aggregations may omit history even with 
#    include_cp_history; the pipeline falls back to the download-data endpoint 
#    and parses aggregate rows (recency_weighted, unweighted).
# 7. Date-Window Metadata: Date-window builds can yield Metaculus markets with 
#    empty histories; metadata is still persisted to ensure source coverage 
#    visibility even when no in-window points exist.
# 8. Empty History Investigation (2026-01-18): Build for Jan 10-17 2026 window showed 80.4%
#    of Metaculus options with empty histories despite include_cp_history=true. Diagnostic
#    tests revealed: (1) API correctly returns history when called manually (26 points for
#    post 41339), (2) All history points existed BEFORE build time, (3) 50% of points fell
#    within the date window, (4) But stored metadata shows empty [] history arrays. This
#    suggests either: (a) Metaculus API had temporary issue during build time window and
#    didn't return history, (b) Specific question types (date/continuous vs binary/multiple
#    choice) require different API parameters or endpoints, or (c) There's a race condition
#    or timing issue with recently-published posts. The download-data fallback was never
#    triggered because aggregation blocks were present (but empty), so `if not points:`
#    evaluated to False. Future investigation needed on question type handling and
#    robustness improvements for API inconsistencies.

