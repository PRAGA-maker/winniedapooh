import sys
import os
import time
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.kalshi.grabber import KalshiGrabber
from src.kalshi.bulk_grabber import KalshiBulkGrabber
from src.kalshi.map_to_canonical import map_kalshi_market, map_kalshi_trade
from src.metaculus.grabber import MetaculusGrabber
from src.metaculus.map_to_canonical import map_metaculus_question, map_metaculus_history_point
from src.common.parquet import write_parquet_dataset
from src.common.logging import logger
from src.common.config import config

def process_kalshi_day(target_date: date, limit: Optional[int] = None):
    """Worker function to process a single day of Kalshi bulk data."""
    bulk_grabber = KalshiBulkGrabber()
    day_points = []
    records_count = 0
    
    try:
        for raw_record in bulk_grabber.fetch_daily_bulk_stream(target_date):
            ticker = raw_record["ticker_name"]
            
            # If we are in "Test Mode" (limit is small), we might want to filter.
            # But in full build, we process all tickers found in bulk.

            try:
                ts_point = bulk_grabber.map_to_timeseries(raw_record)
                
                # We'll create a minimal fallback record here if metadata is missing.
                # The DB layer will use INSERT OR IGNORE, so if we already have 
                # rich metadata from the API, this minimal one will be ignored.
                m_record = {
                    "source": "kalshi",
                    "market_id": ticker,
                    "title": ticker,
                    "description": "Bulk-only record",
                    "url": f"https://kalshi.com/markets/{ticker}",
                    "market_type": "binary",
                    "answer_options_json": json.dumps(["NO", "YES"]),
                    "end_time": datetime.fromisoformat(raw_record["date"]).isoformat(),
                    "status": "unknown",
                    "metadata_json": json.dumps(raw_record)
                }

                day_points.append({
                    "market_id": ticker, 
                    "point": ts_point.model_dump(mode='json'), 
                    "market_record": m_record
                })
                records_count += 1
            except Exception:
                continue
        
        return target_date, day_points, records_count
    except Exception as e:
        logger.error(f"Failed to process Kalshi day {target_date}: {e}")
        return target_date, [], 0

class CanonicalStore:
    """Handles persistent storage of canonical market records using SQLite to avoid memory bloat and allow resuming."""
    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            self.db_path = config.clean_data_dir / "canonical.db"
        else:
            self.db_path = db_path
            
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_conn() as conn:
            # High-performance pragmas for massive bulk ingestion
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=OFF") # Set to OFF for speed during initial build
            conn.execute("PRAGMA cache_size=-2000000") # 2GB cache
            conn.execute("PRAGMA temp_store=MEMORY")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS markets (
                    source TEXT,
                    market_id TEXT,
                    event_id TEXT,
                    title TEXT,
                    description TEXT,
                    url TEXT,
                    market_type TEXT,
                    answer_options_json TEXT,
                    end_time TEXT,
                    status TEXT,
                    resolved_value_json TEXT,
                    created_time TEXT,
                    metadata_json TEXT,
                    PRIMARY KEY (source, market_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    source TEXT,
                    market_id TEXT,
                    ts TEXT,
                    belief_scalar REAL,
                    belief_json TEXT,
                    bid REAL,
                    ask REAL,
                    volume REAL,
                    open_interest REAL,
                    raw_json TEXT,
                    PRIMARY KEY (source, market_id, ts)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    source TEXT,
                    key TEXT,
                    value TEXT,
                    PRIMARY KEY (source, key)
                )
            """)

    def exists(self, source: str, market_id: str) -> bool:
        with self._get_conn() as conn:
            res = conn.execute("SELECT 1 FROM markets WHERE source = ? AND market_id = ?", (source, market_id)).fetchone()
            return res is not None

    def get_existing_market_statuses(self, source: str) -> Dict[str, str]:
        """Return a dict of market_id -> status for a given source."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT market_id, status FROM markets WHERE source = ?", (source,)).fetchall()
            return {r[0]: r[1] for r in rows}

    def get_existing_market_ids(self, source: str) -> List[str]:
        """Return a list of all market_ids for a given source."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT market_id FROM markets WHERE source = ?", (source,)).fetchall()
            return [r[0] for r in rows]

    def save(self, source: str, market_id: str, market_record: Dict[str, Any], timeseries: List[Dict[str, Any]]):
        self.save_batch(source, {market_id: market_record}, {market_id: timeseries})

    def save_batch(self, source: str, market_records: Dict[str, Dict[str, Any]], timeseries_map: Dict[str, List[Dict[str, Any]]]):
        """Save multiple markets and their history points in a single transaction."""
        with self._get_conn() as conn:
            # 1. Save or update market metadata
            fields = ["source", "market_id", "event_id", "title", "description", "url", "market_type", 
                      "answer_options_json", "end_time", "status", "resolved_value_json", "created_time", "metadata_json"]
            
            market_rows = []
            for market_id, market_record in market_records.items():
                record = {f: market_record.get(f) for f in fields}
                record["source"] = source
                record["market_id"] = market_id
                market_rows.append(tuple(record[f] for f in fields))
            
            if market_rows:
                placeholders = ", ".join(["?"] * len(fields))
                conn.executemany(f"INSERT OR REPLACE INTO markets ({', '.join(fields)}) VALUES ({placeholders})", market_rows)
            
            # 2. Save history points
            h_fields = ["source", "market_id", "ts", "belief_scalar", "belief_json", "bid", "ask", "volume", "open_interest", "raw_json"]
            h_placeholders = ", ".join(["?"] * len(h_fields))
            
            history_rows = []
            for market_id, timeseries in timeseries_map.items():
                if timeseries:
                    for pt in timeseries:
                        pt_record = {f: pt.get(f) for f in h_fields}
                        pt_record["source"] = source
                        pt_record["market_id"] = market_id
                        history_rows.append(tuple(pt_record[f] for f in h_fields))
            
            if history_rows:
                conn.executemany(f"INSERT OR IGNORE INTO history ({', '.join(h_fields)}) VALUES ({h_placeholders})", history_rows)

    def add_history_points_batch(self, source: str, points_with_metadata: List[Dict[str, Any]]):
        """Append multiple history points in a single transaction."""
        with self._get_conn() as conn:
            # First, handle any new markets
            fields = ["source", "market_id", "event_id", "title", "description", "url", "market_type", 
                      "answer_options_json", "end_time", "status", "resolved_value_json", "created_time", "metadata_json"]
            
            # Collect unique markets from the points
            market_records = {}
            for item in points_with_metadata:
                market_id = item["market_id"]
                if "market_record" in item:
                    market_records[market_id] = item["market_record"]
            
            if market_records:
                market_rows = []
                for market_id, market_record in market_records.items():
                    # We only insert if it doesn't exist to avoid expensive REPLACE
                    record = {f: market_record.get(f) for f in fields}
                    record["source"] = source
                    record["market_id"] = market_id
                    market_rows.append(tuple(record[f] for f in fields))
                
                placeholders = ", ".join(["?"] * len(fields))
                conn.executemany(f"INSERT OR IGNORE INTO markets ({', '.join(fields)}) VALUES ({placeholders})", market_rows)
            
            # Then insert history points
            h_fields = ["source", "market_id", "ts", "belief_scalar", "belief_json", "bid", "ask", "volume", "open_interest", "raw_json"]
            h_placeholders = ", ".join(["?"] * len(h_fields))
            
            history_rows = []
            for item in points_with_metadata:
                point = item["point"]
                market_id = item["market_id"]
                pt_record = {f: point.get(f) for f in h_fields}
                pt_record["source"] = source
                pt_record["market_id"] = market_id
                history_rows.append(tuple(pt_record[f] for f in h_fields))
            
            if history_rows:
                conn.executemany(f"INSERT OR IGNORE INTO history ({', '.join(h_fields)}) VALUES ({h_placeholders})", history_rows)

    def add_history_point(self, source: str, market_id: str, point: Dict[str, Any], market_record_fallback: Optional[Dict[str, Any]] = None):
        """Append a single history point, creating the market if needed."""
        self.add_history_points_batch(source, [{"market_id": market_id, "point": point, "market_record": market_record_fallback}])

    def set_checkpoint(self, source: str, key: str, value: str):
        with self._get_conn() as conn:
            conn.execute("INSERT OR REPLACE INTO checkpoints (source, key, value) VALUES (?, ?, ?)", (source, key, value))

    def get_checkpoint(self, source: str, key: str) -> Optional[str]:
        with self._get_conn() as conn:
            res = conn.execute("SELECT value FROM checkpoints WHERE source = ? AND key = ?", (source, key)).fetchone()
            return res[0] if res else None

    def is_date_processed(self, source: str, target_date: date) -> bool:
        """Check if a specific date has been fully processed for a source."""
        key = f"processed_date_{target_date.isoformat()}"
        return self.get_checkpoint(source, key) == "done"

    def mark_date_processed(self, source: str, target_date: date):
        """Mark a specific date as fully processed."""
        key = f"processed_date_{target_date.isoformat()}"
        self.set_checkpoint(source, key, "done")

    def load_all_df(self) -> pd.DataFrame:
        """Return all stored canonical records in unified format as a Pandas DataFrame."""
        logger.info("Loading all canonical records from SQLite...")
        
        with self._get_conn() as conn:
            # Load all markets
            markets_df = pd.read_sql("SELECT * FROM markets", conn)
            
            # Load all history points sorted by source, market_id, ts
            logger.info("Fetching all history points...")
            # OPTIMIZATION: Only fetch columns we actually use in the final dataset
            # We skip raw_json and belief_json for now to save massive amounts of memory/time
            history_df = pd.read_sql("""
                SELECT source, market_id, ts, belief_scalar, volume, open_interest, bid, ask 
                FROM history 
                ORDER BY source, market_id, ts
            """, conn)
            
            if history_df.empty:
                return markets_df

            # OPTIMIZATION: Faster datetime conversion with specified format
            logger.info("Converting timestamps to datetime...")
            # Try to detect if it's ISO format which is fast
            history_df["ts"] = pd.to_datetime(history_df["ts"], format='ISO8601', errors='coerce')
            
            # Group by source and market_id to aggregate timeseries into lists
            logger.info("Aggregating timeseries into nested lists...")
            # Pandas aggregation is faster if we use a more direct approach
            # Grouping and then using 'list' is one of the slower paths.
            # But with fewer columns, it should be much better.
            history_agg = history_df.groupby(["source", "market_id"]).agg({
                "ts": list,
                "belief_scalar": list,
                "volume": list,
                "open_interest": list,
                "bid": list,
                "ask": list
            }).reset_index()
            
            # Rename for canonical consistency
            history_agg = history_agg.rename(columns={"belief_scalar": "belief"})
            
            # Merge with metadata
            logger.info("Merging metadata...")
            unified_df = pd.merge(markets_df, history_agg, on=["source", "market_id"], how="inner")
            
            # Final processing for pandas/parquet
            logger.info("Finalizing field types...")
            unified_df["end_time"] = pd.to_datetime(unified_df["end_time"], errors='coerce')
            unified_df["created_time"] = pd.to_datetime(unified_df["created_time"], errors='coerce')
            
            return unified_df

def build_unified_dataset(limit: Optional[int] = None, use_cache: bool = True, kalshi_ticker: Optional[str] = None, 
                          start_date: Optional[date] = None, end_date: Optional[date] = None,
                          metaculus_limit: Optional[int] = None):
    logger.info(f"Starting unified dataset build (limit={limit}, metaculus_limit={metaculus_limit})...")
    
    store = CanonicalStore()
    api_calls_count = 0
    batch_size = 17 # Kalshi rate limit is 20, we use 17 to stay safe
    
    # Use number of cores minus 2, as requested by the user
    num_workers = max(1, (os.cpu_count() or 16) - 2)
    logger.info(f"Using {num_workers} parallel workers for processing.")

    # 1. Kalshi Pipeline (Optimized)
    kalshi_grabber = KalshiGrabber()
    kalshi_bulk = KalshiBulkGrabber()
    existing_statuses = store.get_existing_market_statuses("kalshi")
    
    if kalshi_ticker:
        # Single ticker mode (mostly for debugging)
        k_markets = kalshi_grabber.fetch_markets(limit=1, ticker=kalshi_ticker, use_cache=use_cache)
        metadata_batch = {m["ticker"]: map_kalshi_market(m).model_dump(mode='json') for m in k_markets}
        store.save_batch("kalshi", metadata_batch, {})
    elif start_date and end_date:
        # STEP A: Discovery from S3
        discovered_tickers = kalshi_bulk.scan_all_tickers(start_date, end_date, workers=num_workers)
        
        # STEP B: Filter missing metadata
        missing_tickers = [t for t in discovered_tickers if t not in existing_statuses or not existing_statuses[t]]
        if limit: missing_tickers = missing_tickers[:limit]
        
        # STEP C: Batch Enrich Metadata from API
        if missing_tickers:
            logger.info(f"Enriching metadata for {len(missing_tickers)} missing tickers in batches...")
            new_markets = kalshi_grabber.fetch_markets(tickers=missing_tickers)
            metadata_batch = {}
            for m in new_markets:
                try:
                    record = map_kalshi_market(m)
                    metadata_batch[m["ticker"]] = record.model_dump(mode='json')
                except Exception: continue
            
            if metadata_batch:
                store.save_batch("kalshi", metadata_batch, {})
                logger.info(f"Saved metadata for {len(metadata_batch)} new Kalshi markets.")
        
        # STEP D: History Ingestion (Continue as before but with more workers)
        current_date = start_date
        dates_to_process = []
        while current_date <= end_date:
            if use_cache and store.is_date_processed("kalshi", current_date):
                pass # Already done
            else:
                dates_to_process.append(current_date)
            current_date += timedelta(days=1)
        
        if dates_to_process:
            logger.info(f"Processing {len(dates_to_process)} days of Kalshi history in parallel...")
            processed_days = 0
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = {executor.submit(process_kalshi_day, d, limit): d for d in dates_to_process}
                for future in as_completed(futures):
                    target_date, day_points, records_count = future.result()
                    if day_points:
                        store.add_history_points_batch("kalshi", day_points)
                    store.mark_date_processed("kalshi", target_date)
                    processed_days += 1
                    logger.info(f"  [{processed_days}/{len(dates_to_process)}] Finished Kalshi Day {target_date}: {records_count} records.")
    else:
        # Fallback to old behavior if no dates provided (unlikely given user query)
        logger.warning("No dates provided, skipping Kalshi history collection.")

    # 2. Fetch Metaculus Data (Sequential due to rate limits)
    # Use the specific limit for Metaculus if provided, else use the general limit
    # If both are None, we use a large default (100,000) for Metaculus
    actual_metaculus_limit = metaculus_limit if metaculus_limit is not None else (limit if limit is not None else 100000)
    
    if actual_metaculus_limit == 0:
        logger.info("Metaculus limit is 0, skipping Metaculus collection.")
    else:
        metaculus_grabber = MetaculusGrabber()
        
        # We start from offset 0 but use smart status-aware skipping to "move on"
        posts = metaculus_grabber.fetch_posts(limit=actual_metaculus_limit, use_cache=use_cache)
        
        # Pre-calculate window boundaries for filtering
        window_start = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=None) if start_date else None
        window_end = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=None) if end_date else None

        # Cache existing market statuses to determine if we can skip enrichment
        # If a market is already 'resolved' or 'closed', its history is final.
        existing_statuses = store.get_existing_market_statuses("metaculus") if use_cache else {}

        metaculus_market_records = {}
        metaculus_timeseries_map = {}

        for p in posts:
            p_id = str(p["id"])
            
            # OPTIMIZATION: If we hit a post created long before our window start, 
            # and no window was specified, we could stop.
            # But Metaculus IDs are roughly chronological, so we can use that.
            p_created = datetime.fromisoformat(p["created_time"].replace('Z', '+00:00')).replace(tzinfo=None)
            if window_start and p_created < window_start - timedelta(days=365):
                # If the post is 1 year older than our window start, it's very unlikely 
                # to have history points inside our window. We stop crawling.
                logger.info(f"Metaculus post {p_id} created on {p_created} is too old for window. Stopping crawl.")
                break

            try:
                # Check if all sub-questions in this post are already finalized in DB

                detail = p
                needs_detail = False
                if "question" in detail and detail["question"]:
                    q = detail["question"]
                    if "aggregations" not in q or "recency_weighted" not in q.get("aggregations", {}):
                        needs_detail = True
                
                if needs_detail:
                    detail = metaculus_grabber.fetch_post_detail(p["id"], use_cache=use_cache)
                    api_calls_count += 1
                
                if not detail:
                    continue
                    
                sub_qs = []
                if "question" in detail and detail["question"]:
                    sub_qs.append(detail["question"])
                if "group_questions" in detail and detail["group_questions"]:
                    sub_qs.extend(detail["group_questions"])
                if "conditional" in detail and detail["conditional"]:
                    cond = detail["conditional"]
                    for sub_name in ["condition", "condition_child", "question_yes", "question_no"]:
                        sub_q = cond.get(sub_name)
                        if sub_q and isinstance(sub_q, dict) and "id" in sub_q:
                            sub_qs.append(sub_q)
                
                for q in sub_qs:
                    if not q or not isinstance(q, dict) or "id" not in q:
                        continue
                    q_id = str(q["id"])
                    
                    # Double check finalized status skip
                    if use_cache and existing_statuses.get(q_id) in ["resolved", "closed"]:
                        continue

                    record = map_metaculus_question(detail, q)
                    points = []
                    if "aggregations" in q:
                        aggs = q["aggregations"]
                        for agg_key in ["recency_weighted", "unweighted", "weighted"]:
                            if agg_key in aggs:
                                points = aggs[agg_key].get("history", [])
                                if points:
                                    break
                    
                    ts_points = []
                    for pt in points:
                        ts_point = map_metaculus_history_point(q_id, pt)
                        # Filter by window if provided
                        if window_start and ts_point.ts.replace(tzinfo=None) < window_start:
                            continue
                        if window_end and ts_point.ts.replace(tzinfo=None) > window_end:
                            continue
                        ts_points.append(ts_point.model_dump(mode='json'))
                    
                    # Only save if we have points in the window, or if no window was specified
                    if ts_points or not (start_date and end_date):
                        metaculus_market_records[q_id] = record.model_dump(mode='json')
                        metaculus_timeseries_map[q_id] = ts_points
                        logger.debug(f"Prepared Metaculus question {q_id}")
                
                logger.info(f"Processed Metaculus post {p_id} ({len(sub_qs)} questions)")
                
                # Periodically batch save Metaculus data
                if len(metaculus_market_records) >= 100:
                    logger.info(f"Batch saving {len(metaculus_market_records)} Metaculus markets...")
                    store.save_batch("metaculus", metaculus_market_records, metaculus_timeseries_map)
                    metaculus_market_records = {}
                    metaculus_timeseries_map = {}

                if api_calls_count > 0 and api_calls_count % batch_size == 0:
                    logger.info(f"Progress: {api_calls_count} API-like calls completed.")
                    
            except Exception as e:
                logger.error(f"Error processing Metaculus post {p_id}: {e}")

        # Final batch save for Metaculus
        if metaculus_market_records:
            logger.info(f"Final batch saving {len(metaculus_market_records)} Metaculus markets...")
            store.save_batch("metaculus", metaculus_market_records, metaculus_timeseries_map)

    # 4. Build Final Unified Dataset
    unified_df = store.load_all_df()
    if unified_df.empty:
        logger.error("No data collected. Aborting.")
        return

    version = datetime.now().strftime("%Y%m%d_%H%M")
    write_parquet_dataset(unified_df, "unified", version=version)
    
    logger.info("Unified dataset build complete.")

def build_db_cli():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="General limit for markets per source")
    parser.add_argument("--metaculus-limit", type=int, default=None, help="Specific limit for Metaculus (overrides --limit)")
    parser.add_argument("--no-cache", action="store_false", dest="use_cache", default=True)
    parser.add_argument("--kalshi-ticker", type=str, default=None)
    parser.add_argument("--start", type=str, default=None, help="Kalshi bulk start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="Kalshi bulk end date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    
    build_unified_dataset(
        limit=args.limit, 
        metaculus_limit=args.metaculus_limit,
        use_cache=args.use_cache, 
        kalshi_ticker=args.kalshi_ticker,
        start_date=start_date,
        end_date=end_date
    )

if __name__ == "__main__":
    build_db_cli()

# --- LESSONS LEARNED ---
# 1. Scaling: Building a DB takes time due to strict rate limits (Metaculus).
# 2. Sub-questions: A single Metaculus post can generate multiple canonical rows.
# 3. Join Logic: Linking static market info with time-series lists in a single 
#    row works well for runner ergonomics but requires strict ts sorting.
# 4. Parallelization: Using ProcessPoolExecutor for Kalshi bulk data provides
#    a massive speedup (on the order of 10-20x) by parallelizing S3 downloads 
#    and JSON parsing.
# 5. SQLite Performance: Enabling WAL mode and using `executemany` for batch 
#    inserts is critical when processing millions of records. Avoid one-by-one 
#    inserts which are thousands of times slower due to transaction overhead.
# 6. Memory Management: For large datasets, use generators for streaming data
#    and avoid keeping millions of raw JSON objects in memory. In Pandas,
#    prefer bulk aggregation over per-row list building.
# 7. Worker Allocation: Using `cpu_count() - 4` workers ensures maximum 
#    ingestion speed while keeping the system responsive for other tasks.
# 8. Status-Aware Enrichment: By checking if a market is 'resolved' or 'closed' 
#    in the database, we can safely skip redundant Metaculus API calls while 
#    ensuring that 'open' markets always receive fresh history points. This 
#    removes the need for manual offsets and prevents "dark pools" of stale data.
# 9. Granular Progress Tracking: Tracking Kalshi processed dates individually 
#    ensures that gaps in the historical timeline are identified and filled, 
#    even if processing was interrupted or done in non-contiguous chunks.
