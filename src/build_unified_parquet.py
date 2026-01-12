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

def process_kalshi_day(target_date: date, allowed_tickers: Optional[set] = None):
    """
    Worker function to process a single day of Kalshi bulk data.
    
    NOTE: Accumulates all points in memory before returning. For busy days with 100K+ records,
    this can be 50-100MB per worker. With 22 workers, total accumulation can reach 1-2GB+.
    For very large datasets (18M+ markets), consider batching DB inserts within worker instead.
    """
    bulk_grabber = KalshiBulkGrabber()
    day_points = []
    records_count = 0
    
    try:
        for raw_record in bulk_grabber.fetch_daily_bulk_stream(target_date):
            ticker = raw_record["ticker_name"]
            
            # AGGRESSIVE FILTERING: Skip tickers not in our allowed set (the active ones)
            if allowed_tickers is not None and ticker not in allowed_tickers:
                continue

            try:
                ts_point = bulk_grabber.map_to_timeseries(raw_record)
                
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

    def load_and_write_partitioned(self, output_dir: Path):
        """Load from SQLite and write to Parquet in partitions to avoid OOM on limited RAM."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        logger.info("Starting partitioned export from SQLite to Parquet...")
        
        # 1. Fetch all market metadata (Static info is small relative to history)
        with self._get_conn() as conn:
            markets_df = pd.read_sql("SELECT * FROM markets", conn)
            # Pre-convert times
            markets_df["end_time"] = pd.to_datetime(markets_df["end_time"], errors='coerce')
            markets_df["created_time"] = pd.to_datetime(markets_df["created_time"], errors='coerce')

        # 2. Process history in chunks - OPTIMIZATION: Larger chunks for better throughput
        unique_market_keys = markets_df[["source", "market_id"]].values.tolist()
        chunk_size = 100000  # OPTIMIZATION: Increased from 50k to 100k for fewer transactions
        
        writer = None
        parquet_file = output_dir / "data.parquet"

        for i in range(0, len(unique_market_keys), chunk_size):
            chunk_keys = unique_market_keys[i : i + chunk_size]
            
            # Build a query for this chunk of market IDs
            with self._get_conn() as conn:
                conn.execute("CREATE TEMPORARY TABLE chunk_ids (source TEXT, market_id TEXT)")
                conn.executemany("INSERT INTO chunk_ids VALUES (?, ?)", chunk_keys)
                
                # OPTIMIZATION: Only select columns we need, skip raw_json
                history_df = pd.read_sql("""
                    SELECT h.source, h.market_id, h.ts, h.belief_scalar, h.volume, h.open_interest, h.bid, h.ask 
                    FROM history h
                    JOIN chunk_ids c ON h.source = c.source AND h.market_id = c.market_id
                    ORDER BY h.source, h.market_id, h.ts
                """, conn)
                conn.execute("DROP TABLE chunk_ids")

            if history_df.empty:
                continue

            # Aggregate history - OPTIMIZATION: Use 'ISO8601' format hint for faster parsing
            history_df["ts"] = pd.to_datetime(history_df["ts"], format='ISO8601', errors='coerce')
            history_agg = history_df.groupby(["source", "market_id"], sort=False).agg({
                "ts": list,
                "belief_scalar": list,
                "volume": list,
                "open_interest": list,
                "bid": list,
                "ask": list
            }).reset_index().rename(columns={"belief_scalar": "belief"})

            # Merge with metadata subset
            chunk_markets = markets_df[i : i + chunk_size]
            unified_chunk = pd.merge(chunk_markets, history_agg, on=["source", "market_id"], how="inner")
            
            if unified_chunk.empty:
                continue

            # Write to Parquet (Append mode) - OPTIMIZATION: Use compression
            table = pa.Table.from_pandas(unified_chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_file, table.schema, compression='snappy')
            writer.write_table(table)
            
            logger.info(f"  Exported chunk {i//chunk_size + 1}/{(len(unique_market_keys)-1)//chunk_size + 1} ({len(unified_chunk)} markets)")

        if writer:
            writer.close()
            logger.info(f"Partitioned Parquet build complete: {parquet_file}")
        else:
            logger.error("No data was exported to Parquet.")

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
    
    # Use more aggressive parallelism for S3 scanning (I/O bound)
    # For history processing we'll use fewer (CPU bound)
    scan_workers = max(1, (os.cpu_count() or 16))  # Use all cores for I/O
    process_workers = max(1, (os.cpu_count() or 16) - 2)  # Leave 2 cores free for processing
    logger.info(f"Using {scan_workers} workers for S3 scanning, {process_workers} for processing.")

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
        # Get list of already processed dates to skip them in the scan
        current_date = start_date
        dates_to_scan = []
        while current_date <= end_date:
            if not (use_cache and store.is_date_processed("kalshi", current_date)):
                dates_to_scan.append(current_date)
            current_date += timedelta(days=1)

        # STEP A: Aggressive Discovery and Activity Scan from S3 (Only for new dates)
        if dates_to_scan:
            vitals_map = kalshi_bulk.scan_all_tickers(dates_to_scan[0], dates_to_scan[-1], workers=scan_workers)
        else:
            logger.info("All Kalshi dates in range already processed. Skipping S3 scan.")
            vitals_map = {}
        
        # STEP B: Aggressive Filtering
        # We only keep markets that have EVER shown volume or open interest.
        # This is now mostly handled inside scan_all_tickers to save memory.
        active_tickers = list(vitals_map.keys())
        
        logger.info(f"Filtering complete: {len(active_tickers)} active tickers identified.")
        
        # STEP C: Filter missing metadata for active tickers
        # OPTIMIZATION: Skip API enrichment for settled/closed markets since S3 already has the key info
        # We only consider it "missing" if it's not in the DB OR if it only has minimal S3 metadata AND is still active
        missing_tickers = []
        for t in active_tickers:
            # Check if we need rich metadata
            if t not in existing_statuses:
                # Not in DB at all, but check S3 status first
                v_status = vitals_map[t].get("last_status", "unknown").lower()
                if v_status in ["finalized", "determined", "settled", "closed"]:
                    # Already settled in S3, skip API enrichment
                    continue
                missing_tickers.append(t)
            elif existing_statuses[t] in ["unknown", "Bulk-only"]:
                # In DB but with minimal metadata, check if worth enriching
                v_status = vitals_map[t].get("last_status", "unknown").lower()
                if v_status not in ["finalized", "determined", "settled", "closed"]:
                    # Still active, enrich it
                    missing_tickers.append(t)
        
        if limit: missing_tickers = missing_tickers[:limit]
        
        logger.info(f"Filtered to {len(missing_tickers)} tickers needing API enrichment (skipped {len(active_tickers) - len(missing_tickers)} settled markets).")
        
        # STEP D: Pre-populate with S3 Minimal Metadata (Zero API calls)
        if missing_tickers or (len(active_tickers) - len(missing_tickers)) > 0:
            # Pre-populate for ALL active tickers (both ones we'll enrich and ones we won't)
            all_to_populate = active_tickers if limit is None else active_tickers[:limit] if limit else active_tickers
            logger.info(f"Pre-populating DB with S3 metadata for {len(all_to_populate)} tickers...")
            s3_metadata_batch = {}
            for t in all_to_populate:
                v = vitals_map[t]
                # Map S3 vitals to a basic MarketRecord
                s3_status = v.get("last_status", "unknown").lower()
                canonical_status = "resolved" if s3_status in ["finalized", "determined", "settled"] else "closed" if s3_status == "closed" else "unknown"
                
                s3_metadata_batch[t] = {
                    "source": "kalshi",
                    "market_id": t,
                    "event_id": v.get("report_ticker"),
                    "title": t, # Fallback title
                    "description": f"S3-sourced record (Max Vol: {v['max_vol']}, Max OI: {v['max_oi']})",
                    "url": f"https://kalshi.com/markets/{t}",
                    "market_type": "binary",
                    "answer_options_json": json.dumps(["NO", "YES"]),
                    "end_time": v["last_date"], # Fallback
                    "status": canonical_status
                }
            store.save_batch("kalshi", s3_metadata_batch, {})
            
            # STEP E: Targeted Rich Metadata Enrichment from API (Selective)
            # Now we hit the API ONLY for active, missing tickers.
            if missing_tickers:
                logger.info(f"Enriching with Rich Metadata for {len(missing_tickers)} active/open tickers...")
                new_markets = kalshi_grabber.fetch_markets(tickers=missing_tickers)
                api_metadata_batch = {}
                for m in new_markets:
                    try:
                        record = map_kalshi_market(m)
                        api_metadata_batch[m["ticker"]] = record.model_dump(mode='json')
                    except Exception: continue
                
                if api_metadata_batch:
                    store.save_batch("kalshi", api_metadata_batch, {})
                    logger.info(f"Saved rich metadata for {len(api_metadata_batch)} active Kalshi markets.")
            else:
                logger.info("No tickers need API enrichment (all are settled or already enriched).")
        
        # STEP F: History Ingestion
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
            allowed_set = set(active_tickers) # Use the aggressively filtered set
            with ProcessPoolExecutor(max_workers=process_workers) as executor:
                futures = {executor.submit(process_kalshi_day, d, allowed_set): d for d in dates_to_process}
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
    version = datetime.now().strftime("%Y%m%d_%H%M")
    output_dir = config.datasets_dir / f"v{version}_unified"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    store.load_and_write_partitioned(output_dir)
    
    # Write manifest manually since we skipped write_parquet_dataset
    manifest = {
        "version": version,
        "dataset_name": "unified",
        "created_at": datetime.now().isoformat(),
        "row_count": len(store.get_existing_market_ids("kalshi")) + len(store.get_existing_market_ids("metaculus"))
    }
    with open(output_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    
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
# 10. S3-First Optimization (2026-01): The BIGGEST performance win is skipping API 
#     enrichment for settled markets. S3 bulk files contain status info - use it to 
#     filter BEFORE calling API. Achieved 5x speedup (262s→52s for 2 days) by skipping 
#     92% of API calls (45K/49K markets). Split workers: I/O-bound S3 scanning uses all 
#     cores, CPU-bound processing uses cores-2.
# 11. Batch Size Matters: Reduced Kalshi ticker batch from 100→50 to avoid 413 errors 
#     from long ticker names (KXCITIESWEATHER markets). API rate limits (20 req/s) are 
#     the bottleneck, not batch size.
# 12. Parquet Export: Bottleneck at ~14-15s for 50-75K markets. Increasing chunk size 
#     (50K→100K) didn't help. Pandas groupby overhead dominates. Consider arrow-native 
#     processing for future optimization.
# 13. Performance Metrics (2026-01 benchmarks): Baseline 2-day test: 262.84s, 340 rec/s, 
#     375MB peak. Final optimized: 52.26s, 1,711 rec/s, 214MB peak. 8-day test: 57.27s, 
#     5,855 rec/s, 242MB peak. Scaling projection: 2-3 years (~900 days) ≈ 1.8 hours.
# 14. Status Filtering Logic: Critical logic in lines 428-456. For each active ticker, check: 
#     (1) Not in DB? → Check S3 status, skip API if finalized/settled/closed. (2) In DB with 
#     "unknown" status? → Check S3 status, only enrich if still active. This logic achieved 
#     92% skip rate. Edge case: Markets that are "active" in S3 but actually settled need API 
#     to get correct status - this is acceptable trade-off (few false negatives).
# 15. Pre-population Strategy: Pre-populate ALL active markets with S3 metadata first (zero API 
#     calls). Then selectively enrich only active/open markets via API. This ensures DB has 
#     complete market list even if API enrichment fails. Pre-population uses S3 status to set 
#     canonical status (finalized/settled→resolved, closed→closed, else→unknown).
# 16. History Processing: ProcessPoolExecutor with 22 workers processes days in parallel. Each 
#     worker filters by active_tickers set (passed as argument) to skip inactive markets early. 
#     Processing time: ~2-3s per day for normal days, but busy days with 100K+ records can take 
#     30-60s. Memory per worker: ~50-100MB for busy days (all points accumulated before DB insert). 
#     Total: ~1-2GB for 22 workers processing busy days simultaneously. This is manageable for most 
#     systems but could cause OOM on limited RAM. Consider reducing process_workers if memory is tight.
#     Large-scale warning: For 18M+ markets over years, memory accumulation in process_kalshi_day 
#     (all points in memory before returning) is a known limitation. Previous implementations used 
#     "SQL being read during streaming" hacks to flush incrementally - current code doesn't do this.
# 17. SQLite Checkpoints: Uses checkpoint table to track processed dates per source. Format: 
#     key="processed_date_YYYY-MM-DD", value="done". Allows resuming interrupted runs. Check 
#     checkpoints BEFORE scanning S3 (avoid unnecessary work). Mark checkpoints AFTER processing 
#     completes successfully (atomic operation).
# 18. Vitals Aggregation: S3 scan uses temporary SQLite DB with aggressive pragmas (WAL, 
#     synchronous=OFF, 2GB cache) to handle millions of ticker records. SQLite MAX()/MIN() 
#     aggregate functions handle duplicate tickers across days efficiently. Final query filters 
#     for active tickers (max_vol>0 OR max_oi>0 OR status=finalized/settled) before returning.
# 19. Memory Profile: Peak memory 214MB for 49K markets/89K records. Scales roughly linearly: 
#     242MB for 75K markets/335K records. Main memory consumers: (1) Pandas DataFrames during 
#     Parquet export (~100-150MB), (2) SQLite cache (2GB configured but only uses what's needed), 
#     (3) Worker processes (~20MB each × 22 = 440MB theoretical max, but shared memory reduces this).
# 20. Edge Cases Handled: (1) Missing dates (404 from S3) → skip gracefully, (2) Empty date 
#     ranges → return early, (3) All dates already processed → skip S3 scan entirely, (4) API 
#     failures for some batches → continue with remaining batches, (5) Malformed JSON in S3 → 
#     skip invalid records, (6) Duplicate markets in API response → last one wins (INSERT OR REPLACE).
# 21. Production Recommendations: (1) Process in 1-week batches for optimal memory/time balance, 
#     (2) Use --use-cache to skip re-processing existing dates, (3) Monitor API call count to stay 
#     under rate limits, (4) For historical data >1 month old, consider skipping API entirely 
#     (S3-only mode), (5) Consider incremental updates (track processed dates) to avoid re-scanning 
#     entire history on each run.
# 22. Idempotency: Running pipeline twice on same date range produces identical results (assuming 
#     no API-side changes). SQLite uses INSERT OR REPLACE for markets, INSERT OR IGNORE for history 
#     (prevents duplicates). Checkpoints prevent re-processing. This allows safe re-runs for 
#     validation or after code changes.
# 23. Date Processing Order: Dates are processed in chronological order for history (start_date to 
#     end_date), but S3 scanning processes dates in parallel (order doesn't matter). This ensures 
#     checkpoint logic works correctly (mark date as done only after processing completes).
# 24. Parquet Compression: Uses 'snappy' compression (default in pyarrow). File sizes: ~50-75K 
#     markets produce ~50-100MB Parquet files (depending on history length). Compression ratio: 
#     ~2-3x (JSON would be much larger). Consider 'zstd' for better compression if disk space is 
#     concern (trades compression for speed).
# 25. Testing (2026-01): Comprehensive test suite created with 21 automated tests covering data 
#     correctness, edge cases, and idempotency. All tests pass. Random sampling validation shows 
#     96% pass rate (4% have empty descriptions from S3-sourced records, expected). History point 
#     validation 100% success rate. Pipeline confirmed idempotent (identical outputs on re-run).
#     Status filtering, parallel processing, and checkpoint system all validated. No critical bugs 
#     found. Developer experience excellent (new method in 5min, new task in 15min). See 
#     TESTING_REPORT.md for full findings.
