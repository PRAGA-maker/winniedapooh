import sys
import time
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def save(self, source: str, market_id: str, market_record: Dict[str, Any], timeseries: List[Dict[str, Any]]):
        with self._get_conn() as conn:
            # 1. Save or update market metadata
            fields = ["source", "market_id", "event_id", "title", "description", "url", "market_type", 
                      "answer_options_json", "end_time", "status", "resolved_value_json", "created_time", "metadata_json"]
            
            # Ensure all fields are in the record
            record = {f: market_record.get(f) for f in fields}
            record["source"] = source
            record["market_id"] = market_id
            
            placeholders = ", ".join(["?"] * len(fields))
            conn.execute(f"INSERT OR REPLACE INTO markets ({', '.join(fields)}) VALUES ({placeholders})", 
                         tuple(record[f] for f in fields))
            
            # 2. Save history points
            if timeseries:
                h_fields = ["source", "market_id", "ts", "belief_scalar", "belief_json", "bid", "ask", "volume", "open_interest", "raw_json"]
                h_placeholders = ", ".join(["?"] * len(h_fields))
                
                rows = []
                for pt in timeseries:
                    pt_record = {f: pt.get(f) for f in h_fields}
                    pt_record["source"] = source
                    pt_record["market_id"] = market_id
                    rows.append(tuple(pt_record[f] for f in h_fields))
                
                conn.executemany(f"INSERT OR IGNORE INTO history ({', '.join(h_fields)}) VALUES ({h_placeholders})", rows)

    def add_history_point(self, source: str, market_id: str, point: Dict[str, Any], market_record_fallback: Optional[Dict[str, Any]] = None):
        """Append a single history point, creating the market if needed."""
        with self._get_conn() as conn:
            # Check if market exists, if not use fallback
            if market_record_fallback and not self.exists(source, market_id):
                fields = ["source", "market_id", "event_id", "title", "description", "url", "market_type", 
                          "answer_options_json", "end_time", "status", "resolved_value_json", "created_time", "metadata_json"]
                record = {f: market_record_fallback.get(f) for f in fields}
                record["source"] = source
                record["market_id"] = market_id
                placeholders = ", ".join(["?"] * len(fields))
                conn.execute(f"INSERT OR REPLACE INTO markets ({', '.join(fields)}) VALUES ({placeholders})", 
                             tuple(record[f] for f in fields))
            
            # Insert point
            h_fields = ["source", "market_id", "ts", "belief_scalar", "belief_json", "bid", "ask", "volume", "open_interest", "raw_json"]
            h_placeholders = ", ".join(["?"] * len(h_fields))
            pt_record = {f: point.get(f) for f in h_fields}
            pt_record["source"] = source
            pt_record["market_id"] = market_id
            conn.execute(f"INSERT OR IGNORE INTO history ({', '.join(h_fields)}) VALUES ({h_placeholders})", 
                         tuple(pt_record[f] for f in h_fields))

    def set_checkpoint(self, source: str, key: str, value: str):
        with self._get_conn() as conn:
            conn.execute("INSERT OR REPLACE INTO checkpoints (source, key, value) VALUES (?, ?, ?)", (source, key, value))

    def get_checkpoint(self, source: str, key: str) -> Optional[str]:
        with self._get_conn() as conn:
            res = conn.execute("SELECT value FROM checkpoints WHERE source = ? AND key = ?", (source, key)).fetchone()
            return res[0] if res else None

    def load_all(self) -> List[Dict[str, Any]]:
        """Return all stored canonical records in unified format."""
        logger.info("Loading all canonical records from SQLite...")
        
        with self._get_conn() as conn:
            # Load all markets
            markets_df = pd.read_sql("SELECT * FROM markets", conn)
            
            # Load all history points sorted by source, market_id, ts
            logger.info("Fetching all history points...")
            history_df = pd.read_sql("""
                SELECT source, market_id, ts, belief_scalar, belief_json, volume, open_interest, bid, ask 
                FROM history 
                ORDER BY source, market_id, ts
            """, conn)
            
            # Group by source and market_id to aggregate timeseries into lists
            logger.info("Aggregating timeseries into nested lists...")
            history_agg = history_df.groupby(["source", "market_id"]).agg({
                "ts": list,
                "belief_scalar": list,
                "belief_json": list,
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
            unified_df["end_time"] = pd.to_datetime(unified_df["end_time"], errors='coerce')
            unified_df["created_time"] = pd.to_datetime(unified_df["created_time"], errors='coerce')
            
            # Ensure ts lists are datetime objects
            unified_df["ts"] = unified_df["ts"].apply(lambda x: [pd.to_datetime(t, errors='coerce') for t in x])
            
            return unified_df.to_dict(orient="records")

def build_unified_dataset(limit: int = 1000, use_cache: bool = True, kalshi_ticker: Optional[str] = None, start_date: Optional[date] = None, end_date: Optional[date] = None):
    logger.info(f"Starting unified dataset build (limit={limit} per source)...")
    
    store = CanonicalStore()
    api_calls_count = 0
    batch_size = 17 # Kalshi rate limit is 20, we use 17 to stay safe
    
    # 1. Fetch Kalshi Metadata from API
    kalshi_grabber = KalshiGrabber()
    k_markets = []
    if kalshi_ticker:
        k_markets = kalshi_grabber.fetch_markets(limit=1, ticker=kalshi_ticker, use_cache=use_cache)
    else:
        # Fetch catalog for mapping - ONLY settled markets as requested
        try:
            # If limit is None (prod use), we fetch a large catalog to ensure we cover history
            fetch_limit = limit if limit else 10000 
            logger.info(f"Fetching Kalshi metadata catalog (limit={fetch_limit})...")
            k_markets = kalshi_grabber.fetch_markets(limit=fetch_limit, status="settled", use_cache=use_cache)
        except Exception as e:
            logger.error(f"Failed to fetch settled Kalshi markets: {e}")
    
    # Dedup and build metadata catalog
    market_metadata = {}
    for m in k_markets:
        ticker = m["ticker"]
        if ticker not in market_metadata:
            try:
                record = map_kalshi_market(m)
                market_metadata[ticker] = record.model_dump(mode='json')
            except Exception as e:
                logger.debug(f"Failed to map market {ticker}: {e}")

    logger.info(f"Metadata catalog built for {len(market_metadata)} Kalshi markets.")

    # 2. Fetch Kalshi History from Bulk JSONs (OOM-Safe)
    if start_date and end_date:
        bulk_grabber = KalshiBulkGrabber()
        current_date = start_date
        
        # Check if we can resume
        last_processed_str = store.get_checkpoint("kalshi", "bulk_last_date")
        if last_processed_str:
            last_processed = date.fromisoformat(last_processed_str)
            if last_processed >= current_date:
                logger.info(f"Resuming Kalshi bulk from {last_processed + timedelta(days=1)}")
                current_date = last_processed + timedelta(days=1)

        while current_date <= end_date:
            logger.info(f"Processing Kalshi Bulk Day: {current_date}")
            records_count = 0
            for raw_record in bulk_grabber.fetch_daily_bulk_stream(current_date):
                ticker = raw_record["ticker_name"]
                
                # If we are in "Test Mode" (limit is small), filter strictly by the metadata we fetched.
                # If we are in "Prod Mode" (limit is None), we let the bulk date range drive the ticker list.
                if limit and ticker not in market_metadata:
                    continue

                # Use metadata from API if available
                m_record = market_metadata.get(ticker)
                if not m_record:
                    # Fallback for when limit is disabled
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

                try:
                    ts_point = bulk_grabber.map_to_timeseries(raw_record)
                    store.add_history_point("kalshi", ticker, ts_point.model_dump(mode='json'), market_record_fallback=m_record)
                    records_count += 1
                except Exception as e:
                    pass
                
                if records_count > 0 and records_count % 50000 == 0:
                    logger.info(f"  Processed {records_count} records for today...")
            
            logger.info(f"Finished Day {current_date}: {records_count} records processed.")
            store.set_checkpoint("kalshi", "bulk_last_date", current_date.isoformat())
            current_date += timedelta(days=1)
    else:
        logger.warning("No bulk dates provided, skipping Kalshi history collection.")
            
    # 3. Fetch Metaculus Data
    metaculus_grabber = MetaculusGrabber()
    posts = metaculus_grabber.fetch_posts(limit=limit, use_cache=use_cache)
    
    # Pre-calculate window boundaries for filtering
    window_start = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=None) if start_date else None
    window_end = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=None) if end_date else None

    for p in posts:
        p_id = str(p["id"])
        
        try:
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
                
                if use_cache and store.exists("metaculus", q_id):
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
                    store.save("metaculus", q_id, record.model_dump(mode='json'), ts_points)
                    logger.debug(f"Saved canonical Metaculus question {q_id}")
            
            logger.info(f"Processed Metaculus post {p_id} ({len(sub_qs)} questions)")
            
            if api_calls_count % batch_size == 0:
                logger.info(f"Progress: {api_calls_count} API-like calls completed.")
                
        except Exception as e:
            logger.error(f"Error processing Metaculus post {p_id}: {e}")

    # 3. Build Final Unified Dataset
    unified_rows = store.load_all()
    if not unified_rows:
        logger.error("No data collected. Aborting.")
        return

    unified_df = pd.DataFrame(unified_rows)
    version = datetime.now().strftime("%Y%m%d_%H%M")
    write_parquet_dataset(unified_df, "unified", version=version)
    
    logger.info("Unified dataset build complete.")

def build_db_cli():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--no-cache", action="store_false", dest="use_cache", default=True)
    parser.add_argument("--kalshi-ticker", type=str, default=None)
    parser.add_argument("--start", type=str, default=None, help="Kalshi bulk start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="Kalshi bulk end date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    
    build_unified_dataset(
        limit=args.limit, 
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
