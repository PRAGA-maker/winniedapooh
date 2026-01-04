import sys
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.kalshi.grabber import KalshiGrabber
from src.kalshi.map_to_canonical import map_kalshi_market, map_kalshi_trade
from src.metaculus.grabber import MetaculusGrabber
from src.metaculus.map_to_canonical import map_metaculus_question, map_metaculus_history_point
from src.common.parquet import write_parquet_dataset
from src.common.logging import logger
from src.common.config import config

class CanonicalStore:
    """Handles persistent storage of individual canonical market records."""
    def __init__(self):
        self.base_dir = config.clean_data_dir / "canonical"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_path(self, source: str, market_id: str) -> Path:
        source_dir = self.base_dir / source
        source_dir.mkdir(parents=True, exist_ok=True)
        # Using json for individual records as they are small and easy to inspect/resume
        return source_dir / f"{market_id}.json"

    def exists(self, source: str, market_id: str) -> bool:
        return self._get_path(source, market_id).exists()

    def save(self, source: str, market_id: str, market_record: Dict[str, Any], timeseries: List[Dict[str, Any]]):
        data = {
            "market": market_record,
            "timeseries": timeseries
        }
        with open(self._get_path(source, market_id), "w") as f:
            json.dump(data, f)

    def load_all(self) -> List[Dict[str, Any]]:
        """Load all stored canonical records and return unified format."""
        logger.info("Loading all canonical records from disk...")
        unified_rows = []
        for source_dir in self.base_dir.iterdir():
            if not source_dir.is_dir():
                continue
            
            for f_path in source_dir.glob("*.json"):
                with open(f_path, "r") as f:
                    data = json.load(f)
                
                m_row = data["market"]
                ts_points = data["timeseries"]
                
                # Sort timeseries by ts string (lexicographical sort works for ISO format)
                ts_points = sorted(ts_points, key=lambda x: x["ts"])
                
                unified_row = m_row.copy()
                # Parse datetimes for pandas, handling out-of-bounds dates (e.g. year 2300)
                unified_row["end_time"] = pd.to_datetime(unified_row["end_time"], errors='coerce')
                if unified_row.get("created_time"):
                    unified_row["created_time"] = pd.to_datetime(unified_row["created_time"], errors='coerce')
                
                unified_row["ts"] = [pd.to_datetime(pt["ts"], errors='coerce') for pt in ts_points]
                unified_row["belief"] = [pt["belief_scalar"] for pt in ts_points]
                unified_rows.append(unified_row)
        return unified_rows

def build_unified_dataset(limit: int = 1000, use_cache: bool = True):
    logger.info(f"Starting unified dataset build (limit={limit} per source)...")
    
    store = CanonicalStore()
    api_calls_count = 0
    batch_size = 100
    
    # 1. Fetch Kalshi Data
    kalshi_grabber = KalshiGrabber()
    k_markets = []
    for status in ["settled", "closed"]:
        k_markets.extend(kalshi_grabber.fetch_markets(limit=limit, status=status, use_cache=use_cache))
    
    # Dedup and limit
    seen_tickers = set()
    unique_k_markets = []
    for m in k_markets:
        if m["ticker"] not in seen_tickers:
            unique_k_markets.append(m)
            seen_tickers.add(m["ticker"])
    
    unique_k_markets = sorted(unique_k_markets, key=lambda x: x.get("volume", 0), reverse=True)[:limit]
    
    for i, m in enumerate(unique_k_markets):
        ticker = m["ticker"]
        if store.exists("kalshi", ticker):
            logger.debug(f"Kalshi market {ticker} already in canonical store, skipping.")
            continue

        try:
            record = map_kalshi_market(m)
            # Fetch all trades (no limit)
            trades = kalshi_grabber.fetch_trades(ticker, limit=None, use_cache=use_cache)
            api_calls_count += 1
            
            ts_points = []
            for t in trades:
                ts_point = map_kalshi_trade(ticker, t)
                ts_points.append(ts_point.model_dump(mode='json'))
            
            store.save("kalshi", ticker, record.model_dump(mode='json'), ts_points)
            logger.info(f"Saved canonical Kalshi market {ticker} ({len(trades)} trades)")
            
            if api_calls_count % batch_size == 0:
                logger.info(f"Progress: {api_calls_count} API-like calls completed.")
                
        except Exception as e:
            logger.error(f"Error processing Kalshi market {ticker}: {e}")

    # 2. Fetch Metaculus Data
    metaculus_grabber = MetaculusGrabber()
    posts = metaculus_grabber.fetch_posts(limit=limit, use_cache=use_cache)
    
    for p in posts:
        p_id = str(p["id"])
        
        try:
            # Speed up: fetch_posts now includes history, so we can often use 'p' directly
            # only call fetch_post_detail if we really need to (e.g. for sub-questions history)
            detail = p
            
            # If 'question' or 'group_questions' is missing history, then we fetch detail
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
                
                if store.exists("metaculus", q_id):
                    continue
                    
                record = map_metaculus_question(detail, q)
                points = []
                if "aggregations" in q:
                    aggs = q["aggregations"]
                    if "recency_weighted" in aggs:
                        points = aggs["recency_weighted"].get("history", [])
                
                ts_points = []
                for pt in points:
                    ts_point = map_metaculus_history_point(q_id, pt)
                    ts_points.append(ts_point.model_dump(mode='json'))
                
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
    args = parser.parse_args()
    build_unified_dataset(limit=args.limit, use_cache=args.use_cache)

if __name__ == "__main__":
    build_db_cli()

# --- LESSONS LEARNED ---
# 1. Scaling: Building a DB takes time due to strict rate limits (Metaculus).
# 2. Sub-questions: A single Metaculus post can generate multiple canonical rows.
# 3. Join Logic: Linking static market info with time-series lists in a single 
#    row works well for runner ergonomics but requires strict ts sorting.
