import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.kalshi.grabber import KalshiGrabber
from src.kalshi.map_to_canonical import map_kalshi_market, map_kalshi_trade
from src.metaculus.grabber import MetaculusGrabber
from src.metaculus.map_to_canonical import map_metaculus_question, map_metaculus_history_point
from src.common.parquet import write_parquet_dataset
from src.common.logging import logger

def build_unified_dataset(limit: int = 100):
    logger.info(f"Starting unified dataset build (limit={limit} per source)...")
    
    markets_data = []
    timeseries_data = []
    
    # 1. Fetch Kalshi Data
    kalshi_grabber = KalshiGrabber()
    # Fetch settled and closed markets to get volume/trades
    k_markets = []
    for status in ["settled", "closed"]:
        k_markets.extend(kalshi_grabber.fetch_markets(limit=limit, status=status))
    
    # Dedup and limit
    seen_tickers = set()
    unique_k_markets = []
    for m in k_markets:
        if m["ticker"] not in seen_tickers:
            unique_k_markets.append(m)
            seen_tickers.add(m["ticker"])
    
    # Prioritize those with volume
    unique_k_markets = sorted(unique_k_markets, key=lambda x: x.get("volume", 0), reverse=True)[:limit]
    
    for m in unique_k_markets:
        try:
            ticker = m["ticker"]
            record = map_kalshi_market(m)
            markets_data.append(record.model_dump())
            
            # Fetch trades
            trades = kalshi_grabber.fetch_trades(ticker, limit=1000)
            logger.info(f"Fetched {len(trades)} trades for {ticker}")
            for t in trades:
                ts_point = map_kalshi_trade(ticker, t)
                timeseries_data.append(ts_point.model_dump())
        except Exception as e:
            logger.error(f"Error processing Kalshi market {m.get('ticker')}: {e}")

    # 2. Fetch Metaculus Data
    metaculus_grabber = MetaculusGrabber()
    # Fetch some older posts to ensure we get history
    posts = metaculus_grabber.fetch_posts(limit=limit)
    
    for p in posts:
        try:
            p_id = p["id"]
            # Fetch detail to get history
            detail = metaculus_grabber.fetch_post_detail(p_id)
            
            if not detail:
                continue
                
            # Unpack all questions in this post
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
                    
                q_id = q["id"]
                record = map_metaculus_question(detail, q)
                markets_data.append(record.model_dump())
                
                # Extract history from aggregations
                points = []
                if "aggregations" in q:
                    aggs = q["aggregations"]
                    if "recency_weighted" in aggs:
                        points = aggs["recency_weighted"].get("history", [])
                
                for pt in points:
                    ts_point = map_metaculus_history_point(str(q_id), pt)
                    timeseries_data.append(ts_point.model_dump())
                    
            logger.info(f"Processed Metaculus post {p_id} ({len(sub_qs)} questions)")
        except Exception as e:
            logger.error(f"Error processing Metaculus post {p.get('id')}: {e}")

    if not markets_data:
        logger.error("No data collected. Aborting.")
        return

    # 3. Create DataFrames and Write Parquet
    markets_df = pd.DataFrame(markets_data)
    
    if not timeseries_data:
        logger.warning("No timeseries data collected.")
        timeseries_df = pd.DataFrame(columns=["source", "market_id", "ts", "belief_scalar"])
    else:
        timeseries_df = pd.DataFrame(timeseries_data)
    
    # Create single table
    unified_rows = []
    for _, m_row in markets_df.iterrows():
        m_id = m_row["market_id"]
        source = m_row["source"]
        
        m_ts = timeseries_df[(timeseries_df["market_id"] == str(m_id)) & (timeseries_df["source"] == source)]
        m_ts = m_ts.sort_values("ts")
        
        unified_row = m_row.to_dict()
        unified_row["ts"] = m_ts["ts"].tolist()
        unified_row["belief"] = m_ts["belief_scalar"].tolist()
        unified_rows.append(unified_row)
        
    unified_df = pd.DataFrame(unified_rows)
    
    version = datetime.now().strftime("%Y%m%d_%H%M")
    write_parquet_dataset(unified_df, "unified", version=version)
    
    logger.info("Unified dataset build complete.")

def build_db_cli():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    build_unified_dataset(limit=args.limit)

if __name__ == "__main__":
    build_db_cli()

# --- LESSONS LEARNED ---
# 1. Scaling: Building a DB takes time due to strict rate limits (Metaculus).
# 2. Sub-questions: A single Metaculus post can generate multiple canonical rows.
# 3. Join Logic: Linking static market info with time-series lists in a single 
#    row works well for runner ergonomics but requires strict ts sorting.
