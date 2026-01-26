import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.orchestrator import build_unified_dataset

if __name__ == "__main__":
    import argparse
    from datetime import date
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit number of markets (Test Mode)")
    parser.add_argument("--metaculus-limit", type=int, default=None, help="Specific limit for Metaculus (overrides --limit)")
    parser.add_argument("--no-cache", action="store_false", dest="use_cache", default=True)
    parser.add_argument("--kalshi-ticker", type=str, default=None)
    parser.add_argument("--start", type=str, default=None, help="Kalshi bulk start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="Kalshi bulk end date (YYYY-MM-DD)")
    parser.add_argument("--name", type=str, default=None, help="Custom name for this dataset build (isolates DB and output)")
    parser.add_argument("--kalshi-bid-ask-backfill", action="store_true", help="Force Kalshi bid/ask backfill from candlesticks (use on full runs when auto-backfill is auto-skipped)")
    parser.add_argument("--skip-kalshi", action="store_true", help="Skip Kalshi ingestion (Metaculus-only build)")
    parser.add_argument("--kalshi-batch-size", type=int, default=None, help="Kalshi /markets batch size (default: 50)")
    parser.add_argument("--sequential", action="store_true", help="Use sequential S3 scanning (lowest memory, slower)")
    parser.add_argument("--overclock", action="store_true", help="Target 70%% RAM instead of 50%% for parallel workers (faster, higher memory)")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None

    build_unified_dataset(
        limit=args.limit,
        metaculus_limit=args.metaculus_limit,
        use_cache=args.use_cache,
        kalshi_ticker=args.kalshi_ticker,
        start_date=start_date,
        end_date=end_date,
        name=args.name,
        kalshi_bid_ask_backfill=args.kalshi_bid_ask_backfill,
        skip_kalshi=args.skip_kalshi,
        kalshi_batch_size=args.kalshi_batch_size,
        sequential_scan=args.sequential,
        overclock_scan=args.overclock
    )

