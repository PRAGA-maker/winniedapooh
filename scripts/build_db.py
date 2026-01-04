import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.build_unified_parquet import build_unified_dataset

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

