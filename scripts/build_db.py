import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.build_unified_parquet import build_unified_dataset

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--no-cache", action="store_false", dest="use_cache", default=True)
    args = parser.parse_args()
    
    build_unified_dataset(limit=args.limit, use_cache=args.use_cache)

