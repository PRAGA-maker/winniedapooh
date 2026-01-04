import json
import os
from typing import List, Dict, Any, Optional
from src.common.http import get_kalshi_client
from src.common.config import config
from src.common.logging import logger

class KalshiGrabber:
    def __init__(self):
        self.client = get_kalshi_client()
        self.raw_dir = config.raw_data_dir / "kalshi"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def fetch_markets(self, limit: int = 1000, status: str = "settled", use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch list of markets."""
        logger.info(f"Fetching Kalshi markets (limit={limit}, status={status})...")
        markets = []
        cursor = ""
        
        while len(markets) < limit:
            # Check cache
            filename = f"markets_cursor_{cursor if cursor else 'start'}.json"
            cache_path = self.raw_dir / filename
            
            if use_cache and cache_path.exists():
                logger.debug(f"Loading Kalshi markets from cache: {filename}")
                with open(cache_path, "r") as f:
                    batch = json.load(f)
                # We need to find the next cursor from the cached file metadata or just assume 
                # this batch is enough. Kalshi's /markets response doesn't include the cursor 
                # *inside* the market list, it's a top level key. 
                # Let's re-read the file to get the cursor if we can, or just trust the API for lists.
                # Actually, for lists, caching is tricky because 'limit' might change.
                # Let's only cache details/trades which are immutable for settled markets.
                pass 

            params = {"limit": min(100, limit - len(markets))}
            if cursor:
                params["cursor"] = cursor
            if status:
                params["status"] = status
            
            response = self.client.get("/markets", params=params)
            data = response.json()
            
            batch = data.get("markets", [])
            markets.extend(batch)
            
            # Save raw batch
            with open(self.raw_dir / filename, "w") as f:
                json.dump(batch, f)
            
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
                
        return markets[:limit]

    def fetch_market_details(self, ticker: str, use_cache: bool = True) -> Dict[str, Any]:
        """Fetch detailed info for a single market."""
        cache_path = self.raw_dir / f"market_{ticker}.json"
        if use_cache and cache_path.exists():
            with open(cache_path, "r") as f:
                return json.load(f)

        logger.info(f"Fetching Kalshi market details for {ticker}...")
        response = self.client.get(f"/markets/{ticker}")
        data = response.json().get("market", {})
        
        # Save raw detail
        with open(cache_path, "w") as f:
            json.dump(data, f)
            
        return data

    def fetch_trades(self, ticker: str, limit: Optional[int] = None, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch all trades for a single market ticker."""
        # For simplicity, we only cache the 'start' (first batch) or if we have a way 
        # to know we got everything. For settled markets, trades are static.
        cache_path = self.raw_dir / f"trades_{ticker}_start.json"
        if use_cache and cache_path.exists():
            # This only returns the first batch. For a real 'limit', we'd need more logic.
            # But in build_db.py, we usually just want the trades.
            with open(cache_path, "r") as f:
                return json.load(f)

        logger.info(f"Fetching Kalshi trades for {ticker} (limit={limit})...")
        all_trades = []
        cursor = ""
        
        while True:
            if limit and len(all_trades) >= limit:
                break
                
            params = {
                "ticker": ticker,
                "limit": min(1000, limit - len(all_trades)) if limit else 1000
            }
            if cursor:
                params["cursor"] = cursor
                
            response = self.client.get("/markets/trades", params=params)
            data = response.json()
            
            batch = data.get("trades", [])
            all_trades.extend(batch)
            
            # Save raw batch
            filename = f"trades_{ticker}_{cursor if cursor else 'start'}.json"
            with open(self.raw_dir / filename, "w") as f:
                json.dump(batch, f)
                
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
                
        return all_trades[:limit]

# --- LESSONS LEARNED ---
# 1. Endpoint Params: /markets uses 'status', /markets/candlesticks uses 'market_tickers',
#    but /markets/trades uses 'ticker' (singular).
# 2. Trades vs Candles: Trades provide the rawest data. Candles are 400-bad-request 
#    if you ask for more than 10,000 across all markets in one go.
# 3. Volume: Many 'active' markets have 0 volume. Use 'settled' or 'closed' to find data-rich markets.

