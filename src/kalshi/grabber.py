import json
import os
from typing import List, Dict, Any, Optional
from src.common.http import get_kalshi_client
from src.common.config import config
from src.common.logging import logger

class KalshiGrabber:
    def __init__(self):
        self.client = get_kalshi_client()

    def fetch_markets(self, limit: int = 1000, status: str = "settled", use_cache: bool = True, ticker: Optional[str] = None, tickers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Fetch list of markets."""
        if ticker:
            return [self.fetch_market_details(ticker, use_cache=use_cache)]
        if tickers:
            return self.fetch_markets_by_tickers(tickers)
            
        logger.info(f"Fetching Kalshi markets (limit={limit}, status={status})...")
        markets = []
        cursor = ""
        
        while len(markets) < limit:
            params = {"limit": min(100, limit - len(markets))}
            if cursor:
                params["cursor"] = cursor
            if status:
                params["status"] = status
            
            response = self.client.get("/markets", params=params)
            data = response.json()
            
            batch = data.get("markets", [])
            markets.extend(batch)
            
            if len(markets) % 1000 == 0:
                logger.info(f"  Downloaded {len(markets)} markets metadata...")
            
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
                
        return markets[:limit]

    def fetch_markets_by_tickers(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """Fetch multiple markets by their tickers in batches of 100."""
        logger.info(f"Fetching {len(tickers)} Kalshi markets by tickers in batches...")
        all_markets = []
        
        # Kalshi allows up to 100 tickers per request
        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]
            tickers_str = ",".join(batch_tickers)
            
            try:
                response = self.client.get("/markets", params={"tickers": tickers_str})
                data = response.json()
                batch_results = data.get("markets", [])
                all_markets.extend(batch_results)
                
                if (i // batch_size) % 10 == 0:
                    logger.info(f"  Fetched {len(all_markets)}/{len(tickers)} market records...")
            except Exception as e:
                logger.error(f"Failed to fetch batch starting at {i}: {e}")
                
        return all_markets

    def fetch_market_details(self, ticker: str, use_cache: bool = True) -> Dict[str, Any]:
        """Fetch detailed info for a single market."""
        logger.info(f"Fetching Kalshi market details for {ticker}...")
        response = self.client.get(f"/markets/{ticker}")
        data = response.json().get("market", {})
        return data

# --- LESSONS LEARNED ---
# 1. Hybrid Approach: Using API for metadata and Bulk S3 for historical time-series 
#    is the most efficient way to scale Kalshi data collection.
# 2. Endpoint Params: /markets uses 'status' (open, closed, settled).
# 3. Volume: Use 'settled' or 'closed' to find data-rich historical markets.

