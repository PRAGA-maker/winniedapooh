import json
import os
from typing import List, Dict, Any, Optional
from src.common.http import get_kalshi_client
from src.common.config import config
from src.common.logging import logger

class KalshiGrabber:
    def __init__(self):
        self.client = get_kalshi_client()

    def fetch_markets(self, limit: int = 1000, status: str = "settled", use_cache: bool = True, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch list of markets."""
        if ticker:
            return [self.fetch_market_details(ticker, use_cache=use_cache)]
        logger.info(f"Fetching Kalshi markets (limit={limit}, status={status}, ticker={ticker})...")
        markets = []
        cursor = ""
        
        while len(markets) < limit:
            params = {"limit": min(100, limit - len(markets))}
            if cursor:
                params["cursor"] = cursor
            if status:
                params["status"] = status
            if ticker:
                params["ticker"] = ticker
            
            response = self.client.get("/markets", params=params)
            data = response.json()
            
            batch = data.get("markets", [])
            markets.extend(batch)
            
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
                
        return markets[:limit]

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

