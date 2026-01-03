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

    def fetch_markets(self, limit: int = 100, status: str = "settled") -> List[Dict[str, Any]]:
        """Fetch list of markets."""
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
            
            # Save raw batch
            filename = f"markets_cursor_{cursor if cursor else 'start'}.json"
            with open(self.raw_dir / filename, "w") as f:
                json.dump(batch, f)
            
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
                
        return markets[:limit]

    def fetch_market_details(self, ticker: str) -> Dict[str, Any]:
        """Fetch detailed info for a single market."""
        logger.info(f"Fetching Kalshi market details for {ticker}...")
        response = self.client.get(f"/markets/{ticker}")
        data = response.json().get("market", {})
        
        # Save raw detail
        with open(self.raw_dir / f"market_{ticker}.json", "w") as f:
            json.dump(data, f)
            
        return data

    def fetch_trades(self, ticker: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch all trades for a single market ticker."""
        logger.info(f"Fetching Kalshi trades for {ticker} (limit={limit})...")
        all_trades = []
        cursor = ""
        
        while len(all_trades) < limit:
            params = {
                "ticker": ticker,
                "limit": min(1000, limit - len(all_trades))
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

