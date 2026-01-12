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
        """Fetch multiple markets by their tickers in batches."""
        logger.info(f"Fetching {len(tickers)} Kalshi markets by tickers in batches...")
        all_markets = []
        
        # OPTIMIZATION: Reduced from 100 to 50 to avoid 413 "Request Entity Too Large" errors
        # with very long ticker names (e.g., KXCITIESWEATHER markets)
        batch_size = 50
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]
            tickers_str = ",".join(batch_tickers)
            
            try:
                response = self.client.get("/markets", params={"tickers": tickers_str})
                data = response.json()
                batch_results = data.get("markets", [])
                all_markets.extend(batch_results)
                
                if (i // batch_size) % 20 == 0:
                    logger.info(f"  Fetched {len(all_markets)}/{len(tickers)} market records...")
            except Exception as e:
                logger.error(f"Failed to fetch batch starting at {i}: {e}")
                # Continue to next batch even if this one fails
                
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
# 4. Batch Size Optimization: Reduced from 100 to 50 tickers/batch to avoid 413 
#    "Request Entity Too Large" errors. Some tickers (KXCITIESWEATHER markets) have 
#    extremely long names that exceed URL length limits when batched in 100s. Even at 50, 
#    some batches with very long ticker combinations can still hit 413 - these are logged 
#    as errors and processing continues with other batches.
# 5. Error Handling: Continue processing even if individual batches fail to ensure 
#    partial progress rather than total failure on problematic ticker combinations.
# 6. Rate Limiting: Kalshi API is 20 req/s. Despite optimization to skip 92% of markets, 
#    we still hit 429 errors frequently due to bursty parallel requests. Rate limiting 
#    uses exponential backoff with jitter (1-2s wait on 429). Batch size reduction helps 
#    avoid 413 but doesn't solve 429 - that's inherent to the 20 req/s limit.
# 7. API Call Efficiency: Before optimization: 49K markets = 490 API calls (100 tickers/batch). 
#    After: 3.8K active markets = ~77 API calls (50 tickers/batch). 92% reduction in markets 
#    needing API, but batch size reduction means only ~84% reduction in actual API calls.
# 8. Ticker Name Length: KXCITIESWEATHER markets have extremely long ticker names (100+ chars) 
#    with URL encoding. Example: "KXCITIESWEATHER-24DEC24(AUS)(CHI)(DEN)(HOU)(MIA)(NY)(PHIL)-(T73)(T36)(T48)(T69)(T75)(T34)(T35)". 
#    When batched, these easily exceed typical URL length limits (8KB).
# 9. Progress Logging: Log every 20 batches (1000 markets) to avoid log spam. At 50 tickers/batch, 
#    this means progress every ~1 second during API enrichment phase.
# 10. Response Format: /markets?tickers=... returns {"markets": [...]}. Empty results return 
#     {"markets": []} (not an error). Missing tickers are silently omitted from response.
# 11. Metadata Quality: API returns rich metadata: title, description, expiration_time, 
#     open_time, event_ticker, result, category. This is essential - S3 has none of this.
# 12. Concurrent Requests: HttpClient uses max_concurrency=20 and delay=0.055s (18.2 req/s) 
#     to stay under 20 req/s limit. Despite this, 429s occur due to network jitter and 
#     burst patterns. The semaphore+delay combination helps but doesn't eliminate 429s entirely.

