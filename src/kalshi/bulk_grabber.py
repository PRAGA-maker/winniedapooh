import requests
import json
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Iterator
from pathlib import Path
from src.common.schema import TimeSeriesPoint
from src.common.logging import logger
from src.common.config import config

class KalshiBulkGrabber:
    def __init__(self):
        self.base_url = "https://kalshi-public-docs.s3.amazonaws.com/reporting"
        self.cache_dir = config.raw_data_dir / "kalshi_bulk"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_url_for_date(self, target_date: date) -> str:
        date_str = target_date.strftime("%Y-%m-%d")
        return f"{self.base_url}/market_data_{date_str}.json"

    def fetch_daily_bulk_stream(self, target_date: date, retries: int = 5) -> Iterator[Dict[str, Any]]:
        """
        Fetch and stream market data records for a specific date from Kalshi's S3 bucket.
        Uses Resumable Range Requests to handle connection resets without restarting the download.
        """
        url = self._get_url_for_date(target_date)
        bytes_received = 0
        buffer = ""
        
        for attempt in range(retries):
            # If we've already received bytes, request only the remaining part
            headers = {}
            if bytes_received > 0:
                headers["Range"] = f"bytes={bytes_received}-"
                logger.info(f"Resuming {target_date} from byte {bytes_received} (Attempt {attempt + 1}/{retries})")
            else:
                logger.info(f"Streaming bulk data for {target_date} from {url} (Attempt {attempt + 1}/{retries})")
            
            try:
                # Use a session for better connection pooling
                with requests.get(url, stream=True, headers=headers, timeout=60) as response:
                    # 206 is the status for Partial Content when using Range
                    if response.status_code not in [200, 206]:
                        response.raise_for_status()
                    
                    for chunk in response.iter_content(chunk_size=1024*1024): # 1MB chunks
                        if not chunk:
                            continue
                        
                        bytes_received += len(chunk)
                        buffer += chunk.decode('utf-8', errors='ignore')
                        
                        while True:
                            start_idx = buffer.find('{')
                            if start_idx == -1:
                                buffer = ""
                                break
                            
                            end_idx = buffer.find('},', start_idx)
                            if end_idx == -1:
                                # Look for the very last object in the array
                                end_idx = buffer.find('}\n]', start_idx)
                                if end_idx == -1:
                                    # Incomplete object, keep in buffer and wait for more chunks
                                    break
                                else:
                                    end_idx += 1 
                            else:
                                end_idx += 1 
                            
                            obj_str = buffer[start_idx:end_idx]
                            try:
                                yield json.loads(obj_str)
                            except json.JSONDecodeError:
                                # This might happen if the slice was cut mid-string
                                # We'll skip it for now or try to recover
                                pass
                            
                            buffer = buffer[end_idx:]
                            if buffer.startswith(','):
                                buffer = buffer[1:]
                            buffer = buffer.strip()
                    
                    # If we finish the chunk loop, the file is fully downloaded
                    return

            except (requests.exceptions.RequestException, ConnectionError) as e:
                logger.warning(f"Connection dropped for {target_date} at {bytes_received} bytes: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to finish {target_date} after {retries} attempts.")
                    raise

    def map_to_timeseries(self, raw_record: Dict[str, Any]) -> TimeSeriesPoint:
        """Map a raw bulk record to a canonical TimeSeriesPoint."""
        # The date in the bulk record is YYYY-MM-DD
        ts_date = datetime.fromisoformat(raw_record["date"])
        # We'll set the time to EOD for these daily snapshots
        ts = ts_date.replace(hour=23, minute=59, second=59)
        
        # Prices are in cents (0-100)
        high = float(raw_record.get("high", 0))
        low = float(raw_record.get("low", 0))
        mid_price = (high + low) / 2.0
        belief_scalar = mid_price / 100.0
        
        return TimeSeriesPoint(
            source="kalshi_bulk",
            market_id=raw_record["ticker_name"],
            ts=ts,
            belief_scalar=belief_scalar,
            volume=float(raw_record.get("daily_volume", 0)),
            open_interest=float(raw_record.get("open_interest", 0)),
            raw_json=json.dumps(raw_record)
        )

    def fetch_history_for_range(self, start_date: date, end_date: date, ticker_limit: Optional[int] = None) -> Dict[str, List[TimeSeriesPoint]]:
        """
        Fetch and aggregate market history across a date range.
        Returns a dict of market_id -> list of TimeSeriesPoint.
        """
        history = {}
        current_date = start_date
        
        while current_date <= end_date:
            logger.info(f"Processing date: {current_date}")
            records_count = 0
            for record in self.fetch_daily_bulk_stream(current_date):
                ticker = record["ticker_name"]
                
                # If we have a ticker limit and this is a new ticker, check if we should add it
                if ticker_limit and ticker not in history and len(history) >= ticker_limit:
                    continue
                
                try:
                    point = self.map_to_timeseries(record)
                    if ticker not in history:
                        history[ticker] = []
                    history[ticker].append(point)
                    records_count += 1
                except Exception as e:
                    logger.debug(f"Failed to map record for {ticker}: {e}")
            
            logger.info(f"Finished {current_date}: found {records_count} records.")
            current_date += timedelta(days=1)
            
        return history

