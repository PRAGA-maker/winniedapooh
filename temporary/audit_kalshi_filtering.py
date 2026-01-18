#!/usr/bin/env python
"""
Audit Kalshi S3 filtering logic to identify if valuable markets are being excluded.

The filtering logic in bulk_grabber.py:253-258 keeps only markets where:
  max_vol > 0 OR max_oi > 0 OR last_status IN ('finalized', 'determined', 'settled')

This script analyzes what gets filtered out and whether it's appropriate.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.kalshi.bulk_grabber import KalshiBulkGrabber
from datetime import date
import sqlite3
import tempfile
import time

def analyze_filtering(start_date: date, end_date: date):
    """Analyze what the S3 filtering excludes."""
    print("Kalshi S3 Filtering Audit")
    print("=" * 80)
    print(f"Date range: {start_date} to {end_date}")
    print()
    
    bulk_grabber = KalshiBulkGrabber()
    
    # Scan one day to get vitals data
    print(f"Scanning {start_date} to collect market data...")
    
    # Create temporary database to track ALL tickers (not just active)
    temp_db_path = Path(tempfile.gettempdir()) / f"kalshi_audit_{int(time.time())}.db"
    conn = sqlite3.connect(temp_db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-100000")  # 100MB cache
    
    conn.execute("""
        CREATE TABLE vitals (
            ticker TEXT PRIMARY KEY,
            max_vol REAL,
            max_oi REAL,
            first_date TEXT,
            last_date TEXT,
            last_status TEXT,
            report_ticker TEXT,
            payout_type TEXT,
            has_price_history INTEGER DEFAULT 0,
            max_price REAL,
            min_price REAL,
            price_points INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    
    # Process the day and collect vitals
    try:
        for raw_record in bulk_grabber.fetch_daily_bulk_stream(start_date):
            ticker = raw_record.get("ticker_name")
            if not ticker:
                continue
            
            vol = float(raw_record.get("daily_volume", 0) or 0)
            oi = float(raw_record.get("open_interest", 0) or 0)
            status = raw_record.get("status", "unknown")
            high = float(raw_record.get("high", 0) or 0)
            low = float(raw_record.get("low", 0) or 0)
            
            # Check if there's price movement
            has_price = (high > 0 or low > 0)
            
            conn.execute("""
                INSERT OR REPLACE INTO vitals 
                (ticker, max_vol, max_oi, first_date, last_date, last_status, 
                 report_ticker, payout_type, has_price_history, max_price, min_price, price_points)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(ticker) DO UPDATE SET
                    max_vol = MAX(max_vol, excluded.max_vol),
                    max_oi = MAX(max_oi, excluded.max_oi),
                    last_date = MAX(last_date, excluded.last_date),
                    last_status = CASE WHEN excluded.last_date >= last_date 
                                  THEN excluded.last_status ELSE last_status END,
                    has_price_history = MAX(has_price_history, excluded.has_price_history),
                    max_price = MAX(max_price, excluded.max_price),
                    min_price = CASE WHEN min_price IS NULL THEN excluded.min_price
                                     ELSE MIN(min_price, excluded.min_price) END,
                    price_points = price_points + 1
            """, (ticker, vol, oi, start_date.isoformat(), start_date.isoformat(),
                  status, raw_record.get("report_ticker"), raw_record.get("payout_type"),
                  1 if has_price else 0, high, low))
        
        conn.commit()
        
        # Analyze the data
        print("\nAnalyzing filtering results...")
        print()
        
        # Total tickers
        total_tickers = conn.execute("SELECT COUNT(*) FROM vitals").fetchone()[0]
        print(f"Total unique tickers found: {total_tickers}")
        
        # Current filtering logic (what gets KEPT)
        kept_query = """
            SELECT COUNT(*) FROM vitals 
            WHERE max_vol > 0 
               OR max_oi > 0 
               OR last_status IN ('finalized', 'determined', 'settled')
        """
        kept_count = conn.execute(kept_query).fetchone()[0]
        filtered_count = total_tickers - kept_count
        
        print(f"Markets KEPT by current filter: {kept_count} ({kept_count/total_tickers*100:.1f}%)")
        print(f"Markets FILTERED OUT: {filtered_count} ({filtered_count/total_tickers*100:.1f}%)")
        print()
        
        # Analyze what gets filtered out
        print("=" * 80)
        print("FILTERED OUT MARKET ANALYSIS")
        print("=" * 80)
        print()
        
        filtered_query = """
            SELECT * FROM vitals 
            WHERE max_vol = 0 
              AND max_oi = 0 
              AND last_status NOT IN ('finalized', 'determined', 'settled')
        """
        
        filtered_markets = conn.execute(filtered_query).fetchall()
        
        if not filtered_markets:
            print("No markets filtered out! All markets pass the filter.")
            return
        
        # Count by status
        status_counts = {}
        has_price_count = 0
        for market in filtered_markets:
            status = market[5]  # last_status column
            status_counts[status] = status_counts.get(status, 0) + 1
            if market[8]:  # has_price_history
                has_price_count += 1
        
        print(f"Filtered markets by status:")
        for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
            print(f"  {status}: {count} ({count/len(filtered_markets)*100:.1f}%)")
        print()
        
        print(f"Filtered markets WITH price history: {has_price_count} ({has_price_count/len(filtered_markets)*100:.1f}%)")
        print(f"Filtered markets WITHOUT price history: {len(filtered_markets)-has_price_count}")
        print()
        
        # Check for markets with price movement but no volume
        price_no_volume_query = """
            SELECT COUNT(*) FROM vitals 
            WHERE max_vol = 0 
              AND max_oi = 0 
              AND last_status NOT IN ('finalized', 'determined', 'settled')
              AND has_price_history = 1
        """
        price_no_vol_count = conn.execute(price_no_volume_query).fetchone()[0]
        
        # Check for markets with MEANINGFUL price variation (not just 50-50 defaults)
        meaningful_price_query = """
            SELECT COUNT(*) FROM vitals 
            WHERE max_vol = 0 
              AND max_oi = 0 
              AND last_status NOT IN ('finalized', 'determined', 'settled')
              AND has_price_history = 1
              AND (max_price != 50.0 OR min_price != 50.0)
        """
        meaningful_price_count = conn.execute(meaningful_price_query).fetchone()[0]
        
        if price_no_vol_count > 0:
            print(f"Found {price_no_vol_count} markets with price history but ZERO volume/OI")
            print(f"  Of these, {meaningful_price_count} have MEANINGFUL price variation (not 50-50 defaults)")
            print(f"  And {price_no_vol_count - meaningful_price_count} are just default 50-50 prices")
            print()
            
            if meaningful_price_count > 0:
                print(f"[ALERT] {meaningful_price_count} markets have real belief updates without trades!")
                print(f"        Filter may be incorrectly excluding valuable data.")
                print()
                
                # Sample meaningful ones
                sample_query = """
                    SELECT ticker, max_price, min_price, price_points, last_status 
                    FROM vitals 
                    WHERE max_vol = 0 
                      AND max_oi = 0 
                      AND last_status NOT IN ('finalized', 'determined', 'settled')
                      AND has_price_history = 1
                      AND (max_price != 50.0 OR min_price != 50.0)
                    LIMIT 10
                """
                samples = conn.execute(sample_query).fetchall()
                if samples:
                    print("Sample markets with meaningful price variation but no volume:")
                    for s in samples:
                        print(f"  {s[0]}: price range [{s[2]:.2f}, {s[1]:.2f}], points={s[3]}, status={s[4]}")
                    print()
            else:
                print(f"[OK] All {price_no_vol_count} markets are just 50-50 defaults (no real forecasts).")
                print()
            
            # Also sample some default-only markets to confirm
            default_sample_query = """
                SELECT ticker, max_price, min_price, last_status 
                FROM vitals 
                WHERE max_vol = 0 
                  AND max_oi = 0 
                  AND last_status NOT IN ('finalized', 'determined', 'settled')
                  AND has_price_history = 1
                  AND max_price = 50.0 AND min_price = 50.0
                LIMIT 5
            """
            default_samples = conn.execute(default_sample_query).fetchall()
            if default_samples:
                print("Sample markets with only default 50-50 prices:")
                for s in default_samples:
                    print(f"  {s[0]}: price=[{s[2]:.2f}, {s[1]:.2f}], status={s[3]}")
                print()
        else:
            print("[OK] No markets with price history are being filtered out.")
            print()
        
        # Check for open/active markets being filtered
        open_filtered_query = """
            SELECT COUNT(*) FROM vitals 
            WHERE max_vol = 0 
              AND max_oi = 0 
              AND last_status IN ('active', 'open')
        """
        open_filtered_count = conn.execute(open_filtered_query).fetchone()[0]
        
        if open_filtered_count > 0:
            print(f"[WARNING] {open_filtered_count} OPEN/ACTIVE markets have zero volume/OI")
            print(f"          These are newly created markets that haven't traded yet.")
            print(f"          Filter is excluding them (may be appropriate).")
            print()
        
        # Recommendation
        print("=" * 80)
        print("RECOMMENDATION")
        print("=" * 80)
        print()
        
        if meaningful_price_count > 0:
            print("[ACTION NEEDED] The filter is too aggressive!")
            print()
            print(f"There are {meaningful_price_count} markets with real price variation but zero volume.")
            print("These markets have belief updates without trades and should be included.")
            print()
            print("Suggested fix: Add price variation check to the filter:")
            print()
            print("  WHERE max_vol > 0")
            print("     OR max_oi > 0")
            print("     OR last_status IN ('finalized', 'determined', 'settled')")
            print("     OR (has_price_history = 1 AND price_variation > 0)  -- NEW")
            print()
            print(f"This would rescue {meaningful_price_count} markets with valuable forecasting data.")
            print(f"While correctly excluding {price_no_vol_count - meaningful_price_count} default-only markets.")
        else:
            print("[OK] Current filtering is APPROPRIATE!")
            print()
            print("All filtered markets are genuinely inactive:")
            print("- Zero volume and open interest")
            print("- Only default 50-50 prices (no real forecasts)")
            print("- Not finalized/settled")
            print()
            print("These markets have no forecasting value.")
            print()
            print(f"The filter correctly excludes {price_no_vol_count} newly-created markets")
            print("that haven't received any actual forecasts yet.")
        
    finally:
        conn.close()
        try:
            temp_db_path.unlink()
        except:
            pass

if __name__ == "__main__":
    # Test with a single day first
    test_date = date(2024, 12, 30)
    analyze_filtering(test_date, test_date)
