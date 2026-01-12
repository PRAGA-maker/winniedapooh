"""
Test pipeline idempotency - running twice should produce identical results.
"""
import pytest
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import compare_parquet_datasets


@pytest.mark.slow
def test_parquet_export_determinism(tmp_path):
    """Test that Parquet export is deterministic."""
    from src.build_unified_parquet import CanonicalStore
    
    # Create a test database with sample data
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    # Insert sample markets
    markets = {
        f"M{i}": {
            "source": "kalshi",
            "market_id": f"M{i}",
            "title": f"Market {i}",
            "status": "open",
            "end_time": "2024-12-31T00:00:00"
        }
        for i in range(10)
    }
    
    timeseries = {
        f"M{i}": [
            {
                "source": "kalshi",
                "market_id": f"M{i}",
                "ts": f"2024-12-30T{j:02d}:00:00",
                "belief_scalar": 0.5 + j * 0.01
            }
            for j in range(5)
        ]
        for i in range(10)
    }
    
    store.save_batch("kalshi", markets, timeseries)
    
    # Export to Parquet twice
    output_dir1 = tmp_path / "export1"
    output_dir2 = tmp_path / "export2"
    output_dir1.mkdir()
    output_dir2.mkdir()
    
    store.load_and_write_partitioned(output_dir1)
    store.load_and_write_partitioned(output_dir2)
    
    # Load both exports
    df1 = pd.read_parquet(output_dir1 / "data.parquet")
    df2 = pd.read_parquet(output_dir2 / "data.parquet")
    
    # Compare
    result = compare_parquet_datasets(df1, df2)
    
    print(f"\nIdempotency Test Results:")
    print(f"  Export 1 rows: {result['metrics']['df1_rows']}")
    print(f"  Export 2 rows: {result['metrics']['df2_rows']}")
    print(f"  Identical: {result['identical']}")
    
    if not result['identical']:
        print(f"\nDifferences found:")
        for diff in result['differences']:
            print(f"  - {diff}")
    
    assert result['identical'], "Parquet exports are not identical"


def test_checkpoint_prevents_reprocessing(tmp_path):
    """Test that checkpoints prevent duplicate processing."""
    from src.build_unified_parquet import CanonicalStore
    from datetime import date
    
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    test_date = date(2024, 12, 30)
    
    # First time - not processed
    assert not store.is_date_processed("kalshi", test_date)
    
    # Mark as processed
    store.mark_date_processed("kalshi", test_date)
    
    # Second check - should be processed
    assert store.is_date_processed("kalshi", test_date)
    
    # This simulates the pipeline skipping already-processed dates
    print(f"\nCheckpoint test passed: date {test_date} marked and verified")
