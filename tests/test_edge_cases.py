"""
Edge case testing for boundary conditions and error handling.
"""
import pytest
import sys
from pathlib import Path
from datetime import date, timedelta
import sqlite3

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.build_unified_parquet import CanonicalStore


def test_canonical_store_init(tmp_path):
    """Test CanonicalStore initialization."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    assert db_path.exists(), "Database file not created"
    
    # Check tables exist
    with store._get_conn() as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        
        assert 'markets' in tables
        assert 'history' in tables
        assert 'checkpoints' in tables


def test_checkpoint_system(tmp_path):
    """Test checkpoint system for date processing."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    test_date = date(2024, 12, 30)
    
    # Initially not processed
    assert not store.is_date_processed("kalshi", test_date)
    
    # Mark as processed
    store.mark_date_processed("kalshi", test_date)
    
    # Now should be processed
    assert store.is_date_processed("kalshi", test_date)
    
    # Different date should not be processed
    other_date = date(2024, 12, 31)
    assert not store.is_date_processed("kalshi", other_date)


def test_empty_date_handling(tmp_path):
    """Test handling of dates with no data."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    # Should handle empty batch gracefully
    store.save_batch("kalshi", {}, {})
    
    # Database should still be functional
    with store._get_conn() as conn:
        count = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        assert count == 0


def test_duplicate_market_handling(tmp_path):
    """Test INSERT OR REPLACE behavior for duplicate markets."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    market_id = "TEST-001"
    
    # First insert
    market_record = {
        "source": "kalshi",
        "market_id": market_id,
        "title": "First Title",
        "status": "open"
    }
    store.save_batch("kalshi", {market_id: market_record}, {})
    
    # Second insert with different title
    market_record["title"] = "Updated Title"
    store.save_batch("kalshi", {market_id: market_record}, {})
    
    # Should have only one record with updated title
    with store._get_conn() as conn:
        result = conn.execute(
            "SELECT title FROM markets WHERE source = ? AND market_id = ?",
            ("kalshi", market_id)
        ).fetchone()
        
        assert result[0] == "Updated Title"
        
        count = conn.execute(
            "SELECT COUNT(*) FROM markets WHERE source = ? AND market_id = ?",
            ("kalshi", market_id)
        ).fetchone()[0]
        
        assert count == 1


def test_duplicate_history_point_handling(tmp_path):
    """Test INSERT OR IGNORE behavior for duplicate history points."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    market_id = "TEST-001"
    timestamp = "2024-12-30T12:00:00"
    
    # First insert market
    market_record = {
        "source": "kalshi",
        "market_id": market_id,
        "title": "Test Market",
        "status": "open"
    }
    store.save_batch("kalshi", {market_id: market_record}, {})
    
    # Insert history point
    point = {
        "source": "kalshi",
        "market_id": market_id,
        "ts": timestamp,
        "belief_scalar": 0.5
    }
    store.add_history_points_batch("kalshi", [{"market_id": market_id, "point": point}])
    
    # Insert same point again with different belief
    point2 = {
        "source": "kalshi",
        "market_id": market_id,
        "ts": timestamp,
        "belief_scalar": 0.7  # Different value
    }
    store.add_history_points_batch("kalshi", [{"market_id": market_id, "point": point2}])
    
    # Should still have only one point (INSERT OR IGNORE keeps first)
    with store._get_conn() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM history WHERE source = ? AND market_id = ? AND ts = ?",
            ("kalshi", market_id, timestamp)
        ).fetchone()[0]
        
        assert count == 1
        
        # Should have the original value (INSERT OR IGNORE doesn't update)
        belief = conn.execute(
            "SELECT belief_scalar FROM history WHERE source = ? AND market_id = ? AND ts = ?",
            ("kalshi", market_id, timestamp)
        ).fetchone()[0]
        
        assert belief == 0.5  # Original value preserved


def test_get_existing_market_statuses(tmp_path):
    """Test retrieval of existing market statuses."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    # Insert markets with different statuses
    markets = {
        "M1": {"source": "kalshi", "market_id": "M1", "title": "Market 1", "status": "open"},
        "M2": {"source": "kalshi", "market_id": "M2", "title": "Market 2", "status": "closed"},
        "M3": {"source": "kalshi", "market_id": "M3", "title": "Market 3", "status": "resolved"},
    }
    store.save_batch("kalshi", markets, {})
    
    # Get statuses
    statuses = store.get_existing_market_statuses("kalshi")
    
    assert statuses["M1"] == "open"
    assert statuses["M2"] == "closed"
    assert statuses["M3"] == "resolved"
    assert len(statuses) == 3


def test_batch_save_performance(tmp_path):
    """Test batch save handles multiple markets efficiently."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    # Create 100 test markets
    markets = {}
    timeseries = {}
    
    for i in range(100):
        market_id = f"TEST-{i:03d}"
        markets[market_id] = {
            "source": "kalshi",
            "market_id": market_id,
            "title": f"Test Market {i}",
            "status": "open"
        }
        
        # Add 10 history points per market
        timeseries[market_id] = [
            {
                "source": "kalshi",
                "market_id": market_id,
                "ts": f"2024-12-30T{j:02d}:00:00",
                "belief_scalar": 0.5 + j * 0.01
            }
            for j in range(10)
        ]
    
    # Should complete without errors
    store.save_batch("kalshi", markets, timeseries)
    
    # Verify counts
    with store._get_conn() as conn:
        market_count = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        history_count = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
        
        assert market_count == 100
        assert history_count == 1000  # 100 markets * 10 points


def test_exists_method(tmp_path):
    """Test the exists() method."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    market_id = "TEST-001"
    
    # Should not exist initially
    assert not store.exists("kalshi", market_id)
    
    # Insert market
    market_record = {
        "source": "kalshi",
        "market_id": market_id,
        "title": "Test Market",
        "status": "open"
    }
    store.save_batch("kalshi", {market_id: market_record}, {})
    
    # Now should exist
    assert store.exists("kalshi", market_id)
    
    # Different source should not exist
    assert not store.exists("metaculus", market_id)


def test_get_existing_market_ids(tmp_path):
    """Test retrieval of all market IDs for a source."""
    db_path = tmp_path / "test.db"
    store = CanonicalStore(db_path)
    
    # Insert multiple markets for different sources
    kalshi_markets = {
        "K1": {"source": "kalshi", "market_id": "K1", "title": "Kalshi 1", "status": "open"},
        "K2": {"source": "kalshi", "market_id": "K2", "title": "Kalshi 2", "status": "open"},
    }
    metaculus_markets = {
        "M1": {"source": "metaculus", "market_id": "M1", "title": "Metaculus 1", "status": "open"},
    }
    
    store.save_batch("kalshi", kalshi_markets, {})
    store.save_batch("metaculus", metaculus_markets, {})
    
    # Get Kalshi IDs
    kalshi_ids = store.get_existing_market_ids("kalshi")
    assert set(kalshi_ids) == {"K1", "K2"}
    
    # Get Metaculus IDs
    metaculus_ids = store.get_existing_market_ids("metaculus")
    assert set(metaculus_ids) == {"M1"}
