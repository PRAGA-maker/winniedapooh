"""
Pytest configuration and fixtures for Winnie Da Pooh testing.
"""
import pytest
import sys
import pandas as pd
from pathlib import Path
from datetime import date

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.build_unified_parquet import CanonicalStore
from src.common.config import config


@pytest.fixture
def test_date_range():
    """Small date range for fast tests."""
    return date(2024, 12, 30), date(2024, 12, 31)


@pytest.fixture
def clean_test_db(tmp_path):
    """Create a fresh test database."""
    db_path = tmp_path / "test_canonical.db"
    store = CanonicalStore(db_path)
    return store


@pytest.fixture
def latest_dataset_path():
    """Get path to the latest dataset."""
    datasets_dir = Path("data/datasets")
    if not datasets_dir.exists():
        pytest.skip("No datasets directory found")
    
    datasets = sorted(list(datasets_dir.glob("v*_unified")))
    if not datasets:
        pytest.skip("No datasets found. Run build_db.py first.")
    
    return datasets[-1]


@pytest.fixture
def latest_parquet_df(latest_dataset_path):
    """Load the latest dataset as a DataFrame."""
    parquet_path = latest_dataset_path / "data.parquet"
    if not parquet_path.exists():
        pytest.skip("Parquet file not found")
    
    return pd.read_parquet(parquet_path)


@pytest.fixture
def sample_markets(latest_parquet_df):
    """Get a random sample of markets for validation."""
    if len(latest_parquet_df) < 10:
        return latest_parquet_df
    return latest_parquet_df.sample(n=min(50, len(latest_parquet_df)), random_state=42)
