import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    def __init__(self):
        self.repo_root = Path(__file__).parent.parent.parent
        self.data_dir = self.repo_root / "data"
        self.raw_data_dir = self.data_dir / "raw"
        self.clean_data_dir = self.data_dir / "clean"
        self.datasets_dir = self.data_dir / "datasets"
        
        # API URLs
        self.kalshi_base_url = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
        self.metaculus_base_url = os.getenv("METACULUS_BASE_URL", "https://www.metaculus.com")
        
        # Secrets
        self.kalshi_api_key_id = os.getenv("KALSHI_API_KEY_ID")
        self.kalshi_private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
        self.kalshi_private_key = None
        if self.kalshi_private_key_path:
            key_path = Path(self.kalshi_private_key_path)
            if not key_path.is_absolute():
                key_path = self.repo_root / key_path
            if key_path.exists():
                self.kalshi_private_key = key_path.read_text()
        
        self.metaculus_token = os.getenv("METACULUS_TOKEN")
        
        # Concurrency / Rate Limits
        self.kalshi_rate_limit = 20  # req/sec
        
        # Defaults
        self.kalshi_candle_interval = 60  # minutes

config = Config()

