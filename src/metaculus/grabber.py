import json
import os
from typing import List, Dict, Any, Optional
from src.common.http import get_metaculus_client
from src.common.config import config
from src.common.logging import logger

class MetaculusGrabber:
    def __init__(self):
        self.client = get_metaculus_client()
        self.raw_dir = config.raw_data_dir / "metaculus"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def fetch_posts(self, limit: int = 1000, status: str = None, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch list of posts (which contain questions)."""
        logger.info(f"Fetching Metaculus posts (limit={limit}, status={status})...")
        posts = []
        offset = 0
        
        while len(posts) < limit:
            filename = f"posts_offset_{offset}.json"
            cache_path = self.raw_dir / filename
            
            if use_cache and cache_path.exists():
                logger.debug(f"Loading Metaculus posts from cache: {filename}")
                with open(cache_path, "r") as f:
                    batch = json.load(f)
                posts.extend(batch)
                offset += len(batch)
                continue

            params = {
                "limit": min(100, limit - len(posts)),
                "offset": offset,
                "include_cp_history": "true",
                "include_descriptions": "true",
            }
            if status:
                params["status"] = status
            
            # Using /api/posts/ as it seems more robust for detail
            response = self.client.get("/api/posts/", params=params)
            data = response.json()
            
            batch = data.get("results", [])
            posts.extend(batch)
            
            # Save raw batch
            with open(cache_path, "w") as f:
                json.dump(batch, f)
            
            if not data.get("next") or not batch:
                break
            offset += len(batch)
                
        return posts[:limit]

    def fetch_post_detail(self, post_id: int, use_cache: bool = True) -> Dict[str, Any]:
        """Fetch detailed info for a single post with history."""
        cache_path = self.raw_dir / f"post_{post_id}.json"
        if use_cache and cache_path.exists():
            with open(cache_path, "r") as f:
                return json.load(f)

        logger.info(f"Fetching Metaculus post details for {post_id}...")
        params = {"include_cp_history": "true"}
        response = self.client.get(f"/api/posts/{post_id}/", params=params)
        data = response.json()
        
        # Save raw detail
        with open(cache_path, "w") as f:
            json.dump(data, f)
            
        return data

    def fetch_prediction_history(self, q_id: int, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch prediction history for a single question."""
        cache_path = self.raw_dir / f"history_{q_id}.json"
        if use_cache and cache_path.exists():
            with open(cache_path, "r") as f:
                return json.load(f)

        logger.info(f"Fetching Metaculus prediction history for {q_id}...")
        response = self.client.get(f"/api2/questions/{q_id}/prediction-history/")
        data = response.json()
        
        # Save raw history
        with open(cache_path, "w") as f:
            json.dump(data, f)
            
        return data

