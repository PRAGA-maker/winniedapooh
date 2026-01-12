import json
import os
from typing import List, Dict, Any, Optional
from src.common.http import get_metaculus_client
from src.common.config import config
from src.common.logging import logger

class MetaculusGrabber:
    def __init__(self):
        self.client = get_metaculus_client()

    def fetch_posts(self, limit: int = 1000, offset: int = 0, status: str = None, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch list of posts (which contain questions)."""
        logger.info(f"Fetching Metaculus posts (limit={limit}, offset={offset}, status={status})...")
        posts = []
        current_offset = offset
        
        while len(posts) < limit:
            params = {
                "limit": min(100, limit - len(posts)),
                "offset": current_offset,
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
            
            if not data.get("next") or not batch:
                break
            current_offset += len(batch)
                
        return posts[:limit]

    def fetch_post_detail(self, post_id: int, use_cache: bool = True) -> Dict[str, Any]:
        """Fetch detailed info for a single post with history."""
        logger.info(f"Fetching Metaculus post details for {post_id}...")
        params = {"include_cp_history": "true"}
        response = self.client.get(f"/api/posts/{post_id}/", params=params)
        data = response.json()
        return data

    def fetch_prediction_history(self, q_id: int, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Fetch prediction history for a single question."""
        logger.info(f"Fetching Metaculus prediction history for {q_id}...")
        response = self.client.get(f"/api2/questions/{q_id}/prediction-history/")
        data = response.json()
        return data

# --- NOTES ---
# Key Rotation: This grabber uses get_metaculus_client() which automatically handles API key rotation.
# Multiple keys can be configured via METACULUS_TOKEN_1, METACULUS_TOKEN_2, etc. in .env.
# The rotation system (in http.py) automatically switches keys on rate limits (429) and preemptively
# rotates at 90% usage threshold. With 2 keys, expect ~2x speedup (99.7% efficiency).
# See http.py LESSONS LEARNED section for detailed implementation notes.

