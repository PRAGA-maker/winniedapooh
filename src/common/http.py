import time
import base64
import random
import requests
from threading import Lock, Semaphore
from typing import Optional, Any, Dict, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, utils
from cryptography.hazmat.primitives import serialization
from src.common.config import config
from src.common.logging import logger

class KalshiAuth(requests.auth.AuthBase):
    def __init__(self, api_key_id: str, private_key_str: str):
        self.api_key_id = api_key_id
        # Handle the case where the private key might be passed with literal \n
        private_key_str = private_key_str.replace("\\n", "\n")
        self.private_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=None
        )

    def __call__(self, r):
        # Kalshi V2 uses milliseconds for timestamp
        timestamp = str(int(time.time() * 1000))
        method = r.method.upper()
        
        # Parse path and query from URL
        from urllib.parse import urlparse
        parsed_url = urlparse(r.url)
        path = parsed_url.path
        if parsed_url.query:
            path += "?" + parsed_url.query
            
        body = r.body if r.body else ""
        if isinstance(body, bytes):
            body = body.decode('utf-8')
            
        # V2 msg format: timestamp + method + path
        msg = timestamp + method + path
        
        signature = self.private_key.sign(
            msg.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        
        encoded_signature = base64.b64encode(signature).decode()
        
        r.headers['KALSHI-ACCESS-KEY'] = self.api_key_id
        r.headers['KALSHI-ACCESS-TIMESTAMP'] = timestamp
        r.headers['KALSHI-ACCESS-SIGNATURE'] = encoded_signature
        return r

class HttpClient:
    def __init__(self, base_url: str, auth: Optional[requests.auth.AuthBase] = None, delay: float = 0, max_concurrency: int = 1):
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self.session = requests.Session()
        # Increase connection pool size for concurrency
        adapter = requests.adapters.HTTPAdapter(pool_connections=max_concurrency, pool_maxsize=max_concurrency)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.delay = delay
        self.last_request_time = 0
        self.lock = Lock()
        self.semaphore = Semaphore(max_concurrency)

    def _wait_for_rate_limit(self):
        if self.delay <= 0:
            return
        
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.delay:
                wait_time = self.delay - elapsed
                time.sleep(wait_time)
                self.last_request_time = time.time()
            else:
                self.last_request_time = now

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        max_retries = 5
        base_backoff = 1.0
        
        for i in range(max_retries):
            # Enforce delay and concurrency limit
            with self.semaphore:
                self._wait_for_rate_limit()
                try:
                    response = self.session.request(method, url, auth=self.auth, **kwargs)
                    
                    if response.status_code == 429:
                        # Exponential backoff with jitter
                        wait_time = (base_backoff * (2 ** i)) + random.uniform(0, 1.0)
                        logger.warning(f"Rate limited (429) on {path}. Retrying in {wait_time:.2f}s (attempt {i+1}/{max_retries})...")
                        time.sleep(wait_time)
                        continue
                    
                    if response.status_code == 400:
                        logger.error(f"Bad Request (400) for {path}: {response.text}")
                    
                    response.raise_for_status()
                    return response
                    
                except requests.exceptions.RequestException as e:
                    if i == max_retries - 1:
                        logger.error(f"Request to {path} failed after {max_retries} retries: {e}")
                        raise
                    
                    # Retry on 5xx or connection issues
                    should_retry = True
                    if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                        if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                            should_retry = False
                    
                    if should_retry:
                        wait_time = (base_backoff * (2 ** i)) + random.uniform(0, 1.0)
                        logger.warning(f"Request to {path} failed: {e}. Retrying in {wait_time:.2f}s...")
                        time.sleep(wait_time)
                    else:
                        raise
            
        raise requests.exceptions.RequestException(f"Request to {path} failed after all retries")

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self.request("POST", path, **kwargs)

    def batch_get(self, paths: List[str], max_workers: int = 10, **kwargs) -> List[Optional[requests.Response]]:
        """Fetch multiple paths in parallel with a limited number of workers."""
        results = [None] * len(paths)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {executor.submit(self.get, path, **kwargs): i for i, path in enumerate(paths)}
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    logger.error(f"Batch request failed for {paths[idx]}: {e}")
                    results[idx] = None
        return results

def get_kalshi_client():
    if not config.kalshi_api_key_id or not config.kalshi_private_key:
        logger.warning("Kalshi API credentials not found in environment.")
        # Safe default: 10 req/s
        return HttpClient(config.kalshi_base_url, delay=0.1, max_concurrency=5)
    
    auth = KalshiAuth(config.kalshi_api_key_id, config.kalshi_private_key)
    # OPTIMIZATION: 20 req/s limit. We use 18 max concurrency and a 0.06s delay
    # to stay just under the ceiling (16.6 req/s) while avoiding burst-driven 429s.
    return HttpClient(config.kalshi_base_url, auth=auth, delay=0.06, max_concurrency=18)

def get_metaculus_client():
    headers = {}
    if config.metaculus_token:
        headers["Authorization"] = f"Token {config.metaculus_token}"
    
    # 1000 requests per hour = 3.6s delay between requests
    # Metaculus is strict, keep it sequential (max_concurrency=1)
    client = HttpClient(config.metaculus_base_url, delay=3.6, max_concurrency=1)
    client.session.headers.update(headers)
    return client

# --- LESSONS LEARNED ---
# 1. Kalshi V2 Auth: Use KALSHI-ACCESS-KEY/TIMESTAMP/SIGNATURE headers.
# 2. Signing: V2 requires RSA-PSS with MGF1-SHA256 and salt length = hash length.
# 3. Message format: timestamp + method + path (no body for GET).
# 4. Metaculus Rate Limit: They are strict. 1000 req/hr = ~3.6s delay. 
#    Always use a delay in HttpClient for Metaculus to avoid 429s.
# 5. Rate Limit Jitter: Adding random jitter to retries helps avoid synchronized 
#    "thundering herd" spikes when multiple parallel workers hit a rate limit.
# 6. Session Reuse: Reusing a single requests.Session across multiple calls 
#    is significantly faster due to TCP connection pooling.

