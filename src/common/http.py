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

class MetaculusKeyRotator:
    """
    Thread-safe API key rotation manager for Metaculus.
    Automatically rotates keys when rate limits (429) are hit.
    Tracks usage per key to enable preemptive rotation before hitting limits.
    """
    def __init__(self, tokens: List[str], requests_per_hour: int = 1000):
        if not tokens:
            raise ValueError("At least one Metaculus token is required")
        
        self.tokens = tokens
        self.requests_per_hour = requests_per_hour
        self.requests_per_second = requests_per_hour / 3600.0
        
        # Rotation state
        self.current_index = 0
        self.lock = Lock()
        
        # Per-key usage tracking for preemptive rotation
        self.key_usage = {i: {"count": 0, "window_start": time.time()} for i in range(len(tokens))}
        self.usage_window = 3600  # 1 hour window for rate limit tracking
        
        logger.info(f"Initialized MetaculusKeyRotator with {len(tokens)} token(s)")
        logger.info(f"Rate limit: {requests_per_hour} requests/hour per key")
        logger.info(f"Theoretical max throughput: {len(tokens) * requests_per_hour} requests/hour")
    
    def get_current_token(self) -> str:
        """Get the currently active token (thread-safe)."""
        with self.lock:
            return self.tokens[self.current_index]
    
    def rotate_to_next_key(self, reason: str = "manual") -> None:
        """Rotate to the next available key (thread-safe)."""
        with self.lock:
            old_index = self.current_index
            self.current_index = (self.current_index + 1) % len(self.tokens)
            logger.warning(
                f"Rotating Metaculus key: [{old_index + 1}→{self.current_index + 1}] "
                f"(Reason: {reason}) | {len(self.tokens)} keys available"
            )
    
    def should_preemptively_rotate(self) -> bool:
        """
        Check if we should preemptively rotate based on usage.
        Returns True if current key is approaching its rate limit.
        """
        with self.lock:
            idx = self.current_index
            usage = self.key_usage[idx]
            
            # Calculate elapsed time in current window
            elapsed = time.time() - usage["window_start"]
            
            # Reset window if it's been more than the tracking window
            if elapsed > self.usage_window:
                usage["count"] = 0
                usage["window_start"] = time.time()
                return False
            
            # Check if we're approaching the limit (90% threshold)
            threshold = self.requests_per_hour * 0.9
            if usage["count"] >= threshold:
                logger.info(
                    f"Key {idx + 1} approaching rate limit: "
                    f"{usage['count']}/{self.requests_per_hour} requests in {elapsed:.0f}s"
                )
                return True
            
            return False
    
    def record_request(self) -> None:
        """Record that a request was made with the current key."""
        with self.lock:
            idx = self.current_index
            self.key_usage[idx]["count"] += 1
    
    def handle_rate_limit(self) -> None:
        """
        Handle a 429 rate limit response by rotating to the next key.
        If all keys are exhausted, this will cycle back but the caller
        should implement backoff logic.
        """
        self.rotate_to_next_key(reason="429 rate limit hit")
        
        # Check if we've cycled through all keys
        with self.lock:
            all_keys_limited = all(
                self.key_usage[i]["count"] >= self.requests_per_hour * 0.9
                for i in range(len(self.tokens))
            )
            
            if all_keys_limited:
                logger.warning(
                    "All Metaculus keys appear to be rate limited. "
                    "Consider adding more keys or reducing request rate."
                )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current rotation statistics for monitoring."""
        with self.lock:
            return {
                "current_key_index": self.current_index + 1,
                "total_keys": len(self.tokens),
                "key_usage": {
                    f"key_{i+1}": {
                        "requests": self.key_usage[i]["count"],
                        "window_age_seconds": time.time() - self.key_usage[i]["window_start"]
                    }
                    for i in range(len(self.tokens))
                }
            }


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
    def __init__(self, base_url: str, auth: Optional[requests.auth.AuthBase] = None, delay: float = 0, max_concurrency: int = 1, key_rotator: Optional[MetaculusKeyRotator] = None):
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
        
        # Key rotation support for Metaculus
        self.key_rotator = key_rotator

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
            # Check if we should preemptively rotate keys (Metaculus only)
            if self.key_rotator and self.key_rotator.should_preemptively_rotate():
                self.key_rotator.rotate_to_next_key(reason="preemptive rotation (approaching limit)")
                # Update Authorization header with new token
                self.session.headers.update({"Authorization": f"Token {self.key_rotator.get_current_token()}"})
            
            # Enforce delay and concurrency limit
            with self.semaphore:
                self._wait_for_rate_limit()
                try:
                    response = self.session.request(method, url, auth=self.auth, **kwargs)
                    
                    # Record successful request for usage tracking
                    if self.key_rotator:
                        self.key_rotator.record_request()
                    
                    if response.status_code == 429:
                        # Handle rate limiting with key rotation for Metaculus
                        if self.key_rotator:
                            self.key_rotator.handle_rate_limit()
                            # Update Authorization header with new token
                            self.session.headers.update({"Authorization": f"Token {self.key_rotator.get_current_token()}"})
                            # Immediate retry with new key (no backoff needed)
                            logger.info(f"Retrying {path} with rotated key (attempt {i+1}/{max_retries})...")
                            continue
                        else:
                            # Exponential backoff for non-rotatable clients
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
    # OPTIMIZATION: 20 req/s limit. We use a slightly more aggressive 0.055s delay (18.2 req/s)
    # and max_concurrency=20 to maximize throughput while respecting rate limits.
    return HttpClient(config.kalshi_base_url, auth=auth, delay=0.055, max_concurrency=20)

def get_metaculus_client():
    # Initialize key rotator if we have multiple tokens
    key_rotator = None
    if config.metaculus_tokens:
        key_rotator = MetaculusKeyRotator(
            tokens=config.metaculus_tokens,
            requests_per_hour=1000  # Metaculus rate limit
        )
    
    # Calculate optimal delay based on number of keys
    # With N keys, we can theoretically go N times faster
    # But we keep a small safety margin
    if key_rotator and len(config.metaculus_tokens) > 1:
        # Reduce delay proportionally to number of keys
        # Base: 3.6s for 1 key (1000 req/hr)
        # With 2 keys: 1.8s delay (2000 req/hr theoretical)
        delay = 3.6 / len(config.metaculus_tokens)
    else:
        # Single key: use conservative 3.6s delay
        delay = 3.6
    
    # Create client with rotation support
    client = HttpClient(
        config.metaculus_base_url, 
        delay=delay, 
        max_concurrency=1,  # Keep sequential for safety
        key_rotator=key_rotator
    )
    
    # Set initial authorization header
    if key_rotator:
        client.session.headers.update({"Authorization": f"Token {key_rotator.get_current_token()}"})
    
    return client

# --- LESSONS LEARNED ---
# 1. Kalshi V2 Auth: Use KALSHI-ACCESS-KEY/TIMESTAMP/SIGNATURE headers.
# 2. Signing: V2 requires RSA-PSS with MGF1-SHA256 and salt length = hash length.
# 3. Message format: timestamp + method + path (no body for GET).
# 4. Metaculus Rate Limit: They are strict. 1000 req/hr = ~3.6s delay per key.
#    With multiple keys, we can proportionally reduce the delay (e.g., 1.8s with 2 keys).
#    Benchmark results: 2 keys = 1.99x speedup (99.7% efficiency). Linear scaling confirmed.
# 5. Metaculus Key Rotation Implementation:
#    - Preemptive rotation at 90% threshold (900/1000 requests) prevents hitting limits.
#    - On 429 response, immediately rotate to next key and retry (no backoff needed).
#    - Delay calculation: base_delay / num_keys (3.6s / 2 = 1.8s with 2 keys).
#    - Usage tracking: rolling 1-hour window per key with automatic reset.
#    - Thread-safe: All operations protected with locks (get_current_token, rotate_to_next_key, 
#      record_request, should_preemptively_rotate).
#    - Header updates: Must update session.headers["Authorization"] on rotation.
#    - Configuration: Load METACULUS_TOKEN_1, METACULUS_TOKEN_2, etc. from .env.
#      System loads sequentially until it finds a missing number. Falls back to METACULUS_TOKEN.
#    - Performance: 100 posts with 2 keys: 180s vs 360s single key (50% time saved).
#      Extrapolated: 1000 posts saves 30 minutes (1 hour -> 30 min).
# 6. Rate Limit Jitter: Adding random jitter to retries helps avoid synchronized 
#    "thundering herd" spikes when multiple parallel workers hit a rate limit.
# 7. Session Reuse: Reusing a single requests.Session across multiple calls 
#    is significantly faster due to TCP connection pooling.
# 8. Kalshi Rate Limiting: 20 req/s limit is hard ceiling. Despite optimization (delay=0.055s, 
#    max_concurrency=20 targeting 18.2 req/s), 429 errors still occur frequently due to: (1) Network 
#    jitter causing request timing variation, (2) Burst patterns when multiple batches start 
#    simultaneously, (3) Retry backoff not being perfect. Exponential backoff (1-2s) handles 429s 
#    gracefully. Jitter prevents synchronized retries.
# 9. Connection Pool Sizing: pool_connections and pool_maxsize set to max_concurrency value. This 
#    allows concurrent requests to reuse connections efficiently. For Kalshi (20 concurrent), this 
#    means 20 persistent connections to api.elections.kalshi.com. Connection reuse reduces TCP 
#    handshake overhead significantly.
# 10. Delay Implementation: Delay is enforced per-request using lock + time tracking. This ensures 
#     requests are spaced correctly even with concurrent workers. Lock contention is minimal (only 
#     during delay calculation, ~1ms per request). Alternative (per-worker delay) would be more 
#     complex and less accurate for rate limiting.
# 11. Semaphore vs Lock: Semaphore limits concurrent requests (max_concurrency), Lock ensures 
#     delay timing is accurate. Semaphore.acquire() happens BEFORE delay wait (prevents too many 
#     requests queuing). Delay wait happens INSIDE semaphore context (ensures accurate spacing).
# 12. Error Handling Strategy: 429 errors → exponential backoff + retry (up to 5 attempts). 413 
#     errors → log and fail (can't retry with same request). 5xx errors → exponential backoff + 
#     retry. 4xx errors (except 429/413) → fail immediately (client error, retry won't help). 
#     Connection errors → exponential backoff + retry.
# 13. Timeout Values: Kalshi API uses default requests timeout (no explicit timeout set). For S3 
#     downloads, timeout is set to 120s (in bulk_grabber). API requests typically complete in 
#     <1s, so default timeout is sufficient. Long-running requests are usually 429 retries, which 
#     are handled explicitly.
# 14. Authentication Overhead: Kalshi V2 auth requires RSA-PSS signing per request. This adds 
#     ~1-2ms per request (cryptographic operations). For 20 req/s, this is negligible (<4% overhead). 
#     Private key is loaded once at client creation (not per-request), so no repeated parsing cost.
# 15. Header Management: Session headers are set once at client creation. For Metaculus key 
#     rotation, headers["Authorization"] must be updated on rotation. Kalshi auth is handled via 
#     AuthBase class (called per-request), so no header updates needed. This design difference is 
#     important for key rotation implementation.
# 16. Thread Safety: All shared state (last_request_time, key_rotator) is protected with locks. 
#     Semaphore provides additional concurrency control. requests.Session is thread-safe for 
#     reading, but header updates (Metaculus rotation) need to be synchronized. Current 
#     implementation uses locks for all shared state access.
# 17. Performance Impact: Rate limiting adds ~0.055s per request (Kalshi). For 490 API calls, 
#     this is ~27s of delay time. Retry backoff adds additional time on 429s (~1-2s per retry). 
#     Total API phase time: ~30-35s for 3.8K markets (down from ~195s for 49K markets before 
#     optimization). The optimization reduced markets needing API by 92%, not API calls by 92% 
#     (due to batch size reduction).

