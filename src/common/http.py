import time
import base64
import requests
from typing import Optional, Any, Dict
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
    def __init__(self, base_url: str, auth: Optional[requests.auth.AuthBase] = None, delay: float = 0):
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self.session = requests.Session()
        self.delay = delay
        self.last_request_time = 0

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        # Rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
            
        url = f"{self.base_url}/{path.lstrip('/')}"
        max_retries = 3
        backoff = 1
        
        for i in range(max_retries):
            try:
                self.last_request_time = time.time()
                response = self.session.request(method, url, auth=self.auth, **kwargs)
                if response.status_code == 429:
                    logger.warning(f"Rate limited (429). Retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                if response.status_code == 400:
                    logger.error(f"Bad Request (400): {response.text}")
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if i == max_retries - 1:
                    logger.error(f"Request failed after {max_retries} retries: {e}")
                    raise
                logger.warning(f"Request failed: {e}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
        
        raise requests.exceptions.RequestException("Request failed after all retries")
        
        # This part should be unreachable if raise_for_status or raise is called
        raise requests.exceptions.RequestException("Request failed after all retries")

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self.request("POST", path, **kwargs)

def get_kalshi_client():
    if not config.kalshi_api_key_id or not config.kalshi_private_key:
        logger.warning("Kalshi API credentials not found in environment.")
        return HttpClient(config.kalshi_base_url, delay=0.1)
    
    auth = KalshiAuth(config.kalshi_api_key_id, config.kalshi_private_key)
    return HttpClient(config.kalshi_base_url, auth=auth, delay=0.1)

def get_metaculus_client():
    headers = {}
    if config.metaculus_token:
        headers["Authorization"] = f"Token {config.metaculus_token}"
    
    # 1000 requests per hour = 3.6s delay between requests
    client = HttpClient(config.metaculus_base_url, delay=3.6)
    client.session.headers.update(headers)
    return client

# --- LESSONS LEARNED ---
# 1. Kalshi V2 Auth: Use KALSHI-ACCESS-KEY/TIMESTAMP/SIGNATURE headers.
# 2. Signing: V2 requires RSA-PSS with MGF1-SHA256 and salt length = hash length.
# 3. Message format: timestamp + method + path (no body for GET).
# 4. Metaculus Rate Limit: They are strict. 1000 req/hr = ~3.6s delay. 
#    Always use a delay in HttpClient for Metaculus to avoid 429s.

