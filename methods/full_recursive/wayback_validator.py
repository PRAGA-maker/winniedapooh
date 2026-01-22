"""
Wayback Machine CDX API validator for source date verification.

Provides automated validation of citation URLs against archive.org's CDX API
to detect potential data leakage by verifying whether sources were archived
before the research cutoff date.

Architecture:
- Async batch processing with concurrency limit
- LRU cache to avoid duplicate queries
- Rate limiting to respect archive.org limits
- Graceful error handling and skip patterns
"""

import asyncio
import aiohttp
import re
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from .agents import Citation


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class WaybackResult:
    """Result from a Wayback Machine CDX query."""
    url: str
    earliest_snapshot: Optional[datetime] = None
    status: str = "pending"  # "found", "not_found", "error", "skipped"
    error_message: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None


@dataclass
class ValidationResult:
    """Result of validating a citation against Wayback Machine."""
    url: str
    title: Optional[str] = None
    wayback_result: Optional[WaybackResult] = None
    is_suspicious: bool = False  # True if no pre-cutoff snapshot
    reason: str = ""  # Explanation of the validation result


# =============================================================================
# URL Skip Patterns
# =============================================================================

# Patterns for URLs that should be skipped (not validated via Wayback)
SKIP_PATTERNS = [
    # Social media feeds (highly dynamic, often not archived well)
    r"twitter\.com/[^/]+/status",
    r"x\.com/[^/]+/status",
    r"reddit\.com/r/[^/]+/comments",
    r"facebook\.com/[^/]+/posts",
    r"instagram\.com/p/",
    r"tiktok\.com/@[^/]+/video",

    # Dynamic search/query pages
    r"google\.com/search",
    r"bing\.com/search",
    r"duckduckgo\.com/\?",
    r"youtube\.com/results",

    # Video/media content (often not archived)
    r"youtube\.com/watch",
    r"vimeo\.com/\d+",
    r"twitch\.tv/videos",

    # API endpoints
    r"/api/",
    r"api\.",

    # Local/private addresses
    r"localhost",
    r"127\.0\.0\.1",
    r"192\.168\.",
    r"10\.\d+\.",
    r"172\.(1[6-9]|2[0-9]|3[0-1])\.",

    # Common non-archivable domains
    r"docs\.google\.com",
    r"drive\.google\.com",
    r"dropbox\.com",
    r"onedrive\.live\.com",
]

# Compile patterns for efficiency
_SKIP_PATTERN_RE = re.compile("|".join(SKIP_PATTERNS), re.IGNORECASE)


def should_skip_url(url: str) -> bool:
    """Check if a URL should be skipped from Wayback validation."""
    if not url:
        return True

    # Check against skip patterns
    if _SKIP_PATTERN_RE.search(url):
        return True

    # Check for obviously invalid URLs
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return True
        if parsed.scheme not in ("http", "https"):
            return True
    except Exception:
        return True

    return False


# =============================================================================
# Wayback Validator Class
# =============================================================================

class WaybackValidator:
    """
    Validates citation URLs against the Wayback Machine CDX API.

    Checks whether URLs were archived before a cutoff date to detect
    potential data leakage in research citations.

    Features:
    - Async batch processing with configurable concurrency
    - LRU cache to avoid duplicate API calls
    - Rate limiting to respect archive.org limits
    - Skip patterns for non-archivable URLs
    - Graceful error handling
    """

    CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"

    # Default configuration
    DEFAULT_CONCURRENCY = 3
    DEFAULT_TIMEOUT = 10.0
    DEFAULT_RATE_LIMIT = 0.2  # 200ms between requests
    DEFAULT_MAX_RETRIES = 3

    def __init__(
        self,
        enabled: bool = True,
        cache_size: int = 256,
        timeout: float = DEFAULT_TIMEOUT,
        rate_limit: float = DEFAULT_RATE_LIMIT,
        concurrency: int = DEFAULT_CONCURRENCY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        verbose: bool = False,
    ):
        """
        Initialize the WaybackValidator.

        Args:
            enabled: Whether validation is enabled (allows easy disable)
            cache_size: Size of the LRU cache for query results
            timeout: HTTP request timeout in seconds
            rate_limit: Minimum seconds between requests
            concurrency: Maximum parallel requests
            max_retries: Maximum retry attempts for failed requests
            verbose: Print debug information
        """
        self.enabled = enabled
        self.cache_size = cache_size
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.concurrency = concurrency
        self.max_retries = max_retries
        self.verbose = verbose

        # Internal state
        self._cache: Dict[str, WaybackResult] = {}
        self._last_request_time: float = 0.0
        self._request_lock: Optional[asyncio.Lock] = None

        # Statistics
        self.stats = {
            "total_urls": 0,
            "cached_hits": 0,
            "skipped": 0,
            "found": 0,
            "not_found": 0,
            "errors": 0,
        }

    def _get_cache_key(self, url: str, cutoff: datetime) -> str:
        """Generate cache key for URL + cutoff combination."""
        cutoff_str = cutoff.strftime("%Y%m%d")
        return f"{url}:{cutoff_str}"

    async def _ensure_rate_limit(self):
        """Ensure we don't exceed rate limit."""
        if self._request_lock is None:
            self._request_lock = asyncio.Lock()

        async with self._request_lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request_time
            if elapsed < self.rate_limit:
                await asyncio.sleep(self.rate_limit - elapsed)
            self._last_request_time = asyncio.get_event_loop().time()

    def _build_cdx_query(self, url: str, cutoff: datetime) -> str:
        """Build CDX API query URL."""
        # Format: YYYYMMDD
        cutoff_str = cutoff.strftime("%Y%m%d")

        # Query params:
        # - url: The URL to search for
        # - output: JSON format
        # - limit: Just need 1 result to confirm existence
        # - from: Start from earliest archives (1996)
        # - to: Up to cutoff date
        # - fl: Fields to return (timestamp, original URL, status code)
        params = {
            "url": url,
            "output": "json",
            "limit": "1",
            "from": "19960101",
            "to": cutoff_str,
            "fl": "timestamp,original,statuscode",
        }

        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.CDX_ENDPOINT}?{query_string}"

    async def _query_cdx(
        self,
        session: aiohttp.ClientSession,
        url: str,
        cutoff: datetime,
    ) -> WaybackResult:
        """Query CDX API for a single URL."""
        result = WaybackResult(url=url)

        # Check skip patterns
        if should_skip_url(url):
            result.status = "skipped"
            result.error_message = "URL matches skip pattern"
            return result

        # Build query
        query_url = self._build_cdx_query(url, cutoff)

        for attempt in range(self.max_retries):
            try:
                # Respect rate limit
                await self._ensure_rate_limit()

                async with session.get(
                    query_url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    # Handle rate limiting (429)
                    if response.status == 429:
                        wait_time = 2 ** attempt
                        if self.verbose:
                            print(f"[Wayback] Rate limited, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue

                    # Handle other errors
                    if response.status != 200:
                        result.status = "error"
                        result.error_message = f"HTTP {response.status}"
                        return result

                    # Parse JSON response
                    text = await response.text()
                    if not text.strip():
                        result.status = "not_found"
                        return result

                    try:
                        import json
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        result.status = "error"
                        result.error_message = "Invalid JSON response"
                        return result

                    # CDX returns header row + data rows
                    # Format: [["timestamp", "original", "statuscode"], ["20231015143022", "https://...", "200"]]
                    if not isinstance(data, list) or len(data) < 2:
                        # No results found
                        result.status = "not_found"
                        return result

                    # Parse timestamp from first data row
                    try:
                        timestamp_str = data[1][0]  # e.g., "20231015143022"
                        snapshot_dt = datetime.strptime(timestamp_str[:8], "%Y%m%d")
                        result.earliest_snapshot = snapshot_dt
                        result.status = "found"
                        result.raw_response = {"timestamp": timestamp_str, "rows": len(data) - 1}
                    except (IndexError, ValueError) as e:
                        result.status = "error"
                        result.error_message = f"Failed to parse timestamp: {e}"

                    return result

            except asyncio.TimeoutError:
                if attempt == self.max_retries - 1:
                    result.status = "error"
                    result.error_message = "Timeout"
                    return result
            except aiohttp.ClientError as e:
                if attempt == self.max_retries - 1:
                    result.status = "error"
                    result.error_message = str(e)
                    return result

        return result

    async def validate_url(
        self,
        url: str,
        cutoff: datetime,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> ValidationResult:
        """
        Validate a single URL against Wayback Machine.

        Args:
            url: URL to validate
            cutoff: Research cutoff date
            session: Optional aiohttp session (created if not provided)

        Returns:
            ValidationResult with wayback_result and is_suspicious flag
        """
        self.stats["total_urls"] += 1

        # Check cache
        cache_key = self._get_cache_key(url, cutoff)
        if cache_key in self._cache:
            self.stats["cached_hits"] += 1
            wayback_result = self._cache[cache_key]
        else:
            # Query CDX API
            own_session = session is None
            if own_session:
                session = aiohttp.ClientSession()

            try:
                wayback_result = await self._query_cdx(session, url, cutoff)

                # Cache result
                if len(self._cache) < self.cache_size:
                    self._cache[cache_key] = wayback_result
            finally:
                if own_session:
                    await session.close()

        # Update stats
        if wayback_result.status == "skipped":
            self.stats["skipped"] += 1
        elif wayback_result.status == "found":
            self.stats["found"] += 1
        elif wayback_result.status == "not_found":
            self.stats["not_found"] += 1
        else:
            self.stats["errors"] += 1

        # Determine if suspicious
        # URL is suspicious if:
        # 1. No snapshot found before cutoff (not_found)
        # 2. API error that prevented verification (error)
        # Skipped URLs are not marked suspicious (they're known dynamic content)
        is_suspicious = wayback_result.status in ("not_found", "error")

        # Build reason string
        if wayback_result.status == "found":
            reason = f"Archived on {wayback_result.earliest_snapshot.strftime('%Y-%m-%d')}"
        elif wayback_result.status == "not_found":
            reason = "No archive found before cutoff - SUSPICIOUS"
        elif wayback_result.status == "skipped":
            reason = f"Skipped: {wayback_result.error_message}"
            is_suspicious = False  # Don't mark skipped as suspicious
        else:
            reason = f"Error: {wayback_result.error_message}"

        return ValidationResult(
            url=url,
            wayback_result=wayback_result,
            is_suspicious=is_suspicious,
            reason=reason,
        )

    async def validate_citations(
        self,
        citations: List["Citation"],
        cutoff: datetime,
    ) -> List[ValidationResult]:
        """
        Validate multiple citations in batch.

        Args:
            citations: List of Citation objects to validate
            cutoff: Research cutoff date

        Returns:
            List of ValidationResult objects (same order as input)
        """
        if not self.enabled:
            # Return empty results if disabled
            return [
                ValidationResult(
                    url=c.url,
                    title=c.title,
                    wayback_result=WaybackResult(url=c.url, status="skipped"),
                    is_suspicious=False,
                    reason="Wayback validation disabled",
                )
                for c in citations
            ]

        if not citations:
            return []

        # Deduplicate URLs while preserving order
        seen_urls: Set[str] = set()
        unique_citations: List["Citation"] = []
        url_to_indices: Dict[str, List[int]] = {}

        for i, citation in enumerate(citations):
            url = citation.url
            if url not in seen_urls:
                seen_urls.add(url)
                unique_citations.append(citation)
                url_to_indices[url] = [i]
            else:
                url_to_indices[url].append(i)

        if self.verbose:
            print(f"[Wayback] Validating {len(unique_citations)} unique URLs "
                  f"({len(citations)} total citations)")

        # Create session and semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.concurrency)

        async def validate_with_semaphore(
            session: aiohttp.ClientSession,
            citation: "Citation",
        ) -> ValidationResult:
            async with semaphore:
                result = await self.validate_url(citation.url, cutoff, session)
                result.title = citation.title
                return result

        # Run validation
        async with aiohttp.ClientSession() as session:
            tasks = [
                validate_with_semaphore(session, c)
                for c in unique_citations
            ]
            unique_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Build URL -> result mapping
        url_to_result: Dict[str, ValidationResult] = {}
        for citation, result in zip(unique_citations, unique_results):
            if isinstance(result, Exception):
                # Handle unexpected exceptions
                url_to_result[citation.url] = ValidationResult(
                    url=citation.url,
                    title=citation.title,
                    wayback_result=WaybackResult(
                        url=citation.url,
                        status="error",
                        error_message=str(result),
                    ),
                    is_suspicious=True,
                    reason=f"Validation error: {result}",
                )
            else:
                url_to_result[citation.url] = result

        # Reconstruct results in original order
        results = []
        for citation in citations:
            result = url_to_result.get(citation.url)
            if result:
                # Create a copy with the correct title for this citation
                results.append(ValidationResult(
                    url=result.url,
                    title=citation.title,
                    wayback_result=result.wayback_result,
                    is_suspicious=result.is_suspicious,
                    reason=result.reason,
                ))
            else:
                results.append(ValidationResult(
                    url=citation.url,
                    title=citation.title,
                    is_suspicious=True,
                    reason="URL not found in results",
                ))

        if self.verbose:
            suspicious_count = sum(1 for r in results if r.is_suspicious)
            print(f"[Wayback] Validation complete: {suspicious_count} suspicious URLs")

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return {
            **self.stats,
            "cache_size": len(self._cache),
        }


# =============================================================================
# Synchronous wrapper for non-async contexts
# =============================================================================

def validate_citations_sync(
    citations: List["Citation"],
    cutoff: datetime,
    enabled: bool = True,
    verbose: bool = False,
) -> List[ValidationResult]:
    """
    Synchronous wrapper for validate_citations.

    For use in non-async contexts like the main pipeline.

    Args:
        citations: List of Citation objects to validate
        cutoff: Research cutoff date
        enabled: Whether validation is enabled
        verbose: Print debug information

    Returns:
        List of ValidationResult objects
    """
    validator = WaybackValidator(enabled=enabled, verbose=verbose)
    return asyncio.run(validator.validate_citations(citations, cutoff))


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Wayback Validator Implementation:
#
# CDX API NOTES:
# 1. Endpoint: https://web.archive.org/cdx/search/cdx
# 2. Returns JSON array with header row + data rows
# 3. Empty result (just header) means URL not archived
# 4. Rate limiting returns 429 - use exponential backoff
# 5. Timestamp format: YYYYMMDDHHMMSS (we only need first 8 chars)
#
# SKIP PATTERNS:
# 1. Social media URLs are highly dynamic, often not archived
# 2. Search pages generate different content each time
# 3. Video platforms have poor archive coverage
# 4. API endpoints change frequently
# 5. Local addresses are obviously not archived
#
# PERFORMANCE:
# 1. Concurrency of 3 balances speed vs rate limiting
# 2. 200ms rate limit prevents 429 errors in most cases
# 3. LRU cache prevents duplicate queries for same URL+cutoff
# 4. Deduplication reduces total API calls
#
# INTERPRETATION:
# 1. "found" with pre-cutoff snapshot = HIGH confidence URL existed
# 2. "not_found" = SUSPICIOUS but not definitive (archives incomplete)
# 3. "skipped" = Known dynamic content, don't penalize
# 4. "error" = Mark suspicious but don't over-penalize
#
# INTEGRATION NOTES:
# 1. Call between agent outputs and verifier
# 2. Results passed to verifier as additional context
# 3. Verifier uses results to weight source credibility
# 4. Can be disabled via config for faster runs
#
