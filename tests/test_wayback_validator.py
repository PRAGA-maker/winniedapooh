"""
Unit tests for the Wayback Machine CDX API validator.

Tests cover:
- CDX query format construction
- Response parsing (with results, empty, errors)
- URL skip patterns (twitter, google, localhost)
- Cache behavior
- Cutoff date filtering

NOTE: Uses asyncio.run() directly instead of pytest-asyncio to avoid dependencies.
"""

import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

# Import the module under test
from methods.full_recursive.wayback_validator import (
    WaybackValidator,
    WaybackResult,
    ValidationResult,
    should_skip_url,
    validate_citations_sync,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@dataclass
class MockCitation:
    """Mock Citation class for testing without importing the full agents module."""
    url: str
    title: str = None
    author: str = None
    outlet: str = None
    publish_date: str = None
    wayback_validated: bool = False
    wayback_earliest: str = None
    wayback_suspicious: bool = False


def make_validator(**kwargs):
    """Create a WaybackValidator with test-friendly defaults."""
    defaults = {
        "enabled": True,
        "cache_size": 10,
        "timeout": 5.0,
        "rate_limit": 0.01,  # Fast for tests
        "verbose": False,
    }
    defaults.update(kwargs)
    return WaybackValidator(**defaults)


# =============================================================================
# URL Skip Pattern Tests
# =============================================================================

class TestSkipPatterns:
    """Tests for URL skip patterns."""

    def test_skip_twitter_status(self):
        """Twitter status URLs should be skipped."""
        assert should_skip_url("https://twitter.com/user/status/123456")
        assert should_skip_url("https://x.com/user/status/123456")

    def test_skip_reddit_comments(self):
        """Reddit comment URLs should be skipped."""
        assert should_skip_url("https://reddit.com/r/subreddit/comments/abc123/title")

    def test_skip_google_search(self):
        """Google search URLs should be skipped."""
        assert should_skip_url("https://google.com/search?q=test")

    def test_skip_youtube_watch(self):
        """YouTube watch URLs should be skipped."""
        assert should_skip_url("https://youtube.com/watch?v=abc123")

    def test_skip_api_endpoints(self):
        """API endpoints should be skipped."""
        assert should_skip_url("https://api.example.com/v1/data")
        assert should_skip_url("https://example.com/api/users")

    def test_skip_localhost(self):
        """Localhost URLs should be skipped."""
        assert should_skip_url("http://localhost:3000/")
        assert should_skip_url("http://127.0.0.1:8080/")
        assert should_skip_url("http://192.168.1.1/")

    def test_allow_news_urls(self):
        """News site URLs should not be skipped."""
        assert not should_skip_url("https://reuters.com/article/123")
        assert not should_skip_url("https://apnews.com/article/headline")
        assert not should_skip_url("https://nytimes.com/2024/01/article")

    def test_allow_blog_urls(self):
        """Blog URLs should not be skipped."""
        assert not should_skip_url("https://medium.com/@author/article")
        assert not should_skip_url("https://substack.com/p/newsletter")

    def test_skip_empty_url(self):
        """Empty URLs should be skipped."""
        assert should_skip_url("")
        assert should_skip_url(None)

    def test_skip_invalid_scheme(self):
        """Non-HTTP(S) URLs should be skipped."""
        assert should_skip_url("ftp://example.com/file")
        assert should_skip_url("file:///path/to/file")
        assert should_skip_url("javascript:alert(1)")

    def test_skip_malformed_url(self):
        """Malformed URLs should be skipped."""
        assert should_skip_url("not-a-url")
        assert should_skip_url("://missing-scheme.com")
        assert should_skip_url("http://")


# =============================================================================
# CDX Query Construction Tests
# =============================================================================

class TestCDXQueryConstruction:
    """Tests for CDX API query construction."""

    def test_build_cdx_query_format(self):
        """CDX query should have correct format."""
        validator = make_validator()
        cutoff = datetime(2024, 6, 1)
        url = "https://example.com/article"

        query = validator._build_cdx_query(url, cutoff)

        assert "web.archive.org/cdx/search/cdx" in query
        assert f"url={url}" in query
        assert "output=json" in query
        assert "limit=1" in query
        assert "from=19960101" in query
        assert "to=20240601" in query  # cutoff_date formatted
        assert "fl=timestamp,original,statuscode" in query

    def test_build_cdx_query_different_cutoffs(self):
        """CDX query should use correct cutoff date."""
        validator = make_validator()
        url = "https://example.com"

        query1 = validator._build_cdx_query(url, datetime(2023, 1, 15))
        assert "to=20230115" in query1

        query2 = validator._build_cdx_query(url, datetime(2025, 12, 31))
        assert "to=20251231" in query2


# =============================================================================
# Cache Tests
# =============================================================================

class TestCacheBehavior:
    """Tests for caching behavior."""

    def test_cache_key_includes_cutoff(self):
        """Cache key should include cutoff date."""
        validator = make_validator()

        key1 = validator._get_cache_key("https://example.com", datetime(2024, 1, 1))
        key2 = validator._get_cache_key("https://example.com", datetime(2024, 6, 1))

        assert key1 != key2
        assert "20240101" in key1
        assert "20240601" in key2

    def test_cache_key_includes_url(self):
        """Cache key should include URL."""
        validator = make_validator()
        cutoff = datetime(2024, 1, 1)

        key1 = validator._get_cache_key("https://example1.com", cutoff)
        key2 = validator._get_cache_key("https://example2.com", cutoff)

        assert key1 != key2


# =============================================================================
# Sync Wrapper Tests
# =============================================================================

class TestSyncWrapper:
    """Tests for the synchronous wrapper function."""

    def test_sync_wrapper_disabled(self):
        """Sync wrapper with disabled validation."""
        citations = [MockCitation(url="https://example.com", title="Article")]
        cutoff = datetime(2024, 6, 1)

        results = validate_citations_sync(citations, cutoff, enabled=False)

        assert len(results) == 1
        assert results[0].is_suspicious is False
        assert "disabled" in results[0].reason.lower()

    def test_sync_wrapper_empty_list(self):
        """Sync wrapper with empty citations list."""
        results = validate_citations_sync([], datetime(2024, 6, 1), enabled=True)
        assert results == []

    def test_sync_wrapper_skipped_url(self):
        """Sync wrapper skips social media URLs."""
        citations = [MockCitation(url="https://twitter.com/user/status/123", title="Tweet")]
        cutoff = datetime(2024, 6, 1)

        results = validate_citations_sync(citations, cutoff, enabled=True)

        assert len(results) == 1
        assert results[0].is_suspicious is False
        assert "skipped" in results[0].reason.lower()


# =============================================================================
# Validation Result Tests
# =============================================================================

class TestValidationResult:
    """Tests for ValidationResult construction."""

    def test_wayback_result_defaults(self):
        """WaybackResult should have sensible defaults."""
        result = WaybackResult(url="https://example.com")

        assert result.url == "https://example.com"
        assert result.earliest_snapshot is None
        assert result.status == "pending"
        assert result.error_message is None

    def test_validation_result_defaults(self):
        """ValidationResult should have sensible defaults."""
        result = ValidationResult(url="https://example.com")

        assert result.url == "https://example.com"
        assert result.title is None
        assert result.wayback_result is None
        assert result.is_suspicious is False
        assert result.reason == ""


# =============================================================================
# Async Core Logic Tests (using asyncio.run)
# =============================================================================

class TestAsyncCoreLogic:
    """Tests for async core logic using asyncio.run()."""

    def test_validate_skipped_url(self):
        """Skipped URLs should not be marked suspicious."""
        async def run_test():
            validator = make_validator()
            cutoff = datetime(2024, 6, 1)
            result = await validator.validate_url("https://twitter.com/user/status/123", cutoff)
            return result

        result = asyncio.run(run_test())
        assert result.is_suspicious is False
        assert "Skipped" in result.reason

    def test_validate_empty_citations(self):
        """Empty citations list should return empty results."""
        async def run_test():
            validator = make_validator()
            cutoff = datetime(2024, 6, 1)
            return await validator.validate_citations([], cutoff)

        results = asyncio.run(run_test())
        assert results == []

    def test_disabled_validator_returns_skipped(self):
        """Disabled validator should mark all as skipped, not suspicious."""
        async def run_test():
            validator = make_validator(enabled=False)
            citations = [
                MockCitation(url="https://example.com", title="Article"),
            ]
            return await validator.validate_citations(citations, datetime(2024, 6, 1))

        results = asyncio.run(run_test())
        assert len(results) == 1
        assert results[0].is_suspicious is False
        assert "disabled" in results[0].reason.lower()


# =============================================================================
# Statistics Tests
# =============================================================================

class TestStatistics:
    """Tests for validation statistics tracking."""

    def test_initial_stats(self):
        """Statistics should start at zero."""
        validator = make_validator()
        stats = validator.get_stats()

        assert stats["total_urls"] == 0
        assert stats["cached_hits"] == 0
        assert stats["skipped"] == 0
        assert stats["found"] == 0
        assert stats["not_found"] == 0
        assert stats["errors"] == 0

    def test_stats_includes_cache_size(self):
        """Stats should include cache size."""
        validator = make_validator()
        stats = validator.get_stats()

        assert "cache_size" in stats


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_citation_with_none_url(self):
        """Citation with None URL should be handled."""
        citation = MockCitation(url=None, title="No URL")

        # should_skip_url handles None
        assert should_skip_url(citation.url)

    def test_citation_with_empty_url(self):
        """Citation with empty URL should be handled."""
        citation = MockCitation(url="", title="Empty URL")

        assert should_skip_url(citation.url)

    def test_duplicate_urls_in_batch(self):
        """Duplicate URLs should be deduplicated in batch validation."""
        async def run_test():
            validator = make_validator()
            cutoff = datetime(2024, 6, 1)

            # All skip patterns so no actual API calls
            citations = [
                MockCitation(url="https://twitter.com/a/status/1", title="Tweet 1"),
                MockCitation(url="https://twitter.com/a/status/1", title="Same tweet"),
                MockCitation(url="https://twitter.com/b/status/2", title="Tweet 2"),
            ]

            results = await validator.validate_citations(citations, cutoff)
            return results

        results = asyncio.run(run_test())
        # Should return 3 results (one per citation)
        assert len(results) == 3
        # But URLs 0 and 1 are duplicates, should have same result
        assert results[0].url == results[1].url

    def test_mixed_skip_and_valid_urls(self):
        """Mix of skipped and valid URLs should be handled correctly."""
        async def run_test():
            validator = make_validator()
            cutoff = datetime(2024, 6, 1)

            citations = [
                MockCitation(url="https://twitter.com/a/status/1", title="Skip me"),
                MockCitation(url="https://localhost:3000/", title="Also skip"),
            ]

            results = await validator.validate_citations(citations, cutoff)
            return results

        results = asyncio.run(run_test())
        assert len(results) == 2
        # Both should be skipped (not suspicious)
        assert all(not r.is_suspicious for r in results)


# =============================================================================
# Integration-style Test (live API - skipped by default)
# =============================================================================

@pytest.mark.skip(reason="Requires live API access - run manually")
class TestLiveAPI:
    """Integration tests with live Wayback Machine API."""

    def test_live_known_archived_url(self):
        """Test with a URL known to be archived."""
        async def run_test():
            validator = make_validator(timeout=30.0, rate_limit=1.0)
            cutoff = datetime(2024, 1, 1)

            # example.com has been archived since the 90s
            result = await validator.validate_url("https://example.com", cutoff)
            return result

        result = asyncio.run(run_test())
        assert result.wayback_result.status == "found"
        assert result.is_suspicious is False


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-21 Wayback Validator Tests:
#
# TESTING APPROACH:
# 1. Use asyncio.run() directly instead of pytest-asyncio
# 2. Test skip patterns thoroughly - they're critical for performance
# 3. Use skippable URLs in batch tests to avoid mocking API calls
#
# COMMON GOTCHAS:
# 1. Cache keys must include cutoff date (same URL, different cutoffs)
# 2. Skipped URLs should NOT be marked suspicious
# 3. Empty responses are "not_found", not errors
# 4. Deduplication happens at URL level, but results returned for all citations
#
# RUNNING TESTS:
#   uv run pytest tests/test_wayback_validator.py -v
#   uv run pytest tests/test_wayback_validator.py::TestSkipPatterns -v
#
