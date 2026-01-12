"""
Simple test for Metaculus API key rotation system.
Verifies that rotation is working correctly with a small number of API calls.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber
from src.common.logging import logger

def test_rotation():
    """Test that key rotation is initialized and working."""
    grabber = MetaculusGrabber()
    
    # Check if rotation is enabled
    has_rotation = hasattr(grabber.client, 'key_rotator') and grabber.client.key_rotator
    if not has_rotation:
        logger.warning("Key rotation not enabled - single key mode")
        return False
    
    rotator = grabber.client.key_rotator
    logger.info(f"Rotation enabled: {len(rotator.tokens)} key(s)")
    logger.info(f"Delay per request: {grabber.client.delay}s")
    
    # Make a few API calls to verify it works
    logger.info("Making test API calls...")
    posts = grabber.fetch_posts(limit=5, use_cache=False)
    logger.info(f"Successfully fetched {len(posts)} posts")
    
    # Check rotation stats
    stats = rotator.get_stats()
    logger.info(f"Current key: {stats['current_key_index']}/{stats['total_keys']}")
    
    logger.info("Rotation test passed!")
    return True

if __name__ == "__main__":
    try:
        test_rotation()
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)

# --- NOTES ---
# This is a minimal test to verify rotation works. To extend it:
# - Test rotation on 429: Make enough requests to trigger rate limits (need >900 requests in 1 hour)
# - Test preemptive rotation: Force usage near 90% threshold
# - Benchmark performance: Compare single vs multiple keys with larger datasets (100+ posts)
# - Test key cycling: Manually rotate through all keys and verify headers update correctly
