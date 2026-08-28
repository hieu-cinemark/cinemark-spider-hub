"""Rolling-window burst detection for transient spider errors
(RateLimitedError/NetworkError across Facebook/Threads/TikTok) - mirrors
cinemark-api's own ingest_consumer._note_drop pattern: a single occurrence
already gets logged/alerted by the spider itself (see each search.py's own
except blocks), so this only escalates when the SAME reason keeps
recurring within a rolling window - "1 unlucky retry" vs "this platform is
getting throttled more and more lately" are different signals worth
telling apart, and only the second one needs a human's attention before it
turns into a hard block/checkpoint."""

from __future__ import annotations

from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache

logger = get_logger(__name__)

ALERT_THRESHOLD = 5
# Shorter window than ingest_consumer's 1h - a spider retries every few
# seconds, so the same burst accumulates much faster than Kafka ingest
# drops do.
COUNTER_TTL_SECONDS = 1800


def note_transient_error(platform: str, reason: str, redis_cache: RedisCache | None = None) -> None:
    """Call once per RateLimitedError/NetworkError raised by any spider.
    Best-effort: never raises, so a Redis blip can't take down a crawl
    that's already mid-failure-handling for a different reason."""
    cache = redis_cache or RedisCache()
    key = f"error_burst:{platform}:{reason}"
    try:
        count = cache.incr(key)
        if count == 1:
            cache.expire(key, COUNTER_TTL_SECONDS)
    except Exception as exc:
        logger.warning("error_burst_tracking_failed", platform=platform, reason=reason, error=str(exc))
        return

    if count == ALERT_THRESHOLD:
        logger.error(
            "error_burst_detected",
            telegram=True,
            platform=platform,
            reason=reason,
            count=count,
            window_minutes=COUNTER_TTL_SECONDS // 60,
        )
