"""Proactive proxy connectivity check - pings every enabled platform_proxies
row directly (a plain connectivity probe, not a full feature-level request -
see health_check.py for that different failure class: "looks healthy but
returns no data"). Feeds the exact same circuit breaker
services/pool.acquire_proxy_for_account already reads from
(db/proxies.py record_proxy_outcome) - a proxy that fails here cools down exactly like
one that failed a real crawl request, and one that recovers here has its
cooldown cleared immediately (record_proxy_outcome's own success branch),
symmetric with how a real successful crawl already clears it.

This script only makes dead proxies discoverable fast - it doesn't pause
anything itself. Once every proxy for a platform is degraded,
crawl_request_consumer.py's own loop hits ProxyPoolExhaustedError on its
very next acquire_proxy_for_account(required=True) call and already backs
off + requeues automatically (see that module's PROXY_EXHAUSTED_BACKOFF_*
constants) - this is the "reactive" half of the same mechanism; this
script is the "proactive" half, catching the outage before a real job has
to discover it the hard way.

A proxy row shared across platforms (platform='all') is pinged once per
run, not once per platform that references it - redundant pings would just
be wasted network calls and duplicate consecutive_failures bumps for the
same physical IP.

Run periodically via cron (not a long-running process) - e.g. every 5
minutes:

    */5 * * * * cd /path/to/spider-hub && .venv/bin/python -m social_crawler.proxy_health_check

Exit code is always 0 - failures are reported via logger.error(telegram=True, ...),
same convention as health_check.py.
"""

from __future__ import annotations

import sys

from curl_cffi import requests as curl_requests

from social_crawler.clients.redis import RedisCache
from social_crawler.db import proxies
from social_crawler.db.proxy_settings import get_setting
from social_crawler.logger import get_logger
from social_crawler.services.pool import build_proxy_url

logger = get_logger(__name__)

PLATFORMS = ("facebook", "threads", "tiktok")

# Ping URL/timeout/alert threshold/streak TTL are the dashboard's
# proxy_settings health_check_* values (defaults below reflect the
# original constants).
#
# generate_204 (default ping URL) - a 204-No-Content endpoint several browsers/OSes already use
# for exactly this "is the network path actually usable" check: minimal
# payload, no redirects, no bot-detection to trip. Any response at all
# (status code doesn't matter) proves the proxy tunnel + TLS handshake
# worked; only a connection-level failure (never even got a response) means
# the proxy itself is down - see health_check_timeout_seconds and the RequestsError
# handling below, same "resp is None -> network problem" distinction
# comet_graphql_client.py/tiktok/client.py already use for real requests.


def ping_proxy(proxy: proxies.ProxyRow) -> bool:
    """True if a request actually got a response back through this proxy -
    see module docstring for why the status code itself doesn't matter."""
    proxy_url = build_proxy_url(proxy)
    try:
        curl_requests.get(
            str(get_setting("health_check_ping_url")),
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=float(get_setting("health_check_timeout_seconds")),
            impersonate="chrome",
        )
        return True
    except curl_requests.RequestsError as exc:
        logger.warning("proxy_ping_failed", proxy_url=proxy["url"], platform=proxy["platform"], error=str(exc))
        return False


def _streak_key(proxy_url: str) -> str:
    return f"proxy_health_check:{proxy_url}:consecutive_failures"


def _record_outcome(redis_cache: RedisCache, proxy: proxies.ProxyRow, *, ok: bool) -> int:
    key = _streak_key(proxy["url"])
    proxies.record_proxy_outcome(proxy["platform"], proxy["url"], success=ok)
    if ok:
        redis_cache.delete(key)
        return 0
    streak = redis_cache.incr(key)
    redis_cache.expire(key, int(float(get_setting("health_check_streak_ttl_hours")) * 3600))
    return streak


def run() -> None:
    redis_cache = RedisCache()

    # Dedupe by row id - list_proxies(platform) returns the shared 'all'
    # row for every platform that can use it, so pinging per-platform would
    # otherwise ping the same physical proxy 3x per run.
    by_id: dict[int, proxies.ProxyRow] = {}
    for platform in PLATFORMS:
        for proxy in proxies.list_proxies(platform):
            by_id[proxy["id"]] = proxy

    if not by_id:
        logger.info("proxy_health_check_no_proxies_configured")
        return

    for proxy in by_id.values():
        ok = ping_proxy(proxy)
        streak = _record_outcome(redis_cache, proxy, ok=ok)
        if ok:
            logger.info("proxy_health_check_ok", proxy_url=proxy["url"], platform=proxy["platform"])
            continue
        logger.error(
            "proxy_health_check_failed",
            telegram=streak >= int(get_setting("health_check_alert_after_failures")),
            proxy_url=proxy["url"],
            platform=proxy["platform"],
            consecutive_failures=streak,
            hint="connectivity probe got no response through this proxy - the proxy server itself "
            "may be down, not just rate-limited by a target platform",
        )


if __name__ == "__main__":
    run()
    sys.exit(0)
