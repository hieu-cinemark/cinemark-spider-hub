"""Proactive proxy connectivity check - pings every enabled platform_proxies
row directly (a plain connectivity probe, not a full feature-level request -
see health_check.py for that different failure class: "looks healthy but
returns no data"). Feeds the exact same circuit breaker
services/pool.acquire_proxy_for_account already reads from
(db.record_proxy_outcome) - a proxy that fails here cools down exactly like
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

from social_crawler.logger import get_logger
from social_crawler.services import db
from social_crawler.services.redis import RedisCache

logger = get_logger(__name__)

PLATFORMS = ("facebook", "threads", "tiktok")

# generate_204 - a 204-No-Content endpoint several browsers/OSes already use
# for exactly this "is the network path actually usable" check: minimal
# payload, no redirects, no bot-detection to trip. Any response at all
# (status code doesn't matter) proves the proxy tunnel + TLS handshake
# worked; only a connection-level failure (never even got a response) means
# the proxy itself is down - see PING_TIMEOUT_SECONDS and the RequestsError
# handling below, same "resp is None -> network problem" distinction
# comet_graphql_client.py/tiktok/client.py already use for real requests.
PING_URL = "https://www.google.com/generate_204"
PING_TIMEOUT_SECONDS = 10.0

_ALERT_AFTER_CONSECUTIVE_FAILURES = 2
_STREAK_TTL_SECONDS = 6 * 3600


def _proxy_url_for(proxy: db.ProxyRow) -> str:
    if proxy.get("username") and proxy.get("password"):
        return f"http://{proxy['username']}:{proxy['password']}@{proxy['url']}"
    return f"http://{proxy['url']}"


def ping_proxy(proxy: db.ProxyRow) -> bool:
    """True if a request actually got a response back through this proxy -
    see module docstring for why the status code itself doesn't matter."""
    proxy_url = _proxy_url_for(proxy)
    try:
        curl_requests.get(
            PING_URL,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=PING_TIMEOUT_SECONDS,
            impersonate="chrome",
        )
        return True
    except curl_requests.RequestsError as exc:
        logger.warning("proxy_ping_failed", proxy_url=proxy["url"], platform=proxy["platform"], error=str(exc))
        return False


def _streak_key(proxy_url: str) -> str:
    return f"proxy_health_check:{proxy_url}:consecutive_failures"


def _record_outcome(redis_cache: RedisCache, proxy: db.ProxyRow, *, ok: bool) -> int:
    key = _streak_key(proxy["url"])
    db.record_proxy_outcome(proxy["platform"], proxy["url"], success=ok)
    if ok:
        redis_cache.delete(key)
        return 0
    streak = redis_cache.incr(key)
    redis_cache.expire(key, _STREAK_TTL_SECONDS)
    return streak


def run() -> None:
    redis_cache = RedisCache()

    # Dedupe by row id - list_proxies(platform) returns the shared 'all'
    # row for every platform that can use it, so pinging per-platform would
    # otherwise ping the same physical proxy 3x per run.
    by_id: dict[int, db.ProxyRow] = {}
    for platform in PLATFORMS:
        for proxy in db.list_proxies(platform):
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
            telegram=streak >= _ALERT_AFTER_CONSECUTIVE_FAILURES,
            proxy_url=proxy["url"],
            platform=proxy["platform"],
            consecutive_failures=streak,
            hint="connectivity probe got no response through this proxy - the proxy server itself "
            "may be down, not just rate-limited by a target platform",
        )


if __name__ == "__main__":
    run()
    sys.exit(0)
