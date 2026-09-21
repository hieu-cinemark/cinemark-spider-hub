"""Proactive, low-volume health check for native crawl paths - meant to
catch a *silent* full outage (every request comes back HTTP 200 with
real-looking headers but empty/no data) before a human notices the
dashboard just isn't filling up. This is a different failure class from
what services/pool.py's circuit breaker already covers: a disabled account
or a cooling-down proxy is loud (logger.error, auto-alerted to Telegram)
the moment it happens, but "everything *looks* healthy and still returns
nothing" produces no error at all on its own - confirmed live the hard
way on 2026-09-17, when TikTok's guest-mode item_list endpoint went from
working to silently empty for hours.

TikTok probes with a synthetic guest identity (no account burn).
Facebook/Threads probe through the currently cached session (one cheap
search page) - if no session is bootstrapped they are skipped (not counted
as consecutive failures), so a cold box doesn't Telegram-spam.

Run periodically via cron (not a long-running process) - e.g. every 15-30
minutes:

    */15 * * * * cd /path/to/spider-hub && .venv/bin/python -m social_crawler.health_check

Exit code is always 0 - failures are reported via logger.error(telegram=True, ...).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.comet_graphql_client import SessionExpiredError
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokHashtagClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.hashtag_search.extract import (
    extract_response as extract_tiktok_hashtag,
)

logger = get_logger(__name__)

_PROBE_HASHTAG = "fyp"
# Benign Vietnamese movie-ish queries that usually return something on a
# healthy GraphQL search session - not a specific title (avoids "this film
# just has no posts today" false outages).
_PROBE_FACEBOOK_QUERY = "phim"
_PROBE_THREADS_QUERY = "phim"

_ALERT_AFTER_CONSECUTIVE_FAILURES = 2
_STREAK_TTL_SECONDS = 6 * 3600


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    skipped: bool = False
    detail: str = ""


def check_tiktok_item_list() -> ProbeResult:
    """One real hashtag search against _PROBE_HASHTAG through a fresh
    synthetic identity."""
    try:
        client = TikTokHashtagClient(synthetic=True)
        challenge_id = client.resolve_hashtag(_PROBE_HASHTAG)
        if not challenge_id:
            return ProbeResult(ok=False, detail="hashtag_not_found")
        response = client.search_hashtag(challenge_id, hashtag=_PROBE_HASHTAG)
    except (TikTokBlockedError, TikTokRateLimitedError, TikTokNetworkError) as exc:
        return ProbeResult(ok=False, detail=str(exc))
    videos = extract_tiktok_hashtag(response)
    return ProbeResult(ok=len(videos) > 0, detail=f"videos={len(videos)}")


def check_facebook_search() -> ProbeResult:
    """One GraphQL search page via the cached Facebook session."""
    try:
        from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient
        from social_crawler.spiders.facebook.features.search.extract import extract_response
    except Exception as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"import_failed:{exc}")

    try:
        client = FacebookGraphQLClient()
        response = client.search(_PROBE_FACEBOOK_QUERY, count=5)
        posts, _entities = extract_response(response)
        return ProbeResult(ok=len(posts) > 0, detail=f"posts={len(posts)}")
    except SessionExpiredError as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"no_session:{exc}")
    except Exception as exc:
        return ProbeResult(ok=False, detail=str(exc))


def check_threads_search() -> ProbeResult:
    """One GraphQL search page via the cached Threads session."""
    try:
        from social_crawler.spiders.threads.auth.graphql_client import ThreadsGraphQLClient
        from social_crawler.spiders.threads.features.search.extract import extract_response
    except Exception as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"import_failed:{exc}")

    try:
        client = ThreadsGraphQLClient()
        response = client.search(_PROBE_THREADS_QUERY, count=5)
        posts = extract_response(response)
        return ProbeResult(ok=len(posts) > 0, detail=f"posts={len(posts)}")
    except SessionExpiredError as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"no_session:{exc}")
    except Exception as exc:
        return ProbeResult(ok=False, detail=str(exc))


def _streak_key(name: str) -> str:
    return f"health_check:{name}:consecutive_failures"


def _record_outcome(redis_cache: RedisCache, name: str, *, ok: bool) -> int:
    key = _streak_key(name)
    if ok:
        redis_cache.delete(key)
        return 0
    streak = redis_cache.incr(key)
    redis_cache.expire(key, _STREAK_TTL_SECONDS)
    return streak


def _probe_one(redis_cache: RedisCache, name: str, result: ProbeResult) -> None:
    if result.skipped:
        logger.info(f"health_check_{name}_skipped", detail=result.detail)
        return

    streak = _record_outcome(redis_cache, name, ok=result.ok)
    if result.ok:
        logger.info(f"health_check_{name}_ok", detail=result.detail)
        return

    logger.error(
        f"health_check_{name}_failed",
        telegram=streak >= _ALERT_AFTER_CONSECUTIVE_FAILURES,
        detail=result.detail,
        consecutive_failures=streak,
        hint=(
            "native search returned no data (or errored) through the live session - "
            "check for a platform-side anti-bot / GraphQL change before assuming "
            "proxy/account alone"
        ),
    )


def run() -> None:
    redis_cache = RedisCache()
    _probe_one(redis_cache, "tiktok_item_list", check_tiktok_item_list())
    _probe_one(redis_cache, "facebook_search", check_facebook_search())
    _probe_one(redis_cache, "threads_search", check_threads_search())


if __name__ == "__main__":
    run()
    sys.exit(0)
