"""Listens on Kafka's crawl_requests topic and launches the matching
subprocess for each request - the consumer side of cinemark-api's manual
"run crawl"/"refresh token" endpoints and the daily scheduled job (see
cinemark-api's app/services/kafka.py + app/api/routes/scraper.py).

Requests are processed one at a time, not concurrently - firing several
subprocesses at once against the same Facebook session/account is exactly
the kind of burst this project's throttling/jitter elsewhere is designed to
avoid.

Run with:
    python -m social_crawler.crawl_request_consumer
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
)
from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache

logger = get_logger(__name__)

CRAWL_REQUESTS_TOPIC = "crawl_requests"
CONSUMER_GROUP = "spider-hub.crawl-requests"

SCRAPY_BIN = str(Path(sys.executable).parent / "scrapy")
PYTHON_BIN = sys.executable

REPO_ROOT = Path(__file__).resolve().parent.parent

SPIDER_BY_PLATFORM = {"facebook": "facebook_search", "threads": "threads_search", "tiktok": "tiktok_hashtag_search"}

# Every platform with its own browser-bootstrap token cache (see
# constants/facebook.py + constants/threads.py) - a refresh_token request
# names which one via "platform" (defaults to facebook for any
# already-queued/legacy message from before this was multi-platform).
TOKEN_REFRESH_BOOTSTRAP_MODULE = {
    "facebook": "social_crawler.spiders.facebook.auth.bootstrap",
    "threads": "social_crawler.spiders.threads.auth.bootstrap",
}

TOKEN_REFRESH_QUERY = "tin tức hôm nay"

DEFAULT_SWEEP_DAYS = 60  # ~2 months


def _sweep_days_for(request: dict[str, Any]) -> int:
    """facebook_search's sweep_days always covers the last N days *from
    today* (see _date_windows in the spider) - it has no concept of an
    arbitrary historical end_date. So a user-picked range doesn't map to
    literal start_date/end_date args (a single unfiltered query, capped by
    Facebook at N results total); it maps to *how many days* to sweep,
    using the range's length as that day count. No range picked at all -
    DEFAULT_SWEEP_DAYS, same as before."""
    start_raw, end_raw = request.get("start_date"), request.get("end_date")
    if start_raw and end_raw:
        start, end = date.fromisoformat(start_raw), date.fromisoformat(end_raw)
        return max((end - start).days + 1, 1)
    if start_raw:
        return max((date.today() - date.fromisoformat(start_raw)).days + 1, 1)
    return DEFAULT_SWEEP_DAYS


async def _run_subprocess(args: list[str]) -> int:
    """Runs args as a subprocess, letting stdout/stderr flow straight
    through to this process's own stdout, and returns the exit code. Shared
    by both request types below."""
    # stderr merged into stdout: structlog's console renderer already writes
    # to stdout anyway, so nothing meaningful would show up on a separate
    # stderr stream.
    process = await asyncio.create_subprocess_exec(*args, cwd=REPO_ROOT)
    return await process.wait()


def _facebook_session_is_cached() -> bool:
    """Whether the currently-active Facebook account (or the default, if
    bootstrap has never run) still has a live token cache in Redis. Redis
    itself expiring the key *is* the "expired" signal - graphql_client.py
    raises SessionExpiredError the moment this key is gone, so checking
    existence is enough, no separate TTL math needed."""
    cache = RedisCache()
    account = cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    return cache.exists(CACHE_REDIS_KEY_TMPL.format(account=account))


async def _ensure_facebook_session() -> bool:
    """Refreshes the Facebook token cache first if it's missing/expired, so
    a crawl triggered right after a >6h idle stretch (see
    CACHE_MAX_AGE_SECONDS) succeeds on the first try instead of failing with
    session_expired and needing scripts/refresh_token.sh's next tick or a
    manual re-trigger. Returns whether a session is available to crawl with
    once this returns - False means the refresh itself failed, so the
    caller should give up rather than run a crawl doomed to hit the same
    session_expired error immediately."""
    if _facebook_session_is_cached():
        return True

    logger.info("facebook_session_expired_refreshing_first")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
    returncode = await _run_subprocess(args)
    if returncode != 0:
        logger.error("facebook_session_refresh_before_crawl_failed", returncode=returncode)
        return False
    return True


async def _run_spider(request: dict[str, Any]) -> None:
    platform = request.get("platform")
    spider_name = SPIDER_BY_PLATFORM.get(platform)
    if spider_name is None:
        logger.warning("unsupported_platform", platform=platform, request=request)
        return

    keyword = request.get("keyword")
    if not keyword:
        logger.warning("crawl_request_missing_keyword", request=request)
        return

    if platform == "facebook" and not await _ensure_facebook_session():
        return

    if platform == "tiktok":
        # TikTok's "keyword" is actually a hashtag slug (e.g.
        # "#HoLinhTrangSi" - see spiders/tiktok/features/hashtag_search),
        # not a free-text search query, and its feed has no date filter to
        # sweep - none of query/include_entities/sweep_days/start_date/
        # end_date apply here the way they do for facebook_search/
        # threads_search.
        args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"hashtag={keyword}"]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
    else:
        args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"query={keyword}", "-a", "include_entities=false"]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        # end_date anchors the sweep - without it the spider always sweeps
        # the last sweep_days days ending *today*, regardless of the picked
        # range. start_date is passed for symmetry/log readability only;
        # sweep_days (derived from the range's length, below) is what
        # actually controls how far back the sweep goes.
        if request.get("start_date"):
            args += ["-a", f"start_date={request['start_date']}"]
        if request.get("end_date"):
            args += ["-a", f"end_date={request['end_date']}"]
        # sweep_window_days deliberately omitted - the spider sizes it
        # automatically from sweep_days (see _auto_window_days in
        # search.py) instead of every caller having to pick a fixed width.
        args += ["-a", f"sweep_days={_sweep_days_for(request)}"]

    logger.info("crawl_request_started", platform=platform, keyword=keyword, keyword_id=request.get("keyword_id"))
    returncode = await _run_subprocess(args)

    if returncode != 0:
        logger.error("crawl_request_failed", platform=platform, keyword=keyword, returncode=returncode)
    else:
        logger.info("crawl_request_finished", platform=platform, keyword=keyword)


async def _refresh_token(request: dict[str, Any]) -> None:
    """Same command scripts/refresh_token.sh's 4h cron already runs for
    Facebook - just triggered on demand instead of waiting for the next
    tick. Also handles Threads the same way, since its bootstrap.py mirrors
    Facebook's token-cache flow field for field (see
    spiders/threads/auth/bootstrap.py). platform/started/finished/failed are
    logged explicitly (not just inferred from the module name) because
    cinemark-api's refresh_tracker tails this exact log file and matches on
    "token_refresh_{started,finished,failed} ... platform=<x>" to know when
    a dashboard-triggered refresh is done - see
    cinemark-api/app/services/refresh_tracker.py."""
    platform = request.get("platform", "facebook")
    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform)
        return

    args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY]

    logger.info("token_refresh_started", platform=platform)
    returncode = await _run_subprocess(args)

    if returncode != 0:
        logger.error("token_refresh_failed", platform=platform, returncode=returncode)
    else:
        logger.info("token_refresh_finished", platform=platform)


async def _handle_request(request: dict[str, Any]) -> None:
    if request.get("type") == "refresh_token":
        await _refresh_token(request)
    else:
        await _run_spider(request)


async def run() -> None:
    consumer = AIOKafkaConsumer(
        CRAWL_REQUESTS_TOPIC,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # "earliest": a request queued while this consumer happened to be
        # down should still run once it's back up, not be silently dropped -
        # crawl requests are rare enough (button clicks, one daily batch)
        # that processing a short backlog on restart is never a problem.
        auto_offset_reset="earliest",
        # Commit only after _handle_request returns (see the explicit
        # consumer.commit() call below), not on aiokafka's default 5s timer.
        # A single crawl can run for many minutes - the default timer would
        # commit a message's offset almost immediately after it's received,
        # long before the crawl it triggered actually finishes. If this
        # process then died mid-crawl, Kafka would never redeliver that
        # request - it's just gone, with no trace beyond an orphaned
        # "running" DB row.
        enable_auto_commit=False,
        # This loop awaits one whole crawl (up to sweep_days=60 by default,
        # see _sweep_days_for) before calling back into the consumer for the
        # next message - Kafka's default 5-minute max_poll_interval_ms is
        # nowhere near enough for that; once exceeded, the broker silently
        # revokes group membership mid-crawl. 1h covers any realistic single
        # sweep.
        max_poll_interval_ms=3_600_000,
    )
    try:
        await consumer.start()
        logger.info("crawl_request_consumer_started", topic=CRAWL_REQUESTS_TOPIC)
        async for message in consumer:
            try:
                await _handle_request(message.value)
            except Exception as exc:
                # One bad request must not kill the whole consumer - log and
                # move on to the next one.
                logger.error("crawl_request_error", error=str(exc), request=message.value)
            # Committed whether _handle_request succeeded or was logged and
            # skipped above - either way this message is done, not to be
            # redelivered on the next restart.
            await consumer.commit()
    except KafkaError as exc:
        logger.error("kafka_error", error=str(exc))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())
