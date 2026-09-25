"""Listens on Kafka's crawl_requests topic and launches the matching
subprocess for each request - the consumer side of cinemark-api's manual
"run crawl"/"refresh token" endpoints and the daily scheduled job (see
cinemark-api's app/services/kafka.py + app/api/routes/scraper.py).

Runs one independent consumer loop per platform (see PLATFORM_CONSUMER_GROUPS),
each in its own Kafka consumer group reading the same crawl_requests topic -
every group sees every message, but immediately skips (commits past) whatever
isn't its own platform. This is deliberate, not an oversight: within one
platform, requests are still processed strictly one at a time (firing several
subprocesses at once against the same account/session is exactly the kind of
burst this project's throttling/jitter elsewhere is designed to avoid) - but
that reasoning has nothing to do with a *different* platform's entirely
separate account/session/proxy, so a slow or stuck Facebook crawl must never
delay a Threads or TikTok request sitting in the same topic. Three consumer
groups instead of three topics keeps this a spider-hub-only change - cinemark-
api's producer side still publishes to one shared topic, unaware anything
changed on this side.

Run with:
    python -m social_crawler.crawl_request_consumer
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    REPLIES_REDIS_KEY_TMPL,
)
from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY as THREADS_ACTIVE_ACCOUNT_REDIS_KEY,
)
from social_crawler.constants.threads import (
    CACHE_REDIS_KEY_TMPL as THREADS_CACHE_REDIS_KEY_TMPL,
)
from social_crawler.constants.threads import (
    DEFAULT_ACCOUNT_KEY as THREADS_DEFAULT_ACCOUNT_KEY,
)
from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.kafka import CRAWL_REQUESTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache
from social_crawler.services.task_queue import finish_task, is_platform_draining, start_task

logger = get_logger(__name__)

# One consumer group per platform - see module docstring for why. Keys match
# every platform that can appear in a crawl request's "platform" field (each
# needs its own independent loop here). TikTok hashtag crawls share the
# "tiktok" group with comments/refresh/nurture for that platform.
#
# Operational note for whoever next renames one of these (or adds a new
# platform): a brand-new group id has no committed offset, and
# auto_offset_reset="earliest" below means it starts from the oldest message
# still retained on crawl_requests - replaying every already-handled request
# in that window as a fresh one (duplicate scrapy subprocesses/token
# refreshes). Before rolling out a rename, seek the new group id(s) to
# "latest" first:
#   kafka-consumer-groups.sh --bootstrap-server <host> --group <new-group> \
#     --topic crawl_requests --reset-offsets --to-latest --execute
# (done for spider-hub.crawl-requests.{facebook,threads,tiktok} when this
# split from the single "spider-hub.crawl-requests" group was rolled out.)
PLATFORM_CONSUMER_GROUPS = {
    "facebook": "spider-hub.crawl-requests.facebook",
    "threads": "spider-hub.crawl-requests.threads",
    "tiktok": "spider-hub.crawl-requests.tiktok",
}

# When a batch trigger (e.g. the scheduled cron) queues several keywords at
# once, this consumer's own strict one-at-a-time processing (see module
# docstring) would otherwise let them fire back to back with zero gap -
# same "perfectly uniform/back-to-back" bot signal the per-request jitter
# elsewhere in this project already guards against, just at the
# between-crawls level instead of between-pages. Same 5-20s range as
# Facebook's own sweep_pause_min/max (search.py) - one proven-reasonable
# value reused rather than inventing a second one.
INTER_REQUEST_PAUSE_MIN_SECONDS = 5.0
INTER_REQUEST_PAUSE_MAX_SECONDS = 20.0

# A request that fails because pool.acquire_proxy_for_account(required=True)
# found the whole platform's proxy pool exhausted (see that error's own
# docstring - a pinned account's proxy is cooling down and no healthy
# replacement exists either) is NOT "one bad request" the way a malformed
# payload or a dead account is - every *other* request for this platform
# sitting behind it in the topic is heading for the exact same wall until
# the cooldown clears (real observed range: 10 minutes up to 2 hours, see
# db.record_proxy_outcome's own backoff schedule). Before this backoff
# existed, the platform loop just kept committing past each one at full
# speed - confirmed live (2026-09-17) burning through an entire comments
# backlog in seconds, each attempt an instant, guaranteed-failure spider
# start/stop, right up until the cooldown actually expired on its own.
#
# On top of the backoff itself, this specific failure class is re-published
# back onto crawl_requests (see _requeue_after_proxy_exhaustion below)
# instead of just being logged and left committed-and-lost like every other
# exception here - it's the one failure mode this consumer can positively
# identify as "the request itself was fine, the platform just couldn't
# serve it *right now*", so a genuine retry (not just "stop hammering") is
# actually worth doing. Capped at MAX_PROXY_EXHAUSTED_REQUEUES re-publishes
# (tracked in the request's own "_proxy_exhausted_retries" field) so a
# platform with a permanently dead proxy pool (not just cooling down)
# doesn't loop forever - it eventually gets dropped for real, loudly.
PROXY_EXHAUSTED_BACKOFF_BASE_SECONDS = 30.0
PROXY_EXHAUSTED_BACKOFF_GROWTH_FACTOR = 2.0
PROXY_EXHAUSTED_BACKOFF_MAX_SECONDS = 300.0
MAX_PROXY_EXHAUSTED_REQUEUES = 3

# A single platform's Kafka session getting revoked (broker restart,
# network blip, a rebalance racing consumer.commit() into
# CommitFailedError/IllegalGenerationError - all land in
# _run_platform_consumer_once's own KafkaError handler) is transient and
# recoverable by just rejoining the group with a fresh consumer. Retried
# here first instead of immediately falling through to run()'s last-resort
# response, which cancels the *other two* platforms' loops too and exits
# the whole process for an external supervisor to restart - overkill for a
# hiccup local to one platform, and in a dev environment with no such
# supervisor it just leaves nothing consuming crawl_requests at all until a
# human notices. Only a broker that stays unreachable past
# _CONSUMER_RESTART_MAX_ATTEMPTS restarts within _CONSUMER_RESTART_WINDOW_SECONDS
# gives up and falls through to that heavier response.
_CONSUMER_RESTART_BACKOFF_BASE_SECONDS = 5.0
_CONSUMER_RESTART_BACKOFF_GROWTH_FACTOR = 2.0
_CONSUMER_RESTART_BACKOFF_MAX_SECONDS = 120.0
_CONSUMER_RESTART_MAX_ATTEMPTS = 5
_CONSUMER_RESTART_WINDOW_SECONDS = 600.0

SCRAPY_BIN = str(Path(sys.executable).parent / "scrapy")
PYTHON_BIN = sys.executable

REPO_ROOT = Path(__file__).resolve().parent.parent

SPIDER_BY_PLATFORM = {"facebook": "facebook_search", "threads": "threads_search"}
TIKTOK_HASHTAG_SPIDER = "tiktok_hashtag_search"
TIKTOK_CHANNEL_VIDEOS_SPIDER = "tiktok_channel_videos"


class CrawlJobFailed(Exception):
    """Subprocess finished unsuccessfully. The consumer records `failed`
    and keeps running - this is not a crash of the platform loop."""


class CrawlJobSkipped(Exception):
    """Request should not count as a completed crawl (BFS drain, cancel
    before the spider started)."""


def _raise_for_returncode(returncode: int, context: str) -> None:
    if returncode == PROXY_EXHAUSTED_EXIT_CODE:
        logger.warning("subprocess_proxy_exhausted", context=context, returncode=returncode)
        raise pool.ProxyPoolExhaustedError(context)
    if returncode != 0:
        logger.error("subprocess_failed", context=context, returncode=returncode)
        raise CrawlJobFailed(f"{context} (exit {returncode})")

# Every platform with its own comments/replies spider (see
# social_crawler/spiders/<platform>/features/comments/).
COMMENTS_SPIDER_BY_PLATFORM = {"facebook": "facebook_comments", "threads": "threads_comments", "tiktok": "tiktok_comments"}
# Facebook still needs a comments-query GraphQL cache. Threads only needs
# the search session cookies (REST text_feed replies). TikTok signs its own
# synthetic-identity curl_cffi requests per crawl (see
# spiders/tiktok/features/comments/comments.py's own module docstring - no
# browser at all since 2026-09-18), so it's absent from this set too.
COMMENTS_PLATFORMS_NEEDING_CACHE = {"facebook", "threads"}
COMMENTS_BOOTSTRAP_MODULE = {
    "facebook": "social_crawler.spiders.facebook.auth.bootstrap",
    "threads": "social_crawler.spiders.threads.auth.bootstrap",
}

# Every platform with its own browser-bootstrap token cache (see
# constants/facebook.py + constants/threads.py) - a refresh_token request
# names which one via "platform" (defaults to facebook for any
# already-queued/legacy message from before this was multi-platform).
TOKEN_REFRESH_BOOTSTRAP_MODULE = {
    "facebook": "social_crawler.spiders.facebook.auth.bootstrap",
    "threads": "social_crawler.spiders.threads.auth.bootstrap",
    "tiktok": "social_crawler.spiders.tiktok.auth.bootstrap",
}

TOKEN_REFRESH_QUERY = "tin tức hôm nay"

DEFAULT_SWEEP_DAYS = 60  # ~2 months


def _sweep_days_for(request: dict[str, Any]) -> int:
    """How many days facebook_search should sweep. The spider anchors
    windows at end_date (or today) and walks back sweep_days (see
    _date_windows). A user-picked range maps to that day count rather than
    a single unfiltered query Facebook would cap. No range - DEFAULT_SWEEP_DAYS."""
    start_raw, end_raw = request.get("start_date"), request.get("end_date")
    if start_raw and end_raw:
        start, end = date.fromisoformat(start_raw), date.fromisoformat(end_raw)
        return max((end - start).days + 1, 1)
    if start_raw:
        return max((date.today() - date.fromisoformat(start_raw)).days + 1, 1)
    return DEFAULT_SWEEP_DAYS


CRAWL_JOB_KEY_TMPL = "crawl_job:{platform}"
CRAWL_JOB_CANCEL_KEY_TMPL = "crawl_job_cancel:{run_id}"
CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL = "crawl_job_cancel_platform:{platform}"
# Dashboard refresh_tracker used to only tail consumer.log, but a consumer
# started in a terminal (stdout, no tee) never writes that file - the
# panel stayed on "running" with 0 lines even after token_refresh_finished.
# This key is the durable done-signal (same prefix as every other RedisCache
# key). TTL covers a slow dashboard reconnect without leaving leftovers.
REFRESH_RESULT_KEY_TMPL = "token_refresh_result:{run_id}"
REFRESH_RESULT_TTL_SECONDS = 600
# How often _run_subprocess checks for a cancel request while a crawl
# subprocess is running - short enough that the dashboard's Stop button
# feels responsive, long enough not to hammer Redis for a job that
# normally runs for minutes.
JOB_CANCEL_POLL_SECONDS = 0.25
# SIGTERM first (Scrapy's own Twisted reactor catches it and shuts the
# spider down cleanly - closes the Kafka producer, flushes logs), SIGKILL
# only if that doesn't land in time.
JOB_CANCEL_KILL_GRACE_SECONDS = 10.0


def _mark_refresh_result(run_id: str | None, *, ok: bool) -> None:
    if not run_id:
        return
    RedisCache().set(
        REFRESH_RESULT_KEY_TMPL.format(run_id=run_id),
        {"ok": ok},
        ttl_seconds=REFRESH_RESULT_TTL_SECONDS,
    )


async def _run_subprocess(
    args: list[str],
    *,
    run_id: str | None = None,
    platform: str | None = None,
    honor_platform_cancel: bool = True,
) -> int:
    """Runs args as a subprocess, letting stdout/stderr flow straight
    through to this process's own stdout, and returns the exit code.

    run_id and/or platform, when given, make this cancellable from outside:
    polls CRAWL_JOB_CANCEL_KEY_TMPL / CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL
    (set by cinemark-api's POST /<platform>/stop) and signals the whole
    process group when either key appears. Neither set keeps a plain wait.

    honor_platform_cancel=False is for refresh_token / cookie_import: Stop
    leaves crawl_job_cancel_platform armed so leftover crawls die, but a
    later Restore/refresh must still run. Stop during an in-flight refresh
    still works via crawl_job_cancel:<run_id>."""
    # Own process group so SIGTERM/SIGKILL reach Scrapy *and* nested
    # browsers (Patchright/Chrome) instead of leaving orphans.
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=REPO_ROOT,
        start_new_session=True,
    )
    if run_id is None and platform is None:
        return await process.wait()

    cache = RedisCache()
    cancel_key = CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id) if run_id else None
    platform_cancel_key = (
        CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL.format(platform=platform) if platform else None
    )

    def _signal_group(sig: signal.Signals) -> None:
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass

    while True:
        try:
            return await asyncio.wait_for(process.wait(), timeout=JOB_CANCEL_POLL_SECONDS)
        except TimeoutError:
            cancelled = cancel_key is not None and cache.exists(cancel_key)
            if (
                not cancelled
                and honor_platform_cancel
                and platform_cancel_key is not None
                and cache.exists(platform_cancel_key)
            ):
                cancelled = True
            if not cancelled:
                continue
            logger.warning("crawl_job_cancel_requested", run_id=run_id, platform=platform)
            _signal_group(signal.SIGTERM)
            try:
                return await asyncio.wait_for(process.wait(), timeout=JOB_CANCEL_KILL_GRACE_SECONDS)
            except TimeoutError:
                logger.warning("crawl_job_force_killed", run_id=run_id, platform=platform)
                _signal_group(signal.SIGKILL)
                return await process.wait()


async def _sleep_interruptible(platform: str, seconds: float) -> bool:
    """Sleep up to `seconds`, returning True as soon as Stop armed drain
    so the next Kafka message is skipped instead of waiting out a pause."""
    deadline = time.monotonic() + seconds
    while True:
        if is_platform_draining(platform):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(JOB_CANCEL_POLL_SECONDS, remaining))


def _cancel_requested(*, run_id: str | None = None, platform: str | None = None) -> bool:
    cache = RedisCache()
    if run_id and cache.exists(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id)):
        return True
    if platform and cache.exists(CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL.format(platform=platform)):
        return True
    return False


def _stopped_for(*, run_id: str | None, platform: str, bypass_drain: bool) -> bool:
    """Bypass-aware version of the `_cancel_requested(...) or
    is_platform_draining(...)` check comments/channel_videos/nurture each
    poll before/during their own subprocess. A run_id-specific cancel (a
    Stop click that landed *after* this exact run started) always applies.
    The platform-wide signals (crawl_job_cancel_platform / platform_drain /
    comments_drain - all armed by an earlier Stop, see cinemark-api's
    crawl_jobs.request_stop) are skipped when bypass_drain is set - see
    _handle_request's own docstring for why one of these one-off actions
    must not inherit an old Stop meant for a different, already-drained
    backlog."""
    if _cancel_requested(run_id=run_id):
        return True
    if bypass_drain:
        return False
    return _cancel_requested(platform=platform) or is_platform_draining(platform)


def _facebook_session_is_cached(account: str | None = None) -> bool:
    """Whether `account` (or the currently-active Facebook account, or the
    default if bootstrap has never run) still has a live token cache in
    Redis. Redis itself expiring the key *is* the "expired" signal -
    graphql_client.py raises SessionExpiredError the moment this key is
    gone, so checking existence is enough, no separate TTL math needed.

    `account`, when given, checks that SPECIFIC account instead of
    whichever is currently active - see _run_comments_spider's own comment
    for why a comments-crawl job pins one account across all three of its
    ensure_* calls instead of letting each one independently read/rotate
    ACTIVE_ACCOUNT_REDIS_KEY."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    return cache.exists(CACHE_REDIS_KEY_TMPL.format(account=target))


async def _ensure_facebook_session(
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str | None = None,
    honor_platform_cancel: bool = True,
) -> bool:
    """Refreshes the Facebook token cache first if it's missing/expired, so
    a crawl triggered right after a >6h idle stretch (see
    CACHE_MAX_AGE_SECONDS) succeeds on the first try instead of failing with
    session_expired and needing scripts/refresh_token.sh's next tick or a
    manual re-trigger. Returns whether a session is available to crawl with
    once this returns - False means the refresh itself failed, so the
    caller should give up rather than run a crawl doomed to hit the same
    session_expired error immediately.

    `account`, when given, is passed through to bootstrap.py as --account -
    refreshes/checks that one specifically instead of letting bootstrap.py's
    own next_account() rotate freely (see _run_comments_spider)."""
    # Off the event loop: RedisCache wraps the synchronous `redis` client,
    # and this loop is now shared with threads/tiktok's own consumer loops
    # (see run()) - a slow/hanging Redis call here would otherwise stall
    # their Kafka polling too, exactly the cross-platform coupling this
    # whole per-platform-loop split was meant to eliminate.
    if await asyncio.to_thread(_facebook_session_is_cached, account):
        return True

    logger.info("facebook_session_expired_refreshing_first", account=account)
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
    if account:
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("facebook_session_refresh_before_crawl_failed", returncode=returncode)
        return False
    return True


def _facebook_comments_cache_usable(account: str | None = None) -> bool:
    """Whether `account` (or the currently-active Facebook account) has a
    comments-query cache in Redis that can actually paginate - see
    FacebookGraphQLClient._get_comments_cache. Unlike the search cache
    (_facebook_session_is_cached), this one query template works for *any*
    post's comments once captured (only the `id` variable changes per post
    - see graphql_client.py's get_comments), so it only ever needs
    bootstrapping once per TTL, not once per post.

    Checks for the `pagination` sub-key, not just whether the cache exists
    at all - bootstrap.py only captures a CommentsListComponentsPaginationQuery
    if the post it scrolled during that run actually had enough comments to
    trigger Facebook serving a second page (see comments_trigger); a cache
    bootstrapped against a low-comment post exists but can never fetch past
    ~2 comments for any post, silently, until the next bootstrap happens to
    hit a high-comment post (confirmed happening for real: every comments
    crawl topped out at ~2 for hours because of exactly this). Re-checking
    this on every call instead of trusting existence alone means a bad
    cache gets retried instead of being stuck until its TTL expires."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    comments_cache = cache.get(COMMENTS_REDIS_KEY_TMPL.format(account=target))
    return bool(comments_cache and comments_cache.get("pagination"))


def _facebook_replies_cache_usable(account: str | None = None) -> bool:
    """Whether `account` (or the currently-active Facebook account) has a
    replies-query cache in Redis that can actually paginate - same
    rationale and same `pagination` sub-key requirement as
    _facebook_comments_cache_usable, just for _fetch_replies's own
    get_replies/get_replies_next_page instead of the top-level comments
    loop. A *separate* cache from the comments one (REPLIES_REDIS_KEY_TMPL,
    not COMMENTS_REDIS_KEY_TMPL) because capturing it needs a different
    browser trigger (replies_trigger's own "click one comment's replies
    open", not comments_trigger's "just open+sort the top-level list") -
    confirmed happening for real (2026-09-16): a comments-only bootstrap
    left every comment-with-replies in a real crawl failing
    session_expired on this specific cache, even though the top-level
    comments themselves fetched fine."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    replies_cache = cache.get(REPLIES_REDIS_KEY_TMPL.format(account=target))
    return bool(replies_cache and replies_cache.get("pagination"))


def _threads_comments_cache_exists() -> bool:
    """Whether the currently-active Threads account has a usable session
    cache in Redis. Unlike _facebook_comments_cache_usable, this doesn't
    need a comments-query `pagination` section: threads_comments GETs
    /api/v1/text_feed/<id>/replies/ with the search session's cookies
    (see graphql_client.get_text_feed_replies). Any successful search or
    --post-url bootstrap is enough.

    Checks CACHE_REDIS_KEY_TMPL, not COMMENTS_REDIS_KEY_TMPL - the GraphQL
    replies recipe in COMMENTS_REDIS_KEY_TMPL replays as direct_replies: null."""
    cache = RedisCache()
    account = cache.get(THREADS_ACTIVE_ACCOUNT_REDIS_KEY) or THREADS_DEFAULT_ACCOUNT_KEY
    return cache.exists(THREADS_CACHE_REDIS_KEY_TMPL.format(account=account))


_COMMENTS_CACHE_USABLE_CHECK = {
    "facebook": _facebook_comments_cache_usable,
    "threads": _threads_comments_cache_exists,
}


async def _ensure_comments_cache(
    platform: str,
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    honor_platform_cancel: bool = True,
) -> bool:
    """Same idea as _ensure_facebook_session, for a platform's comments-
    query cache instead of its search one - bootstraps it from post_url (a
    real post this request already named, so bootstrap has something to
    open and capture a comments query from) the first time it's missing/
    unusable (see each platform's own check in _COMMENTS_CACHE_USABLE_CHECK).

    `account`: facebook-only (threads' own check takes no account param -
    see _threads_comments_cache_exists), passed through to bootstrap.py as
    --account. See _run_comments_spider for why this matters."""
    check = _COMMENTS_CACHE_USABLE_CHECK[platform]
    usable = await asyncio.to_thread(check, account) if platform == "facebook" else await asyncio.to_thread(check)
    if usable:
        return True

    logger.info("comments_cache_missing_bootstrapping", platform=platform, post_url=post_url, account=account)
    args = [PYTHON_BIN, "-m", COMMENTS_BOOTSTRAP_MODULE[platform], "--post-url", post_url]
    if account and platform == "facebook":
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("comments_bootstrap_failed", platform=platform, returncode=returncode)
        return False
    return True


async def _ensure_replies_cache(
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str = "facebook",
    honor_platform_cancel: bool = True,
) -> bool:
    """Facebook-only (no Threads/TikTok equivalent - see _fetch_replies's
    own docstring on why Threads doesn't need this and TikTok comments
    aren't in COMMENTS_PLATFORMS_NEEDING_CACHE at all): same idea as
    _ensure_comments_cache, for the separate replies-query cache a
    comment-with-replies needs (_facebook_replies_cache_usable). Bootstraps
    with --type replies, which needs a post_url whose top-level comments
    actually include one with replies to click open - the same post_url
    the comments crawl itself was queued for is good enough in practice
    (most real posts with any engagement have at least one reply
    somewhere), and a bootstrap that can't find one just fails cleanly
    (comments_bootstrap_failed) rather than silently caching nothing."""
    if await asyncio.to_thread(_facebook_replies_cache_usable, account):
        return True

    logger.info("replies_cache_missing_bootstrapping", post_url=post_url, account=account)
    args = [
        PYTHON_BIN,
        "-m",
        "social_crawler.spiders.facebook.auth.bootstrap",
        "--post-url",
        post_url,
        "--type",
        "replies",
    ]
    if account:
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("replies_bootstrap_failed", returncode=returncode)
        return False
    return True


async def _run_comments_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Handles a type="comments" request (see cinemark-api's
    publish_comments_crawl_request) - runs the target platform's comments
    spider for one specific post (see COMMENTS_SPIDER_BY_PLATFORM; a
    platform not in it has no comments feature built at all - see
    get_comment_mapper's docstring on the cinemark-api side). Shares
    crawl_job:<platform> with _run_spider's search crawls, which is
    deliberate, not an oversight - both go through the same
    one-at-a-time-per-platform consumer loop (see module docstring), so a
    comments crawl and a search crawl for the same platform already never
    run concurrently against the same account/session."""
    platform = request.get("platform", "facebook")
    spider_name = COMMENTS_SPIDER_BY_PLATFORM.get(platform)
    if spider_name is None:
        logger.warning("unsupported_comments_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported comments platform {platform}")

    post_id = request.get("post_id")
    post_url = request.get("post_url")
    if not post_id or not post_url:
        logger.warning("comments_request_missing_fields", request=request)
        raise CrawlJobFailed("comments request missing post_id or post_url")

    # Advertise the job *before* ensure_*/bootstrap so Stop can cancel the
    # long comments-cache capture - previously crawl_job was only set once
    # the scrapy spider started, so the first Stop during bootstrap armed
    # drain but never killed anything, and the crawl kept going.
    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "type": "comments",
                "post_id": post_id,
                "started_at": int(time.time()),
            },
        )

    def _stopped() -> bool:
        return _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain)

    returncode: int | None = None
    try:
        # Pin one account across every ensure_* call below instead of letting
        # each one independently read/rotate ACTIVE_ACCOUNT_REDIS_KEY - see
        # _facebook_replies_cache_usable's own docstring for the failure this
        # fixes. Starts as a best-effort guess (whichever account is already
        # active); _ensure_facebook_session may bootstrap a *different* one if
        # that guess turns out stale (no account row yet, or its session
        # expired), so it's re-read right after - every following ensure_* call
        # targets that same, now-confirmed-live account explicitly.
        target_account = RedisCache().get(ACTIVE_ACCOUNT_REDIS_KEY) if platform == "facebook" else None
        if platform == "facebook":
            if _stopped():
                logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
                raise CrawlJobSkipped("comments cancelled before start")
            if not await _ensure_facebook_session(
                target_account, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
            ):
                raise CrawlJobFailed("facebook session refresh failed before comments crawl")
            if target_account is None:
                # Only trust a fresh read here when we didn't already have a
                # specific account pinned - bootstrap.py's own next_account()
                # just picked one (there was nothing to pin it to before), so
                # this is the only place that choice becomes knowable. If
                # target_account was already set, _ensure_facebook_session
                # checked/bootstrapped *that exact one* - re-reading the shared
                # key here could pick up an unrelated concurrent change and
                # silently un-pin us, the exact bug this whole mechanism exists
                # to prevent.
                target_account = RedisCache().get(ACTIVE_ACCOUNT_REDIS_KEY)
        if platform in COMMENTS_PLATFORMS_NEEDING_CACHE:
            if _stopped():
                logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
                raise CrawlJobSkipped("comments cancelled before start")
            comments_ok = (
                await _ensure_comments_cache(
                    platform, post_url, target_account, run_id=run_id, honor_platform_cancel=not bypass_drain
                )
                if platform == "facebook"
                else await _ensure_comments_cache(
                    platform, post_url, run_id=run_id, honor_platform_cancel=not bypass_drain
                )
            )
            if not comments_ok:
                raise CrawlJobFailed("comments cache bootstrap failed")
        include_replies = False
        if platform == "facebook":
            # Best-effort, NOT a gate on the rest of this job (2026-09-16, fixed
            # same day it was introduced) - confirmed live that treating this
            # as required tanked real comment yield to ~0.02% on a 100-post
            # batch: most posts don't have a reply thread this bootstrap can
            # easily latch onto (needs a comment whose replies are big enough
            # to paginate - see _ensure_replies_cache's own docstring), so
            # requiring it before running ANY comments crawl was aborting the
            # entire job - losing that post's perfectly-fetchable top-level
            # comments too - instead of just the one thing that's actually
            # unavailable. comments.py's own _fetch_replies already degrades
            # gracefully per-comment when this cache is missing (logs
            # replies_pagination_not_bootstrapped and moves on to the next
            # comment) - there was never a need to gate the whole job on it.
            #
            # Skip entirely unless the request opts into replies: each reply
            # bootstrap + per-comment reply round trip dominated wall time on
            # comment-heavy posts, and the replies GraphQL path currently
            # returns empty edges for most accounts. Default spider arg is
            # include_replies=false for the same reason.
            include_replies = str(request.get("include_replies", "false")).lower() not in (
                "false",
                "0",
                "no",
                "",
            )
            if include_replies and not await _ensure_replies_cache(
                post_url, target_account, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
            ):
                logger.warning(
                    "replies_cache_unavailable_continuing",
                    post_url=post_url,
                    account=target_account,
                    note="top-level comments will still be fetched; replies on any comment will be skipped this run",
                )

        if _stopped():
            logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
            raise CrawlJobSkipped("comments cancelled before start")

        if platform == "tiktok":
            # tiktok_comments takes video_id/video_url, not post_id/post_url -
            # it never touches a cached doc_id/token the way Facebook/Threads
            # do, so it has no need to share their generic param names either.
            args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"video_id={post_id}", "-a", f"video_url={post_url}"]
        else:
            args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"post_id={post_id}"]
            if platform == "threads":
                # Optional; REST replies only need post_id. Kept so a log/debug
                # line still has the permalink the dashboard queued.
                args += ["-a", f"post_url={post_url}"]
                # text_feed accepts up to ~100 per page; consumer used to omit
                # count and inherit spider 25, burning max_pages budget early on
                # large threads. Still cannot beat Threads' ranked visibility
                # ceiling (cursor ends with unread badge remainder).
                args += ["-a", f"count={request.get('count', 100)}"]
            if platform == "facebook":
                # Pin the exact account every ensure_* call above just verified
                # (or freshly bootstrapped) usable for session+comments+replies,
                # instead of letting FacebookCommentsSpider fall back to reading
                # ACTIVE_ACCOUNT_REDIS_KEY itself once the scrapy subprocess
                # actually starts a moment later - confirmed happening for real
                # (2026-09-16), twice over: first as something else (a
                # concurrent bootstrap run outside this consumer's own
                # one-at-a-time loop, e.g. a human running bootstrap.py by hand,
                # or the 4h refresh_token.sh cron) repointing
                # ACTIVE_ACCOUNT_REDIS_KEY between this check and the subprocess
                # reading it; then again as each ensure_* call's own
                # next_account() rotation picking a *different* account than
                # the one before it, so by the end the "active" account had
                # never actually had all three caches verified together. Using
                # target_account directly (not a fresh Redis read) avoids both.
                if target_account:
                    args += ["-a", f"account={target_account}"]
                # Densest Comet pagination page (browser uses -1) + skip the
                # broken/slow replies path unless the queue explicitly opts in.
                args += ["-a", f"count={request.get('count', -1)}"]
                args += ["-a", f"include_replies={'true' if include_replies else 'false'}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]

        logger.info("comments_crawl_started", platform=platform, post_id=post_id, run_id=run_id)
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode is None:
        raise CrawlJobSkipped("comments cancelled before start")
    _raise_for_returncode(
        returncode, f"{platform} comments crawl post_id={post_id}"
    )
    logger.info("comments_crawl_finished", platform=platform, post_id=post_id)


async def _run_channel_videos_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Handles type="channel_videos" (TikTok only) - scrapy
    tiktok_channel_videos for one @username. Shares crawl_job:tiktok with
    hashtag search / comments (same one-at-a-time consumer loop)."""
    platform = request.get("platform") or "tiktok"
    if platform != "tiktok":
        logger.warning("unsupported_channel_videos_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"channel_videos only supported on tiktok, got {platform}")

    username = str(request.get("username") or "").lstrip("@").strip()
    if not username:
        logger.warning("channel_videos_missing_username", request=request)
        raise CrawlJobFailed("channel_videos request missing username")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "type": "channel_videos",
                "username": username,
                "started_at": int(time.time()),
            },
        )

    def _stopped() -> bool:
        return _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain)

    if _stopped():
        logger.info("channel_videos_cancelled_before_start", username=username)
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))
        raise CrawlJobSkipped("channel_videos cancelled before start")

    args = [SCRAPY_BIN, "crawl", TIKTOK_CHANNEL_VIDEOS_SPIDER, "-a", f"username={username}"]
    if request.get("keyword_id"):
        args += ["-a", f"keyword_id={request['keyword_id']}"]
    if request.get("max_pages"):
        args += ["-a", f"max_pages={request['max_pages']}"]

    logger.info("channel_videos_started", username=username, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    _raise_for_returncode(returncode, f"tiktok channel_videos username={username}")
    logger.info("channel_videos_finished", username=username)


async def _run_spider(request: dict[str, Any]) -> None:
    platform = request.get("platform")

    # A Stop click arms bfs_drain:<platform> in Redis (see cinemark-api's
    # crawl_jobs.request_stop) precisely so the BFS-discovered crawls
    # already queued behind whatever it cancelled don't just keep running
    # one after another - only requests with a bfs_depth are drained
    # (bfs_depth is never set on a real dashboard/cron-triggered request),
    # so a manually queued crawl for a different keyword still runs
    # normally even while a drain is armed.
    if request.get("bfs_depth") is not None and RedisCache().exists(f"bfs_drain:{platform}"):
        logger.info("bfs_request_skipped_drain", platform=platform, keyword=request.get("keyword"))
        raise CrawlJobSkipped("bfs drain")

    keyword = request.get("keyword")
    if not keyword:
        logger.warning("crawl_request_missing_keyword", request=request)
        raise CrawlJobFailed("crawl request missing keyword")

    if platform not in ("facebook", "threads", "tiktok"):
        logger.warning("unsupported_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported platform {platform}")

    if platform == "tiktok" and not str(keyword).startswith("#"):
        logger.info(
            "crawl_request_skipped_tiktok_text",
            keyword=keyword,
            keyword_id=request.get("keyword_id"),
        )
        raise CrawlJobSkipped("tiktok is hashtag-only")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {"run_id": run_id, "keyword": keyword, "keyword_id": request.get("keyword_id"), "started_at": int(time.time())},
        )

    if platform == "tiktok":
        args = [SCRAPY_BIN, "crawl", TIKTOK_HASHTAG_SPIDER, "-a", f"hashtag={keyword.lstrip('#')}"]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        if request.get("bfs_depth"):
            args += ["-a", f"bfs_depth={request['bfs_depth']}"]
    else:
        args = [SCRAPY_BIN, "crawl", SPIDER_BY_PLATFORM[platform], "-a", f"query={keyword}", "-a", "include_entities=false"]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        # Date windows are Facebook-only (Threads has no posted-date filter).
        if platform == "facebook":
            if request.get("start_date"):
                args += ["-a", f"start_date={request['start_date']}"]
            if request.get("end_date"):
                args += ["-a", f"end_date={request['end_date']}"]
            args += ["-a", f"sweep_days={_sweep_days_for(request)}"]
        elif request.get("start_date") or request.get("end_date"):
            logger.info(
                "crawl_request_dates_dropped",
                platform=platform,
                keyword=keyword,
                keyword_id=request.get("keyword_id"),
                start_date=request.get("start_date"),
                end_date=request.get("end_date"),
            )

    # Set by cinemark-api's publish_crawl_request for every dashboard-
    # triggered run, and for operator-approved BFS hops (related-hashtag
    # chips). Whatever's currently running for this platform is what the
    # dashboard's Stop button cancels (see crawl_jobs.py).
    logger.info("crawl_request_started", platform=platform, keyword=keyword, keyword_id=request.get("keyword_id"), run_id=run_id)
    try:
        if platform == "facebook" and not await _ensure_facebook_session(
            run_id=run_id, platform=platform
        ):
            raise CrawlJobFailed("facebook session refresh failed before search crawl")
        returncode = await _run_subprocess(args, run_id=run_id, platform=platform)
        _raise_for_returncode(returncode, f"{platform} crawl keyword={keyword}")
        logger.info("crawl_request_finished", platform=platform, keyword=keyword)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))


async def _refresh_tiktok_identity(request: dict[str, Any]) -> None:
    """TikTok's equivalent of _refresh_token below, but shaped differently:
    there's no GraphQL token cache to recapture. Restore / cookie-import
    follow-up reuses the same job-tracking/cancel plumbing, then runs
    tiktok/auth/bootstrap.py against one pinned platform_accounts row
    (device_id/odinId from the saved cookie)."""
    account_id = request.get("account_id")
    account_key = request.get("account_key")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.tiktok.auth.bootstrap"]
    if account_id is not None:
        args += ["--account-id", str(account_id)]
    elif account_key:
        args += ["--account", str(account_key)]
    else:
        logger.warning("tiktok_refresh_missing_account", request=request)
        raise CrawlJobFailed("tiktok refresh missing account_id/account_key")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform="tiktok")
    if run_id:
        args += ["--run-id", str(run_id)]
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform="tiktok", account_id=account_id, account_key=account_key, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform="tiktok", honor_platform_cancel=False
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode != 0:
        logger.error(
            "token_refresh_failed",
            platform="tiktok",
            account_id=account_id,
            account_key=account_key,
            returncode=returncode,
            run_id=run_id,
        )
        _mark_refresh_result(run_id, ok=False)
        _raise_for_returncode(returncode, "tiktok token refresh")
    else:
        logger.info(
            "token_refresh_finished",
            platform="tiktok",
            account_id=account_id,
            account_key=account_key,
            run_id=run_id,
        )
        _mark_refresh_result(run_id, ok=True)


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
    if platform == "tiktok":
        await _refresh_tiktok_identity(request)
        return

    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform)
        raise CrawlJobFailed(f"unsupported refresh_token platform {platform}")

    run_id = request.get("run_id")
    # --run-id (when present) gets bound into the subprocess's own structlog
    # context (see facebook/threads auth/bootstrap.py's __main__), so every
    # line it logs - not just this function's own started/finished/failed -
    # carries run_id=<x>. cinemark-api's refresh_tracker filters its tailed
    # lines on exactly that marker (see that module's docstring for why a
    # bare platform= substring match wasn't precise enough once
    # crawl_request_consumer.py started running one task per platform
    # concurrently instead of one globally-serial loop).
    args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY]
    account_key = request.get("account_key")
    if account_key:
        # Pin to one row (dashboard restore / cookie-import follow-up).
        # Without this, rotation skips checkpointed accounts and recaptures
        # tokens for whoever is still healthy instead of the account the
        # operator just picked.
        args += ["--account", str(account_key)]
    if run_id:
        args += ["--run-id", str(run_id)]

    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform=platform, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=False
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode != 0:
        logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
        _mark_refresh_result(run_id, ok=False)
        _raise_for_returncode(returncode, f"{platform} token refresh")
    else:
        logger.info("token_refresh_finished", platform=platform, run_id=run_id)
        _mark_refresh_result(run_id, ok=True)


async def _import_cookies(request: dict[str, Any]) -> None:
    """Dashboard-triggered cookie import (see cinemark-api's POST
    /<platform>/import-cookies) - runs the exact same `bootstrap.py
    --cookies-file ... --account ...` flow a human would otherwise type at
    a terminal (see facebook/threads auth/cookies.py's import_cookies),
    just triggered from the Settings page's "Nhập cookie" form instead.

    Still 100% human-authenticated: this only automates the "hand the
    already-exported cookies to Redis" step. The actual login/2FA happened
    in a real, non-automated browser a person drove themselves - see
    project notes on why an *automated* login is refused outright (facebook/
    threads auth/bootstrap.py's "unattended_login_refused" guard) but a
    human-supplied cookie import is fine.

    Chains straight into a normal token refresh once the import succeeds -
    --cookies-file only calls import_cookies() (saves the session to Redis),
    it never goes on to capture the GraphQL replay tokens a crawl actually
    needs (see bootstrap.py's own __main__ dispatch). Without this second
    step, importing cookies would leave the account looking done on the
    dashboard but still fail on the very next crawl - a second manual
    trigger nobody would know to expect. There's deliberately no separate
    "refresh login" button/route anymore (see cinemark-api's token_refresh.py
    history) - a fresh session only ever needs refreshing once it exists,
    and this is the only place that creates one.

    Reuses token_refresh_{started,finished,failed} - not a new event name -
    so cinemark-api's refresh_tracker.py and the dashboard's existing
    RefreshLogPanel/TokenStatusBadge show this run's progress the same way
    an ordinary refresh's already did, no new frontend plumbing needed."""
    platform = request.get("platform", "facebook")
    account_key = request.get("account_key")
    cookies_json = request.get("cookies")
    run_id = request.get("run_id")
    if not account_key or not cookies_json:
        logger.warning("cookie_import_missing_fields", platform=platform, run_id=run_id)
        raise CrawlJobFailed("cookie import missing account_key or cookies")

    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform, run_id=run_id)
        raise CrawlJobFailed(f"unsupported cookie_import platform {platform}")

    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform=platform, run_id=run_id, source="cookie_import")
    try:
        # Step 1: hand the human-exported cookies to Redis. A temp file,
        # not --cookies-json on argv: bootstrap.py's --cookies-file path
        # already exists and is already tested (this is exactly what a
        # human runs by hand today) - reusing it here means zero CLI/
        # parsing changes to bootstrap.py itself, just a new caller.
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        try:
            tmp.write(cookies_json)
            tmp.close()
            import_args = [PYTHON_BIN, "-m", module, "--cookies-file", tmp.name, "--account", account_key]
            if run_id:
                import_args += ["--run-id", str(run_id)]
            returncode = await _run_subprocess(
                import_args, run_id=run_id, platform=platform, honor_platform_cancel=False
            )
        finally:
            Path(tmp.name).unlink(missing_ok=True)

        if returncode != 0:
            logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
            _mark_refresh_result(run_id, ok=False)
            _raise_for_returncode(returncode, f"{platform} cookie import")

        # TikTok Copy-as-cURL import already wrote device_id/odin_id/cookie
        # and renamed account_id to the device_id. A second bootstrap
        # --account <original pin> would miss the row. Identity is complete.
        if platform == "tiktok":
            logger.info("token_refresh_finished", platform=platform, run_id=run_id, source="cookie_import")
            _mark_refresh_result(run_id, ok=True)
            return

        # Facebook/Threads recapture GraphQL replay tokens.
        refresh_args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY, "--account", str(account_key)]
        if run_id:
            refresh_args += ["--run-id", str(run_id)]
        returncode = await _run_subprocess(
            refresh_args, run_id=run_id, platform=platform, honor_platform_cancel=False
        )
        if returncode != 0:
            logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
            _mark_refresh_result(run_id, ok=False)
            _raise_for_returncode(returncode, f"{platform} cookie import token refresh")
        else:
            logger.info("token_refresh_finished", platform=platform, run_id=run_id)
            _mark_refresh_result(run_id, ok=True)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))


async def _nurture_accounts(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Runs nurture_accounts.py for one platform (facebook, threads, or
    tiktok). Dashboard Settings queues these as type=nurture on
    crawl_requests - same one-at-a-time consumer as crawls, so a warm-up
    never overlaps a search for the same account/session."""
    platform = request.get("platform")
    if platform not in ("facebook", "threads", "tiktok"):
        logger.warning("nurture_unsupported_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported nurture platform {platform}")

    args = [PYTHON_BIN, "-m", "social_crawler.nurture_accounts", "--platform", platform]
    account = request.get("account")
    if account:
        # One named account: still honor the daily quota, but don't wait
        # minutes before starting - the operator just clicked that row.
        args += ["--account", str(account), "--limit", "1", "--gap-min", "30", "--gap-max", "90"]
        args += ["--start-delay-min", "15", "--start-delay-max", "75"]
    else:
        # Pool warm-up: at most two accounts per platform, skip anyone
        # already warmed today, wait a bit before the first browse and
        # several minutes between accounts so a click is not a burst.
        args += ["--limit", "2", "--gap-min", "180", "--gap-max", "540"]
        args += ["--start-delay-min", "45", "--start-delay-max", "240"]
    args.append("--like" if request.get("like", True) else "--no-like")

    count = request.get("visits", 3)
    try:
        count_n = max(0, min(8, int(count)))
    except (TypeError, ValueError):
        count_n = 3
    if platform == "tiktok":
        # nurture_accounts.py's tiktok path has no --comment concept (its
        # feed is a continuous scroll, not discrete posts with a composer -
        # see that module's own docstring) and reuses the same request
        # "visits" field cinemark-api's publish_nurture_request docstring
        # already documents as "number of hashtag pages to visit" for this
        # platform - --hashtags 0 isn't valid there (at least 1 page is
        # visited), unlike facebook/threads' --visits 0 (comment/browse
        # only, no posts opened).
        args += ["--hashtags", str(max(1, count_n))]
    else:
        args.append("--comment" if request.get("comment", True) else "--no-comment")
        args += ["--visits", str(count_n)]

    run_id = request.get("run_id")
    if run_id:
        args += ["--run-id", str(run_id)]
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "nurture", "account": account, "started_at": int(time.time())})

    logger.info("nurture_started", platform=platform, account=account, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    _raise_for_returncode(returncode, f"{platform} nurture account={account}")
    logger.info("nurture_finished", platform=platform, account=account)


async def _handle_request(request: dict[str, Any]) -> bool:
    """Returns True when the request was skipped (Stop / drain), so the
    consumer can pick up the next Kafka message without the inter-request
    pause."""
    platform = request.get("platform") or ""
    kind = request.get("type")
    # bypass_drain marks a message cinemark-api tagged as its own targeted
    # trigger (comments/nurture/channel_videos), not a resumption of the
    # bulk backlog Stop was meant to block - see publish_action_request's
    # docstring on the cinemark-api side. Threaded down into each of those
    # three handlers below too (not just this intake gate): they each poll
    # the same platform-wide drain/cancel signals again on their own,
    # before/during their subprocess, and must skip those the same way or
    # a stale signal from an old Stop kills the very job bypass_drain was
    # meant to let through (confirmed happening for real 2026-09-21 - a
    # nurture run got SIGTERM'd by a leftover crawl_job_cancel_platform
    # within seconds of starting).
    bypass_drain = bool(request.get("bypass_drain"))

    # A precise per-job Stop (cinemark-api's POST /<platform>/jobs/{run_id}/
    # stop - see crawl_jobs.cancel_job) targeting THIS run_id specifically,
    # clicked while it was still queued rather than already running. Always
    # honored, bypass_drain or not: unlike the platform-wide drain check
    # below (which a deliberately-targeted trigger must survive), this
    # signal only exists because the user clicked Stop on this exact job.
    run_id = request.get("run_id")
    if run_id and _cancel_requested(run_id=run_id):
        logger.info("request_skipped_job_cancel", platform=platform, run_id=run_id, type=kind or "search")
        finish_task(request, "skipped")
        return True

    if kind not in ("refresh_token", "cookie_import") and not bypass_drain and platform and is_platform_draining(platform):
        logger.info("request_skipped_drain", platform=platform, type=kind or "search", post_id=request.get("post_id"))
        finish_task(request, "skipped")
        return True

    start_task(request)
    try:
        if request.get("type") == "refresh_token":
            await _refresh_token(request)
        elif request.get("type") == "cookie_import":
            await _import_cookies(request)
        elif request.get("type") == "comments":
            await _run_comments_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "channel_videos":
            await _run_channel_videos_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "nurture":
            await _nurture_accounts(request, bypass_drain=bypass_drain)
        else:
            await _run_spider(request)
    except CrawlJobSkipped:
        finish_task(request, "skipped")
        return True
    except CrawlJobFailed as exc:
        finish_task(request, "failed", error=str(exc))
        return False
    except Exception:
        finish_task(request, "failed")
        raise
    finish_task(request, "done")
    return False


def _request_platform(request: dict[str, Any]) -> str | None:
    """Which platform loop should claim this message. Only a refresh_token
    request gets the legacy "facebook" default _refresh_token itself already
    used, for a message queued before this was multi-platform - a plain
    crawl request is expected to always set "platform" explicitly (see
    _run_spider, which has no such default). Defaulting *every* message type
    here used to let a platform-less crawl request get claimed by the
    facebook loop only to be rejected there as unsupported_platform; None
    for that case instead means no loop claims it, so it falls through to
    the unrecognized-platform log below exactly like _run_spider used to
    produce on its own."""
    if request.get("type") == "refresh_token":
        return request.get("platform", "facebook")
    return request.get("platform")


async def _requeue_after_proxy_exhaustion(
    requeue_publisher: KafkaPublisher, request: dict[str, Any], *, platform: str
) -> None:
    """Re-publishes `request` onto crawl_requests with its own
    "_proxy_exhausted_retries" counter incremented - see
    MAX_PROXY_EXHAUSTED_REQUEUES's own comment for why this specific
    failure class (and only this one) gets a real retry instead of being
    logged and left lost like every other exception _handle_request can
    raise. Gives up (logs loudly, does not re-publish) once that counter
    hits the cap - a platform whose proxy pool is permanently dead, not
    just cooling down, must not loop forever."""
    retries = request.get("_proxy_exhausted_retries", 0)
    if retries >= MAX_PROXY_EXHAUSTED_REQUEUES:
        logger.error(
            "proxy_exhausted_requeue_gave_up",
            telegram=True,
            platform=platform,
            retries=retries,
            request=request,
        )
        return
    requeued = {**request, "_proxy_exhausted_retries": retries + 1}
    await requeue_publisher.publish(
        topic=CRAWL_REQUESTS_TOPIC,
        key=request.get("run_id") or platform,
        value=requeued,
    )
    logger.info("proxy_exhausted_requeued", platform=platform, retries=retries + 1, request=request)


async def _run_platform_consumer_once(platform: str, group_id: str) -> bool:
    """One attempt at running a single platform's consumer loop - see
    module docstring for why there's one of these per platform instead of
    one shared loop. Every instance subscribes to the same topic and sees
    every message; whatever doesn't belong to this platform is committed
    past immediately (no crawl, no pause) so this loop's own throughput is
    never affected by how much traffic other platforms are generating.

    Returns True when _run_platform_consumer should stop retrying (a
    deliberate KeyboardInterrupt; CancelledError instead propagates,
    unhandled, since that's an outside caller - run(), or shutdown -
    actively stopping this task), False when this ended on a KafkaError
    worth retrying with a fresh consumer (see _run_platform_consumer)."""
    consumer = AIOKafkaConsumer(
        CRAWL_REQUESTS_TOPIC,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=group_id,
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
        # sweep. Only this platform's own crawls count against it - a
        # different platform's long sweep runs on its own consumer/loop.
        max_poll_interval_ms=3_600_000,
    )
    # Consecutive pool.ProxyPoolExhaustedError hits for THIS platform loop -
    # see PROXY_EXHAUSTED_BACKOFF_* above for why this needs its own growing
    # pause instead of racing straight into the next message. Process-local
    # (not Redis) on purpose: it only needs to survive within one running
    # loop, and resets to 0 the moment anything else (success or a
    # different failure) gets through, exactly like the account/proxy
    # health it's tracking already resets on its own success.
    proxy_exhausted_streak = 0
    # Only ever used to re-publish a proxy-exhausted request back onto
    # crawl_requests (see _requeue_after_proxy_exhaustion) - a separate
    # producer from any spider's own KafkaPublisher since this one's
    # lifecycle is tied to the consumer loop itself, not to one crawl.
    requeue_publisher = KafkaPublisher()
    try:
        await consumer.start()
        await requeue_publisher.start()
        logger.info("crawl_request_consumer_started", topic=CRAWL_REQUESTS_TOPIC, platform=platform, group=group_id)
        async for message in consumer:
            request = message.value
            request_platform = _request_platform(request)
            if request_platform != platform:
                # Every loop sees every message on the shared topic, so a
                # platform value that matches none of them would otherwise
                # go completely unlogged (each of the three loops silently
                # decides "not mine"). Only the facebook loop - arbitrary,
                # any one works - reports it, so an unrecognized/typo'd
                # platform produces exactly one warning instead of three,
                # restoring the observability _run_spider/_refresh_token
                # used to provide on their own before this file had loops
                # to route between at all.
                if platform == "facebook" and request_platform not in PLATFORM_CONSUMER_GROUPS:
                    logger.warning("unsupported_platform", platform=request_platform, request=request)
                await consumer.commit()
                continue
            skipped = False
            proxy_exhausted = False
            try:
                skipped = await _handle_request(request)
            except Exception as exc:
                # One bad request must not kill the whole consumer - log and
                # move on to the next one.
                logger.error("crawl_request_error", platform=platform, error=str(exc), request=request)
                # Every platform's own NetworkError/TikTokNetworkError wraps
                # the pool.ProxyPoolExhaustedError it was raised from (see
                # e.g. comet_graphql_client.py/tiktok/client.py's own
                # `raise ... from exc`) - checking __cause__ here means this
                # stays platform-agnostic, no per-platform exception import
                # needed for this generic dispatch loop to recognize it.
                proxy_exhausted = isinstance(exc, pool.ProxyPoolExhaustedError) or isinstance(
                    exc.__cause__, pool.ProxyPoolExhaustedError
                )
            # Committed whether _handle_request succeeded or was logged and
            # skipped above - either way this message is done, not to be
            # redelivered on the next restart. A proxy-exhaustion failure is
            # the one exception: it gets a real chance at a retry (see
            # _requeue_after_proxy_exhaustion) - re-published *before* this
            # commit, not after, so a crash in between leaves at worst a
            # harmless duplicate (both the original, uncommitted and about
            # to be redelivered on restart, and the fresh requeued copy)
            # rather than silently losing the request if the crash landed
            # the other way around.
            if proxy_exhausted:
                proxy_exhausted_streak += 1
                await _requeue_after_proxy_exhaustion(requeue_publisher, request, platform=platform)
                backoff = min(
                    PROXY_EXHAUSTED_BACKOFF_BASE_SECONDS
                    * (PROXY_EXHAUSTED_BACKOFF_GROWTH_FACTOR ** (proxy_exhausted_streak - 1)),
                    PROXY_EXHAUSTED_BACKOFF_MAX_SECONDS,
                )
                logger.warning(
                    "platform_proxy_exhausted_pausing",
                    platform=platform,
                    consecutive_hits=proxy_exhausted_streak,
                    backoff_seconds=round(backoff, 1),
                )
                await consumer.commit()
                await _sleep_interruptible(platform, backoff)
                continue
            await consumer.commit()
            proxy_exhausted_streak = 0
            if skipped or is_platform_draining(platform):
                continue
            if request.get("type") == "comments" and platform != "tiktok":
                # Facebook/Threads comments are plain curl_cffi - skip the
                # inter-request pause so a 100-post batch isn't dominated by
                # idle waits. TikTok comments open a real Patchright browser
                # every job (see tiktok/features/comments/comments.py), so
                # they fall through to the pause below like search crawls.
                continue
            # See INTER_REQUEST_PAUSE_MIN/MAX_SECONDS above - a gap before
            # picking up whatever's next in *this platform's* queue, not
            # before the very first request of a fresh batch (nothing to
            # space out yet).
            pause = random.uniform(INTER_REQUEST_PAUSE_MIN_SECONDS, INTER_REQUEST_PAUSE_MAX_SECONDS)
            logger.info("inter_request_pause", platform=platform, seconds=round(pause, 1))
            await _sleep_interruptible(platform, pause)
    except asyncio.CancelledError:
        logger.info("platform_consumer_cancelled", platform=platform)
        raise
    except KeyboardInterrupt:
        logger.info("platform_consumer_interrupted", platform=platform)
        return True
    except KafkaError as exc:
        logger.error("kafka_error", platform=platform, error=str(exc))
        return False
    finally:
        await consumer.stop()
        await requeue_publisher.stop()
    return True


async def _run_platform_consumer(platform: str, group_id: str) -> None:
    """Restarts _run_platform_consumer_once with a fresh consumer whenever
    it ends on a KafkaError, instead of letting one platform's transient
    Kafka hiccup take down the other two platforms' loops via run()'s
    heavier last-resort response (see the _CONSUMER_RESTART_* constants'
    own comment). Only returns - handing control back to run(), which
    applies that heavier response - after a deliberate stop (True) or after
    _CONSUMER_RESTART_MAX_ATTEMPTS restarts within
    _CONSUMER_RESTART_WINDOW_SECONDS without one full window of clean
    running in between."""
    restart_times: list[float] = []
    while True:
        stop_cleanly = await _run_platform_consumer_once(platform, group_id)
        if stop_cleanly:
            return
        now = time.monotonic()
        restart_times = [t for t in restart_times if now - t < _CONSUMER_RESTART_WINDOW_SECONDS]
        restart_times.append(now)
        if len(restart_times) > _CONSUMER_RESTART_MAX_ATTEMPTS:
            logger.error(
                "platform_consumer_restart_giving_up",
                platform=platform,
                attempts=len(restart_times),
                window_seconds=_CONSUMER_RESTART_WINDOW_SECONDS,
            )
            return
        backoff = min(
            _CONSUMER_RESTART_BACKOFF_BASE_SECONDS
            * (_CONSUMER_RESTART_BACKOFF_GROWTH_FACTOR ** (len(restart_times) - 1)),
            _CONSUMER_RESTART_BACKOFF_MAX_SECONDS,
        )
        logger.warning(
            "platform_consumer_restarting",
            platform=platform,
            attempt=len(restart_times),
            backoff_seconds=round(backoff, 1),
        )
        await asyncio.sleep(backoff)


async def run() -> None:
    """Runs every platform's consumer loop concurrently in this one process
    - see module docstring. _run_platform_consumer already retries a
    KafkaError internally with a fresh consumer (rebalance/commit-failure/
    broker-blip - see its own docstring), so by the time one of these tasks
    actually finishes here, it's either a deliberate stop or a platform
    that's exhausted its own retry budget against a broker that's been
    unreachable for a while - not a single transient hiccup. Previously
    this used asyncio.gather(..., return_exceptions=True), which only
    logged that and left the *other* two loops running forever: the dead
    platform's crawls silently stopped forever with the process still "up",
    and nothing ever told systemd's Restart=on-failure to bring it back.
    Cancelling the survivors and re-raising here instead makes the whole
    process exit non-zero, so systemd restarts all three loops cleanly -
    the same guarantee the single-consumer version already had."""
    tasks = {
        asyncio.create_task(_run_platform_consumer(platform, group_id), name=platform): platform
        for platform, group_id in PLATFORM_CONSUMER_GROUPS.items()
    }
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except (asyncio.CancelledError, KeyboardInterrupt):
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("crawl_request_consumer_stopped")
        return

    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    finished = next(iter(done))
    platform = tasks[finished]
    if finished.cancelled():
        logger.info("crawl_request_consumer_stopped", platform=platform)
        return
    exc = finished.exception()
    if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
        logger.info("crawl_request_consumer_stopped", platform=platform)
        return
    logger.error("platform_consumer_stopped", platform=platform, error=str(exc) if exc else None)
    raise RuntimeError(f"{platform} consumer loop stopped unexpectedly") from exc


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(0)
