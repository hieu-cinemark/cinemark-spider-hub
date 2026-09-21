"""
Shared base for Facebook's and Threads' GraphQL clients - both run on the
same Comet/Barcelona GraphQL stack (confirmed against a real captured
BarcelonaPostPageStrongIdTargetQuery request on threads.com), so almost
everything below (session setup, throttling, retry/backoff, variable
templating, response parsing) used to be duplicated near-verbatim across
facebook/auth/graphql_client.py and threads/auth/graphql_client.py - only
platform names and a handful of constants differed. Mirrors the same
base/subclass split spiders/tiktok/client.py already uses for the same
reason.

A subclass sets the class attributes below (platform name, per-platform
Redis key templates, request pacing/retry constants) and adds its own
per-request methods (search, comments, ...) that call self._run(...); see
FacebookGraphQLClient/ThreadsGraphQLClient for the shape. Anything that
genuinely differs between the two platforms - Facebook's date-filtered
search/comments, Threads' extra x-csrftoken header - stays in the subclass;
nothing here assumes either.
"""

from __future__ import annotations

import copy
import json
import random
import time
import uuid
from typing import Any, Callable

from curl_cffi import requests as curl_requests

from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.db import disable_account, get_accounts
from social_crawler.services.redis import RedisCache

logger = get_logger(__name__)

# AIMD-style adjustment for the adaptive per-account throttle interval (see
# CometGraphQLClient._adjust_interval): grow fast on any sign of stress (one
# retry is enough to react to), decay slowly so a single clean request right
# after a rough patch doesn't immediately erase the caution.
_ADAPTIVE_INTERVAL_GROWTH_FACTOR = 1.7
_ADAPTIVE_INTERVAL_DECAY_FACTOR = 0.85
# How long a raised interval survives with no new stress signal before
# _current_interval falls back to reading MIN_REQUEST_INTERVAL_SECONDS again -
# an account that had a rough 10 minutes an hour ago shouldn't still be
# throttled extra-cautiously now.
_ADAPTIVE_INTERVAL_TTL_SECONDS = 1800

__all__ = [
    "CometGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]


class SessionExpiredError(RuntimeError):
    """Token cache is missing/expired or the platform rejected the request (401/403) - re-run bootstrap.py."""


class NetworkError(RuntimeError):
    """Every retry failed to even get an HTTP response back (proxy down,
    DNS failure, TLS handshake failure, timeout) - the platform never
    actually saw this request, so the token/session is not the problem.
    Re-running bootstrap.py won't fix a dead proxy; check the configured
    platform_proxies row instead."""


class RateLimitedError(RuntimeError):
    """The platform is rate-limiting this account/IP (429) even after
    retrying with backoff. This is NOT a dead token - re-running
    bootstrap.py won't help and just burns another login cycle against an
    account that's already being throttled. Back off and retry later
    instead."""


class CheckpointRequiredError(RuntimeError):
    """The platform flagged this account mid-session and demanded
    re-verification - seen as a 400 response with body
    {"message": "checkpoint_required", "status": "fail"} on an otherwise
    normal replay request (not just at bootstrap.py's login time, which
    already had its own separate check for this). The cached token still
    looks fresh and every retry would just get the same response, so _run()
    disables the account (see disable_account) and alerts immediately
    instead of retrying - a human has to actually log in through a real
    browser and clear the checkpoint before this account is usable again."""


class CometGraphQLClient:
    """Set by subclasses - see FacebookGraphQLClient/ThreadsGraphQLClient."""

    PLATFORM: str
    REFERER_URL: str
    CACHE_REDIS_KEY_TMPL: str
    ACTIVE_ACCOUNT_REDIS_KEY: str
    DEFAULT_ACCOUNT_KEY: str
    GRAPHQL_URL: str
    MAX_RETRIES: int
    MIN_REQUEST_INTERVAL_SECONDS: float
    REQUEST_INTERVAL_JITTER_SECONDS: float
    RETRY_BACKOFF_BASE_SECONDS: float
    RETRY_BACKOFF_JITTER_SECONDS: float
    THROTTLE_REDIS_KEY_TMPL: str
    ADAPTIVE_INTERVAL_MAX_SECONDS: float

    # Set by a subclass that has a comments feature (see
    # FacebookGraphQLClient/ThreadsGraphQLClient) - a platform with no
    # comments feature (none currently) just never sets this, and
    # get_comments/get_comments_next_page below aren't usable for it.
    # COMMENTS_REDIS_KEY_TMPL: str
    #
    # Set by a subclass that also fetches replies-to-a-comment (currently
    # just FacebookGraphQLClient - see bootstrap.py's `--type replies`). A
    # platform without this never calls get_replies/get_replies_next_page
    # below.
    # REPLIES_REDIS_KEY_TMPL: str
    #
    # Relay variable names for the comments-pagination query's cursor/count -
    # confirmed identical ("commentsAfterCursor"/"commentsAfterCount") on
    # Facebook's own comments query; kept overridable per-subclass (not
    # hardcoded here) since Threads' real names are only known once its own
    # bootstrap has actually captured a paginated comments request - see
    # that subclass for whether it needed to override these.
    COMMENTS_CURSOR_KEY = "commentsAfterCursor"
    COMMENTS_COUNT_KEY = "commentsAfterCount"
    # The variable name the target post's id is passed under - Facebook
    # calls it "id" (a base64 feedback id, see _comment_target_id there);
    # Threads calls it "postID" and passes the raw numeric post id
    # unencoded (confirmed against a real captured
    # BarcelonaPostPageStrongIdDirectRepliesRefetchQuery request).
    COMMENTS_ID_KEY = "id"

    def __init__(self, redis_cache: RedisCache | None = None, account: str | None = None):
        self._redis = redis_cache or RedisCache()
        # Defaults to whichever account bootstrap.py most recently
        # (re)logged in as - see ACTIVE_ACCOUNT_REDIS_KEY - so rotating
        # through platform_accounts in bootstrap runs automatically carries
        # over to `scrapy crawl ...` without needing to pass anything here.
        # Pass `account` explicitly to pin a run to one account instead.
        pinned = account is not None
        self._account = account or self._redis.get(self.ACTIVE_ACCOUNT_REDIS_KEY) or self.DEFAULT_ACCOUNT_KEY
        cache_key = self.CACHE_REDIS_KEY_TMPL.format(account=self._account)
        cached = self._redis.get(cache_key)
        # Only auto-fallback when the caller didn't explicitly pin an
        # account (an explicit `account=` means the caller wants *that one*
        # specifically, e.g. a manual retry against a named account - see
        # `_find_fallback_session`'s own docstring for why this matters
        # more with fewer accounts to go around, not less).
        if cached is None and not pinned:
            cached, self._account = self._find_fallback_session()
        if cached is None:
            raise SessionExpiredError(
                f"No token cache found in Redis (key={cache_key!r}, account={self._account!r}), or Redis "
                "is unreachable, or the cache expired. Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --query "test"'
            )
        self._cache = cached
        age = time.time() - self._cache["captured_at"]
        logger.info("loaded_token_cache", account=self._account, age_hours=round(age / 3600, 1))

        proxy = None
        try:
            self._proxy_cfg = pool.acquire_proxy_for_account(self.PLATFORM, self._account, required=True)
        except pool.ProxyPoolExhaustedError as exc:
            # required=True: this is steady-state crawl traffic, not the
            # one-time login browser - never fall back to running unproxied
            # (see ProxyPoolExhaustedError's own docstring). Re-raised as
            # NetworkError so it flows through the exact retry/Telegram-alert
            # handling every spider already has for "proxy down" (see e.g.
            # facebook/features/search/search.py's `except NetworkError`).
            raise NetworkError(str(exc)) from exc
        self._proxy_outcome_recorded = False
        if self._proxy_cfg:
            proxy = {
                "http": f"http://{self._proxy_cfg['username']}:{self._proxy_cfg['password']}@{self._proxy_cfg['url']}",
                "https": f"http://{self._proxy_cfg['username']}:{self._proxy_cfg['password']}@{self._proxy_cfg['url']}",
            }
        logger.info(
            "graphql_session_ready", account=self._account, proxy=self._proxy_cfg["url"] if self._proxy_cfg else None
        )

        self._session = curl_requests.Session(impersonate="chrome", proxies=proxy)
        self._last_request_at: float | None = None

    def _find_fallback_session(self) -> tuple[dict[str, Any] | None, str]:
        """Called only when the "active" account's own token cache is
        missing/expired (see __init__) - scans every *other* enabled
        account (get_accounts() already excludes anything mid-cooldown or
        checkpointed, so every candidate here is DB-healthy already) for
        one whose own cache still holds an unexpired token, adopting the
        first one found instead of failing the whole run outright. Returns
        (None, self._account) unchanged if nothing else has one either.

        Why this matters more here than it looks: ACTIVE_ACCOUNT_REDIS_KEY
        is one shared pointer, set once per bootstrap.py run, then reused
        by *every* crawl_request for this platform until the next
        bootstrap - a scheduled "run every enabled keyword" sweep (see
        cinemark-api's scheduler.py/POST /<platform>/run) fires one
        subprocess per keyword, each constructing its own fresh client
        that just reads that one pointer. With a small account pool and a
        long keyword list (confirmed live 2026-09-16: 6 enabled Threads
        accounts against 44 enabled keywords in one sweep), the *one*
        account that pointer names getting checkpointed/rate-limited
        partway through - or simply going stale between separate scheduled
        runs - used to fail every keyword still queued behind it, even
        though other accounts already had their own perfectly good cached
        sessions sitting unused in Redis the whole time. Updates
        ACTIVE_ACCOUNT_REDIS_KEY to the account it finds, so the *next*
        keyword's subprocess in the same sweep picks it up too instead of
        repeating this same scan and falling back again from scratch."""
        current_key = self.CACHE_REDIS_KEY_TMPL.format(account=self._account)
        for row in get_accounts(self.PLATFORM):
            candidate = (row.get("email") or row["id"]).strip().lower()
            cache_key = self.CACHE_REDIS_KEY_TMPL.format(account=candidate)
            if cache_key == current_key:
                continue  # already know this one's dead - don't re-check it
            cached = self._redis.get(cache_key)
            if cached is None:
                continue
            logger.warning(
                "active_account_session_dead_falling_back",
                telegram=True,
                platform=self.PLATFORM,
                dead_account=self._account,
                fallback_account=candidate,
            )
            self._redis.set(self.ACTIVE_ACCOUNT_REDIS_KEY, candidate)
            return cached, candidate
        return None, self._account

    def _record_proxy_outcome_once(self, *, success: bool) -> None:
        """Records this client's proxy outcome (see services/pool.py) at
        most once per instance, not once per request - a single sweep can
        fire dozens of requests through the same client, and db.py's own
        connect-fresh-per-call design assumes callers hit it rarely (see its
        module docstring), not once per GraphQL request. The first signal is
        representative enough: a proxy that fails once still earns the
        cooldown that failure implies even if a later request on the same
        client happens to succeed, and a proxy that's clearly healthy
        doesn't need every subsequent request re-confirming that."""
        if self._proxy_cfg is None or self._proxy_outcome_recorded:
            return
        self._proxy_outcome_recorded = True
        pool.release_proxy(self._proxy_cfg, success=success)

    def _throttle_key(self) -> str:
        return self.THROTTLE_REDIS_KEY_TMPL.format(account=self._account)

    def _current_interval(self) -> float:
        """The base pacing interval to use right now - MIN_REQUEST_INTERVAL_SECONDS
        normally, or a higher Redis-persisted value if this account has hit
        429/5xx/network errors recently (see _adjust_interval). Persisted
        (not just in-memory) because bootstrap.py/scrapy crawl runs are
        short-lived subprocesses - without Redis, a run that got throttled
        right before exiting would teach the next run nothing."""
        stored = self._redis.get(self._throttle_key())
        if stored is None:
            return self.MIN_REQUEST_INTERVAL_SECONDS
        return max(self.MIN_REQUEST_INTERVAL_SECONDS, float(stored))

    def _adjust_interval(self, *, stressed: bool) -> None:
        """Called after every request settles: grows the persisted interval
        on any 429/5xx/network signal, decays it back down on a clean
        response with no prior signal this call. See the module-level
        _ADAPTIVE_INTERVAL_* constants for the growth/decay factors and TTL."""
        key = self._throttle_key()
        stored = self._redis.get(key)
        if stored is None:
            if not stressed:
                return  # already at baseline - nothing to persist
            current = self.MIN_REQUEST_INTERVAL_SECONDS
        else:
            current = max(self.MIN_REQUEST_INTERVAL_SECONDS, float(stored))

        if stressed:
            new_interval = min(self.ADAPTIVE_INTERVAL_MAX_SECONDS, current * _ADAPTIVE_INTERVAL_GROWTH_FACTOR)
        else:
            new_interval = max(self.MIN_REQUEST_INTERVAL_SECONDS, current * _ADAPTIVE_INTERVAL_DECAY_FACTOR)

        if new_interval <= self.MIN_REQUEST_INTERVAL_SECONDS:
            self._redis.delete(key)
            return

        logger.info(
            "adaptive_interval_adjusted",
            platform=self.PLATFORM,
            account=self._account,
            stressed=stressed,
            interval_seconds=round(new_interval, 2),
        )
        self._redis.set(key, new_interval, ttl_seconds=_ADAPTIVE_INTERVAL_TTL_SECONDS)

    def _throttle(self) -> None:
        """Space out requests to the platform - nothing else does this,
        since every spider here calls curl_cffi directly instead of going
        through Scrapy's downloader."""
        base_interval = self._current_interval()
        if self._last_request_at is not None:
            target_gap = base_interval + random.uniform(0, self.REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.info(
                    "throttling", delay_seconds=round(remaining, 2), base_interval_seconds=round(base_interval, 2)
                )
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _headers(self, friendly_name: str, lsd: str) -> dict[str, str]:
        """Headers common to both platforms - Threads overrides this to add
        its extra origin/x-csrftoken fields via super()._headers(...)."""
        headers = dict(self._cache["headers"])
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "referer": self.REFERER_URL,
                "x-fb-friendly-name": friendly_name,
                "x-fb-lsd": lsd,
            }
        )
        return headers

    def _run(
        self,
        *,
        doc_id: str | None,
        friendly_name: str | None,
        template: dict[str, Any] | None,
        template_source: str,
        overrides: dict[str, Any],
    ) -> dict[str, Any]:
        if template is None:
            raise SessionExpiredError(
                f"Cache has no {template_source} (it was created by an older bootstrap.py). "
                "Re-run bootstrap.py to refresh the cache."
            )

        static = self._cache["body_static"]
        variables = _apply_variable_overrides(template, overrides)
        logger.info("sending_graphql_request", friendly_name=friendly_name, **_loggable(overrides))

        body = {
            **static,
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": friendly_name,
            "server_timestamps": "true",
            "doc_id": doc_id,
            "variables": json.dumps(variables, separators=(",", ":")),
        }

        self._throttle()
        resp = self._post_with_retry(
            headers=self._headers(friendly_name, static.get("lsd", "")),
            cookies=self._cache["cookies"],
            body=body,
        )

        if resp.status_code in (401, 403):
            raise SessionExpiredError(
                f"{self.PLATFORM.capitalize()} rejected the request (status={resp.status_code}). Re-run bootstrap.py."
            )

        logger.info("received_response", status_code=resp.status_code, bytes=len(resp.text))
        parsed = _parse_graphql_response(resp.text)

        if isinstance(parsed, dict) and parsed.get("message") == "checkpoint_required":
            disabled = disable_account(
                self.PLATFORM, self._account, reason="checkpoint_required response during replay traffic"
            )
            logger.error(
                "account_disabled_checkpoint_suspected" if disabled else "account_checkpoint_suspected",
                telegram=True,
                platform=self.PLATFORM,
                account=self._account,
                disabled=disabled,
                response=parsed,
            )
            raise CheckpointRequiredError(
                f"{self.PLATFORM.capitalize()} returned checkpoint_required for account {self._account!r} - "
                "log in as this account through a real browser to resolve the checkpoint, then re-run bootstrap.py."
            )

        return parsed

    def _comment_target_id(self, post_id: str) -> str:
        """How this platform's comments queries address a post - Facebook
        uses base64("feedback:<post_id>") (see FacebookGraphQLClient), a
        subclass with a comments feature must override this with its own
        confirmed-against-a-real-request scheme."""
        raise NotImplementedError(f"{self.PLATFORM} has no comments feature (no _comment_target_id override)")

    def _reply_target_id(self, legacy_comment_id: str) -> str:
        """How this platform's replies-to-a-comment queries address the
        parent comment. Defaults to the same scheme as _comment_target_id
        (Facebook's own base64("feedback:<id>") addressing, confirmed for
        posts - NOT yet independently confirmed for a comment id; a subclass
        should override this once a real captured replies request shows
        otherwise)."""
        return self._comment_target_id(legacy_comment_id)

    def _get_comments_cache(self) -> dict[str, Any]:
        comments_key = self.COMMENTS_REDIS_KEY_TMPL.format(account=self._account)
        comments = self._redis.get(comments_key)
        if comments is None:
            raise SessionExpiredError(
                f"No comments query cached in Redis (key={comments_key!r}, account={self._account!r}). Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --post-url "<a post url with comments>"'
            )
        return comments

    def get_comments(self, post_id: str) -> dict[str, Any]:
        """Fetch the first page of comments for a post - shared by every
        platform with a comments feature (see _comment_target_id).

        Explicitly resets the cursor to null even though this is the
        "initial" query, not the "paginated" one: on a platform where the
        same refetchable query serves both roles (confirmed on Threads -
        see request_capture.py's pick_paginated_comments_request), the
        request captured mid-scroll during bootstrap already carries a
        real (by-now-stale) cursor value baked into its template. Left
        alone, page 1 would silently ask for "whatever comes after that
        stale cursor" instead of the actual first page, and get back an
        empty direct_replies. Harmless on a platform whose root/paginated
        comments queries are genuinely separate (Facebook): that template
        simply has no cursor key for this walk to touch."""
        comments = self._get_comments_cache()
        return self._run(
            doc_id=comments.get("doc_id"),
            friendly_name=comments.get("fb_api_req_friendly_name"),
            template=comments.get("variables_template"),
            template_source="comments variables_template",
            overrides={self.COMMENTS_ID_KEY: self._comment_target_id(post_id), self.COMMENTS_CURSOR_KEY: None},
        )

    def get_comments_next_page(self, post_id: str, cursor: str, count: int = -1) -> dict[str, Any]:
        """Fetch the next page of comments, using the `end_cursor` from a
        previous page's `page_info` (see `find_page_info`). Requires
        bootstrap.py to have captured a paginated comments request - it does
        this automatically by scrolling the comment list after switching
        sort order (see each platform's own comments_trigger).

        Default count=-1 matches Comet's own CommentsListComponents
        PaginationQuery (confirmed live 2026-09-16): positive page sizes
        still get capped ~10; -1 is what the browser sends for the densest
        page after the cursor."""
        comments = self._get_comments_cache()
        pagination = comments.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no comments pagination info (no paginated comments query was captured). "
                "Re-run bootstrap.py --post-url against a post with more comments than fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="comments pagination.variables_template",
            overrides={
                self.COMMENTS_ID_KEY: self._comment_target_id(post_id),
                self.COMMENTS_CURSOR_KEY: cursor,
                self.COMMENTS_COUNT_KEY: count,
            },
        )

    def _get_replies_cache(self) -> dict[str, Any]:
        replies_key = self.REPLIES_REDIS_KEY_TMPL.format(account=self._account)
        replies = self._redis.get(replies_key)
        if replies is None:
            raise SessionExpiredError(
                f"No replies query cached in Redis (key={replies_key!r}, account={self._account!r}). Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --post-url "<a post url whose top-level '
                'comment has replies>" --type replies'
            )
        return replies

    def get_replies(self, legacy_comment_id: str) -> dict[str, Any]:
        """Fetch the first page of replies to one top-level comment - mirrors
        get_comments above, just addressed at a comment instead of a post
        (see _reply_target_id) and cached under REPLIES_REDIS_KEY_TMPL."""
        replies = self._get_replies_cache()
        return self._run(
            doc_id=replies.get("doc_id"),
            friendly_name=replies.get("fb_api_req_friendly_name"),
            template=replies.get("variables_template"),
            template_source="replies variables_template",
            overrides={self.COMMENTS_ID_KEY: self._reply_target_id(legacy_comment_id), self.COMMENTS_CURSOR_KEY: None},
        )

    def get_replies_next_page(self, legacy_comment_id: str, cursor: str, count: int = 10) -> dict[str, Any]:
        """Fetch the next page of replies, using the `end_cursor` from a
        previous page's `page_info` (see `find_page_info`). Mirrors
        get_comments_next_page above."""
        replies = self._get_replies_cache()
        pagination = replies.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no replies pagination info (no paginated replies query was captured). "
                "Re-run bootstrap.py --post-url ... --type replies against a comment with more replies than fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="replies pagination.variables_template",
            overrides={
                self.COMMENTS_ID_KEY: self._reply_target_id(legacy_comment_id),
                self.COMMENTS_CURSOR_KEY: cursor,
                self.COMMENTS_COUNT_KEY: count,
            },
        )

    def _post_with_retry(self, headers: dict[str, str], cookies: dict[str, str], body: dict[str, Any]) -> Any:
        """POST with exponential-backoff retry on rate limiting (429), server
        errors (5xx) and network-level failures - these are transient and
        usually recover on their own, unlike a dead token (401/403), which
        the caller handles separately and never retries here."""
        last_exc: Exception | None = None
        resp = None
        # True as soon as any attempt this call sees 429/5xx/a network error -
        # feeds _adjust_interval so a request that only succeeded after
        # retrying still counts as stress, not a clean response.
        stressed = False

        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self._session.post(self.GRAPHQL_URL, headers=headers, cookies=cookies, data=body, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                stressed = True
                logger.warning(
                    "request_failed", platform=self.PLATFORM, attempt=attempt, max_retries=self.MAX_RETRIES, error=str(exc)
                )
            else:
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    stressed = True
                    logger.warning(
                        "graphql_returned_error_status",
                        platform=self.PLATFORM,
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                    )
                else:
                    self._adjust_interval(stressed=stressed)
                    self._record_proxy_outcome_once(success=not stressed)
                    return resp

            if attempt < self.MAX_RETRIES:
                # Jitter on top of the exponential base - a retry landing at
                # exactly 2s/4s/8s every time is itself the kind of uniform
                # pattern the per-request pacing jitter elsewhere already
                # avoids.
                delay = self.RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(
                    0, self.RETRY_BACKOFF_JITTER_SECONDS
                )
                logger.info("retrying", delay_seconds=round(delay, 1))
                time.sleep(delay)

        self._adjust_interval(stressed=True)
        self._record_proxy_outcome_once(success=False)

        # A 429 that survives every retry means the platform is genuinely
        # rate-limiting this account/IP, not that the token died - keep that
        # distinct from SessionExpiredError so callers don't misdiagnose it
        # as "re-run bootstrap.py" (which would just add more login traffic
        # right when the platform is already throttling this account).
        if resp is not None and resp.status_code == 429:
            raise RateLimitedError(
                f"{self.PLATFORM.capitalize()} rate-limited this request (status=429) even after "
                f"{self.MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        # resp is still None here - every attempt raised RequestsError
        # (connection-level failure), never even reached the platform's
        # server, so this is a network/proxy problem, not a dead session.
        raise NetworkError(f"Request failed after {self.MAX_RETRIES} attempts: {last_exc}") from last_exc


def _loggable(overrides: dict[str, Any]) -> dict[str, Any]:
    """Some override values (Relay pagination cursors) are opaque encoded
    blobs thousands of characters long - truncate anything long before it
    hits the log instead of drowning every request in noise."""
    return {
        key: (f"{value[:40]}...({len(value)} chars)" if isinstance(value, str) and len(value) > 60 else value)
        for key, value in overrides.items()
    }


def _apply_variable_overrides(template: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Deep-copy a variables_template cached by bootstrap.py and only
    override the given keys (wherever they appear in the tree) + regenerate
    any *session_id field - every other value (e.g. __relay_internal__pv__...
    flags) is left untouched since we don't know the full current schema,
    which the platform changes on every deploy. Shared by every query type
    (search, comments, ...) so adding a new one never needs its own
    variable-patching logic."""
    variables = copy.deepcopy(template)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in overrides:
                    node[key] = overrides[key]
                elif key.endswith("session_id") and isinstance(value, str):
                    node[key] = str(uuid.uuid4())
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(variables)
    return variables


def _iter_matching(node: Any, predicate: Callable[[dict], bool]):
    """Recursively walk a dict/list tree, yielding every dict for which
    predicate(node) is true - the one tree-walk this project needs whenever
    a field's real path isn't guaranteed to stay stable across a platform's
    deploys."""
    if isinstance(node, dict):
        if predicate(node):
            yield node
        for value in node.values():
            yield from _iter_matching(value, predicate)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_matching(value, predicate)


def find_page_info(node: Any) -> dict[str, Any] | None:
    """Search a parsed GraphQL response for a Relay `page_info` dict (has
    both `has_next_page` and `end_cursor`). Results are nested several
    levels deep and that path isn't guaranteed to stay stable across
    deploys, so this walks the whole tree instead of hardcoding it. Shared
    by Facebook and Threads (both Relay/Comet-based) - confirmed identical
    shape on both."""
    for match in _iter_matching(node, lambda n: "has_next_page" in n and "end_cursor" in n):
        return match
    return None


def _parse_graphql_response(raw: str) -> dict[str, Any]:
    text = raw.strip()
    prefix = "for (;;);"
    if text.startswith(prefix):
        text = text[len(prefix) :]
    # Sometimes returns several JSON objects back-to-back (streaming
    # response) - just take the first line.
    line = text.splitlines()[0] if "\n" in text else text
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        raise SessionExpiredError(
            f"Could not parse GraphQL response (token may have expired): {exc}. Body: {text[:300]!r}"
        ) from exc
