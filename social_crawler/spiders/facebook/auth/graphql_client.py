"""
Plain HTTP client (no browser) that calls the Facebook GraphQL endpoint back
using the token/doc_id cached by bootstrap.py in Redis.

Uses curl_cffi to impersonate a real Chrome TLS/JA3 fingerprint - plain
requests/httpx are easily flagged as bots by Facebook via the TLS handshake.
"""

from __future__ import annotations

import base64
import copy
import json
import random
import time
import uuid
from datetime import date
from typing import Any

from curl_cffi import requests as curl_requests

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
)
from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache
from social_crawler.settings import PROXY_PASSWORD, PROXY_URL, PROXY_USERNAME
from social_crawler.spiders.facebook.response_utils import find_first

logger = get_logger(__name__)


class SessionExpiredError(RuntimeError):
    """Token cache is missing/expired or Facebook rejected the request (401/403) - re-run bootstrap.py."""


class RateLimitedError(RuntimeError):
    """Facebook is rate-limiting this account/IP (429) even after retrying with backoff. This is NOT a
    dead token - re-running bootstrap.py won't help and just burns another login cycle against an
    account that's already being throttled. Back off and retry later instead."""


class FacebookGraphQLClient:
    def __init__(self, redis_cache: RedisCache | None = None, account: str | None = None):
        self._redis = redis_cache or RedisCache()
        # Defaults to whichever account bootstrap.py most recently
        # (re)logged in as - see ACTIVE_ACCOUNT_REDIS_KEY - so rotating
        # FACEBOOK_ACCOUNTS in bootstrap runs automatically carries over to
        # `scrapy crawl ...` without needing to pass anything here. Pass
        # `account` explicitly to pin a run to one account instead.
        self._account = account or self._redis.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
        cache_key = CACHE_REDIS_KEY_TMPL.format(account=self._account)
        cached = self._redis.get(cache_key)
        if cached is None:
            raise SessionExpiredError(
                f"No token cache found in Redis (key={cache_key!r}, account={self._account!r}), or Redis "
                "is unreachable, or the cache expired. Run this first:\n"
                "  python -m social_crawler.spiders.facebook.auth.bootstrap --query \"test\""
            )
        self._cache = cached
        age = time.time() - self._cache["captured_at"]
        logger.info("loaded_token_cache", account=self._account, age_hours=round(age / 3600, 1))

        proxy = None

        if PROXY_URL and PROXY_USERNAME and PROXY_PASSWORD:
            proxy = {
                "http": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_URL}",
                "https": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_URL}"
            }
        logger.info("graphql_session_ready", account=self._account, proxy=PROXY_URL if proxy else None)

        self._session = curl_requests.Session(impersonate="chrome", proxies=proxy)
        self._last_request_at: float | None = None

    def _throttle(self) -> None:
        """Space out requests to Facebook - nothing else does this, since
        every spider here calls curl_cffi directly instead of going through
        Scrapy's downloader (see MIN_REQUEST_INTERVAL_SECONDS)."""
        if self._last_request_at is not None:
            target_gap = MIN_REQUEST_INTERVAL_SECONDS + random.uniform(0, REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.info("throttling", delay_seconds=round(remaining, 2))
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _headers(self, friendly_name: str, lsd: str) -> dict[str, str]:
        headers = dict(self._cache["headers"])
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "referer": "https://www.facebook.com/",
                "x-fb-friendly-name": friendly_name,
                "x-fb-lsd": lsd,
            }
        )
        return headers

    def search(
        self,
        query: str,
        count: int = 5,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Fetch the first page of search results. Pass start_date/end_date
        (both required together) to use Facebook's own "Date posted" search
        filter and only get posts created in that range."""
        return self._run(
            doc_id=self._cache["doc_id"],
            friendly_name=self._cache["fb_api_req_friendly_name"],
            template=self._cache.get("variables_template"),
            template_source="variables_template",
            overrides=_search_overrides(query, None, count, start_date, end_date),
        )

    def search_next_page(
        self,
        query: str,
        cursor: str,
        count: int = 5,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Fetch the next page, using the `end_cursor` from a previous page's
        `page_info` (see `find_page_info`). Requires bootstrap.py to have
        captured a SearchCometResultsPaginatedResultsQuery request - it does
        this automatically by scrolling the results page. Pass the same
        start_date/end_date used on the first page to keep the date filter
        applied across pages."""
        pagination = self._cache.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no pagination info (no SearchCometResultsPaginatedResultsQuery "
                "was captured). Re-run bootstrap.py, which scrolls the results page to capture one."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="pagination.variables_template",
            overrides=_search_overrides(query, cursor, count, start_date, end_date),
        )

    def get_comments(self, post_id: str) -> dict[str, Any]:
        """Fetch the first page of comments for a post."""
        comments = self._get_comments_cache()
        return self._run(
            doc_id=comments.get("doc_id"),
            friendly_name=comments.get("fb_api_req_friendly_name"),
            template=comments.get("variables_template"),
            template_source="comments variables_template",
            overrides={"id": _feedback_id(post_id)},
        )

    def get_comments_next_page(self, post_id: str, cursor: str, count: int = 10) -> dict[str, Any]:
        """Fetch the next page of comments, using the `end_cursor` from a
        previous page's `page_info` (see `find_page_info`). Requires
        bootstrap_comments() to have captured a CommentsListComponentsPaginationQuery
        request - it does this automatically by scrolling the comment list
        after switching sort order."""
        comments = self._get_comments_cache()
        pagination = comments.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no comments pagination info (no CommentsListComponentsPaginationQuery "
                "was captured). Re-run bootstrap_comments() against a post with more comments than "
                "fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="comments pagination.variables_template",
            overrides={
                "id": _feedback_id(post_id),
                "commentsAfterCursor": cursor,
                "commentsAfterCount": count,
            },
        )

    def _get_comments_cache(self) -> dict[str, Any]:
        comments_key = COMMENTS_REDIS_KEY_TMPL.format(account=self._account)
        comments = self._redis.get(comments_key)
        if comments is None:
            raise SessionExpiredError(
                f"No comments query cached in Redis (key={comments_key!r}, account={self._account!r}). Run this first:\n"
                '  python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a post url with comments>"'
            )
        return comments

    def _run(
        self,
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
            raise SessionExpiredError(f"Facebook rejected the request (status={resp.status_code}). Re-run bootstrap.py.")

        logger.info("received_response", status_code=resp.status_code, bytes=len(resp.text))
        return _parse_graphql_response(resp.text)

    def _post_with_retry(self, headers: dict[str, str], cookies: dict[str, str], body: dict[str, Any]) -> Any:
        """POST with exponential-backoff retry on rate limiting (429), server
        errors (5xx) and network-level failures - these are transient and
        usually recover on their own, unlike a dead token (401/403), which
        the caller handles separately and never retries here."""
        last_exc: Exception | None = None
        resp = None

        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.post(GRAPHQL_URL, headers=headers, cookies=cookies, data=body, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                logger.warning("request_failed", attempt=attempt, max_retries=MAX_RETRIES, error=str(exc))
            else:
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    logger.warning(
                        "facebook_returned_error_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=MAX_RETRIES,
                    )
                else:
                    return resp

            if attempt < MAX_RETRIES:
                delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                logger.info("retrying", delay_seconds=delay)
                time.sleep(delay)

        # A 429 that survives every retry means Facebook is genuinely
        # rate-limiting this account/IP, not that the token died - keep that
        # distinct from SessionExpiredError so callers don't misdiagnose it
        # as "re-run bootstrap.py" (which would just add more login traffic
        # right when Facebook is already throttling this account).
        if resp is not None and resp.status_code == 429:
            raise RateLimitedError(
                f"Facebook rate-limited this request (status=429) even after {MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        raise SessionExpiredError(f"Request failed after {MAX_RETRIES} attempts: {last_exc}") from last_exc


def _search_overrides(
    query: str,
    cursor: str | None,
    count: int | None,
    start_date: date | None,
    end_date: date | None,
) -> dict[str, Any]:
    """Shared by search() and search_next_page() - the only difference
    between a first page and a follow-up page is the cursor."""
    overrides: dict[str, Any] = {"text": query, "cursor": cursor}
    if count is not None:
        overrides["count"] = count
    if start_date and end_date:
        overrides["filters"] = _build_date_filters(start_date, end_date)
    return overrides


def _build_date_filters(start_date: date, end_date: date) -> list[str]:
    """Build the `filters` override for Facebook's own "Date posted" search
    filter, to only get posts created between start_date and end_date
    (inclusive). Format confirmed against a real captured request (clicking
    a year in the search UI's date filter), not guessed: month/day are
    unpadded "YYYY-M"/"YYYY-M-D" strings, and the whole filter is
    double-JSON-encoded - Facebook stores the inner date args as a JSON
    *string*, not a nested object, inside the outer filter object, which
    itself is also a JSON string inside the `filters` list (not an object)."""
    inner_args = {
        "start_year": str(start_date.year),
        "start_month": f"{start_date.year}-{start_date.month}",
        "start_day": f"{start_date.year}-{start_date.month}-{start_date.day}",
        "end_year": str(end_date.year),
        "end_month": f"{end_date.year}-{end_date.month}",
        "end_day": f"{end_date.year}-{end_date.month}-{end_date.day}",
    }
    filter_obj = {"name": "creation_time", "args": json.dumps(inner_args, separators=(",", ":"))}
    return [json.dumps(filter_obj, separators=(",", ":"))]


def _feedback_id(post_id: str) -> str:
    """Facebook's comment-list queries address a post by its feedback id,
    which is just base64("feedback:<post_id>") - confirmed against a real
    captured request rather than assumed."""
    return base64.b64encode(f"feedback:{post_id}".encode()).decode()


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
    which Facebook changes on every deploy. Shared by every query type
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


def find_page_info(node: Any) -> dict[str, Any] | None:
    """Search a parsed GraphQL response for a Relay `page_info` dict (has
    both `has_next_page` and `end_cursor`). Search results are nested
    several levels deep under `data.serpResponse.results.page_info`, but
    that path isn't guaranteed to stay stable across Facebook deploys, so
    this walks the whole tree instead of hardcoding it."""
    return find_first(node, lambda n: "has_next_page" in n and "end_cursor" in n)


def _parse_graphql_response(raw: str) -> dict[str, Any]:
    text = raw.strip()
    prefix = "for (;;);"
    if text.startswith(prefix):
        text = text[len(prefix):]
    # Facebook sometimes returns several JSON objects back-to-back (streaming
    # response) - just take the first line
    line = text.splitlines()[0] if "\n" in text else text
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        raise SessionExpiredError(
            f"Could not parse GraphQL response (token may have expired): {exc}. Body: {text[:300]!r}"
        ) from exc
