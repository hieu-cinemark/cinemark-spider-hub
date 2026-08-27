"""
Plain HTTP client (no browser) that calls the Threads GraphQL endpoint back
using the token/doc_id cached by bootstrap.py in Redis. threads.com runs on
the same Comet/Barcelona GraphQL stack as Facebook (confirmed against a real
captured BarcelonaPostPageStrongIdTargetQuery request), so this mirrors
social_crawler.spiders.facebook.auth.graphql_client - see that module for
the reasoning behind curl_cffi/TLS impersonation, throttling, and the
retry/backoff split between a dead token (401/403, not retried) and genuine
rate limiting (429, retried then reported separately).

Note: the exact `variables` key names used by the real search-results query
(query text / cursor / count) are only known once bootstrap.py has actually
captured one - _apply_variable_overrides only overrides whichever of these
keys are present in the captured template, so an override that doesn't
match anything just silently leaves that part of the template unchanged
rather than erroring. If search() results stop changing across different
`query` arguments, re-check the real captured variables_template in Redis
against the override keys below.
"""

from __future__ import annotations

import copy
import json
import random
import time
import uuid
from typing import Any

from curl_cffi import requests as curl_requests

from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
)
from social_crawler.logger import get_logger
from social_crawler.services.db import get_proxy
from social_crawler.services.redis import RedisCache
# Generic Relay page_info search (has_next_page/end_cursor) - no Facebook-
# specific assumptions, so reused as-is instead of duplicated.
from social_crawler.spiders.facebook.auth.graphql_client import find_page_info

logger = get_logger(__name__)

__all__ = ["ThreadsGraphQLClient", "SessionExpiredError", "RateLimitedError", "NetworkError", "find_page_info"]


class SessionExpiredError(RuntimeError):
    """Token cache is missing/expired or Threads rejected the request (401/403) - re-run bootstrap.py."""


class NetworkError(RuntimeError):
    """Every retry failed to even get an HTTP response back (proxy down,
    DNS failure, TLS handshake failure, timeout) - Threads never actually
    saw this request, so the token/session is not the problem. Re-running
    bootstrap.py won't fix a dead proxy; check connectivity to the
    configured platform_proxies row (platform='threads') instead."""


class RateLimitedError(RuntimeError):
    """Threads is rate-limiting this account/IP (429) even after retrying with backoff. This is NOT a
    dead token - re-running bootstrap.py won't help and just burns another login cycle against an
    account that's already being throttled. Back off and retry later instead."""


class ThreadsGraphQLClient:
    def __init__(self, redis_cache: RedisCache | None = None, account: str | None = None):
        self._redis = redis_cache or RedisCache()
        self._account = account or self._redis.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
        cache_key = CACHE_REDIS_KEY_TMPL.format(account=self._account)
        cached = self._redis.get(cache_key)
        if cached is None:
            raise SessionExpiredError(
                f"No token cache found in Redis (key={cache_key!r}, account={self._account!r}), or Redis "
                "is unreachable, or the cache expired. Run this first:\n"
                '  python -m social_crawler.spiders.threads.auth.bootstrap --query "test"'
            )
        self._cache = cached
        age = time.time() - self._cache["captured_at"]
        logger.info("loaded_token_cache", account=self._account, age_hours=round(age / 3600, 1))

        proxy = None
        proxy_cfg = get_proxy("threads")
        if proxy_cfg:
            proxy = {
                "http": f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}",
                "https": f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}",
            }
        logger.info("graphql_session_ready", account=self._account, proxy=proxy_cfg["url"] if proxy_cfg else None)

        self._session = curl_requests.Session(impersonate="chrome", proxies=proxy)
        self._last_request_at: float | None = None

    def _throttle(self) -> None:
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
                "origin": "https://www.threads.com",
                "referer": "https://www.threads.com/",
                "x-fb-friendly-name": friendly_name,
                "x-fb-lsd": lsd,
                # /graphql/query (unlike /api/graphql) 403s outright without
                # this - confirmed against a real captured request, where it
                # was set to the exact same value as the csrftoken cookie.
                # Read from the cookie at request time (not cached as a
                # static header) since it has to keep matching whatever
                # csrftoken is current for this session.
                "x-csrftoken": self._cache["cookies"].get("csrftoken", ""),
            }
        )
        return headers

    def search(self, query: str, count: int = 10, cursor: str | None = None) -> dict[str, Any]:
        """Fetch a page of search results (the first page when cursor is None)."""
        return self._run(
            doc_id=self._cache["doc_id"],
            friendly_name=self._cache["fb_api_req_friendly_name"],
            template=self._cache.get("variables_template"),
            template_source="variables_template",
            overrides=_search_overrides(query, cursor, count),
        )

    def search_next_page(self, query: str, cursor: str, count: int = 10) -> dict[str, Any]:
        """Fetch the next page, using the `end_cursor` from a previous page's
        `page_info` (see `find_page_info`). Requires bootstrap.py to have
        captured a paginated search-results request - it does this
        automatically by scrolling the results page."""
        pagination = self._cache.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no pagination info (no paginated search-results query was captured). "
                "Re-run bootstrap.py, which scrolls the results page to capture one."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="pagination.variables_template",
            overrides=_search_overrides(query, cursor, count),
        )

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
            raise SessionExpiredError(f"Threads rejected the request (status={resp.status_code}). Re-run bootstrap.py.")

        logger.info("received_response", status_code=resp.status_code, bytes=len(resp.text))
        return _parse_graphql_response(resp.text)

    def _post_with_retry(self, headers: dict[str, str], cookies: dict[str, str], body: dict[str, Any]) -> Any:
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
                        "threads_returned_error_status",
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

        if resp is not None and resp.status_code == 429:
            raise RateLimitedError(
                f"Threads rate-limited this request (status=429) even after {MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        # resp is still None here - every attempt raised RequestsError
        # (connection-level failure), never even reached Threads' server,
        # so this is a network/proxy problem, not a dead session.
        raise NetworkError(f"Request failed after {MAX_RETRIES} attempts: {last_exc}") from last_exc


def _search_overrides(query: str, cursor: str | None, count: int | None) -> dict[str, Any]:
    """Field names confirmed against a real captured
    BarcelonaSearchResultsRefetchableQuery request: Relay-style "after" for
    the cursor (not "cursor") and "first" for the page size (not "count") -
    both differ from what Facebook's own search query uses."""
    overrides: dict[str, Any] = {"query": query, "after": cursor}
    if count is not None:
        overrides["first"] = count
    return overrides


def _loggable(overrides: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (f"{value[:40]}...({len(value)} chars)" if isinstance(value, str) and len(value) > 60 else value)
        for key, value in overrides.items()
    }


def _apply_variable_overrides(template: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """See facebook.auth.graphql_client._apply_variable_overrides - same
    deep-copy-and-patch-in-place approach, kept separate here only because
    it's bound to no shared state worth importing across."""
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


def _parse_graphql_response(raw: str) -> dict[str, Any]:
    text = raw.strip()
    prefix = "for (;;);"
    if text.startswith(prefix):
        text = text[len(prefix):]
    line = text.splitlines()[0] if "\n" in text else text
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        raise SessionExpiredError(
            f"Could not parse GraphQL response (token may have expired): {exc}. Body: {text[:300]!r}"
        ) from exc
