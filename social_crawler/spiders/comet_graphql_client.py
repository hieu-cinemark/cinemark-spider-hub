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
from social_crawler.services.db import disable_account, get_proxy
from social_crawler.services.redis import RedisCache

logger = get_logger(__name__)

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

    def __init__(self, redis_cache: RedisCache | None = None, account: str | None = None):
        self._redis = redis_cache or RedisCache()
        # Defaults to whichever account bootstrap.py most recently
        # (re)logged in as - see ACTIVE_ACCOUNT_REDIS_KEY - so rotating
        # through platform_accounts in bootstrap runs automatically carries
        # over to `scrapy crawl ...` without needing to pass anything here.
        # Pass `account` explicitly to pin a run to one account instead.
        self._account = account or self._redis.get(self.ACTIVE_ACCOUNT_REDIS_KEY) or self.DEFAULT_ACCOUNT_KEY
        cache_key = self.CACHE_REDIS_KEY_TMPL.format(account=self._account)
        cached = self._redis.get(cache_key)
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
        proxy_cfg = get_proxy(self.PLATFORM)
        if proxy_cfg:
            proxy = {
                "http": f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}",
                "https": f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}",
            }
        logger.info("graphql_session_ready", account=self._account, proxy=proxy_cfg["url"] if proxy_cfg else None)

        self._session = curl_requests.Session(impersonate="chrome", proxies=proxy)
        self._last_request_at: float | None = None

    def _throttle(self) -> None:
        """Space out requests to the platform - nothing else does this,
        since every spider here calls curl_cffi directly instead of going
        through Scrapy's downloader."""
        if self._last_request_at is not None:
            target_gap = self.MIN_REQUEST_INTERVAL_SECONDS + random.uniform(0, self.REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.info("throttling", delay_seconds=round(remaining, 2))
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

    def _post_with_retry(self, headers: dict[str, str], cookies: dict[str, str], body: dict[str, Any]) -> Any:
        """POST with exponential-backoff retry on rate limiting (429), server
        errors (5xx) and network-level failures - these are transient and
        usually recover on their own, unlike a dead token (401/403), which
        the caller handles separately and never retries here."""
        last_exc: Exception | None = None
        resp = None

        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self._session.post(self.GRAPHQL_URL, headers=headers, cookies=cookies, data=body, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                logger.warning("request_failed", attempt=attempt, max_retries=self.MAX_RETRIES, error=str(exc))
            else:
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    logger.warning(
                        "graphql_returned_error_status",
                        platform=self.PLATFORM,
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                    )
                else:
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
