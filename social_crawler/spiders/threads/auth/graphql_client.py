"""
Plain HTTP client (no browser) that calls the Threads GraphQL endpoint back
using the token/doc_id cached by bootstrap.py in Redis. threads.com runs on
the same Comet/Barcelona GraphQL stack as Facebook (confirmed against a real
captured BarcelonaPostPageStrongIdTargetQuery request), so everything not
specific to Threads (session setup, throttling, retry/backoff, variable
templating, response parsing) lives in CometGraphQLClient (see
spiders/comet_graphql_client.py's module docstring) - this file only adds
what's actually Threads-specific: the extra x-csrftoken/origin header and
the search query and the cookie-auth REST replies GET (no date filter).

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

import random
import time
from typing import Any
from urllib.parse import urlencode

from curl_cffi import requests as curl_requests

from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    IG_APP_ID,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    REST_READ_UA,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    TEXT_FEED_REPLIES_URL,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.logger import get_logger
from social_crawler.spiders.comet_graphql_client import (
    CheckpointRequiredError,
    CometGraphQLClient,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    find_page_info,
)

__all__ = [
    "ThreadsGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]

logger = get_logger(__name__)


class ThreadsGraphQLClient(CometGraphQLClient):
    PLATFORM = "threads"
    REFERER_URL = "https://www.threads.com/"
    CACHE_REDIS_KEY_TMPL = CACHE_REDIS_KEY_TMPL
    ACTIVE_ACCOUNT_REDIS_KEY = ACTIVE_ACCOUNT_REDIS_KEY
    DEFAULT_ACCOUNT_KEY = DEFAULT_ACCOUNT_KEY
    GRAPHQL_URL = GRAPHQL_URL
    MAX_RETRIES = MAX_RETRIES
    MIN_REQUEST_INTERVAL_SECONDS = MIN_REQUEST_INTERVAL_SECONDS
    REQUEST_INTERVAL_JITTER_SECONDS = REQUEST_INTERVAL_JITTER_SECONDS
    RETRY_BACKOFF_BASE_SECONDS = RETRY_BACKOFF_BASE_SECONDS
    RETRY_BACKOFF_JITTER_SECONDS = RETRY_BACKOFF_JITTER_SECONDS
    THROTTLE_REDIS_KEY_TMPL = THROTTLE_REDIS_KEY_TMPL
    ADAPTIVE_INTERVAL_MAX_SECONDS = ADAPTIVE_INTERVAL_MAX_SECONDS
    COMMENTS_REDIS_KEY_TMPL = COMMENTS_REDIS_KEY_TMPL
    # Confirmed against a real captured BarcelonaPostPageStrongIdDirectRepliesRefetchQuery
    # request - Threads' Relay variable names differ from Facebook's own
    # comments query on every one of these (see CometGraphQLClient's
    # defaults, which are Facebook's).
    COMMENTS_ID_KEY = "postID"
    COMMENTS_CURSOR_KEY = "after"
    COMMENTS_COUNT_KEY = "first"

    def _comment_target_id(self, post_id: str) -> str:
        """Unlike Facebook's base64 feedback id, Threads addresses a post's
        replies by its raw numeric post id, unencoded - confirmed against
        the same real captured request as the variable names above."""
        return str(post_id)

    def _headers(self, friendly_name: str, lsd: str) -> dict[str, str]:
        headers = super()._headers(friendly_name, lsd)
        headers.update(
            {
                "origin": "https://www.threads.com",
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

    def _rest_headers(self) -> dict[str, str]:
        """Headers for /api/v1/text_feed/... reads. Not the GraphQL set:
        that surface wants the captured Chrome UA; this one 403s it."""
        cookies = self._cache["cookies"]
        captured = self._cache.get("headers") or {}
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": captured.get("accept-language") or "en-US,en;q=0.9",
            "referer": self.REFERER_URL,
            "user-agent": REST_READ_UA,
            "x-asbd-id": captured.get("x-asbd-id") or "129477",
            "x-csrftoken": cookies.get("csrftoken", ""),
            "x-ig-app-id": captured.get("x-ig-app-id") or IG_APP_ID,
            "x-ig-www-claim": "0",
        }

    def get_text_feed_replies(
        self, post_id: str, count: int = 25, cursor: str | None = None
    ) -> dict[str, Any]:
        """One page of replies for `post_id` via GET /api/v1/text_feed/.../replies/.

        Same cookie session search() already uses - no extra comments-query
        bootstrap, no browser. `cursor` is paging_tokens.downward from the
        previous page (None for the first; some dumps spell it downwards)."""
        params: dict[str, str] = {"count": str(count)}
        if cursor:
            params["paging_token"] = cursor
        url = f"{TEXT_FEED_REPLIES_URL.format(post_id=post_id)}?{urlencode(params)}"
        logger.info("sending_text_feed_replies_request", post_id=post_id, count=count, has_cursor=bool(cursor))
        self._throttle()
        resp = self._get_with_retry(url, self._rest_headers())

        if resp.status_code in (401, 403):
            raise SessionExpiredError(
                f"Threads rejected the text_feed replies request (status={resp.status_code}). Re-run bootstrap.py."
            )

        logger.info("received_response", status_code=resp.status_code, bytes=len(resp.content), endpoint="text_feed_replies")
        try:
            parsed = resp.json()
        except ValueError as exc:
            raise SessionExpiredError(
                f"Threads text_feed replies returned non-JSON (status={resp.status_code})."
            ) from exc

        if isinstance(parsed, dict) and parsed.get("message") == "checkpoint_required":
            from social_crawler.services.db import disable_account

            disabled = disable_account(
                self.PLATFORM, self._account, reason="checkpoint_required response during text_feed replies"
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
                f"Threads returned checkpoint_required for account {self._account!r} - "
                "log in as this account through a real browser to resolve the checkpoint, then re-run bootstrap.py."
            )
        if isinstance(parsed, dict) and parsed.get("status") == "fail":
            raise SessionExpiredError(
                f"Threads text_feed replies failed: {parsed.get('message') or parsed}"
            )
        if not isinstance(parsed, dict):
            raise SessionExpiredError("Threads text_feed replies returned a non-object JSON body.")
        return parsed

    def _get_with_retry(self, url: str, headers: dict[str, str]):
        """GET twin of CometGraphQLClient._post_with_retry - same 429/5xx/
        network backoff, different verb/URL (text_feed is not GraphQL)."""
        last_exc: Exception | None = None
        resp = None
        stressed = False
        transient = {429, 500, 502, 503, 504}

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self._session.get(
                    url, headers=headers, cookies=self._cache["cookies"], timeout=15
                )
            except curl_requests.RequestsError as exc:
                last_exc = exc
                stressed = True
                logger.warning(
                    "request_failed", platform=self.PLATFORM, attempt=attempt, max_retries=self.MAX_RETRIES, error=str(exc)
                )
            else:
                if resp.status_code in transient:
                    stressed = True
                    logger.warning(
                        "threads_returned_error_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                    )
                else:
                    self._adjust_interval(stressed=stressed)
                    self._record_proxy_outcome_once(success=not stressed)
                    return resp

            if attempt < self.MAX_RETRIES:
                delay = self.RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(
                    0, self.RETRY_BACKOFF_JITTER_SECONDS
                )
                logger.info("retrying", delay_seconds=round(delay, 1))
                time.sleep(delay)

        self._adjust_interval(stressed=True)
        self._record_proxy_outcome_once(success=False)

        if resp is not None and resp.status_code == 429:
            raise RateLimitedError(
                f"Threads rate-limited this request (status=429) even after {self.MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        raise NetworkError(f"Request failed after {self.MAX_RETRIES} attempts: {last_exc}") from last_exc

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


def _search_overrides(query: str, cursor: str | None, count: int | None) -> dict[str, Any]:
    """Field names confirmed against a real captured
    BarcelonaSearchResultsRefetchableQuery request: Relay-style "after" for
    the cursor (not "cursor") and "first" for the page size (not "count") -
    both differ from what Facebook's own search query uses."""
    overrides: dict[str, Any] = {"query": query, "after": cursor}
    if count is not None:
        overrides["first"] = count
    return overrides
