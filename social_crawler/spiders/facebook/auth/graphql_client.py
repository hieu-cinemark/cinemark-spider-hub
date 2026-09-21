"""
Plain HTTP client (no browser) that calls the Facebook GraphQL endpoint back
using the token/doc_id cached by bootstrap.py in Redis.

Uses curl_cffi to impersonate a real Chrome TLS/JA3 fingerprint - plain
requests/httpx are easily flagged as bots by Facebook via the TLS handshake.

Everything not specific to Facebook (session setup, throttling,
retry/backoff, variable templating, response parsing) lives in
CometGraphQLClient (see spiders/comet_graphql_client.py's module docstring)
- this file only adds what's actually Facebook-specific: the date-filtered
search query and the comments feature (Threads has neither).
"""

from __future__ import annotations

import base64
import json
from datetime import date
from typing import Any

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REPLIES_REDIS_KEY_TMPL,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.spiders.comet_graphql_client import (
    CheckpointRequiredError,
    CometGraphQLClient,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    find_page_info,
)

__all__ = [
    "FacebookGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]


class FacebookGraphQLClient(CometGraphQLClient):
    PLATFORM = "facebook"
    REFERER_URL = "https://www.facebook.com/"
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
    REPLIES_REDIS_KEY_TMPL = REPLIES_REDIS_KEY_TMPL

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

    def _comment_target_id(self, post_id: str) -> str:
        return _feedback_id(post_id)

    def _reply_target_id(self, legacy_comment_id: str) -> str:
        # Not yet independently confirmed against a real captured replies
        # request (see CometGraphQLClient._reply_target_id's own docstring) -
        # assumed identical to how a post's feedback id is built, since a
        # comment's replies thread is itself a feedback object on Facebook's
        # object graph. Correct this if a real bootstrap --type replies
        # capture shows a different scheme.
        return _feedback_id(legacy_comment_id)


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
