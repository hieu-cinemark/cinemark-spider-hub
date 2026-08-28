"""
Plain HTTP client (no browser) that calls the Threads GraphQL endpoint back
using the token/doc_id cached by bootstrap.py in Redis. threads.com runs on
the same Comet/Barcelona GraphQL stack as Facebook (confirmed against a real
captured BarcelonaPostPageStrongIdTargetQuery request), so everything not
specific to Threads (session setup, throttling, retry/backoff, variable
templating, response parsing) lives in CometGraphQLClient (see
spiders/comet_graphql_client.py's module docstring) - this file only adds
what's actually Threads-specific: the extra x-csrftoken/origin header and
the search query (Threads has no comments feature here, no date filter).

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

from typing import Any

from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
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
    "ThreadsGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]


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
