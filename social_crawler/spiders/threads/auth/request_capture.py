"""
Captures GraphQL requests fired while a Playwright trigger runs - mirrors
social_crawler.spiders.facebook.auth.request_capture.capture_graphql_requests,
but matches threads.com's real endpoint URLs instead of Facebook's.

This can't just reuse Facebook's capture_graphql_requests unchanged: that
function requires "/api/graphql/" (with a trailing slash) in the URL, which
matches Facebook's endpoint but not threads.com's - confirmed against real
captured traffic that threads.com posts to both "/api/graphql" (no trailing
slash) and a second endpoint, "/graphql/query", neither of which the
trailing-slash check matches. Using Facebook's version unchanged here always
silently captured zero requests, even once login/search were working fully
correctly - the bug looked like an auth problem but wasn't one.

name_requests/pick_initial_request/pick_paginated_request are genuinely
generic (they only look at the already-filtered request list's post_data,
not the URL) and are still imported from facebook.auth.request_capture
as-is - no need to duplicate those here.
"""

from __future__ import annotations

import time

from patchright.sync_api import Request

from social_crawler.logger import get_logger

logger = get_logger(__name__)

THREADS_GRAPHQL_URL_MARKERS = ("/api/graphql", "/graphql/query")

# Confirmed against real captured traffic while typing a search query,
# pressing Enter, and scrolling: threads.com fires several queries -
# "AccountSearch"/"KeywordSearch" per keystroke (typeahead dropdown
# suggestions only - confirmed by inspecting a captured KeywordSearch
# request's variables_template: just {"query", "has_communities",
# "has_favicons"}, no cursor/count field at all, so it can't be paginated),
# and "SearchResultsRefetchableQuery" (Relay's naming convention for a
# paginated/refetchable connection) after Enter/scroll - that one is the
# real full-results feed and is preferred here. KeywordSearch is kept as a
# fallback only in case a future deploy stops firing the Refetchable query
# under this same name.
_RESULTS_QUERY_NAME_MARKERS = ("searchresultsrefetchable", "keywordsearch")


def capture_graphql_requests(page, trigger, timeout_s: float = 25.0) -> list[Request]:
    """Run `trigger(page)` and collect every GraphQL request (with a doc_id)
    captured within `timeout_s` seconds - not tied to a specific query name
    since Threads renames these frequently, and posts to more than one
    endpoint path (see module docstring)."""
    captured: list[Request] = []

    def on_request(request: Request) -> None:
        if request.method != "POST" or not any(marker in request.url for marker in THREADS_GRAPHQL_URL_MARKERS):
            return
        if "doc_id=" in (request.post_data or ""):
            captured.append(request)

    page.on("request", on_request)
    trigger(page)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        page.wait_for_timeout(250)
    page.remove_listener("request", on_request)
    return captured


def pick_initial_request(named: list[tuple[Request, str]]) -> Request:
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while typing the search query. "
            "Threads may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for marker in _RESULTS_QUERY_NAME_MARKERS:
        for request, name in named:
            if marker in name.lower():
                return request

    raise RuntimeError(
        "No search-results GraphQL request was captured (only saw: "
        f"{[name for _, name in named]}). Threads may not have returned real results for this "
        "query, or its UI changed - try a more natural search phrase, scroll further, or re-run "
        "with --show-browser to see what happened."
    )


def pick_paginated_request(named: list[tuple[Request, str]]) -> Request | None:
    """Same query serves both the first page and follow-up pages (only its
    cursor variable changes) - see pick_initial_request's docstring."""
    for marker in _RESULTS_QUERY_NAME_MARKERS:
        for request, name in named:
            if marker in name.lower():
                return request
    return None


# Threads calls a post's comments "replies" in its own UI/terminology, not
# "comments" like Facebook - matching both substrings here since the real
# GraphQL query name is only confirmed once bootstrap.py --post-url has
# actually captured one (see this module's own docstring on why capture
# has to match reality, not be assumed).
_COMMENTS_QUERY_NAME_MARKERS = ("comment", "repl")


def pick_comments_request(named: list[tuple[Request, str]]) -> Request:
    """Same idea as pick_initial_request but for the reply-list "root"
    query - avoids any paginated variant (that's the follow-up page, not
    the first one)."""
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while opening the post. "
            "Threads may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for request, name in named:
        lname = name.lower()
        if any(marker in lname for marker in _COMMENTS_QUERY_NAME_MARKERS) and "pagina" not in lname:
            return request

    logger.warning(
        "falling_back_request_choice",
        reason="no_comment_query_found",
        note="doc_id may not match the replies feature, double-check the result",
    )
    return named[-1][0]


def pick_paginated_comments_request(named: list[tuple[Request, str]]) -> Request | None:
    for request, name in named:
        lname = name.lower()
        if "pagina" in lname and any(marker in lname for marker in _COMMENTS_QUERY_NAME_MARKERS):
            return request

    # Threads has no separately-named paginated variant of its replies
    # query - confirmed against a real capture: "...DirectRepliesRefetchQuery"
    # is a Relay "refetchable" query, already carrying its own "after"
    # cursor variable, and is what fires again (same name) on every
    # subsequent scroll/page. Reuse it for pagination too instead of
    # reporting "no pagination captured" for something that actually
    # works fine with a cursor override - same idea as
    # pick_paginated_request's own "only_paginated_results_query_captured"
    # fallback, just the mirror-image situation (one query serving both
    # roles, found under the "initial" name instead of the "paginated" one).
    for request, name in named:
        lname = name.lower()
        if any(marker in lname for marker in _COMMENTS_QUERY_NAME_MARKERS):
            logger.warning(
                "falling_back_request_choice",
                reason="only_initial_replies_query_captured",
                chosen=name,
                note="this Threads deploy may use one refetchable query for every page - reusing it for pagination too",
            )
            return request
    return None
