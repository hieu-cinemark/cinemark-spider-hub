"""
Captures GraphQL requests fired while a Playwright trigger runs, and picks
the "initial" / "paginated" request among them by their
fb_api_req_friendly_name - Facebook renames these across deploys, so the
matching here is by substring/keyword, not exact name.
"""

from __future__ import annotations

import time
from urllib.parse import parse_qsl

from patchright.sync_api import Request

from social_crawler.logger import get_logger

logger = get_logger(__name__)


def capture_graphql_requests(page, trigger, timeout_s: float = 25.0) -> list[Request]:
    """Run `trigger(page)` and collect every GraphQL request (with a doc_id)
    captured within `timeout_s` seconds - not tied to a specific query name
    since Facebook renames these frequently."""
    captured: list[Request] = []

    def on_request(request: Request) -> None:
        if request.method != "POST" or "/api/graphql/" not in request.url:
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


def name_requests(requests_seen: list[Request]) -> list[tuple[Request, str]]:
    named = []
    for request in requests_seen:
        body = dict(parse_qsl(request.post_data or "", keep_blank_values=True))
        named.append((request, body.get("fb_api_req_friendly_name", "")))
    return named


def pick_initial_request(named: list[tuple[Request, str]]) -> Request:
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while typing the search query. "
            "Facebook may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for request, name in named:
        lname = name.lower()
        if "initialresults" in lname and "parallelfetch" not in lname:
            return request

    for request, name in named:
        lname = name.lower()
        if "results" in lname and "parallelfetch" not in lname and "paginated" not in lname:
            logger.warning("falling_back_request_choice", reason="no_exact_initial_results_query", chosen=name)
            return request

    # Some Facebook deploys don't have a separate "initial" results query at
    # all - the "paginated" one is used for every page, page 1 included,
    # just called with cursor=None (client.search() already does this via
    # its overrides). Fall back to it rather than failing outright.
    for request, name in named:
        lname = name.lower()
        if "results" in lname and "parallelfetch" not in lname and "paginated" in lname:
            logger.warning(
                "falling_back_request_choice",
                reason="only_paginated_results_query_captured",
                chosen=name,
                note="this Facebook deploy may use one query for every page - replaying it with cursor=None for page 1",
            )
            return request

    raise RuntimeError(
        "No search-results GraphQL request was captured (only saw: "
        f"{[name for _, name in named]}). Facebook may not have returned real "
        "results for this query, or its UI changed - try a more natural search "
        "phrase, or re-run with --show-browser to see what happened."
    )


def _pick_paginated(named: list[tuple[Request, str]], require: str | None = None) -> Request | None:
    """Find the query used for follow-up pages (name contains "paginated" or
    "pagination" - Facebook deploys aren't consistent about which spelling
    they use), optionally also requiring another keyword (e.g. "comment") to
    disambiguate from a different feature's paginated query."""
    for request, name in named:
        lname = name.lower()
        if ("paginated" in lname or "pagination" in lname) and (require is None or require in lname):
            return request
    return None


def pick_paginated_request(named: list[tuple[Request, str]]) -> Request | None:
    return _pick_paginated(named)


def pick_comments_request(named: list[tuple[Request, str]]) -> Request:
    """Same idea as pick_initial_request but for the comments list "root"
    query - Facebook names it something with 'Comment' in it (exact name
    varies by deploy), and we still want to avoid any ParallelFetch/warm-up
    variant, and avoid the Pagination one (that's the follow-up page, not
    the first one)."""
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while opening the post. "
            "Facebook may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for request, name in named:
        lname = name.lower()
        if "comment" in lname and "parallelfetch" not in lname and "pagination" not in lname:
            return request

    logger.warning(
        "falling_back_request_choice",
        reason="no_comment_query_found",
        note="doc_id may not match the comments feature, double-check the result",
    )
    return named[-1][0]


def pick_paginated_comments_request(named: list[tuple[Request, str]]) -> Request | None:
    return _pick_paginated(named, require="comment")
