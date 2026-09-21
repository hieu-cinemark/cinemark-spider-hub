"""
Captures GraphQL requests fired while a Playwright trigger runs, and picks
the "initial" / "paginated" request among them by their
fb_api_req_friendly_name - Facebook renames these across deploys, so the
matching here is by substring/keyword, not exact name.
"""

from __future__ import annotations

import re
import time
from typing import Any, Callable
from urllib.parse import parse_qsl

from patchright.sync_api import Request, Response

from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Facebook serves comment page-2+ via CommentsListComponentsPaginationQuery,
# which headless scrolling often never fires (only the root
# CommentListComponentsRootQuery shows up). The persisted doc_id still lands
# in a Relay JS chunk as "...PaginationQuery_facebookRelayOperation":"<id>".
_COMMENTS_PAGINATION_DOC_ID_RE = re.compile(
    r"(CommentsListComponentsPaginationQuery\w*)[^0-9]{0,80}(\d{15,})"
)


def capture_graphql_requests(
    page,
    trigger,
    timeout_s: float = 25.0,
    on_response: Callable[[Response], None] | None = None,
) -> list[Request]:
    """Run `trigger(page)` and collect every GraphQL request (with a doc_id)
    captured within `timeout_s` seconds - not tied to a specific query name
    since Facebook renames these frequently.

    Optional on_response is installed for the same window (used by comments
    bootstrap to scrape PaginationQuery doc_ids out of Relay JS chunks when
    Facebook never actually fires the paginated GraphQL request)."""
    captured: list[Request] = []

    def on_request(request: Request) -> None:
        if request.method != "POST" or "/api/graphql/" not in request.url:
            return
        if "doc_id=" in (request.post_data or ""):
            captured.append(request)

    page.on("request", on_request)
    if on_response is not None:
        page.on("response", on_response)
    try:
        trigger(page)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            page.wait_for_timeout(250)
    finally:
        page.remove_listener("request", on_request)
        if on_response is not None:
            page.remove_listener("response", on_response)
    return captured


def scrape_comments_pagination_doc_id(response: Response, into: dict[str, str]) -> None:
    """If this response is a JS chunk that defines CommentsListComponents
    PaginationQuery's persisted doc_id, record it on `into` keyed by the
    Relay operation name. Safe to call from a page.on('response') handler."""
    try:
        content_type = (response.headers.get("content-type") or "").lower()
        url = response.url
        if "javascript" not in content_type and not url.endswith(".js"):
            return
        if response.status != 200:
            return
        text = response.text()
    except Exception as exc:
        # Debug, not warning: this fires on every JS-chunk response this
        # page.on('response') hook sees, most of which legitimately aren't
        # the chunk being looked for - but a persistent failure to ever
        # read a body here would otherwise leave this doc_id permanently
        # uncaptured with zero trace anywhere, since nothing else calls
        # this defensively enough to notice.
        logger.debug("comments_pagination_scrape_failed", url=response.url, error=str(exc))
        return
    for match in _COMMENTS_PAGINATION_DOC_ID_RE.finditer(text):
        into[match.group(1)] = match.group(2)


def synthesize_comments_pagination(
    root_variables: dict[str, Any],
    *,
    doc_id: str,
    friendly_name: str = "CommentsListComponentsPaginationQuery",
) -> dict[str, Any]:
    """Build the Redis `pagination` block when bootstrap never captured a
    live paginated GraphQL request. Shape matches a real Comet
    CommentsListComponentsPaginationQuery body (confirmed 2026-09-16):
    commentsAfterCount=-1 asks for the densest page Facebook will return
    after the cursor (passing 10/50 still capped at ~10)."""
    template: dict[str, Any] = {
        "commentsAfterCount": -1,
        "commentsAfterCursor": None,
        "commentsBeforeCount": None,
        "commentsBeforeCursor": None,
        # Live Comet often sends null here even when the root query used a
        # REVERSE_CHRONOLOGICAL_* intent - keep null so pagination matches
        # the browser request, not the root template's sort token.
        "commentsIntentToken": None,
        "feedLocation": root_variables.get("feedLocation", "POST_PERMALINK_DIALOG"),
        "focusCommentID": root_variables.get("focusCommentID"),
        "scale": root_variables.get("scale", 2),
        "targetDialect": None,
        "useDefaultActor": root_variables.get("useDefaultActor", False),
        "id": root_variables.get("id"),
    }
    for key, value in root_variables.items():
        if key.startswith("__relay_internal__"):
            template[key] = value
    return {
        "doc_id": doc_id,
        "fb_api_req_friendly_name": friendly_name,
        "variables_template": template,
    }


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

    # Never fall back to an unrelated GraphQL name (e.g. CSExperienceStateQuery /
    # CometLogoutHandlerQuery). Saving that as the comments cache makes
    # _facebook_comments_cache_usable stay False (no pagination) while the
    # consumer still logs saved_comments_query_cache — every later comments
    # job then re-bootstraps forever. Fail loud so the operator fixes the
    # session/proxy/UI ("Không thể tải đoạn chat") instead.
    seen = [name for _, name in named]
    raise RuntimeError(
        "Did not capture a comments GraphQL query while opening the post "
        f"(saw {seen}). Facebook often shows 'Không thể tải đoạn chat' when "
        "the comments panel fails under this proxy/session - refresh in a "
        "headed browser until comments load, then re-run bootstrap."
    )


def pick_paginated_comments_request(named: list[tuple[Request, str]]) -> Request | None:
    return _pick_paginated(named, require="comment")
