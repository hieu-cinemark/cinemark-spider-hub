"""Drives a real Patchright browser through a post's permalink and scrolls,
trying to trigger DirectRepliesRefetchQuery. Parked: threads_comments now
GETs /api/v1/text_feed/<id>/replies/ instead (see comments.py). Left here
because a curl_cffi replay of the GraphQL refetch still comes back
direct_replies: null, and a cold permalink load never fires that query
(logged-out BarcelonaPermalinkMobilePostColumnRoute uses /ajax/bz).

Confirmed by direct debugging (2026-09-15) why this currently still comes
back empty even on posts with 1000+ real replies: a cold page.goto() straight
to a post's permalink gets served Threads' own logged-out/public
route (comet.barcelonawebloggedout.BarcelonaPermalinkMobilePostColumnRoute -
visible in every ajax request's own __crn param), regardless of this
account's cookies being valid - this is Threads serving the
publicly-shareable, SEO/preview-friendly variant of a permalink, not a
broken session (real post content, including replies, does render on this
route - the reply pagination-by-scroll fix below does load more of them
into the DOM, confirmed by a real capture growing from 51 to 75 rendered
replies while scrolling). That logged-out route's own "load more replies"
mechanism turns out to be Meta's older BigPipe-style partial-page endpoint
(/ajax/bz, GET) instead of a GraphQL doc - it never fires a
DirectRepliesRefetchQuery POST at all, so there is nothing for this
function's response listener to ever capture no matter how correctly it
scrolls. DirectRepliesRefetchQuery is real (confirmed present in Threads'
own client bundle) but only ever gets used by the fully-hydrated, logged-in
SPA experience - reached by navigating within the app while already
authenticated (e.g. clicking a post from an authenticated feed/search
results view), not by a fresh page.goto() straight to the permalink URL.
Fixing this for real needs that different navigation shape, not another
scroll/selector tweak - not implemented yet.

Same "needs a real browser, not a signed replay" class of problem as
TikTok channel video capture (spiders/tiktok/features/channel_videos),
just a layer deeper here (a real browser gets real *content*, but not
necessarily the specific network calls this was built around).

Reuses threads/auth/bootstrap.py's own account rotation/login/proxy
machinery (_get_authenticated_context) instead of building a separate one
here - same account pool, same pinned-proxy/storage_state reuse every other
Threads browser flow (search bootstrap, open_browser.py) already goes
through, so this doesn't need its own pre-bootstrapped cache the way the
old curl_cffi comments path did (see crawl_request_consumer.py, which no
longer lists "threads" in COMMENTS_PLATFORMS_NEEDING_CACHE for this
reason - comparable to TikTok's comments spider being absent from that set
too, for the same "fully self-contained per crawl" reason)."""

from __future__ import annotations

import json
from typing import Any

from patchright.sync_api import sync_playwright

from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.browser_utils import scroll_feed_to_bottom
from social_crawler.spiders.threads.auth.bootstrap import _get_authenticated_context
from social_crawler.spiders.threads.auth.request_capture import THREADS_GRAPHQL_URL_MARKERS
from social_crawler.spiders.threads.auth.triggers import comments_trigger
from social_crawler.spiders.threads.features.comments.extract import find_direct_replies_in_json

logger = get_logger(__name__)

# Consecutive scrolls with no new reply page mean this post's replies are
# exhausted (or Threads' UI stopped responding to scrolling), not a
# transient hiccup worth waiting out.
_MAX_STALL_SCROLLS = 5


def capture_reply_pages(post_url: str, max_pages: int = 20) -> list[dict[str, Any]]:
    """Every DirectRepliesRefetchQuery response body captured while
    scrolling `post_url`'s permalink, in scroll order (page 1 first) - each
    one is a raw parsed JSON object, not yet reduced to reply dicts (see
    extract.extract_replies_from_json, called once per page by the
    caller). Currently always empty - see this module's own docstring for
    why (a cold permalink load never fires this query at all, not a
    per-post or per-account fluke)."""
    pages: list[dict[str, Any]] = []
    checkpointed = False
    redis_cache = RedisCache()

    with sync_playwright() as pw:
        browser, context, page, account_key, account = _get_authenticated_context(pw, redis_cache, headless=True)

        def on_response(resp):
            nonlocal checkpointed
            if resp.request.method != "POST" or not any(marker in resp.url for marker in THREADS_GRAPHQL_URL_MARKERS):
                return
            try:
                body = resp.text()
            except Exception as exc:
                logger.warning("replies_response_read_failed", error=str(exc))
                return
            if not body:
                return
            # Confirmed against a real capture: every GraphQL POST for a
            # checkpointed session comes back as this exact tiny body
            # (status 200, not 401/403 - the HTTP layer looks fine) instead
            # of the query's real shape, on *every* query the page fires,
            # not just this one - a same-session-wide condition, not a
            # per-query fluke worth retrying.
            if '"checkpoint_required"' in body:
                checkpointed = True
                return
            # A single response can newline-delimit more than one JSON
            # object (Relay's @stream-annotated queries do this) - parse
            # line by line rather than assuming exactly one document, same
            # defensive shape as extract.py's own __bbox walk over HTML.
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if find_direct_replies_in_json(data) is not None:
                    pages.append(data)

        page.on("response", on_response)
        try:
            try:
                comments_trigger(post_url)(page)
            except Exception as exc:
                # A broken selector/navigation timeout must not crash the
                # whole spider - indistinguishable from "this account
                # captured zero pages" to the caller, which already retries
                # with a different account for that (see comments.py).
                logger.warning("comments_trigger_failed", post_url=post_url, error=str(exc))
                return pages

            stalled_scrolls = 0
            while len(pages) < max_pages and stalled_scrolls < _MAX_STALL_SCROLLS:
                before = len(pages)
                # Not page.mouse.wheel() - confirmed by direct testing (see
                # scroll_feed_to_bottom's own docstring, first found on
                # TikTok's hashtag feed) that a simulated mouse-wheel event
                # at a fixed screen position doesn't move this page's real
                # scroll container at all (a big inner div, not window/body -
                # confirmed by direct inspection: window.scrollY stays 0
                # throughout). This *does* load more of the DOM's rendered
                # replies as you scroll (confirmed: 51 -> 75 in one capture)
                # - it just doesn't make this specific query fire, see this
                # module's own docstring for why.
                scroll_feed_to_bottom(page)
                page.wait_for_timeout(1500)
                if len(pages) == before:
                    stalled_scrolls += 1
                else:
                    stalled_scrolls = 0
        finally:
            page.remove_listener("response", on_response)
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()

    if checkpointed and not pages:
        # Same treatment as facebook/auth/bootstrap.py's own
        # account_disabled_logged_out_mid_session: a session that looked
        # valid enough to skip a fresh login (need_login=False) can still
        # be dead server-side, discovered only once a real request goes
        # out. Disabling now (instead of leaving it claimed for the next
        # rotation to hit the same wall) is what actually matters here -
        # `pages` being empty for a genuinely-exhausted post is the normal,
        # non-broken case this must not be confused with, which is why this
        # only fires when *both* conditions hold.
        reason = f"Account {account_key!r} is checkpointed (every GraphQL response came back checkpoint_required)."
        if account is not None:
            pool.release_account("threads", account["id"], success=False, hard_failure=True, reason=reason)
            logger.error("account_disabled_checkpointed", telegram=True, platform="threads", account=account_key)

    return pages
