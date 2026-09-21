"""Shared Playwright helpers for spiders that drive a real browser to
trigger the next batch of an infinite-scroll feed (TikTok's hashtag_search/
search/comments, Threads' reply-pagination) - each one needs the same fix
for the same underlying issue (see scroll_feed_to_bottom)."""

from __future__ import annotations

from social_crawler.logger import get_logger

logger = get_logger(__name__)

_FIND_SCROLL_CONTAINER_JS = """() => {
    let best = null, bestScore = 0;
    for (const el of document.querySelectorAll('*')) {
        // documentElement/body are excluded, not just deprioritized - their
        // clientHeight is the full viewport height by definition, so
        // whenever the page's own overall height overflows the viewport by
        // 200px+ (true of nearly every real page, just from header/footer
        // chrome), one of these two would otherwise beat any *nested*
        // content container - which is normally shorter, having a header/
        // nav carved out of it already - purely on being outermost, not on
        // being the actual feed. Confirmed live (2026-09-15, TikTok
        // hashtag_search): this picked <html> (clientHeight == the launched
        // viewport's own 900px) over the page's real feed region, so every
        // scroll_feed_to_bottom() call scrolled the wrong element and the
        // capture loop stalled out after its first (often only) page -
        // same root cause the module docstring already ruled out window/
        // body scrolling for, just re-appearing one level up here.
        if (el === document.documentElement || el === document.body) continue;
        const sh = el.scrollHeight, ch = el.clientHeight;
        if (sh > ch + 200 && ch > 300 && ch > bestScore) {
            bestScore = ch;
            best = el;
        }
    }
    if (!best) return false;
    best.scrollTop = best.scrollHeight;
    return true;
}"""


def scroll_feed_to_bottom(page, item_selector: str | None = None) -> None:
    """Tries, in order, every way this project has confirmed can actually
    move a stuck TikTok/Threads feed - stacked rather than picked between,
    since each only sometimes applies and none has been proven sufficient
    on its own (see the three tiers below, each confirmed by direct live
    testing on 2026-09-15 against TikTok hashtag_search):

    1. Re-finds whatever element is currently the page's own nested
       feed/results scroll container and sets its scrollTop to the bottom.
       documentElement/body are excluded from candidacy, not just
       deprioritized - their clientHeight is the full viewport height by
       definition, so whenever the page's own overall height overflows the
       viewport by 200px+ (true of nearly any real page, just from header/
       footer chrome) one of these two would otherwise beat any *nested*
       container - which is normally shorter, having a header/nav carved
       out of it - purely on being outermost, not on being the actual feed.
       Confirmed live: this exact bug picked <html> over the real feed
       region, stalling capture after its first (often only) page. Re-finds
       the container on every call rather than caching it once, since which
       element qualifies can change as more of the feed mounts - confirmed
       live to go from "a real nested container exists" to "none does" the
       moment the page's own loading skeleton unmounts and real content
       replaces it.

    2. item_selector (opt-in per caller, e.g. "a[href*='/video/']" for
       TikTok - a stable link pattern that survives a markup/CSS redesign):
       once (1) finds nothing, scrolls the *last* matching already-rendered
       feed item into view via Playwright's own scroll_into_view_if_needed,
       which resolves whatever the real scroll chain is instead of this
       function guessing at it. None (the default) skips this tier - every
       call site from before it existed keeps its old behavior.

    3. A real, correctly-*positioned* mouse wheel at the viewport's own
       center. Easy to misjudge as useless: page.mouse.wheel() fires
       wherever the virtual mouse currently sits, which defaults to (0, 0)
       (top-left corner, e.g. over the navbar) until something moves it -
       confirmed live that firing it from there left window.scrollY pinned
       at 0 even though documentElement carried ~2000px of real, measurable
       overflow, but repositioning the mouse over the actual feed first let
       the *same* wheel call move window.scrollY normally, all the way to
       that overflow's real max. Kept as a last resort, not a fix on its
       own: also confirmed live that reaching max scroll this way didn't by
       itself make a stalled feed (hasMore=true, but not fetching) resume -
       whatever TikTok's own trigger for that is, it's not simply "the user
       reached the bottom." Failing quietly here (caught, not raised) is
       deliberate - each caller's own stall-guard already decides what
       "scrolling isn't producing new pages" means for it, regardless of
       which of these three a given page turns out to need.
    """
    try:
        moved = page.evaluate(_FIND_SCROLL_CONTAINER_JS)
    except Exception as exc:
        logger.warning("scroll_to_bottom_failed", error=str(exc))
        moved = False

    if moved:
        return

    if item_selector:
        try:
            items = page.locator(item_selector)
            count = items.count()
            if count:
                items.nth(count - 1).scroll_into_view_if_needed(timeout=5000)
        except Exception as exc:
            logger.warning("scroll_into_view_fallback_failed", error=str(exc), item_selector=item_selector)

    try:
        viewport = page.viewport_size or {"width": 1366, "height": 900}
        page.mouse.move(viewport["width"] / 2, viewport["height"] / 2)
        page.mouse.wheel(0, viewport["height"] * 1.5)
    except Exception as exc:
        logger.warning("wheel_fallback_failed", error=str(exc))
