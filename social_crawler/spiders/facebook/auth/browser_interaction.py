"""
Generic Playwright interaction helpers shared by every flow in this
package (login, search trigger, comments trigger, ...) - none of these
know anything about Facebook's specific DOM, just how to look/act less like
an automated browser while using one.
"""

from __future__ import annotations

import hashlib
import random
from pathlib import Path

from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Only used for local debugging artifacts (a screenshot), never for cache data.
BASE_DIR = Path(__file__).resolve().parent

# Every currently-visible element click_via_ai_fallback considers a
# candidate - deliberately the same broad net as click_first_via_js's own
# role targets plus a bare [aria-label], since an icon-only control (the
# exact case that motivated this - see triggers.py's Reels comment-button
# fix) often carries no ARIA role at all beyond its aria-label.
_AI_FALLBACK_CANDIDATE_SELECTOR = '[role="button"], [role="link"], [role="menuitem"], [aria-label]'
# () => {...} snapshot: role/aria-label/visible-text only for every visible
# match, deliberately excluding anything with neither - Kira has nothing
# useful to judge relevance from an element with no accessible name or text
# at all, and including it would just burn tokens on the (large, free-tier
# rate-limited) prompt for no benefit.
_AI_FALLBACK_SNAPSHOT_JS = f"""
    () => {{
        const nodes = document.querySelectorAll('{_AI_FALLBACK_CANDIDATE_SELECTOR}');
        const out = [];
        for (const el of nodes) {{
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) continue;
            const label = el.getAttribute('aria-label') || '';
            const text = (el.innerText || '').trim().slice(0, 40);
            if (!label && !text) continue;
            out.push({{role: el.getAttribute('role') || el.tagName.toLowerCase(), label, text}});
        }}
        return out;
    }}
"""

# Overrides navigator.webdriver, the single most common automation signal
# bot-detection systems check first - Playwright's default Chromium exposes
# it as true on every page otherwise. Applied to every fresh context (login
# or plain capture), not just auto-login, since it's cheap and harmless.
_STEALTH_INIT_SCRIPT = "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"

# A small pool of common desktop viewport sizes - every account logging in
# with the exact same 1366x768 fingerprint is itself a shared-fingerprint
# tell across what's supposed to look like unrelated real users.
_VIEWPORT_POOL = (
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1600, "height": 900},
    {"width": 1920, "height": 1080},
)


def _viewport_for(account_key: str | None) -> dict:
    """Deterministic per-account pick from _VIEWPORT_POOL, not a fresh
    random one every run - Facebook trusts a *consistent* device fingerprint
    session to session more than it trusts any particular resolution, so an
    account's viewport shouldn't jitter between bootstraps the way scroll
    distance/pacing should. No account_key (manual/anonymous flows) falls
    back to the original fixed size."""
    if not account_key:
        return _VIEWPORT_POOL[0]
    digest = hashlib.sha256(account_key.encode()).hexdigest()
    return _VIEWPORT_POOL[int(digest, 16) % len(_VIEWPORT_POOL)]


def new_context(browser, account_key: str | None = None, **kwargs):
    """browser.new_context() plus a plausible desktop VN fingerprint (locale/
    timezone/viewport instead of Playwright's blank defaults) and the
    navigator.webdriver patch above - used for every context this package
    creates so login and headless capture alike look like an ordinary
    browser, not automation. Pass account_key so the viewport is stable for
    that account across runs (see _viewport_for) instead of every account
    sharing the one hardcoded size."""
    context = browser.new_context(
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
        viewport=_viewport_for(account_key),
        **kwargs,
    )
    context.add_init_script(_STEALTH_INIT_SCRIPT)
    return context


def find_first_visible(
    page, selectors: tuple[str, ...], label: str, debug_name: str, timeout_ms: int = 4000, required: bool = True
):
    """Try each selector in order until one matches a visible element -
    Facebook changes its UI/language/markup frequently (login form ids are
    now React-generated at runtime, e.g. "_r_2_", not stable), so a single
    hardcoded selector breaks easily. Screenshots and raises on total
    failure, unless required=False - use that for an opportunistic check
    (e.g. "is this optional screen showing?") where not finding anything is
    an expected, silent outcome rather than an error worth a screenshot."""
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=timeout_ms)
            return locator
        except Exception:
            continue
    if not required:
        return None
    debug_path = BASE_DIR / f"{debug_name}.png"
    page.screenshot(path=str(debug_path))
    raise RuntimeError(
        f"Could not find {label} (Facebook may have changed its UI, shown a cookie-consent/checkpoint "
        f"screen, or a locale-specific variant - proxy/IP geolocation can trigger this). "
        f"Landed on: {page.url!r}. Saved a screenshot to {debug_path} for inspection."
    )


def click_first(locators, timeout_ms: int = 2000, force: bool = False) -> bool:
    """Try clicking each locator in order until one succeeds - Facebook
    changes its button text/markup across deploys/locales, so a single
    hardcoded locator is fragile. Returns whether any click succeeded; never
    raises - a control that's simply not showing (e.g. no cookie banner,
    no 2FA prompt) is an expected outcome here, not an error.

    force=True skips Playwright's actionability checks (visible/stable/not-
    obscured) - only pass it for a locator already confirmed to resolve to
    the right element by role+accessible name, where the sole reason a
    normal click fails is an unrelated overlay covering that exact screen
    position (see triggers.py's Reels comment-button fix for the case that
    motivated this)."""
    for locator in locators:
        try:
            locator.first.click(timeout=timeout_ms, force=force)
            return True
        except Exception:
            continue
    return False


def click_first_via_js(locators, timeout_ms: int = 3000) -> bool:
    """Like click_first, but dispatches the click by calling .click()
    directly on the resolved DOM element instead of a normal Playwright
    mouse click - a real mouse click (even with force=True, which only
    skips Playwright's own pre-click checks) still goes through the
    browser's actual hit-testing at the element's on-screen coordinates,
    so an unrelated overlay genuinely covering that pixel (confirmed live:
    a Facebook Reels view's Messenger chat-widget error card sitting on
    top of the action rail) swallows the click no matter what Playwright
    options are set. Calling .click() on the element itself skips
    hit-testing entirely - Facebook's React handler still receives it
    (delegated listeners key off the event's real target, not screen
    position), confirmed live to actually open the reel's comments panel
    where force=True did not."""
    for locator in locators:
        try:
            locator.first.wait_for(state="visible", timeout=timeout_ms)
            locator.first.evaluate("el => el.click()")
            return True
        except Exception:
            continue
    return False


def click_via_ai_fallback(page, goal: str, max_candidates: int = 40) -> bool:
    """Last-resort click for when every hardcoded selector strategy for one
    UI interaction has already failed - see services/kira.py's
    suggest_element_index for the full rationale (this exists specifically
    to cut down on hand-fixing selectors every time Facebook's DOM shifts).
    Snapshots every currently-visible interactive element's role/
    accessible-name/text, asks Kira which one matches `goal`, then clicks
    that EXACT element by re-running the identical filter/order and
    indexing into it - Kira only ever picks from a list of elements that
    already, verifiably exist; it never sees or invents a selector, so a
    wrong pick can only mean "clicked the wrong real thing", never "clicked
    something that doesn't exist" or crashed on bad markup Kira imagined.

    Returns False (never raises) on any failure - Kira unconfigured, rate-
    limited, said no candidate matches, or the DOM changed between the two
    snapshots - so callers keep their own existing error/screenshot path
    for when this also comes back empty, same as before this existed."""
    from social_crawler.services.kira import suggest_element_index

    elements = page.evaluate(_AI_FALLBACK_SNAPSHOT_JS)[:max_candidates]
    if not elements:
        return False
    descriptions = [f"role={e['role']} aria-label={e['label']!r} text={e['text']!r}" for e in elements]
    index = suggest_element_index(goal, descriptions)
    if index is None:
        return False
    try:
        clicked = page.evaluate(
            f"""
            (i) => {{
                const nodes = document.querySelectorAll('{_AI_FALLBACK_CANDIDATE_SELECTOR}');
                const visible = [];
                for (const el of nodes) {{
                    const rect = el.getBoundingClientRect();
                    if (rect.width === 0 || rect.height === 0) continue;
                    if (!el.getAttribute('aria-label') && !el.innerText.trim()) continue;
                    visible.push(el);
                }}
                if (i >= visible.length) return false;
                visible[i].click();
                return true;
            }}
            """,
            index,
        )
    except Exception as exc:
        logger.warning("ai_selector_fallback_click_failed", goal=goal, error=str(exc))
        return False
    if clicked:
        logger.warning("ai_selector_fallback_used", goal=goal, chosen=descriptions[index])
    return bool(clicked)


def click_first_selector(page, selectors: tuple[str, ...], timeout_ms: int = 3000) -> bool:
    return click_first((page.locator(selector) for selector in selectors), timeout_ms=timeout_ms)


def click_first_by_role(page, texts: tuple[str, ...], role: str = "button", timeout_ms: int = 2000) -> bool:
    return click_first((page.get_by_role(role, name=text) for text in texts), timeout_ms=timeout_ms)


def human_wait(page, base_ms: int, jitter_ms: int) -> None:
    """Wait base_ms plus a random extra up to jitter_ms - same idea as
    MIN_REQUEST_INTERVAL_SECONDS/REQUEST_INTERVAL_JITTER_SECONDS in
    graphql_client.py: a perfectly uniform pause between actions is itself a
    bot-like signal, so every gap between Playwright actions should vary
    instead of being the exact same fixed number every run."""
    page.wait_for_timeout(base_ms + random.randint(0, jitter_ms))


def type_like_human(locator, text: str, min_delay_ms: int = 40, max_delay_ms: int = 180) -> None:
    """Type one character at a time with an independently randomized delay
    before each keystroke. press_sequentially()'s own `delay` applies a
    single fixed value to every character in the string, which is itself a
    detectable rhythm (real typing speed varies key to key) - this reproduces
    that variance by calling it once per character instead of once per string."""
    for ch in text:
        locator.press_sequentially(ch, delay=random.randint(min_delay_ms, max_delay_ms))


def natural_scroll(
    page,
    min_scrolls: int,
    max_scrolls: int,
    min_px: int,
    max_px: int,
    pause_base_ms: int,
    pause_jitter_ms: int,
    backscroll_chance: float = 0.15,
) -> None:
    """Scroll down a randomized number of times, each a randomized distance
    and pause, with an occasional short scroll back up thrown in - a fixed
    "scroll N times by exactly X px" loop is its own detectable rhythm (real
    scroll-wheel/trackpad input varies count, distance and pace, and
    sometimes overshoots and corrects). min_scrolls/min_px are floors, not
    just flavor - callers that need a minimum amount of scrolling to trigger
    a pagination fetch (see search_trigger/comments_trigger) should keep
    them at least as high as what's already confirmed to work."""
    for _ in range(random.randint(min_scrolls, max_scrolls)):
        page.mouse.wheel(0, random.randint(min_px, max_px))
        human_wait(page, pause_base_ms, pause_jitter_ms)
        if random.random() < backscroll_chance:
            page.mouse.wheel(0, -random.randint(150, 400))
            human_wait(page, 300, 400)


def move_mouse_naturally(page, locator) -> None:
    """Move the cursor toward `locator` in two hops with a short pause
    between them, instead of letting .click() teleport it straight to the
    element's center in one instant jump - a real cursor approaches from
    wherever it already was, not from nowhere. Silently does nothing if the
    element has no bounding box yet (not worth failing the whole action over)."""
    box = locator.bounding_box()
    if not box:
        return
    target_x = box["x"] + box["width"] / 2
    target_y = box["y"] + box["height"] / 2
    page.mouse.move(target_x + random.uniform(-150, 150), target_y + random.uniform(-100, 100))
    page.wait_for_timeout(random.randint(80, 220))
    page.mouse.move(target_x, target_y, steps=random.randint(5, 15))
