"""
Generic Playwright interaction helpers shared by every flow in this
package (login, search trigger, comments trigger, ...) - none of these
know anything about Facebook's specific DOM, just how to look/act less like
an automated browser while using one.
"""

from __future__ import annotations

import random
from pathlib import Path

# Only used for local debugging artifacts (a screenshot), never for cache data.
BASE_DIR = Path(__file__).resolve().parent

# Overrides navigator.webdriver, the single most common automation signal
# bot-detection systems check first - Playwright's default Chromium exposes
# it as true on every page otherwise. Applied to every fresh context (login
# or plain capture), not just auto-login, since it's cheap and harmless.
_STEALTH_INIT_SCRIPT = "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"


def new_context(browser, **kwargs):
    """browser.new_context() plus a plausible desktop VN fingerprint (locale/
    timezone/viewport instead of Playwright's blank defaults) and the
    navigator.webdriver patch above - used for every context this package
    creates so login and headless capture alike look like an ordinary
    browser, not automation."""
    context = browser.new_context(
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
        viewport={"width": 1366, "height": 768},
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
