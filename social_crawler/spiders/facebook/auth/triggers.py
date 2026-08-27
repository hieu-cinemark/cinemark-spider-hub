"""
Facebook-specific Playwright flows: fills and submits the login form, and
drives the search/comments pages to fire the GraphQL requests
request_capture.py listens for.
"""

from __future__ import annotations

import random
from urllib.parse import quote

import pyotp

from social_crawler.constants.facebook import (
    COOKIE_CONSENT_BUTTON_SELECTORS,
    LOGIN_BUTTON_TEXTS,
    LOGIN_EMAIL_SELECTORS,
    LOGIN_PASSWORD_SELECTORS,
    TWO_FA_CODE_SELECTORS,
    TWO_FA_CONTINUE_BUTTON_TEXTS,
)
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    click_first_by_role,
    click_first_selector,
    find_first_visible,
    human_wait,
    move_mouse_naturally,
    type_like_human,
)

logger = get_logger(__name__)


def dismiss_cookie_banner(page, timeout_ms: int = 3000) -> None:
    """Click through Facebook's cookie-consent modal if it's covering the
    page - a no-op (quick, silent) if it never shows, e.g. a reused context
    that already has a consent decision saved."""
    click_first_selector(page, COOKIE_CONSENT_BUTTON_SELECTORS, timeout_ms=timeout_ms)


def auto_login(page, account: dict) -> None:
    """Fill and submit Facebook's login form with a stored account instead of
    pausing for manual input. account["id"] is the login identifier (email/
    phone/username depending on how the account was set up)."""
    page.goto("https://www.facebook.com/login", wait_until="domcontentloaded")
    dismiss_cookie_banner(page)
    email_box = find_first_visible(page, LOGIN_EMAIL_SELECTORS, "the login email field", "debug_login", timeout_ms=15000)
    move_mouse_naturally(page, email_box)
    email_box.click()
    # Type character by character with per-keystroke jitter (like
    # search_trigger does for the search box) instead of .fill(), which
    # sets the value instantly with no key events - a much stronger
    # automation signal that makes Facebook more likely to challenge the
    # login with a checkpoint even with a correct password.
    type_like_human(email_box, account["id"])
    human_wait(page, 300, 400)
    password_box = find_first_visible(page, LOGIN_PASSWORD_SELECTORS, "the login password field", "debug_login")
    move_mouse_naturally(page, password_box)
    password_box.click()
    type_like_human(password_box, account["password"])
    human_wait(page, 400, 500)
    # Belt-and-suspenders submit: Enter works for a native form submit, but
    # this React-rendered form may swallow it without submitting - so also
    # try clicking the login control by its accessible name (matches a real
    # <button> or a <div role="button"> alike, since the current redesign
    # gives it no stable name="login"/type="submit"). Whether either one
    # actually worked is verified by the caller checking for the c_user
    # cookie afterwards, not assumed here.
    password_box.press("Enter")
    human_wait(page, 1000, 800)
    click_first_by_role(page, LOGIN_BUTTON_TEXTS)
    # Not "networkidle" - Facebook's homepage keeps background connections
    # open indefinitely (chat/notifications websocket, polling), so "0
    # network connections for 500ms" never happens and this would just hang
    # until Playwright's 30s timeout even though the page has genuinely
    # finished loading. "load" already fires once and returns immediately.
    page.wait_for_load_state("load")
    human_wait(page, 1000, 1000)

    two_fa_secret = account.get("2fa")
    if two_fa_secret:
        submit_two_factor_code(page, two_fa_secret)


def submit_two_factor_code(page, secret: str, timeout_ms: int = 6000) -> bool:
    """If Facebook is showing a 2FA code prompt after login, generate a TOTP
    code from the account's secret and submit it. Returns False (silently,
    no screenshot) if the prompt never appears - most runs reuse a session
    Facebook already trusts, so this is the common case, not an error."""
    code_box = find_first_visible(
        page, TWO_FA_CODE_SELECTORS, "the 2FA code field", "debug_2fa", timeout_ms=timeout_ms, required=False
    )
    if code_box is None:
        return False

    logger.info("two_factor_prompt_detected")
    code = pyotp.TOTP(secret).now()
    move_mouse_naturally(page, code_box)
    code_box.click()
    type_like_human(code_box, code)
    human_wait(page, 400, 400)
    click_first_by_role(page, TWO_FA_CONTINUE_BUTTON_TEXTS)
    page.wait_for_load_state("load")
    human_wait(page, 1000, 1000)
    return True


def search_trigger(query: str):
    def trigger(page):
        # Navigate straight to the search-results URL instead of typing into
        # the search box and pressing Enter. That used to work, but Facebook's
        # typeahead dropdown can now have a suggestion (a Page/Profile/Group)
        # highlighted by the time Enter is pressed, so Enter navigates to that
        # suggestion instead of submitting the search - silently skipping the
        # results GraphQL call request_capture.py needs (see
        # request_capture.py's pick_initial_request, which then fails with
        # "No search-results GraphQL request was captured"). Going straight to
        # the URL sidesteps the dropdown entirely.
        page.goto(f"https://www.facebook.com/search/top/?q={quote(query)}", wait_until="domcontentloaded")
        human_wait(page, 1500, 1000)
        # scroll down to force Facebook to fetch the next page, so we can
        # also capture a real SearchCometResultsPaginatedResultsQuery request
        # - randomized distance too, a fixed 2000px every time is its own tell
        for _ in range(4):
            page.mouse.wheel(0, random.randint(1400, 2400))
            human_wait(page, 700, 600)

    return trigger


def comments_trigger(post_url: str):
    def trigger(page):
        page.goto(post_url, wait_until="domcontentloaded")
        human_wait(page, 2000, 1000)

        page.get_by_text("Most relevant", exact=False).first.click(timeout=5000)
        human_wait(page, 600, 500)
        page.locator('div[role="menuitem"]').filter(has_text="Newest").first.click(timeout=5000)
        human_wait(page, 1500, 1000)

        reply_link = page.get_by_text("Reply", exact=True).first
        box = reply_link.bounding_box(timeout=5000)
        if box:
            page.mouse.move(box["x"], box["y"])
            for _ in range(8):
                page.mouse.wheel(0, random.randint(500, 1100))
                human_wait(page, 500, 500)

    return trigger
