"""
Threads-specific Playwright flows: fills and submits threads.com's own
native login form (at /login/ - a plain username+password form; "Continue
with Instagram" is offered there too but isn't used here, see auto_login's
docstring for why), and drives the search page to fire the GraphQL requests
request_capture.py listens for. Generic mouse/typing/selector-fallback
helpers come straight from the Facebook auth package - none of that code is
Facebook-specific, see its own docstring.
"""

from __future__ import annotations

import random

import pyotp

from social_crawler.constants.threads import (
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
    click_first_selector(page, COOKIE_CONSENT_BUTTON_SELECTORS, timeout_ms=timeout_ms)


def auto_login(page, account: dict) -> None:
    """Fill and submit threads.com's own login form with a stored account
    instead of pausing for manual input. account["id"] is the login
    identifier (email/phone/username).

    Logging in here directly - instead of at instagram.com and then
    bridging over - only works for an account that has already joined
    Threads (picked a username, etc.) at least once before, e.g. by hand
    through the "Continue with Instagram" prompt. A brand-new Instagram
    account that has never touched Threads has no threads.com login of its
    own yet and needs that one-time join step done first; this function
    doesn't attempt it. Confirmed against a real run: after joining once,
    this native login sets ds_user_id/sessionid on the .threads.com domain
    immediately, with no instagram.com round trip needed at all."""
    page.goto("https://www.threads.com/login/", wait_until="domcontentloaded")
    dismiss_cookie_banner(page)
    email_box = find_first_visible(
        page, LOGIN_EMAIL_SELECTORS, "the login username field", "debug_login", timeout_ms=15000
    )
    move_mouse_naturally(page, email_box)
    email_box.click()
    type_like_human(email_box, account["id"])
    human_wait(page, 300, 400)
    password_box = find_first_visible(page, LOGIN_PASSWORD_SELECTORS, "the login password field", "debug_login")
    move_mouse_naturally(page, password_box)
    password_box.click()
    type_like_human(password_box, account["password"])
    human_wait(page, 400, 500)
    password_box.press("Enter")
    page.wait_for_load_state("load")
    human_wait(page, 1500, 1000)
    click_first_by_role(page, LOGIN_BUTTON_TEXTS)

    two_fa_secret = account.get("2fa")
    if two_fa_secret:
        submit_two_factor_code(page, two_fa_secret)

    page.wait_for_timeout(4000)


def submit_two_factor_code(page, secret: str, timeout_ms: int = 8000) -> bool:
    """If threads.com is showing a 2FA code prompt after login, generate a
    TOTP code from the account's secret and submit it. Returns False
    (silently, no screenshot) if the prompt never appears - most runs reuse
    a session Threads already trusts, so this is the common case, not an
    error.

    Unlike Instagram's own login page, threads.com/login/ keeps the
    username field mounted behind the 2FA modal, so a bare
    input[type="text"] selector matches two elements - the code field is
    targeted by its own placeholder text instead (see
    TWO_FA_CODE_SELECTORS)."""
    code_box = find_first_visible(page, TWO_FA_CODE_SELECTORS, "the 2FA code field", "debug_2fa", timeout_ms=timeout_ms, required=False)
    if code_box is None:
        # Placeholder text changed/translated differently than expected -
        # fall back to finding whichever input[type="text"] is still empty
        # (both the stale username field and the code field match the bare
        # selector, but only the code field starts blank).
        text_inputs = page.locator('input[type="text"]')
        try:
            text_inputs.first.wait_for(state="visible", timeout=2000)
        except Exception:
            return False
        for i in range(text_inputs.count()):
            candidate = text_inputs.nth(i)
            if candidate.input_value() == "":
                code_box = candidate
                break
    if code_box is None:
        logger.warning("two_factor_prompt_detected_but_no_empty_input_found")
        return False

    logger.info("two_factor_prompt_detected")
    code = pyotp.TOTP(secret).now()
    # force=True: an animating modal overlay routinely intercepts pointer
    # events on this field for the first ~1-2s it's visible (confirmed
    # against a real run) - a plain click times out waiting for that to
    # settle even though the field itself is already interactable.
    code_box.click(force=True)
    type_like_human(code_box, code)
    human_wait(page, 400, 400)
    click_first_by_role(page, TWO_FA_CONTINUE_BUTTON_TEXTS)
    page.wait_for_timeout(6000)
    return True


def search_trigger(query: str):
    def trigger(page):
        # Navigate to the bare search page and type into the search box
        # (rather than a direct deep link to /search?q=...) - confirmed
        # against real runs that typing + Enter is what actually fires the
        # GraphQL search-results request; a direct deep-linked URL alone
        # rendered a blank page with no request captured.
        page.goto("https://www.threads.com/search", wait_until="domcontentloaded")
        human_wait(page, 1500, 1000)
        search_input = page.locator('input[type="search"]').first
        search_input.wait_for(state="visible", timeout=15000)
        # force=True: same animating-overlay issue as the 2FA field above.
        search_input.click(force=True)
        type_like_human(search_input, query)
        human_wait(page, 1000, 500)
        search_input.press("Enter")
        # Give the initial results list time to fully mount before
        # scrolling - scrolling too early lands inside content that's
        # already loaded and never reaches the "fetch more" threshold, so
        # the paginated BarcelonaSearchResultsRefetchableQuery request never
        # fires at all (confirmed: this was captured in one run and missing
        # in another with the same code, the only difference being timing).
        page.wait_for_timeout(4000)
        # Scroll further and with longer pauses than before for the same
        # reason - the fetch-more trigger needs to actually reach near the
        # bottom of the currently-loaded list, not just move partway down it.
        for _ in range(6):
            page.mouse.wheel(0, random.randint(2500, 4000))
            page.wait_for_timeout(1500)

    return trigger
