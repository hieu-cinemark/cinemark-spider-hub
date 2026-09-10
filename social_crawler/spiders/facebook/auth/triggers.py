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
    COMMENT_OPEN_BUTTON_TEXTS,
    COMMENT_REPLY_TEXTS,
    COMMENT_SORT_NEWEST_TEXTS,
    COMMENT_SORT_TRIGGER_TEXTS,
    COOKIE_CONSENT_BUTTON_SELECTORS,
    LOGIN_BUTTON_TEXTS,
    LOGIN_EMAIL_SELECTORS,
    LOGIN_PASSWORD_SELECTORS,
    TWO_FA_CODE_SELECTORS,
    TWO_FA_CONTINUE_BUTTON_TEXTS,
    TWO_FA_PROMPT_TEXT_HINTS,
)
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    BASE_DIR,
    click_first,
    click_first_by_role,
    click_first_selector,
    find_first_visible,
    human_wait,
    move_mouse_naturally,
    type_like_human,
)

logger = get_logger(__name__)


class MissingTotpSecretError(RuntimeError):
    """Facebook showed a 2FA code prompt for an account whose
    platform_accounts row has no totp_secret ('2fa' column) - a config gap
    (nobody has ever captured this account's authenticator-app secret), not
    evidence the account itself is checkpointed/banned. bootstrap.py must
    not disable_account() on this the way it does for a real "no c_user for
    any other reason" failure - confirmed the hard way: an account with a
    perfectly fine password got auto-disabled because its 2FA code field
    was simply never filled in (no secret to fill it with), which then
    surfaced identically to a genuine checkpoint."""


class TwoFactorPromptNotHandledError(RuntimeError):
    """Facebook's page text matched a known 2FA-prompt hint (see
    TWO_FA_PROMPT_TEXT_HINTS) but no known selector/locator could find the
    actual code input - almost certainly Facebook shipped yet another
    markup variant for this screen (confirmed happening for real: a
    completely redesigned 2FA card UI where none of TWO_FA_CODE_SELECTORS
    matched, so the code field was silently never filled, Continue stayed
    disabled, login "failed" with no c_user cookie, and the caller's
    generic failure path then disabled a perfectly good account thinking
    it was a real checkpoint - same class of misdiagnosis
    MissingTotpSecretError already guards against for the "no secret"
    case). bootstrap.py must NOT disable_account() on this either - a
    human needs to inspect the saved screenshot and update the selectors/
    locators, not punish the account."""


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
    email_box = find_first_visible(
        page, LOGIN_EMAIL_SELECTORS, "the login email field", "debug_login", timeout_ms=15000
    )
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

    submit_two_factor_code(page, account.get("2fa"))


def submit_two_factor_code(page, secret: str | None, timeout_ms: int = 6000) -> bool:
    """If Facebook is showing a 2FA code prompt after login, generate a TOTP
    code from the account's secret and submit it. Returns False (silently,
    no screenshot) if the prompt never appears - most runs reuse a session
    Facebook already trusts, so this is the common case, not an error.

    Always checks for the prompt even when `secret` is falsy (rather than
    the caller skipping this function entirely, as before) - raises
    MissingTotpSecretError if the prompt DOES appear with nothing to fill
    it with, so that case surfaces distinctly instead of silently falling
    through to a bare "no c_user" a few lines later, indistinguishable
    from a real checkpoint."""
    code_box = find_first_visible(
        page, TWO_FA_CODE_SELECTORS, "the 2FA code field", "debug_2fa", timeout_ms=timeout_ms, required=False
    )
    if code_box is None:
        # TWO_FA_CODE_SELECTORS is a fixed CSS list - Facebook has already
        # shipped at least one 2FA markup variant it didn't match (a
        # floating-label input with no matching name/autocomplete/
        # aria-label/placeholder among those selectors). get_by_label/
        # get_by_placeholder resolve the accessible name however it's
        # actually wired (associated <label>, aria-labelledby, placeholder,
        # ...) instead of guessing one more fixed attribute to add.
        for locator_factory in (
            lambda: page.get_by_label("Code", exact=False),
            lambda: page.get_by_label("Mã", exact=False),
            lambda: page.get_by_placeholder("Code", exact=False),
            lambda: page.get_by_placeholder("Mã", exact=False),
        ):
            try:
                candidate = locator_factory().first
                candidate.wait_for(state="visible", timeout=2000)
                code_box = candidate
                break
            except Exception:
                continue

    if code_box is None:
        page_text = page.inner_text("body").lower()
        if any(hint in page_text for hint in TWO_FA_PROMPT_TEXT_HINTS):
            debug_path = BASE_DIR / "debug_2fa_prompt_not_handled.png"
            page.screenshot(path=str(debug_path))
            raise TwoFactorPromptNotHandledError(
                "Facebook's page text matches a known 2FA-prompt hint, but no known selector/locator "
                f"could find the code input - Facebook likely changed this screen's markup again. "
                f"Saved a screenshot to {debug_path} for inspection."
            )
        return False

    logger.info("two_factor_prompt_detected")
    if not secret:
        raise MissingTotpSecretError(
            "Facebook is asking for a 2FA code but this account has no totp_secret configured "
            "(platform_accounts.totp_secret) - set up (or re-view) Facebook's own Security Settings "
            "> Two-Factor Authentication (authenticator app) for this account and save the secret "
            "key shown there, then retry."
        )
    code = pyotp.TOTP(secret).now()
    move_mouse_naturally(page, code_box)
    code_box.click()
    type_like_human(code_box, code)
    human_wait(page, 400, 400)
    click_first_by_role(page, TWO_FA_CONTINUE_BUTTON_TEXTS)
    # Not wait_for_load_state("load"): confirmed against a real run that
    # this redesigned 2FA card is an in-page SPA transition (Continue's own
    # spinner, no full navigation/load event) - "load" resolves instantly
    # since the document never reloads, so the caller's c_user check ran
    # while Facebook was still verifying the code server-side and a
    # perfectly valid login got misread as failed. Poll for the code
    # field to actually disappear (proof the verification step moved on)
    # instead of guessing a fixed delay is long enough.
    try:
        code_box.wait_for(state="hidden", timeout=15000)
    except Exception:
        logger.warning("two_factor_code_field_still_visible_after_submit", timeout_ms=15000)
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

        # A /videos/ URL (Video Home player) doesn't show the comment list
        # at all until this is clicked - a normal post permalink already
        # has comments open, so this is best-effort (click_first swallows
        # "found nothing" silently, same as dismiss_cookie_banner) rather
        # than required.
        if click_first((page.get_by_text(t, exact=False) for t in COMMENT_OPEN_BUTTON_TEXTS), timeout_ms=3000):
            human_wait(page, 1200, 800)

        # Every context this project creates is locale="vi-VN" (see
        # browser_interaction.new_context), so Facebook renders this UI in
        # Vietnamese ("Phù hợp nhất"/"Mới nhất"/"Phản hồi") - a fixed
        # English-only string here just times out and never fires the
        # comments GraphQL request at all (confirmed happening for real).
        if not click_first(
            (page.get_by_text(t, exact=False) for t in COMMENT_SORT_TRIGGER_TEXTS), timeout_ms=5000
        ):
            debug_path = BASE_DIR / "debug_comments_sort_trigger_not_found.png"
            page.screenshot(path=str(debug_path))
            raise RuntimeError(
                f"Could not find the comment-sort control (tried {COMMENT_SORT_TRIGGER_TEXTS}) - "
                f"Facebook may have changed this UI. Saved a screenshot to {debug_path}."
            )
        human_wait(page, 600, 500)
        if not click_first(
            (page.locator('div[role="menuitem"]').filter(has_text=t) for t in COMMENT_SORT_NEWEST_TEXTS),
            timeout_ms=5000,
        ):
            debug_path = BASE_DIR / "debug_comments_sort_newest_not_found.png"
            page.screenshot(path=str(debug_path))
            raise RuntimeError(
                f"Could not find the 'Newest' sort menu item (tried {COMMENT_SORT_NEWEST_TEXTS}) - "
                f"Facebook may have changed this UI. Saved a screenshot to {debug_path}."
            )
        human_wait(page, 1500, 1000)

        reply_link = None
        for reply_text in COMMENT_REPLY_TEXTS:
            candidate = page.get_by_text(reply_text, exact=True).first
            if candidate.count() > 0:
                reply_link = candidate
                break
        box = reply_link.bounding_box(timeout=5000) if reply_link else None
        if box:
            page.mouse.move(box["x"], box["y"])
            for _ in range(8):
                page.mouse.wheel(0, random.randint(500, 1100))
                human_wait(page, 500, 500)

    return trigger
