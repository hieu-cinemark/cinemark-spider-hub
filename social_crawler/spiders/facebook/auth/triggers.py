"""
Facebook-specific Playwright flows: fills and submits the login form, and
drives the search/comments pages to fire the GraphQL requests
request_capture.py listens for.
"""

from __future__ import annotations

import re
from urllib.parse import quote

import pyotp

from social_crawler.constants.facebook import (
    COMMENT_OPEN_BUTTON_TEXTS,
    COMMENT_REPLY_TEXTS,
    COMMENT_SORT_NEWEST_TEXTS,
    COMMENT_SORT_TRIGGER_TEXTS,
    COMMENT_VIEW_REPLIES_PATTERN,
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
    click_first_via_js,
    click_via_ai_fallback,
    find_first_visible,
    human_wait,
    move_mouse_naturally,
    natural_scroll,
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
        # /search/posts/ (not /search/top/): "Top" is Facebook's algorithmic,
        # per-account-personalized ranking - it mixes in people/pages/groups
        # results and can rank an older, high-engagement post above a
        # brand-new matching one, which is exactly why a crawl through this
        # cached recipe returned different posts than a human manually
        # searching the same query and clicking the "Posts" filter tab
        # (confirmed the mismatch by comparing the two). /search/posts/ is
        # Facebook's own dedicated posts-only tab - not perfectly
        # chronological either, but scoped to actual post content instead of
        # a personalized cross-entity ranking, matching what "search for
        # posts mentioning X" actually means here.
        page.goto(f"https://www.facebook.com/search/posts/?q={quote(query)}", wait_until="domcontentloaded")
        human_wait(page, 1500, 1000)
        # scroll down to force Facebook to fetch the next page, so we can
        # also capture a real SearchCometResultsPaginatedResultsQuery request.
        # min_scrolls/min_px kept at the fixed loop's old floor (4 x 1400px)
        # - that's the confirmed-working minimum to actually trigger the
        # pagination fetch; only the count/distance/pace above that floor is
        # randomized, plus an occasional overshoot-and-correct scroll-up.
        natural_scroll(page, min_scrolls=4, max_scrolls=7, min_px=1400, max_px=2600, pause_base_ms=700, pause_jitter_ms=600)

    return trigger


def _open_comments_sorted_newest(page) -> None:
    """Navigate a post permalink to a comments list sorted "Newest" - shared
    by comments_trigger and replies_trigger below, since a replies fetch
    needs the exact same setup (comments open, sorted) before it can find a
    comment with replies to expand."""
    # A /videos/ URL (Video Home player) or a /reel/ URL doesn't show the
    # comment list at all until this is clicked - a normal post permalink
    # already has comments open, so this is best-effort (click_first
    # swallows "found nothing" silently, same as dismiss_cookie_banner)
    # rather than required.
    opened = click_first((page.get_by_text(t, exact=False) for t in COMMENT_OPEN_BUTTON_TEXTS), timeout_ms=3000)
    if not opened:
        # Reels' comment control is an icon-only button - its visible text
        # is just the engagement count ("6", "3,6K"), never the word
        # "Comment"/"Bình luận" itself, which only exists in its aria-label
        # - confirmed live (2026-09-16) that get_by_text never matches it,
        # so the whole comments panel silently never opened for any reel,
        # which is what actually caused debug_comments_sort_trigger_not_found
        # (the sort control this function looks for next was never in the
        # DOM at all, not a changed sort-control selector). Retry by
        # accessible role/name, which resolves aria-label-only controls
        # fine.
        #
        # click_first_via_js, not click_first(force=True): also confirmed
        # live that a reel view has an unrelated Messenger chat-widget
        # error card ("Không thể tải đoạn chat") whose container fully
        # covers the action rail's entire bounding box - elementFromPoint
        # at the comment button's own center resolves to the chat card, not
        # the button, so a real mouse click there (even with force=True,
        # which only skips Playwright's pre-click checks, not the browser's
        # actual coordinate hit-testing) lands on the chat card and does
        # nothing. Dispatching .click() directly on the resolved element
        # skips hit-testing entirely - React's delegated handler still
        # receives it since it keys off the event's real target, not screen
        # position - confirmed live to actually open the comments panel
        # where force=True did not.
        #
        # reversed(): this project's contexts are always locale="vi-VN" (see
        # COMMENT_SORT_TRIGGER_TEXTS' own comment above) - trying "Comment"
        # first would burn its own full timeout guaranteed-failing before
        # ever trying the locator that can actually match. timeout_ms=8000,
        # well above COMMENT_OPEN_BUTTON_TEXTS' normal 3000ms above:
        # confirmed live this matters - a reel's action rail (unlike a
        # normal post's, already in the DOM immediately) attaches
        # progressively as the video buffers, and a real run with only
        # 3000ms here intermittently lost that race even on an account with
        # a perfectly valid session.
        opened = click_first_via_js(
            (page.get_by_role("button", name=t) for t in reversed(COMMENT_OPEN_BUTTON_TEXTS)), timeout_ms=8000
        )
    if not opened:
        # Last resort, only reached once every hardcoded strategy above has
        # already failed: ask Kira to pick the right element off a live
        # snapshot of the page's own interactive elements instead of
        # hand-fixing yet another selector every time Facebook reshuffles
        # this markup (see services/kira.py's suggest_element_index for the
        # full rationale). A no-op (returns False, no exception) whenever
        # Kira isn't configured (KIRA_ENABLED, off by default) - this is
        # purely additive on top of the strategies above, never a
        # replacement for them.
        opened = click_via_ai_fallback(
            page, goal="Open this post's comment list (an icon-only button may show only a number, not text)"
        )
    if opened:
        human_wait(page, 1200, 800)

    # Every context this project creates is locale="vi-VN" (see
    # browser_interaction.new_context), so Facebook renders this UI in
    # Vietnamese ("Phù hợp nhất"/"Mới nhất"/"Phản hồi") - a fixed
    # English-only string here just times out and never fires the
    # comments GraphQL request at all (confirmed happening for real).
    #
    # Both steps below are best-effort, not required: confirmed live
    # (2026-09-16) that a Reels comments panel simply has no sort-order
    # control at all (unlike a normal post permalink) - not a changed
    # selector, a genuinely different, more compact UI for this content
    # type. request_capture.py's own comments-list matcher (pick_comments_
    # request) doesn't care what order the captured request sorts by, and
    # the actual production crawler (features/comments/comments.py) has no
    # "newest first" assumption either (paginates/dedupes independent of
    # order) - so a missing sort control isn't a reason to fail the whole
    # capture, just proceed with whatever default order this content type
    # gives.
    if click_first((page.get_by_text(t, exact=False) for t in COMMENT_SORT_TRIGGER_TEXTS), timeout_ms=5000):
        human_wait(page, 600, 500)
        if not click_first(
            (page.locator('div[role="menuitem"]').filter(has_text=t) for t in COMMENT_SORT_NEWEST_TEXTS),
            timeout_ms=5000,
        ):
            logger.warning(
                "comment_sort_newest_menu_item_not_found",
                hint=f"tried {COMMENT_SORT_NEWEST_TEXTS} - continuing with whatever order was already showing",
            )
        else:
            human_wait(page, 1500, 1000)
    else:
        logger.warning(
            "comment_sort_control_not_found",
            hint=f"tried {COMMENT_SORT_TRIGGER_TEXTS} - this content type (e.g. Reels) may not expose a sort "
            "control at all; continuing with whatever default order it renders",
        )


def comments_trigger(post_url: str):
    def trigger(page):
        page.goto(post_url, wait_until="domcontentloaded")
        human_wait(page, 2000, 1000)
        _open_comments_sorted_newest(page)

        reply_link = None
        for reply_text in COMMENT_REPLY_TEXTS:
            candidate = page.get_by_text(reply_text, exact=True).first
            if candidate.count() > 0:
                reply_link = candidate
                break
        box = reply_link.bounding_box(timeout=5000) if reply_link else None
        if box:
            page.mouse.move(box["x"], box["y"])
            # min_scrolls/min_px raised (2026-09-16, from an original 8 x
            # 500px floor) - see _facebook_comments_cache_usable's own
            # docstring on why under-scrolling a low-comment post silently
            # produces a comments cache that can never paginate past ~2
            # comments for any post. The original floor was already enough
            # to *eventually* hit Facebook's own pagination fetch on a
            # busy post, but confirmed live (2026-09-16) that it often
            # didn't: bootstrap kept landing on a comments cache with no
            # `pagination` section even against posts with hundreds of
            # comments, forcing a fresh browser bootstrap on every single
            # use of that account instead of once per TTL. Scrolling
            # further before this trigger gives up raises the odds this
            # one bootstrap run actually reaches Facebook's own page-2
            # fetch instead of needing a lucky future retry.
            natural_scroll(page, min_scrolls=18, max_scrolls=25, min_px=800, max_px=1600, pause_base_ms=500, pause_jitter_ms=500)

    return trigger


def replies_trigger(post_url: str):
    """Like comments_trigger, but goes on to actually expand one comment's
    replies (clicking "N phản hồi"/"N replies") instead of just scrolling
    past it - that click is what fires the GraphQL request bootstrap.py
    needs to capture for `--type replies` (see request_capture.py's
    pick_comments_request, reused as-is for this capture too)."""

    def trigger(page):
        page.goto(post_url, wait_until="domcontentloaded")
        human_wait(page, 2000, 1000)
        _open_comments_sorted_newest(page)

        # Scroll well past the old 3-5x500-1000 floor (2026-09-16) - a
        # comment with replies isn't guaranteed to be the very first one
        # rendered, and the real goal here isn't just "find any reply
        # link" (the old floor already did that fine) but "find one whose
        # thread is actually big enough to paginate" - the more comments
        # loaded into the DOM, the more candidates the count-based pick
        # below has to choose from.
        natural_scroll(page, min_scrolls=10, max_scrolls=15, min_px=700, max_px=1400, pause_base_ms=500, pause_jitter_ms=400)

        pattern = re.compile(COMMENT_VIEW_REPLIES_PATTERN, re.IGNORECASE)
        candidates = page.get_by_text(pattern).all()
        if not candidates:
            debug_path = BASE_DIR / "debug_no_replies_link_found.png"
            page.screenshot(path=str(debug_path))
            raise RuntimeError(
                f"Could not find a 'view replies' link (tried pattern {COMMENT_VIEW_REPLIES_PATTERN!r}) among "
                "this post's visible comments - either none of them have replies yet (try a post/URL where a "
                f"top-level comment clearly has replies), or Facebook changed this UI. Saved a screenshot to "
                f"{debug_path}."
            )
        # Picking the highest reply-count link, not just the first one
        # (2026-09-16) - the old .first pick took whichever thread
        # happened to render first in the DOM, which is just as often a
        # "2 replies" thread as a "200 replies" one; a small thread's
        # entire content fits in one response, so its own expand-click
        # never fires a *paginated* replies request at all, no matter how
        # this trigger scrolls beforehand. Best-effort: any candidate
        # whose own count can't be parsed just sorts last rather than
        # aborting the whole bootstrap over one unexpected text shape.
        number_pattern = re.compile(r"\d+")

        def _reply_count(locator) -> int:
            try:
                match = number_pattern.search(locator.inner_text())
                return int(match.group()) if match else -1
            except Exception:
                return -1

        candidate = max(candidates, key=_reply_count)
        move_mouse_naturally(page, candidate)
        candidate.click()
        human_wait(page, 1500, 1000)

        # Scroll the now-expanded thread too (2026-09-16) - Facebook lazy-
        # loads a busy thread's own replies the same way it does the
        # top-level comment list, so the single expand-click above only
        # ever captures that thread's *first* page; this is what actually
        # gives it a chance to serve (and this bootstrap a chance to
        # capture) a genuine next-page replies fetch.
        natural_scroll(page, min_scrolls=8, max_scrolls=12, min_px=500, max_px=1000, pause_base_ms=500, pause_jitter_ms=400)

    return trigger
