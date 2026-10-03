"""One unattended Facebook credential login for one account - fills the
login form and solves 2FA automatically (facebook/auth/triggers.py's
auto_login/submit_two_factor_code), then writes the freshly-captured
cookies straight back to that account's platform_accounts.cookie column
(and its Redis storage_state cache, so the very next crawl/bootstrap run
can reuse it immediately with no further work).

Shared by the auto-login orchestrator (orchestrator.py) and the operator's
batch script (scripts/relogin_facebook_accounts.py), so there's one version
of "how to log Facebook in unattended" instead of two drifting apart.
"""

from __future__ import annotations

from collections.abc import Callable

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY, STATE_REDIS_KEY_TMPL
from social_crawler.db.accounts import Account, get_account_pk, record_cookie_check, update_account_cookie
from social_crawler.services import pool
from social_crawler.spiders.facebook.auth.browser_interaction import has_display, new_context
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    extract_user_agent,
    parse_cookie_header,
)
from social_crawler.spiders.facebook.auth.triggers import (
    MissingTotpSecretError,
    TwoFactorPromptNotHandledError,
    auto_login,
)

PLATFORM = "facebook"


def relogin_one(
    pw,
    redis_cache: RedisCache,
    account: Account,
    *,
    headless: bool | None = None,
    code_provider: Callable[[], str | None] | None = None,
) -> tuple[str, str | None]:
    """Returns (status, note). status is "relogged_in"/"needs_human"/"failed"/"error".

    account is the db.accounts.Account shape (account["id"] is the login
    identifier, "2fa" the TOTP secret). headless=None opens a visible
    browser only where the host has a display. code_provider supplies the
    2FA code for an account with no TOTP secret (see
    triggers.submit_two_factor_code). On success the account's last check is
    recorded as "alive", so it drops out of every "dead"-status relogin
    list."""
    account_key = (account.get("email") or account["id"]).strip().lower()

    old_cookies = parse_cookie_header(account.get("cookie") or "")
    account_user_agent = extract_user_agent(old_cookies)
    context_kwargs = {"user_agent": account_user_agent} if account_user_agent else {}

    if not account.get("password"):
        return "needs_human", "no password stored on this row - cannot auto-login"
    try:
        # The same proxy policy as bootstrap.py's auto-login: only ever the
        # account's own pinned proxy, never unproxied or an unpinned one -
        # the session this login creates is replayed through that same pin.
        proxy = pool.pinned_login_proxy(PLATFORM, account_key)
    except RuntimeError as exc:
        return "error", f"no usable pinned proxy: {exc}"

    browser = pw.chromium.launch(headless=not has_display() if headless is None else headless, proxy=proxy)
    try:
        context = new_context(browser, account_key=account_key, **context_kwargs)
        page = context.new_page()
        try:
            auto_login(page, account, code_provider=code_provider)
        except MissingTotpSecretError:
            return "needs_human", "2FA prompt shown, no totp_secret on this row and no code from its email"
        except TwoFactorPromptNotHandledError as exc:
            return "needs_human", f"2FA prompt shown but not recognized: {exc}"
        except Exception as exc:
            return "error", f"login flow crashed: {exc}"

        cookies_after = context.cookies()
        cookie_names = {c["name"] for c in cookies_after}
        if not all(name in cookie_names for name in REQUIRED_LOGIN_COOKIES):
            debug_path = f"debug_relogin_failed_{account['id']}.png"
            page.screenshot(path=debug_path)
            return "needs_human", f"no c_user after login - possible checkpoint, see {debug_path}"

        cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies_after if "facebook.com" in c["domain"])
        row_id = get_account_pk(PLATFORM, account["id"])
        if row_id is None:
            return "error", "could not resolve this account's row id to write the new cookie back"
        if not update_account_cookie(PLATFORM, row_id, cookie_header):
            return "error", "login succeeded but writing the new cookie to Supabase failed"

        redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        # So the dashboard and the next check_facebook_cookies.py run reflect
        # this immediately - and so the auto-login scheduler/consumer stop
        # listing it as dead and logging it in again every tick.
        record_cookie_check(PLATFORM, account["id"], status="alive", note=None)
        return "relogged_in", None
    finally:
        browser.close()
