"""One-time, sequential re-login pass for facebook accounts whose cached
cookie has gone dead (see scripts/check_facebook_cookies.py) - fills the
login form and solves TOTP 2FA automatically (facebook/auth/triggers.py's
auto_login/submit_two_factor_code), then writes the freshly-captured
cookies straight back to that account's platform_accounts.cookie column
(and its Redis storage_state cache, so the very next crawl/bootstrap run
can reuse it immediately with no further work).

Deliberately sequential, one account at a time (never parallel within this
script) - each account gets its own pinned proxy via
pool.acquire_proxy_for_account, so back-to-back logins never share an IP
in a tight loop even though they run one after another with a paced delay.

Cannot solve an email-based verification code (no account here has that
automated - see Account["email_password"]'s own docstring in services/db.py)
- an account whose row has no totp_secret and gets shown a 2FA prompt just
gets reported as needing a human (`bootstrap.py --show-browser --manual`),
never auto-disabled (see MissingTotpSecretError's own docstring for why
guessing that's a checkpoint would be wrong).

Usage:
    # Every facebook account last recorded as "dead" by check_facebook_cookies.py
    python -m scripts.relogin_facebook_accounts

    # Just specific accounts
    python -m scripts.relogin_facebook_accounts --account 61570510702486 --account someone@example.com
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from patchright.sync_api import sync_playwright

from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY, STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.db import (
    get_account_pk,
    get_accounts_by_check_status,
    record_cookie_check,
    update_account_cookie,
)
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.browser_interaction import new_context
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

logger = get_logger(__name__)

PLATFORM = "facebook"
_MIN_PAUSE_SECONDS = 20.0
_MAX_PAUSE_SECONDS = 60.0


def _relogin_one(pw, redis_cache: RedisCache, account: dict) -> tuple[str, str | None]:
    """Returns (status, note). status is "relogged_in"/"needs_human"/"failed"/"error"."""
    account_key = (account.get("email") or account["id"]).strip().lower()

    old_cookies = parse_cookie_header(account.get("cookie") or "")
    account_user_agent = extract_user_agent(old_cookies)
    context_kwargs = {"user_agent": account_user_agent} if account_user_agent else {}

    proxy = None
    try:
        # Same as a fresh manual login in bootstrap.py: required=False -
        # this is the one-time login step itself, not steady-state replay
        # traffic, so it's allowed to tolerate "no proxy" as a last resort
        # rather than blocking the whole re-login on proxy availability.
        proxy_cfg = pool.acquire_proxy_for_account(PLATFORM, account_key, required=False)
    except pool.ProxyPoolExhaustedError as exc:
        return "error", f"no usable proxy: {exc}"
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

    browser = pw.chromium.launch(headless=False, proxy=proxy)
    try:
        context = new_context(browser, account_key=account_key, **context_kwargs)
        page = context.new_page()
        try:
            auto_login(page, account)
        except MissingTotpSecretError:
            return "needs_human", "2FA prompt shown, no totp_secret on this row"
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
        return "relogged_in", None
    finally:
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--account",
        action="append",
        help="account_id/email to re-login (repeatable). Default: every facebook account last checked 'dead'.",
    )
    args = parser.parse_args()

    # Piping this to a file/tee (as any background run does) switches
    # stdout to block-buffered, so `print()`'s progress lines can sit in a
    # buffer for minutes with nothing actually written to the log file yet
    # - confirmed happening for real: a run that was quietly succeeding
    # account after account looked identical, from the log file alone, to
    # one stuck on the very first account, and got killed as a false
    # "hung" diagnosis. Line-buffering here means what's in the log file is
    # always what has actually happened so far.
    sys.stdout.reconfigure(line_buffering=True)

    # A slow/flaky network (VPN reconnecting, wifi handoff) can make the
    # very first Supabase connection stall well past _connect()'s own
    # connect_timeout - psycopg/libpq's timeout doesn't reliably cover a
    # stuck DNS resolution on every platform. Printing before the call (not
    # after) means a stall shows up as "still on this line" instead of a
    # script that looks frozen/dead with zero output, like it did in
    # practice - see the KeyboardInterrupt while this project's user was
    # waiting on this exact line with nothing printed yet.
    print("Connecting to Supabase to find accounts marked 'dead'...")
    if args.account:
        all_dead = get_accounts_by_check_status(PLATFORM, "dead")
        needles = {a.strip().lower() for a in args.account}
        accounts = [a for a in all_dead if a["id"].lower() in needles or (a.get("email") or "").lower() in needles]
        missing = needles - {a["id"].lower() for a in accounts} - {(a.get("email") or "").lower() for a in accounts}
        if missing:
            print(f"Not found among accounts last checked 'dead': {sorted(missing)}")
    else:
        accounts = get_accounts_by_check_status(PLATFORM, "dead")

    if not accounts:
        print("No facebook accounts to re-login (none currently recorded as 'dead').")
        return

    print(f"Re-logging in {len(accounts)} facebook account(s), one at a time...\n")
    redis_cache = RedisCache()
    results: list[tuple[str, str, str | None]] = []
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            label = account.get("email") or account["id"]
            try:
                status, note = _relogin_one(pw, redis_cache, account)
            except Exception as exc:
                status, note = "error", str(exc)
                logger.error("relogin_crashed", account=label, error=str(exc))
            # "relogged_in" -> record_cookie_check("alive") so the dashboard
            # and the next run of check_facebook_cookies.py both reflect
            # this immediately; "needs_human"/"error" leave the existing
            # "dead" status alone - it's still dead until someone fixes it.
            if status == "relogged_in":
                record_cookie_check(PLATFORM, account["id"], status="alive", note=None)
            results.append((label, status, note))
            print(f"  {label:45s} {status:14s} {note or ''}")
            if i < len(accounts) - 1:
                time.sleep(random.uniform(_MIN_PAUSE_SECONDS, _MAX_PAUSE_SECONDS))

    ok = sum(1 for _, s, _ in results if s == "relogged_in")
    needs_human = sum(1 for _, s, _ in results if s == "needs_human")
    other = len(results) - ok - needs_human
    print(f"\n{ok} re-logged in, {needs_human} need a human, {other} error (out of {len(results)}).")
    if needs_human:
        print("These need a real human-supervised login instead:")
        print("  python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual --account <email_or_id>")


if __name__ == "__main__":
    main()
