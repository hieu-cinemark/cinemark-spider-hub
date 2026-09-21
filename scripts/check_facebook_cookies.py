"""Passive liveness check for facebook platform_accounts' cached cookies -
no password/2FA/login attempt involved. Reuses each account's existing
`cookie` field (exactly like a real crawl's "reuse cached session" path in
facebook/auth/bootstrap.py) to load a real Facebook page through that
account's pinned proxy, then checks whether the session is still accepted
(c_user cookie still present, not redirected to a login/checkpoint page) or
dead (session was invalidated - Facebook logged it out server-side).

Deliberately never attempts a fresh login - see bootstrap.py's own
"unattended_login_refused" guard and its docstring for why this project
refuses to automate that (a documented real incident got an account
flagged for "suspected automated behavior" from exactly this). Use this
first, on a freshly-added batch of accounts, to see which ones actually
need a real human-supervised re-login (`bootstrap.py --show-browser
--manual --account <key>`) before spending any of that effort - most
accounts with a cookie that already has c_user/xs need no login at all.

Paces one account at a time with a random pause in between (not
parallel/back-to-back) - many of these will resolve to the very same
pinned proxy given how few proxies are currently configured, and a burst
of identity-switching page loads from one IP is its own suspicious
pattern independent of whether any of it involves a password.

Usage:
    python -m scripts.check_facebook_cookies
    python -m scripts.check_facebook_cookies --account 61570510702486
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from patchright.sync_api import sync_playwright

from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.db import get_accounts, record_cookie_check
from social_crawler.spiders.facebook.auth.browser_interaction import new_context
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    parse_cookie_header,
)

logger = get_logger(__name__)

PLATFORM = "facebook"
# Between accounts - deliberately not back-to-back, see module docstring.
_MIN_PAUSE_SECONDS = 4.0
_MAX_PAUSE_SECONDS = 10.0


def _check_one(pw, account: dict) -> tuple[str, str | None]:
    """Returns (status, note). status is one of "alive"/"dead"/"skipped"/
    "error" - "error" means the check itself couldn't run (no proxy, a
    crashed browser, ...), not that the cookie was confirmed dead."""
    account_key = (account.get("email") or account["id"]).strip().lower()
    cookie = account.get("cookie") or ""
    if not cookie:
        return "skipped", "no cookie on this row"

    cookie_names = set(parse_cookie_header(cookie).keys())
    missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in cookie_names]
    if missing:
        return "dead", f"missing required cookie(s) {missing}"

    proxy = None
    try:
        # required=True - same reasoning as the real crawl path (see
        # facebook/auth/bootstrap.py): this reuses an existing session,
        # it's steady-state traffic, not the one-time opt-in login browser.
        proxy_cfg = pool.acquire_proxy_for_account(PLATFORM, account_key, required=True)
    except pool.ProxyPoolExhaustedError as exc:
        return "error", f"no usable proxy: {exc}"
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

    storage_state = build_storage_state_from_cookies(cookie)
    browser = pw.chromium.launch(headless=True, proxy=proxy)
    try:
        context = new_context(browser, account_key=account_key, storage_state=storage_state)
        page = context.new_page()
        try:
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:
            return "error", f"page load failed: {exc}"
        page.wait_for_timeout(2000)

        still_has_c_user = any(c["name"] == "c_user" for c in context.cookies())
        bounced = "login" in page.url or "checkpoint" in page.url

        if still_has_c_user and not bounced:
            return "alive", None
        return "dead", f"c_user_present={still_has_c_user} url={page.url}"
    finally:
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--account", help="Only check this one account_id/email instead of every enabled facebook account"
    )
    args = parser.parse_args()

    # See relogin_facebook_accounts.py's identical fix for why this matters
    # for any backgrounded/piped run: without it, a healthy run in progress
    # and one truly stuck on the first account look identical in the log
    # file for minutes at a time.
    sys.stdout.reconfigure(line_buffering=True)

    accounts = get_accounts(PLATFORM)
    if args.account:
        needle = args.account.strip().lower()
        accounts = [a for a in accounts if a["id"] == args.account or (a.get("email") or "").lower() == needle]

    if not accounts:
        print("No enabled facebook accounts to check.")
        return

    print(f"Checking {len(accounts)} facebook account(s) - one at a time, paced apart...\n")
    results: list[tuple[str, str, str | None]] = []
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            label = account.get("email") or account["id"]
            try:
                status, note = _check_one(pw, account)
            except Exception as exc:
                status, note = "error", str(exc)
                logger.error("cookie_check_crashed", account=label, error=str(exc))
            if status in ("alive", "dead"):
                record_cookie_check(PLATFORM, account["id"], status=status, note=note)
            results.append((label, status, note))
            print(f"  {label:45s} {status:8s} {note or ''}")
            if i < len(accounts) - 1:
                time.sleep(random.uniform(_MIN_PAUSE_SECONDS, _MAX_PAUSE_SECONDS))

    alive = sum(1 for _, s, _ in results if s == "alive")
    dead = sum(1 for _, s, _ in results if s == "dead")
    other = len(results) - alive - dead
    print(f"\n{alive} alive, {dead} dead, {other} skipped/error (out of {len(results)}).")
    if dead:
        print("Dead accounts need a real human-supervised re-login:")
        print("  python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual --account <email_or_id>")


if __name__ == "__main__":
    main()
