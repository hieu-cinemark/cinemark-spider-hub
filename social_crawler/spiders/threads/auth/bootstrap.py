"""
Bootstraps a threads.com login session and captures one real GraphQL request
to extract doc_id / fb_dtsg / lsd / __rev... which are then used to replay
requests over plain HTTP (curl_cffi). Mirrors
social_crawler.spiders.facebook.auth.bootstrap - see that module's docstring
for the full rationale (storage_state reuse, account rotation/cookie import,
why the token cache is captured rather than hand-built). This module only
covers the "search" flow for now, so there's no type dispatch like
Facebook's bootstrap() has for search vs. comments.

Run once (or periodically once the cache expires):

    python -m social_crawler.spiders.threads.auth.bootstrap --query "test"

The first run has no storage_state yet: if the platform_accounts table (see
accounts.py, services/db.py) has an enabled threads row, it either imports
the rotated account's "cookie" field directly (no browser login at all) or
logs in automatically with its id/password (+ TOTP from "2fa" if the
account has 2FA enabled); otherwise it opens a visible browser for manual
login. Subsequent runs reuse the saved storage_state and run headless.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from patchright.sync_api import Playwright, sync_playwright

from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_MAX_AGE_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    STATE_REDIS_KEY_TMPL,
    STATIC_BODY_FIELDS,
    STATIC_HEADER_FIELDS,
)
from social_crawler.logger import get_logger
from social_crawler.services.db import disable_account, get_proxy
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, new_context
from social_crawler.spiders.facebook.auth.request_capture import name_requests
from social_crawler.spiders.threads.auth.request_capture import (
    capture_graphql_requests,
    pick_initial_request,
    pick_paginated_request,
)
from social_crawler.spiders.threads.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.threads.auth.accounts import next_account
from social_crawler.spiders.threads.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    extract_user_agent,
    import_cookies,
    parse_cookie_header,
)
from social_crawler.spiders.threads.auth.triggers import auto_login, search_trigger

logger = get_logger(__name__)


def _is_valid_storage_state(state: Any) -> bool:
    """Same rationale as facebook.auth.bootstrap._is_valid_storage_state -
    a corrupted/partial cache should trigger a fresh login instead of an
    undiagnosable crash deep inside Playwright."""
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        return False
    cookie_names = {c.get("name") for c in state["cookies"] if isinstance(c, dict)}
    return all(name in cookie_names for name in REQUIRED_LOGIN_COOKIES)


def _get_authenticated_context(
    pw: Playwright, redis_cache: RedisCache, headless: bool | None, force_manual: bool = False
):
    """Shared login/session-reuse logic - mirrors
    facebook.auth.bootstrap._get_authenticated_context field for field, just
    sourced from the platform_accounts table (platform='threads') /
    threads.com constants instead."""
    account = next_account(redis_cache)
    if account is not None:
        account_key = normalize_account_key(account["id"])
    else:
        account_key = DEFAULT_ACCOUNT_KEY

    state_key = STATE_REDIS_KEY_TMPL.format(account=account_key)
    stored_state = redis_cache.get(state_key)
    if stored_state is not None and not _is_valid_storage_state(stored_state):
        logger.warning("discarding_invalid_stored_state", account=account_key, key=state_key)
        stored_state = None

    account_cookies = parse_cookie_header(account["cookie"]) if account and account.get("cookie") else None
    account_user_agent = extract_user_agent(account_cookies) if account_cookies else None
    context_kwargs = {"user_agent": account_user_agent} if account_user_agent else {}

    if stored_state is None and account_cookies and not force_manual:
        stored_state = build_storage_state_from_cookies(account_cookies)
        cookie_names = {c["name"] for c in stored_state["cookies"]}
        missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in cookie_names]
        if missing:
            raise RuntimeError(
                f"Account {account_key!r} has a 'cookie' value but it's missing required cookie(s) "
                f"{missing} - a valid logged-in session needs at least {REQUIRED_LOGIN_COOKIES}."
            )
        redis_cache.set(state_key, stored_state)
        logger.info(
            "imported_cookie_from_account",
            account=account_key,
            cookie_count=len(cookie_names),
            matched_user_agent=account_user_agent is not None,
        )

    need_login = stored_state is None

    proxy = None
    proxy_cfg = get_proxy("threads")
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {"server": f"http://{proxy_cfg['url']}", "username": proxy_cfg["username"], "password": proxy_cfg["password"]}

    browser_headless = headless if headless is not None else not need_login
    logger.info(
        "launching_browser",
        account=account_key,
        headless=browser_headless,
        need_login=need_login,
        proxy=proxy["server"] if proxy else None,
    )
    browser = pw.chromium.launch(headless=browser_headless, proxy=proxy)

    try:
        if need_login:
            context = new_context(browser, **context_kwargs)
            page = context.new_page()
            if account and not force_manual:
                logger.info("auto_login_attempt", account=account_key)
                auto_login(page, account)
            else:
                page.goto("https://www.threads.com/login/")
                logger.info(
                    "manual_login_required",
                    account=account_key,
                    hint="press Enter here once you're done logging in",
                )
                input()

            if not any(c["name"] == "ds_user_id" for c in context.cookies()):
                debug_path = BASE_DIR / "debug_login_failed.png"
                page.screenshot(path=str(debug_path))
                # Same rationale as facebook.auth.bootstrap's own check: a
                # real login attempt with this account's own stored
                # credentials failing to produce a logged-in cookie is the
                # clearest signal this specific account is checkpointed, so
                # disable it instead of letting every future rotation hit
                # the same wall. account can be None (purely manual login,
                # no platform_accounts row) - nothing to disable then.
                if account is not None:
                    disabled = disable_account(
                        "threads", account["id"], reason="no ds_user_id cookie after login attempt"
                    )
                    logger.error(
                        "account_disabled_checkpoint_suspected" if disabled else "account_checkpoint_suspected",
                        telegram=True,
                        platform="threads",
                        account=account_key,
                        disabled=disabled,
                        debug_screenshot=str(debug_path),
                    )
                raise RuntimeError(
                    f"Login for account {account_key!r} did not succeed - no ds_user_id cookie present "
                    f"afterwards (wrong password, or Instagram may have shown a checkpoint/2FA prompt "
                    f"instead of logging straight in). Saved a screenshot to {debug_path} for inspection. "
                    f"Re-run with --show-browser to watch it live."
                )

            redis_cache.set(state_key, context.storage_state())
        else:
            context = new_context(browser, storage_state=stored_state, **context_kwargs)
            page = context.new_page()

        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        return browser, context, page, account_key
    except Exception:
        browser.close()
        raise


def bootstrap(query: str, headless: bool | None = None, force_manual: bool = False) -> None:
    redis_cache = RedisCache()

    with sync_playwright() as pw:
        browser, context, page, account_key = _get_authenticated_context(pw, redis_cache, headless, force_manual)

        try:
            requests_seen = capture_graphql_requests(page, search_trigger(query))

            named = name_requests(requests_seen)
            logger.info("captured_graphql_requests", names=[name for _, name in named], count=len(named))

            initial_request = pick_initial_request(named)
            paginated_request = pick_paginated_request(named)

            if paginated_request is None:
                logger.warning(
                    "no_paginated_request_captured",
                    note="pagination will be unavailable until a future bootstrap run captures one",
                )

            headers = {k.lower(): v for k, v in initial_request.headers.items()}
            cookies = {c["name"]: c["value"] for c in context.cookies()}

            if not cookies.get("ds_user_id"):
                raise RuntimeError("Cookie ds_user_id is missing - the session does not appear to be logged in.")

            initial_body = dict(parse_qsl(initial_request.post_data or "", keep_blank_values=True))
            cache = {
                "captured_at": int(time.time()),
                "cookies": cookies,
                "headers": {k: headers[k] for k in STATIC_HEADER_FIELDS if k in headers},
                "body_static": {k: initial_body[k] for k in STATIC_BODY_FIELDS if k in initial_body},
                "doc_id": initial_body.get("doc_id"),
                "fb_api_req_friendly_name": initial_body.get("fb_api_req_friendly_name"),
                "variables_template": json.loads(initial_body.get("variables", "{}")),
            }

            if paginated_request is not None:
                paginated_body = dict(parse_qsl(paginated_request.post_data or "", keep_blank_values=True))
                cache["pagination"] = {
                    "doc_id": paginated_body.get("doc_id"),
                    "fb_api_req_friendly_name": paginated_body.get("fb_api_req_friendly_name"),
                    "variables_template": json.loads(paginated_body.get("variables", "{}")),
                }

            cache_key = CACHE_REDIS_KEY_TMPL.format(account=account_key)
            redis_cache.set(cache_key, cache, ttl_seconds=CACHE_MAX_AGE_SECONDS)
            logger.info(
                "saved_token_cache",
                telegram=True,
                key=cache_key,
                account=account_key,
                ttl_seconds=CACHE_MAX_AGE_SECONDS,
            )
        finally:
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Search keyword used to trigger a GraphQL search request")
    parser.add_argument(
        "--cookies-file",
        help="Path to a JSON file with cookies from an already-logged-in browser session "
        '(either {"ds_user_id": "...", "sessionid": "...", ...} or a full Playwright cookie list). '
        "Skips manual login entirely - run this once, then run --query normally.",
    )
    parser.add_argument(
        "--account",
        help="Only used with --cookies-file: the 'id' of the platform_accounts row these cookies "
        "belong to, so the session is saved under that account's key instead of the default slot.",
    )
    parser.add_argument("--show-browser", action="store_true", help="Show the browser window even if a session already exists")
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Log in by hand even if platform_accounts has credentials for the rotated account - use this "
        "once when that account hits a checkpoint/verification screen auto-login can't click through.",
    )
    args = parser.parse_args()

    if args.cookies_file:
        import_cookies(json.loads(Path(args.cookies_file).read_text(encoding="utf-8")), account=args.account)
    else:
        bootstrap(args.query or "test", headless=False if args.show_browser else None, force_manual=args.manual)
