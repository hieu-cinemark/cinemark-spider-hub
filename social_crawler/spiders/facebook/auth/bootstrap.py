"""
Bootstraps a Facebook login session and captures one real GraphQL request to
extract doc_id / fb_dtsg / lsd / __rev... which are then used to replay
requests over plain HTTP (curl_cffi).

Run once (or periodically once the cache expires):

    python -m social_crawler.spiders.facebook.auth.bootstrap --query "test"

The first run has no storage_state yet: if the platform_accounts table (see
accounts.py, services/db.py) has an enabled facebook row, it either imports
the rotated account's "cookie" field directly (no browser login at all) or
logs in automatically with its id/password (+ TOTP from "2fa" if the
account has 2FA enabled); otherwise it opens a visible browser for manual
login. Subsequent runs reuse the saved storage_state and run headless.

Both the login session (cookies) and the captured token cache are stored in
Redis, not on disk - Playwright accepts storage_state as a dict directly, so
no local file is needed at all.

The actual login-form interaction, GraphQL-request picking, account
rotation, and cookie-import logic live in sibling modules
(triggers.py / request_capture.py / accounts.py / cookies.py /
browser_interaction.py) - this file just wires them together and exposes
the CLI.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable, NamedTuple
from urllib.parse import parse_qsl

# patchright, not playwright: a patched Playwright fork that fixes the CDP
# (Chrome DevTools Protocol) leaks bot-detection systems like reCAPTCHA
# Enterprise key off of (Runtime.enable, addScriptToEvaluateOnNewDocument
# side effects, etc.) - same API, drop-in replacement. Regular Playwright's
# navigator.webdriver override alone doesn't hide these deeper traces, which
# is what kept triggering a captcha here even with human-like typing/mouse
# movement and a geography-matched proxy.
from patchright.sync_api import Playwright, sync_playwright

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_MAX_AGE_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    STATE_REDIS_KEY_TMPL,
    STATIC_BODY_FIELDS,
    STATIC_HEADER_FIELDS,
)
from social_crawler.logger import get_logger
from social_crawler.services.db import disable_account, get_proxy
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.facebook.auth.accounts import next_account
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, new_context
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    extract_user_agent,
    import_cookies,
    parse_cookie_header,
)
from social_crawler.spiders.facebook.auth.request_capture import (
    capture_graphql_requests,
    name_requests,
    pick_comments_request,
    pick_initial_request,
    pick_paginated_comments_request,
    pick_paginated_request,
)
from social_crawler.spiders.facebook.auth.triggers import auto_login, comments_trigger, search_trigger

logger = get_logger(__name__)


def _is_valid_storage_state(state: Any) -> bool:
    """Sanity-check a Playwright storage_state dict loaded from Redis before
    handing it to new_context() - a corrupted/partial cache (schema change
    across a deploy, manual edit, interrupted write) should trigger a fresh
    login instead of an undiagnosable crash deep inside Playwright."""
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        return False
    cookie_names = {c.get("name") for c in state["cookies"] if isinstance(c, dict)}
    return all(name in cookie_names for name in REQUIRED_LOGIN_COOKIES)


def _get_authenticated_context(
    pw: Playwright, redis_cache: RedisCache, headless: bool | None, force_manual: bool = False
):
    """Shared login/session-reuse logic for every bootstrap flow (search,
    comments, ...). Picks which account this run acts as - rotating through
    whatever's enabled in the platform_accounts table (platform='facebook',
    see services/db.py) if any, otherwise a single fixed "default" slot for
    manual login / imported cookies - then reuses that account's own cached
    storage_state if present, imports its "cookie" field directly if one is
    set (skipping the browser login entirely), or opens a visible browser
    for one-time login otherwise. Returns the account_key too, so the
    caller saves the token cache under that same account instead of a shared
    global one.

    Pass force_manual=True to log in by hand even when the rotated account
    has credentials configured - needed the first time an account hits a
    checkpoint/verification screen that auto-login can't click through;
    storage_state still gets saved under that same account's key, so every
    later run resumes headlessly as usual."""
    account = next_account(redis_cache)
    if account is not None:
        account_key = normalize_account_key(account.get("email") or account["id"])
    else:
        account_key = DEFAULT_ACCOUNT_KEY

    state_key = STATE_REDIS_KEY_TMPL.format(account=account_key)
    stored_state = redis_cache.get(state_key)
    if stored_state is not None and not _is_valid_storage_state(stored_state):
        # A corrupted/partial cache (schema change across a deploy, manual
        # edit, interrupted write) would otherwise surface as a raw,
        # undiagnosable exception deep inside Playwright's context-creation
        # call - discard it and fall through to a fresh login instead, same
        # as if nothing had been cached.
        logger.warning("discarding_invalid_stored_state", account=account_key, key=state_key)
        stored_state = None

    # The account's own "cookie" field can carry a synthetic useragent=...
    # entry (see cookies.extract_user_agent) recording the browser Facebook
    # actually saw at login - matching it here (for both the cookie-import
    # and the auto/manual-login paths below) makes this session look
    # consistent across runs instead of jumping to Playwright's default UA.
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
    proxy_cfg = get_proxy("facebook")
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

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
                page.goto("https://www.facebook.com/login")
                logger.info(
                    "manual_login_required",
                    account=account_key,
                    hint="press Enter here once you're done logging in",
                )
                input()

            # Fail here, loudly, if login didn't actually take - otherwise the
            # next step (navigating to the homepage to search) just lands back
            # on the logged-out page and fails with a confusing "can't find the
            # search box" error instead of the real problem.
            if not any(c["name"] == "c_user" for c in context.cookies()):
                debug_path = BASE_DIR / "debug_login_failed.png"
                page.screenshot(path=str(debug_path))
                # A real login attempt with this account's own stored
                # credentials, not a human typing at a manual prompt - the
                # clearest signal available that this specific account (not
                # just this one run) is checkpointed, so disable it rather
                # than let every future rotation hit the same wall. account
                # can be None here (no platform_accounts row at all, purely
                # manual login) - nothing to disable in that case.
                if account is not None:
                    disabled = disable_account("facebook", account["id"], reason="no c_user cookie after login attempt")
                    logger.error(
                        "account_disabled_checkpoint_suspected" if disabled else "account_checkpoint_suspected",
                        telegram=True,
                        platform="facebook",
                        account=account_key,
                        disabled=disabled,
                        debug_screenshot=str(debug_path),
                    )
                raise RuntimeError(
                    f"Login for account {account_key!r} did not succeed - no c_user cookie present "
                    f"afterwards (wrong password, or Facebook may have shown a checkpoint/2FA prompt "
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
        # Nothing below this point returned the browser to the caller, so
        # nobody else will ever call browser.close() on it - close it here
        # before re-raising instead of leaking the Chromium process on every
        # failed login/checkpoint.
        browser.close()
        raise


class _BootstrapType(NamedTuple):
    """Everything that differs between a "search" and a "comments" bootstrap
    run, in one place - previously the type=="search"/"comments" dispatch
    was repeated three separate times across bootstrap(), each an if/elif
    with no `else`, so a new type added to one and not another would
    silently no-op instead of failing loudly."""

    trigger: Callable[[str], Callable]
    pick_initial: Callable[[list], Any]
    pick_paginated: Callable[[list], Any | None]
    cache_key_tmpl: str
    saved_log_event: str


_BOOTSTRAP_TYPES = {
    "search": _BootstrapType(
        trigger=search_trigger,
        pick_initial=pick_initial_request,
        pick_paginated=pick_paginated_request,
        cache_key_tmpl=CACHE_REDIS_KEY_TMPL,
        saved_log_event="saved_token_cache",
    ),
    "comments": _BootstrapType(
        trigger=comments_trigger,
        pick_initial=pick_comments_request,
        pick_paginated=pick_paginated_comments_request,
        cache_key_tmpl=COMMENTS_REDIS_KEY_TMPL,
        saved_log_event="saved_comments_query_cache",
    ),
}


def bootstrap(query: str, headless: bool | None = None, type: str = "search", force_manual: bool = False) -> None:
    bootstrap_type = _BOOTSTRAP_TYPES.get(type)
    if bootstrap_type is None:
        raise ValueError(f"Unknown bootstrap type: {type}")

    redis_cache = RedisCache()

    with sync_playwright() as pw:
        browser, context, page, account_key = _get_authenticated_context(pw, redis_cache, headless, force_manual)

        try:
            requests_seen = capture_graphql_requests(page, bootstrap_type.trigger(query))

            named = name_requests(requests_seen)
            logger.info("captured_graphql_requests", names=[name for _, name in named], count=len(named))

            initial_request = bootstrap_type.pick_initial(named)
            paginated_request = bootstrap_type.pick_paginated(named)

            if paginated_request is None:
                logger.warning(
                    "no_paginated_request_captured",
                    note="pagination will be unavailable until a future bootstrap run captures one",
                )

            headers = {k.lower(): v for k, v in initial_request.headers.items()}
            cookies = {c["name"]: c["value"] for c in context.cookies()}

            # confirm we're actually logged in (c_user must be a real user id)
            if not cookies.get("c_user"):
                raise RuntimeError("Cookie c_user is missing - the session does not appear to be logged in.")

            # cache the real request's variables as-is (including every
            # __relay_internal__pv__... flag the current schema requires)
            # instead of hand-building them - only override text/count/cursor
            # when replaying
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

            cache_key = bootstrap_type.cache_key_tmpl.format(account=account_key)
            redis_cache.set(cache_key, cache, ttl_seconds=CACHE_MAX_AGE_SECONDS)
            logger.info(
                bootstrap_type.saved_log_event,
                telegram=True,
                key=cache_key,
                account=account_key,
                ttl_seconds=CACHE_MAX_AGE_SECONDS,
            )
        finally:
            # storage_state may have changed (FB rotates cookies) - save it
            # again even if the capture/pick steps above failed (e.g. no
            # GraphQL request captured), so a login that succeeded isn't
            # discarded, and always close the browser so a failure here
            # doesn't leak the Chromium process.
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Search keyword used to trigger a GraphQL search request")
    parser.add_argument("--post-url", help="Post/reel URL used to trigger a GraphQL comments-list request")
    parser.add_argument(
        "--cookies-file",
        help="Path to a JSON file with cookies from an already-logged-in browser session "
        '(either {"c_user": "...", "xs": "...", ...} or a full Playwright cookie list). '
        "Skips manual login entirely - run this once, then run --query/--post-url normally.",
    )
    parser.add_argument(
        "--account",
        help="Only used with --cookies-file: the 'email' (or 'id' if 'email' is blank) of the "
        "platform_accounts row these cookies belong to, so the session is saved under that "
        "account's key instead of the default slot.",
    )
    parser.add_argument(
        "--show-browser", action="store_true", help="Show the browser window even if a session already exists"
    )
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Log in by hand even if platform_accounts has credentials for the rotated account - use this "
        "once when that account hits a checkpoint/verification screen auto-login can't click through. "
        "The session still gets saved under that same account, so later runs go back to headless auto-login.",
    )
    args = parser.parse_args()

    if args.cookies_file:
        import_cookies(json.loads(Path(args.cookies_file).read_text(encoding="utf-8")), account=args.account)
    elif args.post_url:
        bootstrap(
            args.post_url, headless=False if args.show_browser else None, type="comments", force_manual=args.manual
        )
    else:
        bootstrap(args.query or "test", headless=False if args.show_browser else None, force_manual=args.manual)
