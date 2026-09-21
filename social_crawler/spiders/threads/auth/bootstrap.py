"""
Bootstraps a threads.com login session and captures one real GraphQL request
to extract doc_id / fb_dtsg / lsd / __rev... which are then used to replay
requests over plain HTTP (curl_cffi). Mirrors
social_crawler.spiders.facebook.auth.bootstrap - see that module's docstring
for the full rationale (storage_state reuse, account rotation/cookie import,
why the token cache is captured rather than hand-built, and the same
_BootstrapType dispatch for search vs. comments).

Run once (or periodically once the cache expires):

    python -m social_crawler.spiders.threads.auth.bootstrap --query "test"

The first run has no storage_state yet: if the platform_accounts table (see
accounts.py, services/db.py) has an enabled threads row, it imports the
rotated account's "cookie" field directly (no browser login at all).
Otherwise there's no automated login path at all anymore - an unattended
run (no --manual) with no usable cookie/session just refuses loudly (see
the "unattended_login_refused" guard) rather than typing the account's
password/2FA with nobody watching; a human has to run this module
themselves with --show-browser --manual to establish a fresh session.
Subsequent runs reuse the saved storage_state and run headless.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable, NamedTuple
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
from social_crawler.logger import bind_run_id, get_logger
from social_crawler.services import pool
from social_crawler.services.db import get_account_by_key, reactivate_account
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, new_context
from social_crawler.spiders.facebook.auth.request_capture import name_requests
from social_crawler.spiders.threads.auth.request_capture import (
    capture_graphql_requests,
    pick_comments_request,
    pick_initial_request,
    pick_paginated_comments_request,
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
from social_crawler.spiders.threads.auth.triggers import comments_trigger, search_trigger

logger = get_logger(__name__)


class _BootstrapType(NamedTuple):
    """Everything that differs between a "search" and a "comments" bootstrap
    run - mirrors facebook.auth.bootstrap's own _BootstrapType, see that
    module's docstring for why this exists (a single type=="search"/
    "comments" if/elif repeated three times used to silently no-op when a
    new type was added to one spot and not another)."""

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
        # CACHE_REDIS_KEY_TMPL, not COMMENTS_REDIS_KEY_TMPL - Threads'
        # comments spider reads GET /api/v1/text_feed/<id>/replies/ with
        # the same cookie session search uses (see graphql_client.get_text_feed_replies),
        # never the GraphQL comments-query recipe COMMENTS_REDIS_KEY_TMPL holds.
        # Saving this run under COMMENTS_REDIS_KEY_TMPL instead left the
        # base session cache empty, so a `--post-url`-only bootstrap
        # reported success but the next comments crawl raised SessionExpiredError.
        cache_key_tmpl=CACHE_REDIS_KEY_TMPL,
        saved_log_event="saved_comments_query_cache",
    ),
}


def _is_valid_storage_state(state: Any) -> bool:
    """Same rationale as facebook.auth.bootstrap._is_valid_storage_state -
    a corrupted/partial cache should trigger a fresh login instead of an
    undiagnosable crash deep inside Playwright."""
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        return False
    cookie_names = {c.get("name") for c in state["cookies"] if isinstance(c, dict)}
    return all(name in cookie_names for name in REQUIRED_LOGIN_COOKIES)


def _get_authenticated_context(
    pw: Playwright,
    redis_cache: RedisCache,
    headless: bool | None,
    force_manual: bool = False,
    prefer_account: str | None = None,
):
    """Shared login/session-reuse logic - mirrors
    facebook.auth.bootstrap._get_authenticated_context field for field, just
    sourced from the platform_accounts table (platform='threads') /
    threads.com constants instead."""
    if prefer_account:
        account = get_account_by_key("threads", prefer_account)
        if account is None:
            raise RuntimeError(f"No threads account matching {prefer_account!r}")
        account_key = normalize_account_key(account["id"])
    else:
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
            failure_reason = (
                f"Account {account_key!r} has a 'cookie' value but it's missing required cookie(s) "
                f"{missing} - a valid logged-in session needs at least {REQUIRED_LOGIN_COOKIES}."
            )
            if account is not None:
                # A malformed cookie column never self-heals - disable it
                # instead of leaving it claimed-but-unreleased: without
                # this, the row keeps getting handed out by next_account()
                # every rotation (last_used_at was already stamped at claim
                # time) and raising here again, silently monopolizing an
                # LRU slot instead of being flagged for a human to fix.
                pool.release_account("threads", account["id"], success=False, hard_failure=True, reason=failure_reason)
                logger.error(
                    "account_disabled_bad_cookie",
                    telegram=True,
                    platform="threads",
                    account=account_key,
                )
            raise RuntimeError(failure_reason)
        redis_cache.set(state_key, stored_state)
        logger.info(
            "imported_cookie_from_account",
            account=account_key,
            cookie_count=len(cookie_names),
            matched_user_agent=account_user_agent is not None,
        )

    need_login = stored_state is None
    # Same rationale as facebook.auth.bootstrap's own guard: unattended runs
    # (the refresh cron, a dashboard-triggered refresh) must never fall back
    # to typing this account's password/2FA with nobody watching the
    # browser - a fresh, unattended, automated login is one of the
    # strongest bot signals Meta's fraud detection watches for. Refuse
    # loudly instead, before even launching a browser.
    if need_login and account is not None and not force_manual:
        logger.error(
            "unattended_login_refused",
            telegram=True,
            platform="threads",
            account=account_key,
            hint="run by hand: python -m social_crawler.spiders.threads.auth.bootstrap --show-browser --manual",
        )
        # Soft failure, not hard_failure - nothing is wrong with this
        # account, it's just waiting on a one-time manual login (already
        # alerted above). Still release it (rather than leaving it
        # claimed-but-unreleased): next_account()'s LRU already stamped
        # last_used_at at claim time, so without this the pool has no
        # record anything happened and the same account comes right back
        # up next rotation, refusing again in a tight loop.
        pool.release_account(
            "threads",
            account["id"],
            success=False,
            reason=f"Account {account_key!r} has no valid cached session and unattended auto-login is disabled.",
        )
        raise RuntimeError(
            f"Account {account_key!r} has no valid cached session and unattended auto-login is disabled. "
            "Run by hand: python -m social_crawler.spiders.threads.auth.bootstrap --show-browser --manual"
        )

    proxy = None
    # required=not need_login: a fresh manual login (need_login=True) is a
    # one-time, human-supervised event that may reasonably run unproxied if
    # no proxy is currently available (see acquire_proxy_for_account's own
    # docstring) - but reusing a cached session (need_login=False, the
    # routine unattended refresh path) is steady-state traffic exactly like
    # comet_graphql_client.py's replay client, which requires the pinned
    # proxy. Without this, an unattended refresh could silently run
    # unproxied (real server IP) while every later GraphQL replay for the
    # same account strictly enforces the pin - a session established on one
    # IP and replayed from another, the sticky-pinning mismatch pinning
    # exists to prevent.
    proxy_cfg = pool.acquire_proxy_for_account("threads", account_key if account else None, required=not need_login)
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
            # Reaching here with need_login=True already guarantees either
            # account is None (purely manual, no platform_accounts row) or
            # force_manual=True (the guard above raised otherwise) - so this
            # is always a human-supervised login now, never auto_login().
            context = new_context(browser, **context_kwargs)
            page = context.new_page()
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
                failure_reason = "no ds_user_id cookie after login attempt"
                if account is not None:
                    pool.release_account(
                        "threads", account["id"], success=False, hard_failure=True, reason=failure_reason
                    )
                    logger.error(
                        "account_disabled_checkpoint_suspected",
                        telegram=True,
                        platform="threads",
                        account=account_key,
                        debug_screenshot=str(debug_path),
                    )
                raise RuntimeError(
                    f"Login for account {account_key!r} did not succeed - no ds_user_id cookie present "
                    f"afterwards (wrong password, or Instagram may have shown a checkpoint/2FA prompt "
                    f"instead of logging straight in). Saved a screenshot to {debug_path} for inspection. "
                    f"Re-run with --show-browser to watch it live."
                )

            if account is not None:
                pool.release_account("threads", account["id"], success=True)
            redis_cache.set(state_key, context.storage_state())
        else:
            context = new_context(browser, storage_state=stored_state, **context_kwargs)
            page = context.new_page()

        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        return browser, context, page, account_key
    except Exception:
        browser.close()
        raise


def bootstrap(
    query: str,
    headless: bool | None = None,
    type: str = "search",
    force_manual: bool = False,
    prefer_account: str | None = None,
) -> None:
    bootstrap_type = _BOOTSTRAP_TYPES.get(type)
    if bootstrap_type is None:
        raise ValueError(f"Unknown bootstrap type: {type}")

    redis_cache = RedisCache()

    with sync_playwright() as pw:
        browser, context, page, account_key = _get_authenticated_context(
            pw, redis_cache, headless, force_manual, prefer_account=prefer_account
        )

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

            cache_key = bootstrap_type.cache_key_tmpl.format(account=account_key)
            redis_cache.set(cache_key, cache, ttl_seconds=CACHE_MAX_AGE_SECONDS)
            logger.info(
                bootstrap_type.saved_log_event,
                telegram=True,
                key=cache_key,
                account=account_key,
                ttl_seconds=CACHE_MAX_AGE_SECONDS,
            )
            if prefer_account:
                row = get_account_by_key("threads", prefer_account)
                if row is not None:
                    reactivate_account("threads", row["id"])
                    logger.info("account_reactivated_after_restore", platform="threads", account=account_key)
        finally:
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Search keyword used to trigger a GraphQL search request")
    parser.add_argument("--post-url", help="Post URL used to trigger a GraphQL replies-list request")
    parser.add_argument(
        "--cookies-file",
        help="Path to a JSON file with cookies from an already-logged-in browser session "
        '(either {"ds_user_id": "...", "sessionid": "...", ...} or a full Playwright cookie list). '
        "Skips manual login entirely - run this once, then run --query normally.",
    )
    parser.add_argument(
        "--account",
        help="platform_accounts id (or email). With --cookies-file, saves the imported session "
        "under that account. With --query/--post-url, reuses that account's cached storage_state "
        "or cookie field instead of rotating the pool.",
    )
    parser.add_argument(
        "--show-browser", action="store_true", help="Show the browser window even if a session already exists"
    )
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Log in by hand even if platform_accounts has credentials for the rotated account - use this "
        "once when that account hits a checkpoint/verification screen auto-login can't click through.",
    )
    parser.add_argument(
        "--run-id",
        help="Set by crawl_request_consumer.py for a dashboard-triggered refresh - binds this value onto "
        "every log line this process emits (see logger.bind_run_id) so cinemark-api's refresh_tracker can "
        "isolate this exact run's own lines out of the shared consumer.log. Not needed for manual/local use.",
    )
    args = parser.parse_args()

    if args.run_id:
        bind_run_id(args.run_id)

    if args.cookies_file:
        from social_crawler.spiders.facebook.auth.cookies import load_exported_cookies

        import_cookies(
            load_exported_cookies(Path(args.cookies_file).read_text(encoding="utf-8")),
            account=args.account,
        )
    elif args.post_url:
        bootstrap(
            args.post_url,
            headless=False if args.show_browser else None,
            type="comments",
            force_manual=args.manual,
            prefer_account=args.account,
        )
    else:
        bootstrap(
            args.query or "test",
            headless=False if args.show_browser else None,
            force_manual=args.manual,
            prefer_account=args.account,
        )
