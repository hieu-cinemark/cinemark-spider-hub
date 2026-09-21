"""
Refreshes one tiktok platform_accounts row's device_id/odin_id - the
device-trust identity TikTokClient signs every request with (see
constants/tiktok.py and client.py's module docstrings). Mirrors
facebook/threads' own auth/bootstrap.py in spirit (reuse the account's
existing cookie, drive a real browser, capture what a real session actually
sends, write it back), but captures a signed REST request's query params
instead of a GraphQL doc_id/fb_dtsg bundle, and writes straight to Supabase
instead of a Redis token cache - TikTokClient reads identity from
platform_accounts on every run (see auth/accounts.py), there is no
separate cache to invalidate.

Only ever narrows an *already* real-usage-trusted session captured once
from a genuine browser - re-verify against constants/tiktok.py's documented
experiment before relying on this for a device that has never been used
for real before running it: a session with no browsing history at all
(a from-scratch Playwright context, even with a freshly captured cookie)
was confirmed not to pass TikTok's device-trust check on this endpoint. If
this account's cookie already came from a real, previously-used browser
session, opening it here and letting TikTok's own frontend fire its normal
requests should surface a valid device_id/odinId pair without ever
touching DevTools by hand.

Run once (or whenever an account's odin_id has gone stale - see
TikTokBlockedError in client.py):

    python -m social_crawler.spiders.tiktok.auth.bootstrap --account-id 5
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from patchright.sync_api import Request, sync_playwright

from social_crawler.constants.tiktok import STATIC_UA
from social_crawler.logger import bind_run_id, get_logger
from social_crawler.services import pool
from social_crawler.services.db import get_account_by_row_id, get_account_pk, reactivate_account, update_tiktok_identity
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, human_wait, new_context
from social_crawler.spiders.tiktok.auth.cookies import (
    build_storage_state_from_cookies,
    cookie_map,
    to_cookie_header,
)
from social_crawler.spiders.tiktok.auth.identity import (
    choose_identity,
    cookies_for_identity,
    identity_from_url,
    is_item_list_url,
)

logger = get_logger(__name__)


def _capture_identity(page, url: str, timeout_s: float = 20.0) -> tuple[str, str] | None:
    """Browses to `url` and prefers (device_id, odinId) from
    /api/challenge/item_list/ - the same signed call the crawler replays.
    Falls back to any other signed TikTok URL if item_list never fires.
    Scrolls during the wait: item_list often only starts after the first
    feed screen is scrolled past."""
    found_item_list: tuple[str, str] | None = None
    found_any: tuple[str, str] | None = None

    def on_request(request: Request) -> None:
        nonlocal found_item_list, found_any
        pair = identity_from_url(request.url)
        if pair is None:
            return
        if is_item_list_url(request.url):
            found_item_list = pair
            return
        if found_any is None:
            found_any = pair

    page.on("request", on_request)
    page.goto(url)
    deadline = time.time() + timeout_s
    while time.time() < deadline and found_item_list is None:
        page.mouse.wheel(0, 1800)
        human_wait(page, base_ms=1200, jitter_ms=800)
    page.remove_listener("request", on_request)
    return found_item_list or found_any


def refresh_identity(row_id: int, hashtag: str = "fyp", headless: bool | None = None) -> None:
    account = get_account_by_row_id("tiktok", row_id)
    if account is None:
        raise RuntimeError(f"No tiktok platform_accounts row with id={row_id}")
    if not account["cookie"]:
        raise RuntimeError(
            f"Account id={row_id} has no cookie set yet - this only refreshes an *existing* "
            "session's device_id/odin_id, it can't bootstrap one from nothing. Capture an "
            "initial ttwid/msToken/s_v_web_id cookie from a real browser session first."
        )

    storage_state = build_storage_state_from_cookies(account["cookie"])

    proxy = None
    proxy_cfg = pool.acquire_proxy_for_account("tiktok", account["id"])
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

    captured: tuple[str, str] | None = None
    original_cookies = cookie_map(account["cookie"])
    playwright_cookies: dict[str, str] = {}
    # Headful by default: the same class of empty-200 TikTok applies to
    # logged-in item_list as comments.py already documented for comment/list
    # (headless Patchright often never fires a usable item_list). Pass
    # headless=True only when the operator knows they are on a display-less host.
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False if headless is None else headless, proxy=proxy)
        try:
            context = new_context(browser, storage_state=storage_state, user_agent=STATIC_UA)
            page = context.new_page()

            captured = _capture_identity(page, f"https://www.tiktok.com/tag/{hashtag}")
            if captured is None:
                debug_path = BASE_DIR / f"tiktok_refresh_failed_{row_id}.png"
                page.screenshot(path=str(debug_path))
                logger.warning(
                    "tiktok_identity_not_seen_in_browser",
                    row_id=row_id,
                    screenshot=str(debug_path),
                )

            playwright_cookies = cookie_map(
                [
                    cookie
                    for cookie in context.cookies()
                    if (cookie.get("domain") or "").lstrip(".").lower().endswith("tiktok.com")
                ]
            )
        finally:
            browser.close()

    choice = choose_identity(account, captured)
    device_id, odin_id = choice.device_id, choice.odin_id
    persisted = cookies_for_identity(choice.source, original_cookies, playwright_cookies)
    cookie_header = to_cookie_header(persisted)
    logger.info(
        "tiktok_cookies_after_refresh",
        row_id=row_id,
        cookie_names=sorted(persisted),
        has_sessionid=bool(persisted.get("sessionid") or persisted.get("sid_tt")),
        cookie_source=choice.source,
    )
    if choice.source == "stored" and choice.playwright_pair and choice.playwright_pair != (device_id, odin_id):
        logger.info(
            "tiktok_kept_existing_identity",
            row_id=row_id,
            device_id=device_id,
            playwright_device_id=choice.playwright_pair[0],
        )
    elif captured is None and choice.source == "stored":
        logger.info("tiktok_kept_existing_identity", row_id=row_id, device_id=device_id, playwright_device_id=None)

    saved = update_tiktok_identity(row_id, device_id=device_id, odin_id=odin_id, cookie=cookie_header)
    if saved:
        reactivate_account("tiktok", device_id)
        RedisCache().delete(f"tiktok_block_streak:{device_id}")
    logger.info(
        "tiktok_identity_refreshed" if saved else "tiktok_identity_refresh_not_saved",
        telegram=True,
        row_id=row_id,
        device_id=device_id,
        identity_source=choice.source,
        saved=saved,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", type=int, help="platform_accounts.id of the tiktok row to refresh")
    parser.add_argument(
        "--account",
        help="platform_accounts email or account_id. With --cookies-file, saves the imported "
        "session on that row. With a restore/refresh, pins that row instead of requiring --account-id.",
    )
    parser.add_argument("--cookies-file", help="JSON object/list or a raw Cookie header, same as Facebook/Threads import")
    parser.add_argument("--hashtag", default="fyp", help="Hashtag page to browse while capturing identity (default: fyp)")
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Kept for older callers; restore is already headful unless --headless is set",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Hide the Patchright window. Logged-in item_list is more likely to come back empty.",
    )
    parser.add_argument(
        "--run-id",
        help="Set by crawl_request_consumer.py for a dashboard-triggered refresh - binds this value onto "
        "every log line this process emits so cinemark-api's refresh_tracker can isolate the run.",
    )
    args = parser.parse_args()

    if args.run_id:
        bind_run_id(args.run_id)

    if args.cookies_file:
        from social_crawler.spiders.facebook.auth.cookies import load_exported_cookies
        from social_crawler.spiders.tiktok.auth.cookies import import_cookies

        import_cookies(
            load_exported_cookies(Path(args.cookies_file).read_text(encoding="utf-8")),
            account=args.account,
        )
    else:
        row_id = args.account_id
        if row_id is None:
            if not args.account:
                parser.error("need --account-id or --account")
            row_id = get_account_pk("tiktok", args.account)
            if row_id is None:
                raise RuntimeError(f"No tiktok platform_accounts row matching {args.account!r}")
        refresh_identity(row_id, hashtag=args.hashtag, headless=True if args.headless else False)
