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
from urllib.parse import parse_qs, urlparse

from patchright.sync_api import Request, sync_playwright

from social_crawler.constants.tiktok import STATIC_UA
from social_crawler.logger import get_logger
from social_crawler.services.db import get_account_by_row_id, get_proxy, update_tiktok_identity
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, human_wait, new_context
from social_crawler.spiders.tiktok.auth.cookies import build_storage_state_from_cookies

logger = get_logger(__name__)

# Any signed TikTok request carries both on every call, not just the
# hashtag endpoints this spider happens to use - whichever request the
# frontend fires first while browsing is fine to read them off of.
_IDENTITY_PARAMS = ("device_id", "odinId")


def _capture_identity(page, url: str, timeout_s: float = 20.0) -> tuple[str, str] | None:
    """Browses to `url` and returns the first (device_id, odinId) pair seen
    on an outgoing request TikTok's own page JS makes, or None if nothing
    carrying both showed up within timeout_s. Scrolls partway through the
    wait since the video feed's own pagination call (the one most reliably
    carrying odinId) only fires once the initially-rendered items are
    scrolled past, not on page load alone."""
    found: tuple[str, str] | None = None

    def on_request(request: Request) -> None:
        nonlocal found
        if found is not None or "tiktok.com" not in request.url:
            return
        query = parse_qs(urlparse(request.url).query)
        device_id = (query.get("device_id") or [""])[0]
        odin_id = (query.get("odinId") or [""])[0]
        if device_id and odin_id:
            found = (device_id, odin_id)

    page.on("request", on_request)
    page.goto(url)
    deadline = time.time() + timeout_s
    while time.time() < deadline and found is None:
        page.mouse.wheel(0, 1800)
        human_wait(page, base_ms=1200, jitter_ms=800)
    page.remove_listener("request", on_request)
    return found


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
    proxy_cfg = get_proxy("tiktok")
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless if headless is not None else True, proxy=proxy)
        try:
            context = new_context(browser, storage_state=storage_state, user_agent=STATIC_UA)
            page = context.new_page()

            identity = _capture_identity(page, f"https://www.tiktok.com/tag/{hashtag}")
            if identity is None:
                debug_path = BASE_DIR / f"tiktok_refresh_failed_{row_id}.png"
                page.screenshot(path=str(debug_path))
                raise RuntimeError(
                    f"No request carrying both device_id and odinId was seen within the capture "
                    f"window for account id={row_id}. Either this session's cookie has gone stale "
                    f"(re-capture ttwid/msToken/s_v_web_id from a real browser), or this device "
                    f"was never actually used for real before now (see this module's docstring). "
                    f"Saved a screenshot to {debug_path} - re-run with --show-browser to watch it live."
                )

            device_id, odin_id = identity
            cookie_header = "; ".join(
                f"{c['name']}={c['value']}" for c in context.cookies() if c["domain"].endswith("tiktok.com")
            )
        finally:
            browser.close()

    saved = update_tiktok_identity(row_id, device_id=device_id, odin_id=odin_id, cookie=cookie_header)
    logger.info(
        "tiktok_identity_refreshed" if saved else "tiktok_identity_refresh_not_saved",
        telegram=True,
        row_id=row_id,
        device_id=device_id,
        saved=saved,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", type=int, required=True, help="platform_accounts.id of the tiktok row to refresh")
    parser.add_argument("--hashtag", default="fyp", help="Hashtag page to browse while capturing identity (default: fyp)")
    parser.add_argument("--show-browser", action="store_true", help="Show the browser window instead of running headless")
    args = parser.parse_args()

    refresh_identity(args.account_id, hashtag=args.hashtag, headless=False if args.show_browser else None)
