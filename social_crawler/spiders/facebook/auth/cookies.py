"""
Imports a Facebook session from cookies obtained outside of Playwright (an
already-logged-in, non-automated browser) straight into Redis - skips the
interactive/auto login flow entirely.
"""

from __future__ import annotations

import base64
import time
from urllib.parse import unquote

from social_crawler.constants.facebook import DEFAULT_ACCOUNT_KEY, STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key

logger = get_logger(__name__)

# A logged-in Facebook session needs at least these two cookies.
REQUIRED_LOGIN_COOKIES = ("c_user", "xs")

# Some Facebook account marketplaces append a synthetic "useragent" pseudo-
# cookie to the raw cookie string - not a real cookie, just the base64
# (+ percent) encoded User-Agent of the browser that was actually used to
# log in and capture these cookies. Presenting the session to Facebook with
# a UA that doesn't match what it saw at login is itself a mismatch signal,
# so it's stripped out before building the real cookie list and decoded
# separately for callers to launch their browser context with instead.
USER_AGENT_COOKIE_KEY = "useragent"


def extract_user_agent(cookies: dict[str, str]) -> str | None:
    """Pulls the real User-Agent out of a USER_AGENT_COOKIE_KEY pseudo-cookie,
    if present. Returns None if absent or undecodable (best-effort - a
    missing/garbled UA hint isn't worth failing the whole import over)."""
    raw = cookies.get(USER_AGENT_COOKIE_KEY)
    if not raw:
        return None
    try:
        return base64.b64decode(unquote(raw)).decode("utf-8")
    except Exception:
        return None


def parse_cookie_header(raw: str) -> dict[str, str]:
    """Parse a raw `Cookie:` header string (the easiest thing to copy from a
    browser's DevTools -> Network tab -> right-click a facebook.com request
    -> Copy -> Copy as cURL / Copy request headers), e.g.
    "c_user=123; xs=abc; datr=xyz" -> {"c_user": "123", "xs": "abc", "datr": "xyz"}."""
    cookies = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, _, value = part.partition("=")
        cookies[name.strip()] = value.strip()
    return cookies


def build_storage_state_from_cookies(cookies: dict[str, str] | list[dict] | str) -> dict:
    """Build a Playwright storage_state dict from cookies obtained outside
    of this script (an already-logged-in browser session). Accepts:
      - a raw `Cookie:` header string ("c_user=123; xs=abc; ...")
      - a simple {name: value} mapping
      - a full list of Playwright-style cookie dicts (name/value/domain/
        path/expires/httpOnly/secure/sameSite), e.g. exported by a cookie
        manager extension - used as-is, no guessing needed."""
    if isinstance(cookies, str):
        cookies = parse_cookie_header(cookies)

    if isinstance(cookies, dict):
        expires = time.time() + 365 * 24 * 3600
        cookie_list = [
            {
                "name": name,
                "value": value,
                "domain": ".facebook.com",
                "path": "/",
                "expires": expires,
                "httpOnly": name in ("xs", "c_user", "fr"),
                "secure": True,
                "sameSite": "Lax",
            }
            for name, value in cookies.items()
            if name.lower() != USER_AGENT_COOKIE_KEY
        ]
    else:
        cookie_list = cookies

    return {"cookies": cookie_list, "origins": []}


def import_cookies(cookies: dict[str, str] | list[dict] | str, account: str | None = None) -> None:
    """Skip the interactive login flow entirely: import cookies from an
    already-logged-in browser session straight into Redis, so the next
    bootstrap()/bootstrap_comments() call reuses them and goes straight to
    headless capture - no manual login step at all. Useful when Facebook's
    captcha/checkpoint keeps re-challenging a Playwright-driven browser (its
    automation fingerprint is what's flagged, not the account or password) -
    log in from a normal, non-automated Chrome instead, export its cookies,
    and import those here.

    Usage:
        python -m social_crawler.spiders.facebook.auth.bootstrap \\
            --cookies-file my_cookies.json --account "you@example.com"
        # my_cookies.json can be either {"c_user": "...", "xs": "...", ...}
        # or a full Playwright-style cookie list.
        # --account should match an entry's "email" (or "id" if "email" is
        # blank) in FACEBOOK_ACCOUNTS so rotation picks up this session
        # instead of trying to auto-login again; omit it only when
        # FACEBOOK_ACCOUNTS isn't configured at all. Note: an account with
        # its own "cookie" field set in FACEBOOK_ACCOUNTS doesn't need this -
        # bootstrap.py imports that automatically.
    """
    storage_state = build_storage_state_from_cookies(cookies)
    cookie_names = {c["name"] for c in storage_state["cookies"]}

    missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in cookie_names]
    if missing:
        raise RuntimeError(
            f"Missing required cookie(s) {missing} - a valid logged-in session needs at least "
            f"{REQUIRED_LOGIN_COOKIES}. Got: {sorted(cookie_names)}"
        )

    key = normalize_account_key(account) if account else DEFAULT_ACCOUNT_KEY
    RedisCache().set(STATE_REDIS_KEY_TMPL.format(account=key), storage_state)
    logger.info("imported_cookies", account=key, cookie_count=len(cookie_names), names=sorted(cookie_names))
