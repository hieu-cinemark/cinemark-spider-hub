"""
Builds a Playwright storage_state from a tiktok platform_accounts row's raw
`cookie` field - same idea as facebook.auth.cookies.build_storage_state_from_cookies
/ threads.auth.cookies.build_storage_state_from_cookies, but for the
.tiktok.com cookie domain. parse_cookie_header is fully generic (no
Facebook-specific assumptions), so it's reused from there as-is.
"""

from __future__ import annotations

import time

from social_crawler.spiders.facebook.auth.cookies import parse_cookie_header

__all__ = ["build_storage_state_from_cookies", "parse_cookie_header"]


def build_storage_state_from_cookies(cookies: dict[str, str] | str) -> dict:
    """Same idea as facebook.auth.cookies.build_storage_state_from_cookies,
    but for the .tiktok.com cookie domain. Only ever fed a plain {name:
    value} mapping or raw header string here (a tiktok platform_accounts row
    never carries a full Playwright cookie-list export), unlike the
    Facebook/Threads versions which also accept one."""
    if isinstance(cookies, str):
        cookies = parse_cookie_header(cookies)

    expires = time.time() + 365 * 24 * 3600
    cookie_list = [
        {
            "name": name,
            "value": value,
            "domain": ".tiktok.com",
            "path": "/",
            "expires": expires,
            "httpOnly": False,
            "secure": True,
            "sameSite": "Lax",
        }
        for name, value in cookies.items()
    ]
    return {"cookies": cookie_list, "origins": []}
