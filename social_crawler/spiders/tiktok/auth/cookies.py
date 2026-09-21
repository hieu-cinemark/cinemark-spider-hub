"""
Builds a Playwright storage_state from a tiktok platform_accounts row's raw
`cookie` field - same idea as facebook.auth.cookies.build_storage_state_from_cookies
/ threads.auth.cookies.build_storage_state_from_cookies, but for the
.tiktok.com cookie domain. parse_cookie_header / load_exported_cookies are
fully generic, so they're reused from facebook.auth.cookies.

import_cookies writes the pasted session onto the pinned platform_accounts
row (TikTok has no Redis storage_state cache). The follow-up identity
capture still lives in auth/bootstrap.py, same two-step as Facebook's
cookie-import then token refresh.
"""

from __future__ import annotations

import json
import re
import time

from social_crawler.logger import get_logger
from social_crawler.services.db import get_account_pk, update_account_cookie, update_tiktok_identity
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.facebook.auth.cookies import parse_cookie_header as parse_raw_cookie_header

logger = get_logger(__name__)

# Logged-in TikTok web: ttwid is the guest/device cookie every request needs;
# sessionid is the actual login (client.py sets user_is_login from it).
REQUIRED_LOGIN_COOKIES = ("ttwid", "sessionid")
_COOKIE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_MAX_COOKIE_NAME_LEN = 64

__all__ = [
    "REQUIRED_LOGIN_COOKIES",
    "build_storage_state_from_cookies",
    "cookie_map",
    "import_cookies",
    "is_valid_cookie_name",
    "parse_cookie_header",
    "to_cookie_header",
]


def is_valid_cookie_name(name: str) -> bool:
    """Drop Playwright/header-split debris (values promoted to names). A
    restore once stored a 400-char MSA blob ending in `|tt_csrf_token` as a
    cookie name; logged-in item_list then 200'd empty."""
    return bool(name) and len(name) <= _MAX_COOKIE_NAME_LEN and _COOKIE_NAME_RE.fullmatch(name) is not None


def parse_cookie_header(raw: str) -> dict[str, str]:
    return cookie_map(parse_raw_cookie_header(raw or ""))


def cookie_map(cookies: dict[str, str] | list[dict] | str) -> dict[str, str]:
    if isinstance(cookies, str):
        text = cookies.strip()
        if text[:1] in "[{":
            try:
                return cookie_map(json.loads(text))
            except json.JSONDecodeError:
                pass
        return cookie_map(parse_raw_cookie_header(text))
    if isinstance(cookies, list):
        items = ((str(c["name"]), str(c.get("value") or "")) for c in cookies if c.get("name"))
    else:
        items = ((str(k), str(v)) for k, v in cookies.items())
    return {name: value for name, value in items if is_valid_cookie_name(name) and ";" not in value}


def to_cookie_header(cookies: dict[str, str] | list[dict] | str) -> str:
    mapping = cookie_map(cookies)
    return "; ".join(f"{name}={value}" for name, value in mapping.items())


def build_storage_state_from_cookies(cookies: dict[str, str] | list[dict] | str) -> dict:
    """Same idea as facebook.auth.cookies.build_storage_state_from_cookies,
    but for the .tiktok.com cookie domain."""
    mapping = cookie_map(cookies)
    expires = time.time() + 365 * 24 * 3600
    cookie_list = [
        {
            "name": name,
            "value": value,
            "domain": ".tiktok.com",
            "path": "/",
            "expires": expires,
            "httpOnly": name in ("sessionid", "sid_tt", "sid_guard"),
            "secure": True,
            "sameSite": "Lax",
        }
        for name, value in mapping.items()
    ]
    return {"cookies": cookie_list, "origins": []}


def import_cookies(cookies: dict[str, str] | list[dict] | str, account: str | None = None) -> None:
    """Write a human-exported logged-in Cookie header onto one tiktok
    platform_accounts row. A Chrome Copy-as-cURL paste is accepted: the
    query string's device_id/odinId is saved with the jar so restore does
    not keep a stale identity from a previous session.

        python -m social_crawler.spiders.tiktok.auth.bootstrap \\
            --cookies-file my_cookies.txt --account "the-account-id"
    """
    if not account or not str(account).strip():
        raise RuntimeError("TikTok cookie import needs --account so the session is saved on the right row.")

    pair = None
    if isinstance(cookies, str):
        from social_crawler.spiders.tiktok.auth.identity import parse_browser_export

        cookie_header, pair = parse_browser_export(cookies)
        mapping = cookie_map(cookie_header)
    else:
        mapping = cookie_map(cookies)

    missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in mapping]
    if missing:
        raise RuntimeError(
            f"Missing required cookie(s) {missing} - a logged-in TikTok session needs at least "
            f"{REQUIRED_LOGIN_COOKIES}. Got: {sorted(mapping)}"
        )
    key = normalize_account_key(account)
    row_id = get_account_pk("tiktok", key)
    if row_id is None:
        raise RuntimeError(f"No tiktok platform_accounts row matching {key!r}")
    header = to_cookie_header(mapping)
    if pair is not None:
        if not update_tiktok_identity(
            row_id, device_id=pair[0], odin_id=pair[1], cookie=header, lookup_key=key
        ):
            raise RuntimeError(f"Could not save TikTok identity for {key!r}")
        logger.info(
            "imported_cookies",
            account=key,
            row_id=row_id,
            cookie_count=len(mapping),
            names=sorted(mapping),
            device_id=pair[0],
            identity_source="curl",
        )
        return
    if not update_account_cookie("tiktok", row_id, header):
        raise RuntimeError(f"Could not save TikTok cookies for {key!r}")
    logger.info(
        "imported_cookies",
        account=key,
        row_id=row_id,
        cookie_count=len(mapping),
        names=sorted(mapping),
        identity_source="cookie_header",
    )
