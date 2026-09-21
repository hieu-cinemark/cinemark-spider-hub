"""Helpers for picking a TikTok web identity without Playwright.

device_id/odin_id on a logged-in row must stay the pair from the original
trusted Chrome session. A later Patchright visit often emits a *new* pair
that signs challenge/detail fine as a guest and then gets an empty
item_list when user_is_login=true.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse
import re

from social_crawler.spiders.facebook.auth.cookies import parse_cookie_header as parse_raw_cookie_header

ITEM_LIST_PATH = "/api/challenge/item_list/"


def _quoted_cli_arg(text: str, flag: str) -> str | None:
    token = f"{flag} "
    idx = text.find(token)
    if idx < 0:
        return None
    rest = text[idx + len(token) :].lstrip()
    if not rest or rest[0] not in "'\"":
        return None
    quote = rest[0]
    end = rest.find(quote, 1)
    if end < 0:
        return None
    return rest[1:end]


def odin_from_multi_sids(cookie: str | None) -> str | None:
    """multi_sids is `{odinId}:{sessionid}` (URL-encoded)."""
    names = parse_raw_cookie_header(cookie or "")
    raw = unquote(names.get("multi_sids") or "")
    prefix = raw.split(":", 1)[0].strip()
    return prefix if is_trusted_device_id(prefix) else None


def parse_browser_export(raw: str) -> tuple[str, tuple[str, str] | None]:
    """Cookie header plus device_id/odinId when the paste is a Chrome cURL."""
    text = (raw or "").strip()
    url = _quoted_cli_arg(text, "--url")
    if url is None:
        match = re.search(r"'(https://www\.tiktok\.com[^']+)'", text)
        url = match.group(1) if match else None
    cookie_header = _quoted_cli_arg(text, "-b") or _quoted_cli_arg(text, "--cookie")
    if cookie_header is None and "curl " not in text[:80].lower():
        cookie_header = text
        pair = identity_from_url(text)
        return cookie_header, pair
    pair = identity_from_url(url or "")
    return cookie_header or text, pair


def is_trusted_device_id(value: str | None) -> bool:
    """TikTok web device_id/odinId are long numeric strings. Email-shaped
    account_id leftovers from before identity capture are not trusted."""
    text = (value or "").strip()
    return text.isdigit() and len(text) >= 10


def stored_identity(account: dict) -> tuple[str, str] | None:
    device_id = str(account.get("id") or "").strip()
    odin_id = str(account.get("token") or "").strip()
    if not (is_trusted_device_id(device_id) and is_trusted_device_id(odin_id)):
        return None
    cookie_odin = odin_from_multi_sids(account.get("cookie"))
    if cookie_odin is not None and cookie_odin != odin_id:
        return None
    return device_id, odin_id


def identity_from_url(url: str) -> tuple[str, str] | None:
    if "tiktok.com" not in url:
        return None
    query = parse_qs(urlparse(url).query)
    device_id = (query.get("device_id") or [""])[0]
    odin_id = (query.get("odinId") or [""])[0]
    if device_id and odin_id:
        return device_id, odin_id
    return None


def is_item_list_url(url: str) -> bool:
    return ITEM_LIST_PATH in urlparse(url).path


def prefer_item_list_identity(urls: list[str]) -> tuple[str, str] | None:
    """First item_list pair wins; otherwise the first signed TikTok URL."""
    fallback: tuple[str, str] | None = None
    for url in urls:
        pair = identity_from_url(url)
        if pair is None:
            continue
        if is_item_list_url(url):
            return pair
        if fallback is None:
            fallback = pair
    return fallback


@dataclass(frozen=True)
class IdentityChoice:
    device_id: str
    odin_id: str
    source: str
    playwright_pair: tuple[str, str] | None = None


def choose_identity(account: dict, captured: tuple[str, str] | None) -> IdentityChoice:
    """Keep a trusted stored pair even when Playwright captured a different one."""
    existing = stored_identity(account)
    if existing is not None:
        return IdentityChoice(
            device_id=existing[0],
            odin_id=existing[1],
            source="stored",
            playwright_pair=captured,
        )
    if captured is not None:
        return IdentityChoice(
            device_id=captured[0],
            odin_id=captured[1],
            source="playwright",
            playwright_pair=captured,
        )
    raise RuntimeError(
        "No device_id/odin_id on this tiktok row and Playwright did not capture a pair. "
        "Paste a Cookie header from a real Chrome session that already has browsing history, "
        "or capture identity from a logged-in /api/challenge/item_list/ request."
    )


def cookies_for_identity(source: str, original: dict[str, str], playwright: dict[str, str]) -> dict[str, str]:
    """Stored device_id/odin_id must stay paired with the cookie jar they
    were captured with. Merging Playwright's ttwid/msToken/s_v_web_id onto
    that pair is what makes challenge/detail succeed and item_list 200-empty."""
    if source == "stored":
        return dict(original)
    return {**original, **playwright}
