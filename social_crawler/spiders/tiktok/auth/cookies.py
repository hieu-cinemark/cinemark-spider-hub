"""
Dựng storage_state Playwright từ trường `cookie` thô của một dòng platform_accounts tiktok
- cùng ý tưởng với facebook.auth.cookies.build_storage_state_from_cookies /
threads.auth.cookies.build_storage_state_from_cookies, nhưng cho domain cookie
.tiktok.com. parse_cookie_header / load_exported_cookies hoàn toàn chung, nên được dùng
lại từ facebook.auth.cookies.

import_cookies ghi session đã dán lên dòng platform_accounts đã ghim (TikTok không có cache
storage_state trong Redis). Bước bắt danh tính tiếp theo vẫn nằm ở auth/bootstrap.py, cùng
hai bước như import cookie rồi refresh token của Facebook.
"""

from __future__ import annotations

import json
import re
import time

from social_crawler.db.accounts import get_account_pk, update_account_cookie, update_tiktok_identity
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.facebook.auth.cookies import parse_cookie_header as parse_raw_cookie_header

logger = get_logger(__name__)

# TikTok web đã đăng nhập: ttwid là cookie khách/thiết bị mà mọi request cần; sessionid mới
# là phiên đăng nhập thật (client.py đặt user_is_login dựa vào nó).
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
    """Bỏ rác do Playwright/tách header (giá trị bị đẩy lên thành tên). Một lần restore từng lưu
    một khối MSA 400 ký tự kết thúc bằng `|tt_csrf_token` làm tên cookie; item_list đã đăng
    nhập sau đó trả 200 rỗng."""
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
    """Cùng ý tưởng với facebook.auth.cookies.build_storage_state_from_cookies, nhưng cho domain
    cookie .tiktok.com."""
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
    """Ghi một header Cookie đã đăng nhập do người xuất lên một dòng platform_accounts tiktok.
    Nhận cả bản dán Copy-as-cURL của Chrome: device_id/odinId trong query string được lưu cùng
    jar để restore không giữ một danh tính cũ từ phiên trước.

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
        if not update_tiktok_identity(row_id, device_id=pair[0], odin_id=pair[1], cookie=header, lookup_key=key):
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
