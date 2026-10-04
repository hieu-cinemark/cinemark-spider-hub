"""
Import một phiên threads.com từ cookie lấy được ngoài Playwright (một trình duyệt đã đăng
nhập, không tự động) thẳng vào Redis - giống social_crawler.spiders.facebook.auth.cookies,
nhưng threads.com cần domain cookie và bộ cookie bắt buộc riêng: một phiên
Instagram/threads đã đăng nhập mang ds_user_id/sessionid, không phải c_user/xs của
Facebook (đã xác nhận với header Cookie của một request thật bắt được).
parse_cookie_header/extract_user_agent hoàn toàn chung (không giả định gì riêng của
Facebook), nên được dùng lại nguyên từ đó thay vì lặp lại ở đây.
"""

from __future__ import annotations

import time

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.threads import DEFAULT_ACCOUNT_KEY, REQUIRED_LOGIN_COOKIES, STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.cookies import extract_user_agent, parse_cookie_header
from social_crawler.spiders.threads.auth.accounts import account_key as normalize_account_key

logger = get_logger(__name__)

__all__ = [
    "extract_user_agent",
    "parse_cookie_header",
    "build_storage_state_from_cookies",
    "import_cookies",
    "REQUIRED_LOGIN_COOKIES",
]


def build_storage_state_from_cookies(cookies: dict[str, str] | list[dict] | str) -> dict:
    """Cùng ý tưởng với facebook.auth.cookies.build_storage_state_from_cookies, nhưng cho domain
    cookie .threads.com."""
    if isinstance(cookies, str):
        cookies = parse_cookie_header(cookies)

    if isinstance(cookies, dict):
        expires = time.time() + 365 * 24 * 3600
        cookie_list = [
            {
                "name": name,
                "value": value,
                "domain": ".threads.com",
                "path": "/",
                "expires": expires,
                "httpOnly": name in ("sessionid", "ds_user_id"),
                "secure": True,
                "sameSite": "Lax",
            }
            for name, value in cookies.items()
            if name.lower() != "useragent"
        ]
    else:
        cookie_list = cookies

    return {"cookies": cookie_list, "origins": []}


def import_cookies(cookies: dict[str, str] | list[dict] | str, account: str | None = None) -> None:
    """Bỏ qua hẳn luồng đăng nhập tương tác - xem facebook.auth.cookies.import_cookies để biết
    đầy đủ lý do. Cách dùng:

        python -m social_crawler.spiders.threads.auth.bootstrap \\
            --cookies-file my_cookies.json --account "you@example.com"
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
