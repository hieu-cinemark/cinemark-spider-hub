"""
Import một phiên Facebook từ cookie lấy được ngoài Playwright (một trình duyệt đã đăng
nhập, không tự động) thẳng vào Redis - bỏ qua hẳn luồng đăng nhập tương tác/tự động.
"""

from __future__ import annotations

import base64
import json
import time
from urllib.parse import unquote

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import DEFAULT_ACCOUNT_KEY, STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key

logger = get_logger(__name__)

# Một phiên Facebook đã đăng nhập cần ít nhất hai cookie này.
REQUIRED_LOGIN_COOKIES = ("c_user", "xs")

# Một số chợ mua bán tài khoản Facebook nối thêm một cookie giả "useragent" vào chuỗi cookie
# thô - không phải cookie thật, chỉ là User-Agent mã hoá base64 (+ percent) của trình duyệt
# đã thực sự dùng để đăng nhập và lấy các cookie này. Đưa session cho Facebook với một UA
# không khớp với UA nó thấy lúc đăng nhập tự nó đã là tín hiệu lệch, nên nó bị tách ra trước
# khi dựng danh sách cookie thật và được giải mã riêng để chỗ gọi khởi chạy browser context
# bằng nó.
USER_AGENT_COOKIE_KEY = "useragent"


def extract_user_agent(cookies: dict[str, str]) -> str | None:
    """Lấy User-Agent thật ra từ cookie giả USER_AGENT_COOKIE_KEY, nếu có. Trả về None nếu không
    có hoặc không giải mã được (cố gắng hết mức - gợi ý UA thiếu/hỏng không đáng làm hỏng cả
    lần import)."""
    raw = cookies.get(USER_AGENT_COOKIE_KEY)
    if not raw:
        return None
    try:
        return base64.b64decode(unquote(raw)).decode("utf-8")
    except Exception:
        return None


def load_exported_cookies(raw: str) -> dict[str, str] | list[dict] | str:
    """Nhận bất cứ thứ gì người dùng thực sự dán từ trình duyệt thật: JSON ({name: value} hoặc
    danh sách cookie Playwright) *hoặc* một header Cookie thô (`c_user=...; xs=...`). Người
    vận hành dashboard chép từ tab Network của DevTools thường hơn là xuất file .json;
    bootstrap.py trước đây json.loads file vô điều kiện và crash với JSONDecodeError ở ký tự 0
    với các chuỗi header đó."""
    text = (raw or "").strip().lstrip("\ufeff")
    if not text:
        raise RuntimeError("Cookie import was empty.")
    if text.lower().startswith("cookie:"):
        text = text.split(":", 1)[1].strip()
        if not text:
            raise RuntimeError("Cookie import was empty.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return text
    if isinstance(parsed, str):
        inner = parsed.strip()
        if not inner:
            raise RuntimeError("Cookie import was empty.")
        try:
            return json.loads(inner)
        except json.JSONDecodeError:
            return inner
    return parsed


def parse_cookie_header(raw: str) -> dict[str, str]:
    """Parse một chuỗi header `Cookie:` thô (thứ dễ chép nhất từ DevTools của trình duyệt ->
    tab Network -> chuột phải vào một request facebook.com -> Copy -> Copy as cURL / Copy
    request headers), ví dụ
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
    """Dựng một dict storage_state Playwright từ cookie lấy được ngoài script này (một phiên
    trình duyệt đã đăng nhập). Nhận:
      - một chuỗi header `Cookie:` thô ("c_user=123; xs=abc; ...")
      - một ánh xạ {name: value} đơn giản
      - một danh sách đầy đủ các dict cookie kiểu Playwright (name/value/domain/
        path/expires/httpOnly/secure/sameSite), ví dụ xuất từ một extension quản lý
        cookie - dùng nguyên, không cần đoán gì."""
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
    """Bỏ qua hẳn luồng đăng nhập tương tác: import cookie từ một phiên trình duyệt đã đăng nhập
    thẳng vào Redis, để lời gọi bootstrap()/bootstrap_comments() tiếp theo dùng lại chúng và
    đi thẳng tới bắt request headless - hoàn toàn không có bước đăng nhập tay. Hữu ích khi
    captcha/checkpoint của Facebook cứ thử thách lại trình duyệt do Playwright điều khiển
    (thứ bị gắn cờ là dấu vân tay tự động hoá của nó, không phải tài khoản hay mật khẩu) -
    thay vào đó đăng nhập từ một Chrome bình thường, không tự động, xuất cookie của nó, rồi
    import ở đây.

    Cách dùng:
        python -m social_crawler.spiders.facebook.auth.bootstrap \\
            --cookies-file my_cookies.json --account "you@example.com"
        # my_cookies.json có thể là {"c_user": "...", "xs": "...", ...}
        # hoặc một danh sách cookie đầy đủ kiểu Playwright.
        # --account nên khớp "email" của một mục (hoặc "id" nếu "email"
        # để trống) trong FACEBOOK_ACCOUNTS để vòng xoay dùng session này
        # thay vì thử auto-login lại; chỉ bỏ nó khi FACEBOOK_ACCOUNTS
        # hoàn toàn chưa được cấu hình. Lưu ý: tài khoản đã đặt trường
        # "cookie" riêng trong FACEBOOK_ACCOUNTS thì không cần bước này -
        # bootstrap.py tự import nó.
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
