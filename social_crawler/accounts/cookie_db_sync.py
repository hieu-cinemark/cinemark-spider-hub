"""Ghi cookie Facebook đã xoay về DB (27/11/2026).

Facebook cấp cookie mới (xoay `xs`, `fr`...) trong lúc cào. Trước đây chúng chỉ nằm ở Redis của MÁY ĐANG CHẠY, còn cột `cookie`
trong DB giữ nguyên bản nhập ban đầu. Mac và VPS dùng chung DB nhưng mỗi máy một Redis, nên máy thứ hai dựng phiên từ cookie
gốc đã cũ trong khi máy thứ nhất đã có cookie mới: Facebook thấy cookie cũ bị phát lại nên đăng xuất/checkpoint.

Giờ cookie mới được ghi về cột `cookie` (giữ nguyên `useragent=<base64>`), nên máy nào dựng phiên từ DB cũng lấy bản mới nhất.
Chỉ ghi cột cookie, không đụng `updated_at`: spider-hub coi `updated_at` mới là "đã có cookie mới" cho tài khoản đã chết, còn đây
chỉ là cookie của phiên đang sống. Hạn chế một lần ghi mỗi SYNC_INTERVAL_SECONDS cho mỗi tài khoản (hạn mức ghi D1).
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from social_crawler.core.logger import get_logger
from social_crawler.db import accounts as db_accounts

logger = get_logger(__name__)

SYNC_INTERVAL_SECONDS = 600
USER_AGENT_PART = "useragent"
_STATE_KEY = "facebook:cookie_db_sync:{account}"


def build_header(stored: str, cookies: dict[str, str]) -> str | None:
    """Chuỗi cookie mới: giữ thứ tự và `useragent` của bản đang lưu, cập nhật giá trị theo `cookies`, thêm cookie mới, bỏ
    cookie đã bị Facebook xoá. None khi phiên không còn là phiên đăng nhập hợp lệ (không bao giờ ghi phiên hỏng đè lên DB)."""
    c_user, xs = cookies.get("c_user", ""), cookies.get("xs", "")
    if not c_user.isdigit() or not xs or xs == "deleted":
        return None
    parts: dict[str, str] = {}
    for part in stored.split(";"):
        name, sep, value = part.strip().partition("=")
        if sep and name:
            parts[name.strip()] = value.strip()
    for name, value in cookies.items():
        if value in (None, "", "deleted"):
            parts.pop(name, None)
        elif name != USER_AGENT_PART:
            parts[name] = str(value)
    return "; ".join(f"{name}={value}" for name, value in parts.items())


def sync(cache: Any, account: str, cookies: dict[str, str], *, now: float | None = None) -> bool:
    """Ghi cookie hiện tại của `account` về DB nếu đổi và đã quá SYNC_INTERVAL_SECONDS kể từ lần ghi trước. Không bao giờ ném
    lỗi: cào quan trọng hơn đồng bộ. True khi thật sự đã ghi."""
    now = time.time() if now is None else now
    try:
        key = _STATE_KEY.format(account=account)
        last = cache.get(key) or {}
        row_pk = db_accounts.get_account_pk("facebook", account)
        row = db_accounts.get_account_by_key("facebook", account)
        if row_pk is None or row is None or str(row.get("id")) != cookies.get("c_user"):
            return False
        header = build_header(row.get("cookie") or "", cookies)
        if header is None:
            return False
        digest = hashlib.sha256(header.encode()).hexdigest()[:16]
        if digest == last.get("digest") or header == (row.get("cookie") or ""):
            return False
        if now - float(last.get("at", 0)) < SYNC_INTERVAL_SECONDS:
            return False
        if not db_accounts.update_account_cookie("facebook", row_pk, header):
            return False
        cache.set(key, {"digest": digest, "at": now}, 7 * 86400)
        logger.info("facebook_cookie_db_synced", account=account, cookie_names=sorted(cookies))
        return True
    except Exception as exc:  # noqa: BLE001 - đồng bộ DB không được làm hỏng lượt cào
        logger.warning("facebook_cookie_db_sync_failed", account=account, error_type=type(exc).__name__)
        return False
