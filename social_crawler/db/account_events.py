"""Dòng thời gian theo tài khoản (04/11/2026): tài khoản chết thì đọc lại được chuyện gì đã xảy ra, theo thứ tự.

Không có khoá ngoại tới platform_accounts: dòng tài khoản bị xoá thì lịch sử vẫn còn. Ghi lỗi không bao giờ làm hỏng việc đang
chạy. Không bao giờ truyền giá trị cookie/token/mật khẩu vào `details`.
"""

from __future__ import annotations

from typing import Any

from social_crawler.core.logger import get_logger
from social_crawler.db import d1, d1_pool

logger = get_logger(__name__)


def record(platform: str, account_key: str | None, event: str, **details: Any) -> None:
    key = (account_key or "").strip().lower()
    if not key:
        return
    try:
        d1_pool.record_event(platform, key, event, details)
    except (d1.D1Error, TypeError) as exc:
        logger.warning("account_event_write_failed", event_name=event, error_type=type(exc).__name__)


def timeline(platform: str, keys: list[str], *, limit: int = 200) -> list[dict[str, Any]]:
    """Cũ trước, mới sau. Một tài khoản có thể được ghi theo email hoặc account_id nên nhận nhiều key."""
    return d1_pool.timeline(platform, keys, limit)
