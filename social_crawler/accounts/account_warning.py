"""Dò cảnh báo của Meta trên trang đang mở ("Chúng tôi nghi ngờ hành vi tự động", "Bạn tạm thời bị chặn"...) - 30/10/2026.

Cảnh báo này là nấc trước checkpoint: tài khoản đã bị gắn cờ nhưng chưa bị khoá. Trước đây spider-hub chỉ nhận ra khi đã
bị đẩy sang /checkpoint/ hoặc mất phiên; ý từ repo hopfian/fbk (ACCOUNT_WARNING_MARKERS - quét mọi trang tải về và dừng
đẩy tiếp khi thấy). Thấy cảnh báo thì cho tài khoản nghỉ WARNING_REST_HOURS giờ (cooldown_until, không tắt, không ghi
lỗi) và báo Telegram - nghỉ sớm rẻ hơn mất tài khoản.

Chỉ đọc CHỮ HIỂN THỊ, không đọc HTML thô: HTML/JS bundle của Facebook chứa sẵn mọi chuỗi giao diện kể cả mấy câu này
(bản fbk quét HTML thô). Hộp thoại / thông báo (role=dialog|alert) luôn được đọc; cả thân trang chỉ đọc khi trang không
có bài viết nào (trang chặn chiếm cả màn hình) - nội dung bài viết có thể chứa đúng mấy chữ đó ("hoạt động bất thường").
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

WARNING_REST_HOURS = float(os.getenv("META_WARNING_REST_HOURS", "12"))

# Chữ thường; khớp chuỗi con. Câu dài, đủ riêng để không trùng chữ thường gặp trong giao diện.
WARNING_MARKERS = (
    "suspect automated behavio",  # behavior / behaviour
    "you're temporarily blocked",
    "you’re temporarily blocked",
    "temporarily restricted",
    "unusual activity on your account",
    "suspicious activity on your account",
    "nghi ngờ có hành vi tự động",
    "nghi ngờ hành vi tự động",
    "bạn tạm thời bị chặn",
    "tạm thời bị hạn chế",
    "hoạt động bất thường trên tài khoản",
    "hoạt động đáng ngờ trên tài khoản",
)

_OVERLAY_SELECTOR = '[role="dialog"], [role="alertdialog"], [role="alert"]'
_CONTENT_SELECTOR = '[role="article"], [role="feed"]'


def find_marker(text: str) -> str | None:
    lowered = (text or "").lower()
    return next((marker for marker in WARNING_MARKERS if marker in lowered), None)


def _visible_text(page) -> str:
    parts: list[str] = []
    try:
        for locator in page.locator(_OVERLAY_SELECTOR).all()[:10]:
            parts.append(locator.inner_text(timeout=2000))
        if page.locator(_CONTENT_SELECTOR).count() == 0:
            parts.append(page.inner_text("body", timeout=3000))
    except Exception as exc:  # noqa: BLE001 - dò cảnh báo không được làm hỏng lượt chạy
        logger.debug("account_warning_read_failed", error=str(exc)[:200])
    return "\n".join(parts)


def detect_warning(page) -> str | None:
    """Câu cảnh báo tìm thấy trên trang Playwright đang mở, None khi không có."""
    return find_marker(_visible_text(page))


def rest_warned_account(platform: str, account_key: str, marker: str, *, where: str, url: str = "") -> bool:
    """Cho tài khoản nghỉ WARNING_REST_HOURS giờ + báo Telegram. Trả về True khi đã ghi được cooldown."""
    from social_crawler.db.accounts import rest_account_until

    until = datetime.now(tz=UTC) + timedelta(hours=WARNING_REST_HOURS)
    rested = rest_account_until(platform, account_key, until)
    logger.error(
        "account_warning_detected",
        telegram=True,
        platform=platform,
        account=account_key,
        marker=marker,
        where=where,
        url=url[:200],
        rest_hours=WARNING_REST_HOURS if rested else None,
    )
    return rested
