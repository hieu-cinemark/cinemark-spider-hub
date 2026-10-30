"""Một dòng log cho mỗi bài crawl được và đã gửi lên Kafka, để terminal / consumer.log /
trang Nhật ký của dashboard thấy ngay nội dung đang được thu thập (trước đây chỉ có
page_crawled với số lượng, phải mở D1 mới biết bài nào lọt vào)."""

from __future__ import annotations

import re
from typing import Any

_PREVIEW_CHARS = 120
_WHITESPACE_RE = re.compile(r"\s+")


def preview(text: str | None, limit: int = _PREVIEW_CHARS) -> str:
    """Nội dung gọn trên một dòng: gộp xuống dòng/khoảng trắng, cắt ở `limit` ký tự."""
    flat = _WHITESPACE_RE.sub(" ", text or "").strip()
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


def log_crawled_post(
    logger: Any,
    *,
    platform: str,
    post_id: str,
    text: str | None,
    author: str | None,
    url: str | None,
    **stats: Any,
) -> None:
    """`stats`: các số đếm của nền tảng (likes/comments/shares/views); giá trị None bị bỏ qua.
    `platform` truyền rõ vì logger gắn platform theo module gọi log, ở đây là module này."""
    logger.info(
        "post_crawled",
        platform=platform,
        post_id=post_id,
        author=author,
        text=preview(text),
        **{key: value for key, value in stats.items() if value is not None},
        url=url,
    )
