"""Đọc JSON từ nguồn ngoài (Redis) mà không để một bản ghi hỏng làm hỏng cả danh sách."""

from __future__ import annotations

import json
from typing import Any

from social_crawler.core.logger import get_logger

logger = get_logger(__name__)


def loads_or_none(raw: str | bytes | None, *, source: str) -> Any | None:
    """`json.loads` cho một bản ghi đọc từ Redis. Rỗng hoặc hỏng thì trả None; bản hỏng được log `json_decode_skipped`
    (mức debug, kèm `source`) để chỗ gọi bỏ qua bản ghi đó mà vẫn truy vết được."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError) as exc:
        logger.debug("json_decode_skipped", source=source, error=exc)
        return None
