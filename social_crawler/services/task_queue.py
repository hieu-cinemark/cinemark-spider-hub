"""Helper hàng đợi thu thập ở phía consumer - cùng các key Redis mà
cinemark-api/app/services/task_queue.py ghi khi một message crawl_requests được publish."""

from __future__ import annotations

import time
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.logger import get_logger

logger = get_logger(__name__)

HISTORY_LIMIT = 200


def _pending_key(platform: str) -> str:
    return f"task_pending:{platform}"


def is_platform_draining(platform: str) -> bool:
    cache = RedisCache()
    return cache.exists(f"platform_drain:{platform}") or cache.exists(f"comments_drain:{platform}")


def stopped_at(platform: str) -> float | None:
    """Thời điểm epoch của lần bấm Dừng gần nhất của nền tảng - crawl_jobs.request_stop của
    cinemark-api lưu nó làm giá trị của platform_drain. None khi không drain, hoặc khi cờ đến
    từ một bản cinemark-api cũ chỉ lưu "1" trơn."""
    value = RedisCache().get(f"platform_drain:{platform}")
    return float(value) if isinstance(value, (int, float)) and value > 1 else None


def start_task(request: dict[str, Any]) -> None:
    platform = request.get("platform")
    run_id = request.get("run_id")
    if not platform or not run_id:
        # Request thiếu một trong hai cái này thì hoàn toàn không theo dõi được - màn hình
        # đang chạy/lịch sử trên dashboard sẽ âm thầm không bao giờ hiển thị nó, trừ khi được log
        # ở đây.
        logger.warning("task_tracking_skipped", reason="missing_platform_or_run_id", request=request)
        return
    cache = RedisCache()
    cache.lrem_by_id(_pending_key(platform), run_id)
    logger.debug("task_started", platform=platform, run_id=run_id)


def finish_task(request: dict[str, Any], status: str, error: str | None = None) -> None:
    platform = request.get("platform")
    run_id = request.get("run_id")
    if platform and run_id:
        RedisCache().lrem_by_id(_pending_key(platform), run_id)
    else:
        # Cùng lỗ hổng theo dõi như phép kiểm tra của start_task - mục lịch sử bên dưới vẫn được
        # ghi dù thế nào, nhưng task này không bao giờ được xoá khỏi task_pending, nên nó sẽ hiện
        # mãi là "đang chạy" trên dashboard.
        logger.warning("task_pending_clear_skipped", reason="missing_platform_or_run_id", request=request)
    logger.debug("task_finished", platform=platform, run_id=run_id, status=status)
    item = {
        "id": run_id or "",
        "platform": platform or "",
        "type": request.get("type") or "search",
        "label": request.get("keyword")
        or request.get("post_id")
        or request.get("account")
        or request.get("account_key")
        or "",
        "keyword_id": request.get("keyword_id"),
        "post_id": request.get("post_id"),
        "status": status,
        "finished_at": int(time.time()),
        "error": error,
    }
    RedisCache().lpush_capped("task_history", item, HISTORY_LIMIT)
