"""Collection-queue helpers on the consumer side - same Redis keys
cinemark-api/app/services/task_queue.py writes when a crawl_requests
message is published."""

from __future__ import annotations

import json
import time
from typing import Any

from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache

logger = get_logger(__name__)

HISTORY_LIMIT = 200


def _pending_key(platform: str) -> str:
    return f"task_pending:{platform}"


def is_platform_draining(platform: str) -> bool:
    cache = RedisCache()
    return cache.exists(f"platform_drain:{platform}") or cache.exists(f"comments_drain:{platform}")


def start_task(request: dict[str, Any]) -> None:
    platform = request.get("platform")
    run_id = request.get("run_id")
    if not platform or not run_id:
        # A request missing either of these can't be tracked at all - the
        # dashboard's running/history view will simply never show it,
        # silently, unless this is logged here.
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
        # Same tracking gap as start_task's own check - the history item
        # below still gets written either way, but this task never clears
        # from task_pending, so it would show as perpetually "running" on
        # the dashboard.
        logger.warning("task_pending_clear_skipped", reason="missing_platform_or_run_id", request=request)
    logger.debug("task_finished", platform=platform, run_id=run_id, status=status)
    item = {
        "id": run_id or "",
        "platform": platform or "",
        "type": request.get("type") or "search",
        "label": request.get("keyword") or request.get("post_id") or request.get("account") or request.get("account_key") or "",
        "keyword_id": request.get("keyword_id"),
        "post_id": request.get("post_id"),
        "status": status,
        "finished_at": int(time.time()),
        "error": error,
    }
    RedisCache().lpush_capped("task_history", item, HISTORY_LIMIT)
