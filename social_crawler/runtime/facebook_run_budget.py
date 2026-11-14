"""Giới hạn thời gian làm việc liên tục có đăng nhập qua nhiều job mà không bỏ con trỏ phân trang.

Tài khoản vẫn được giữ trong lúc nghỉ; consumer vẫn theo dõi/huỷ được tiến trình con.
Các mặc định này là ngân sách của ứng dụng, không phải ngưỡng an toàn của Facebook.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable

from social_crawler.core.logger import get_logger

logger = get_logger(__name__)
MAX_REQUESTS = int(os.getenv("FACEBOOK_RUN_MAX_REQUESTS", "100"))
MAX_SECONDS = int(os.getenv("FACEBOOK_RUN_MAX_SECONDS", "1800"))
REST_SECONDS = int(os.getenv("FACEBOOK_RUN_REST_SECONDS", "900"))
KEY = "facebook:run_budget:{account}"


def reserve(account: str, cache, check_lease: Callable[[], None], on_resume: Callable[[], None]) -> None:
    if MAX_REQUESTS <= 0 and MAX_SECONDS <= 0:
        return
    if REST_SECONDS <= 0:
        raise RuntimeError("Facebook run rest must be positive when run limits are enabled")
    key = KEY.format(account=account.strip().lower())
    state = cache.get(key)
    if state is not None and not isinstance(state, dict):
        raise RuntimeError("Invalid Facebook run budget state")
    now = time.time()
    state = state or {"started_at": now, "used": 0, "last_at": now}
    if not state.get("pause_until") and now - float(state["last_at"]) >= REST_SECONDS:
        state = {"started_at": now, "used": 0, "last_at": now}
    exhausted = (MAX_REQUESTS > 0 and int(state["used"]) >= MAX_REQUESTS) or (
        MAX_SECONDS > 0 and now - float(state["started_at"]) >= MAX_SECONDS
    )
    if exhausted and not state.get("pause_until"):
        state = {**state, "pause_until": now + REST_SECONDS}
        cache.set(key, state, ttl_seconds=172800)
        logger.info("facebook_run_rest", account=account, requests=state["used"], rest_seconds=REST_SECONDS)
    if state.get("pause_until"):
        while (remaining := float(state["pause_until"]) - time.time()) > 0:
            check_lease()
            time.sleep(min(30, remaining))
        check_lease()
        on_resume()
        now = time.time()
        state = {"started_at": now, "used": 0, "last_at": now}
        logger.info("facebook_run_resumed", account=account)
    check_lease()
    cache.set(key, {**state, "used": int(state["used"]) + 1, "last_at": time.time()}, ttl_seconds=172800)
