"""Giới hạn thời gian làm việc liên tục có đăng nhập qua nhiều job mà không bỏ con trỏ phân trang.

Tài khoản vẫn được giữ trong lúc nghỉ; consumer vẫn theo dõi/huỷ được tiến trình con.
Các mặc định này là ngân sách của ứng dụng, không phải ngưỡng an toàn của Facebook.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from social_crawler.core.logger import get_logger
from social_crawler.db.proxy_settings import get_setting

logger = get_logger(__name__)
KEY = "facebook:run_budget:{account}"


def cfg_max_requests() -> int:
    return int(get_setting("fb_run_max_requests"))


def cfg_max_seconds() -> int:
    return int(get_setting("fb_run_max_seconds"))


def cfg_rest_seconds() -> int:
    return int(get_setting("fb_run_rest_seconds"))


def reserve(account: str, cache, check_lease: Callable[[], None], on_resume: Callable[[], None]) -> None:
    if cfg_max_requests() <= 0 and cfg_max_seconds() <= 0:
        return
    if cfg_rest_seconds() <= 0:
        raise RuntimeError("Facebook run rest must be positive when run limits are enabled")
    key = KEY.format(account=account.strip().lower())
    state = cache.get(key)
    if state is not None and not isinstance(state, dict):
        raise RuntimeError("Invalid Facebook run budget state")
    now = time.time()
    state = state or {"started_at": now, "used": 0, "last_at": now}
    if not state.get("pause_until") and now - float(state["last_at"]) >= cfg_rest_seconds():
        state = {"started_at": now, "used": 0, "last_at": now}
    exhausted = (cfg_max_requests() > 0 and int(state["used"]) >= cfg_max_requests()) or (
        cfg_max_seconds() > 0 and now - float(state["started_at"]) >= cfg_max_seconds()
    )
    if exhausted and not state.get("pause_until"):
        state = {**state, "pause_until": now + cfg_rest_seconds()}
        cache.set(key, state, ttl_seconds=172800)
        logger.info("facebook_run_rest", account=account, requests=state["used"], rest_seconds=cfg_rest_seconds())
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
