"""Job hồ sơ / follower kênh Threads (type="profiles")."""

from __future__ import annotations

import time
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CRAWL_JOB_KEY_TMPL,
    PYTHON_BIN,
    THREADS_PROFILES_MODULE,
    CrawlJobFailed,
    CrawlJobSkipped,
    _raise_for_returncode,
    logger,
)
from social_crawler.consumer.process import _run_subprocess, _stopped_for
from social_crawler.consumer.sessions import _ensure_threads_session


async def _run_threads_profiles(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Xử lý type="profiles" (chỉ Threads - publish_threads_profiles_request của cinemark-api): chạy worker hồ sơ kênh
    (spiders/threads/features/profiles/profiles.py) trên hàng đợi handle mà cinemark-api đã SADD vào Redis. Dùng chung
    crawl_job:threads và vòng lặp từng-cái-một với search/comments, nên không bao giờ hỏi song song trên cùng session."""
    platform = request.get("platform") or "threads"
    if platform != "threads":
        logger.warning("unsupported_profiles_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"profiles only supported on threads, got {platform}")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "profiles", "started_at": int(time.time())})

    def _cleanup() -> None:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain):
        _cleanup()
        raise CrawlJobSkipped("profiles cancelled before start")
    args = [PYTHON_BIN, "-m", THREADS_PROFILES_MODULE]
    if request.get("max"):
        args += ["--max", str(request["max"])]
    if run_id:
        args += ["--run-id", str(run_id)]
    logger.info("threads_profiles_started", run_id=run_id, max=request.get("max"))
    try:
        if not await _ensure_threads_session(run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain):
            raise CrawlJobFailed("threads session refresh failed before profiles run")
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        _cleanup()
    _raise_for_returncode(returncode, "threads profiles")
    logger.info("threads_profiles_finished", run_id=run_id)
