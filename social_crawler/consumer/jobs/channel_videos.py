"""Job danh sách video một kênh TikTok (type="channel_videos")."""

from __future__ import annotations

import time
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CRAWL_JOB_KEY_TMPL,
    SCRAPY_BIN,
    TIKTOK_CHANNEL_VIDEOS_SPIDER,
    CrawlJobFailed,
    CrawlJobSkipped,
    _raise_for_returncode,
    logger,
)
from social_crawler.consumer.process import _run_subprocess, _stopped_for


async def _run_channel_videos_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Xử lý type="channel_videos" (chỉ TikTok) - scrapy tiktok_channel_videos cho một
    @username. Dùng chung crawl_job:tiktok với tìm kiếm hashtag / comments (cùng vòng lặp
    consumer từng-cái-một)."""
    platform = request.get("platform") or "tiktok"
    if platform != "tiktok":
        logger.warning("unsupported_channel_videos_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"channel_videos only supported on tiktok, got {platform}")

    username = str(request.get("username") or "").lstrip("@").strip()
    if not username:
        logger.warning("channel_videos_missing_username", request=request)
        raise CrawlJobFailed("channel_videos request missing username")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "type": "channel_videos",
                "username": username,
                "started_at": int(time.time()),
            },
        )

    def _stopped() -> bool:
        return _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain)

    if _stopped():
        logger.info("channel_videos_cancelled_before_start", username=username)
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))
        raise CrawlJobSkipped("channel_videos cancelled before start")

    args = [SCRAPY_BIN, "crawl", TIKTOK_CHANNEL_VIDEOS_SPIDER, "-a", f"username={username}"]
    if request.get("keyword_id"):
        args += ["-a", f"keyword_id={request['keyword_id']}"]
    if request.get("max_pages"):
        args += ["-a", f"max_pages={request['max_pages']}"]

    logger.info("channel_videos_started", username=username, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    _raise_for_returncode(returncode, f"tiktok channel_videos username={username}")
    logger.info("channel_videos_finished", username=username)
