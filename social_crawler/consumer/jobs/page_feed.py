"""Job cào bảng tin + tab Reels của một Page Facebook (type="page_feed")."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    DEFAULT_ACCOUNT_KEY,
    PAGE_FEED_REDIS_KEY_TMPL,
)
from social_crawler.consumer.common import (
    COMMENTS_BOOTSTRAP_MODULE,
    CRAWL_JOB_CANCEL_KEY_TMPL,
    FACEBOOK_PAGE_FEED_SPIDER,
    PYTHON_BIN,
    SCRAPY_BIN,
    CrawlJobFailed,
    CrawlJobSkipped,
    _raise_for_returncode,
    crawl_job_key,
    logger,
)
from social_crawler.consumer.process import _run_subprocess, _stopped_for
from social_crawler.consumer.sessions import _ensure_facebook_session, _ensure_page_reels_query
from social_crawler.runtime import lanes


async def _run_page_feed_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Xử lý type="page_feed" (chỉ Facebook) - cào theo nguồn: bảng tin bài của một Page/Profile (spider
    facebook_page_feed). Cần phiên Facebook + query bảng tin đã bắt cho tài khoản đang dùng; thiếu thì bootstrap
    --page-url ngay trên chính trang của request (trang nào cũng bắt được cùng một query bảng tin)."""
    platform = request.get("platform") or "facebook"
    if platform != "facebook":
        raise CrawlJobFailed(f"page_feed only supported on facebook, got {platform}")
    page_url = str(request.get("page_url") or "").strip()
    page_id = str(request.get("page_id") or "").strip()
    if not page_url and not page_id:
        raise CrawlJobFailed("page_feed request missing page_url/page_id")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = crawl_job_key(platform)
    if run_id:
        cache.set(
            job_key,
            {"run_id": run_id, "type": "page_feed", "keyword": page_url or page_id, "started_at": int(time.time())},
        )

    def _cleanup() -> None:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain):
        _cleanup()
        raise CrawlJobSkipped("page_feed cancelled before start")
    try:
        # Chạy theo làn (services/lanes.py): đúng tài khoản của làn; không có làn -> con trỏ active chung như cũ.
        lane_account = await asyncio.to_thread(lanes.account_for_job) if lanes.current_lane() is not None else None
        if lanes.current_lane() is not None and lane_account is None:
            raise CrawlJobSkipped("no facebook account free for this lane")
        if not await _ensure_facebook_session(
            lane_account, run_id=run_id, platform=platform, allow_other_account=lane_account is None
        ):
            raise CrawlJobFailed("facebook session refresh failed before page_feed crawl")
        account = lane_account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
        if not cache.get(PAGE_FEED_REDIS_KEY_TMPL.format(account=account)) and page_url:
            logger.info("page_feed_cache_missing_bootstrapping", account=account, page_url=page_url)
            returncode = await _run_subprocess(
                [PYTHON_BIN, "-m", COMMENTS_BOOTSTRAP_MODULE["facebook"], "--page-url", page_url, "--account", account],
                run_id=run_id,
                platform=platform,
                honor_platform_cancel=not bypass_drain,
            )
            if returncode != 0:
                raise CrawlJobFailed("page_feed bootstrap failed")
        # Lượt xem reel: thiếu query Reels thì thử bắt trên trang này (lỗi không làm hỏng lượt cào bảng tin). Trang chung
        # (không keyword_id) không cào Reels - xem FacebookPageFeedSpider.shared_page.
        if request.get("keyword_id"):
            await _ensure_page_reels_query(cache, account, page_url, run_id)
        args = [SCRAPY_BIN, "crawl", FACEBOOK_PAGE_FEED_SPIDER]
        # dedupe=false: job cào lại bài booking đang theo dõi (làn ưu tiên) - bài đã thấy vẫn phải gửi lại để cập nhật số.
        for key in ("page_url", "page_id", "keyword_id", "max_pages", "since_days", "dedupe"):
            if request.get(key):
                args += ["-a", f"{key}={request[key]}"]
        if lane_account:
            args += ["-a", f"account={lane_account}"]
        logger.info("page_feed_started", page_url=page_url, page_id=page_id, run_id=run_id)
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        _cleanup()
    _raise_for_returncode(returncode, f"facebook page_feed {page_url or page_id}")
    logger.info("page_feed_finished", page_url=page_url, page_id=page_id)
