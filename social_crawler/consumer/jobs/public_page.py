"""Job đọc một page Facebook không đăng nhập (type="public_page", 03/11/2026), chạy song song ở các worker riêng trong
consumer/loop.py (hàng đợi crawl_public_pages:facebook).

cinemark-api (source_pages.fill_public_pages_queue) thấy hàng đợi vơi là nạp thêm. Bài gửi lên raw_posts đúng dạng spider
page_feed, nên ingest và lịch giãn cách trang nguồn không phải đổi gì. Proxy chỉ lấy trong FB_PUBLIC_PAGES_PROXY_IDS và phải
không gán cho tài khoản Meta nào; mỗi proxy chạy tối đa MAX_PER_PROXY job cùng lúc; bị chặn liền BLOCK_STRIKES lần thì nghỉ
COOLDOWN_MINUTES.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from typing import Any

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import SEEN_POSTS_KEY, SEEN_POSTS_TTL_SECONDS
from social_crawler.consumer.common import CrawlJobFailed, CrawlJobRetryable, CrawlJobSkipped, logger
from social_crawler.db.proxies import ProxyRow, list_proxies_without_meta_accounts
from social_crawler.runtime.pool import build_proxy_url
from social_crawler.runtime.task_queue import is_platform_draining
from social_crawler.spiders.facebook.features.public_page.public_page import BLOCKED_STATUSES, fetch_public_page

PROXY_IDS = [int(x) for x in os.getenv("FB_PUBLIC_PAGES_PROXY_IDS", "15,10").split(",") if x.strip()]
# Cùng một IP chạy song song là nhân đôi tốc độ request lên IP đó, nên mặc định mỗi IP một job.
MAX_PER_PROXY = max(1, int(os.getenv("FB_PUBLIC_PAGES_PER_PROXY", "1")))
WORKERS = len(PROXY_IDS) * MAX_PER_PROXY
BLOCK_STRIKES = int(os.getenv("FB_PUBLIC_PAGES_BLOCK_STRIKES", "3"))
# #10 vẫn bị chặn sau hơn 40 phút trong lần đo 03/11/2026.
COOLDOWN_MINUTES = int(os.getenv("FB_PUBLIC_PAGES_COOLDOWN_MINUTES", "120"))
MAX_PAGES = int(os.getenv("FB_PUBLIC_PAGES_MAX_PAGES", "0"))
SINCE_DAYS = int(os.getenv("FB_PUBLIC_PAGES_SINCE_DAYS", "60"))
COOLDOWN_KEY_TMPL = "facebook:public_pages:proxy_cooldown:{proxy_id}"
STRIKES_KEY_TMPL = "facebook:public_pages:proxy_strikes:{proxy_id}"
INFO_KEY_TMPL = "facebook:public_page_info:{url}"
INFO_TTL_SECONDS = 30 * 86400
# Hết proxy dùng được: báo cinemark-api ngừng xếp một lúc, không thì page bị tính là đã đọc và phải chờ tới hạn sau.
PAUSED_KEY = "facebook:public_pages:paused"
PAUSED_TTL_SECONDS = 600


_active: Counter[int] = Counter()
_pick_lock = asyncio.Lock()


def _pick_proxy(cache: RedisCache) -> ProxyRow | None:
    usable = {p["id"]: p for p in list_proxies_without_meta_accounts()}
    for proxy_id in PROXY_IDS:
        if (
            proxy_id in usable
            and _active[proxy_id] < MAX_PER_PROXY
            and not cache.get(COOLDOWN_KEY_TMPL.format(proxy_id=proxy_id))
        ):
            return usable[proxy_id]
    return None


def has_running_jobs() -> bool:
    return any(_active.values())


def _report(cache: RedisCache, proxy: ProxyRow, status: str, http_status: int) -> None:
    key = STRIKES_KEY_TMPL.format(proxy_id=proxy["id"])
    if status not in BLOCKED_STATUSES:
        cache.delete(key)
        return
    strikes = int(cache.get(key) or 0) + 1
    if strikes < BLOCK_STRIKES:
        cache.set(key, strikes, ttl_seconds=COOLDOWN_MINUTES * 60)
        return
    cache.delete(key)
    cache.set(COOLDOWN_KEY_TMPL.format(proxy_id=proxy["id"]), status, ttl_seconds=COOLDOWN_MINUTES * 60)
    logger.warning(
        "public_pages_proxy_blocked",
        proxy_id=proxy["id"],
        status=status,
        http_status=http_status,
        cooldown_minutes=COOLDOWN_MINUTES,
    )


async def _publish_new_posts(cache: RedisCache, request: dict[str, Any], posts: list[dict[str, Any]]) -> int:
    fresh = [p for p in posts if not cache.get(f"{SEEN_POSTS_KEY}:{p['post_id']}")]
    if not fresh:
        return 0
    kafka = KafkaPublisher()
    await kafka.start()
    delivered = 0
    try:
        for post in fresh:
            seen_key = f"{SEEN_POSTS_KEY}:{post['post_id']}"
            if not cache.add_if_new(seen_key, SEEN_POSTS_TTL_SECONDS):
                continue
            try:
                sent = await kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"facebook:{post['post_id']}",
                    value={
                        "platform": "facebook",
                        "keyword_id": request.get("keyword_id"),
                        "source": "page",
                        "source_page": request["page_url"],
                        **post,
                    },
                )
            except Exception:
                cache.delete(seen_key)
                raise
            if sent:
                delivered += 1
            else:
                cache.delete(seen_key)
    finally:
        await kafka.stop()
    # A concurrent worker may have claimed some of these posts; only unsent, unclaimed posts need retry.
    if any(not cache.get(f"{SEEN_POSTS_KEY}:{post['post_id']}") for post in fresh):
        raise CrawlJobRetryable(f"public page posts not delivered: {request['page_url']}")
    return delivered


async def _run_public_page(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    url = request.get("page_url")
    if not url:
        raise CrawlJobFailed("public_page request missing page_url")
    try:
        max_pages = int(request.get("max_pages", MAX_PAGES))
        since_days = int(request.get("since_days", SINCE_DAYS))
        if max_pages < 0 or since_days <= 0:
            raise ValueError
    except TypeError, ValueError:
        raise CrawlJobFailed("public_page requires max_pages >= 0 and since_days > 0") from None
    cache = RedisCache()
    async with _pick_lock:
        proxy = await asyncio.to_thread(_pick_proxy, cache)
        if proxy is None:
            if not has_running_jobs():
                cache.set(PAUSED_KEY, 1, ttl_seconds=PAUSED_TTL_SECONDS)
            raise CrawlJobSkipped("no public page proxy available")
        _active[proxy["id"]] += 1
    try:
        await _crawl_page(cache, request, url, proxy, max_pages, since_days)
    finally:
        _active[proxy["id"]] -= 1


async def _crawl_page(
    cache: RedisCache, request: dict[str, Any], url: str, proxy: ProxyRow, max_pages: int, since_days: int
) -> None:
    result = await asyncio.to_thread(
        fetch_public_page,
        url,
        build_proxy_url(proxy),
        max_pages=max_pages,
        since_days=since_days,
        cache=cache,
        should_stop=lambda: is_platform_draining("facebook"),
    )
    await asyncio.to_thread(_report, cache, proxy, result.pagination_status or result.status, result.http_status)
    if result.status != "ok":
        raise CrawlJobSkipped(f"public page {result.status}")
    if result.pagination_status:
        logger.warning(
            "public_page_pagination_stopped",
            page_url=url,
            proxy_id=proxy["id"],
            pages_fetched=result.pages_fetched,
            stop_reason=result.stop_reason,
            status=result.pagination_status,
        )
    new_posts = await _publish_new_posts(cache, request, result.posts)
    cache.set(INFO_KEY_TMPL.format(url=url), result.info, ttl_seconds=INFO_TTL_SECONDS)
    # Cùng key spider page_feed ghi: cinemark-api đọc để lưu page_id và giãn lịch trang không có bài mới.
    cache.set(
        f"page_feed_last:{url}",
        {
            "new": new_posts,
            "at": int(time.time()),
            "page_id": result.info.get("page_id"),
            "pages_fetched": result.pages_fetched,
            "stop_reason": result.stop_reason,
            "pagination_status": result.pagination_status,
            "since_ts": result.since_ts,
            "until_ts": result.until_ts,
        },
        ttl_seconds=14 * 86400,
    )
    logger.info(
        "public_page_crawled",
        page_url=url,
        proxy_id=proxy["id"],
        followers=result.info.get("followers"),
        posts=len(result.posts),
        new_posts=new_posts,
        pages_fetched=result.pages_fetched,
        stop_reason=result.stop_reason,
        pagination_status=result.pagination_status,
        since_days=since_days,
    )
