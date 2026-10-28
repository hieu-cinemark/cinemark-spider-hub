"""Job cập nhật like/comment/share bài Facebook theo link, KHÔNG đăng nhập (type="post_metrics", 25/10/2026).

cinemark-api (services/tracked_posts.py) gửi danh sách bài đã có trong D1: {external_id, url, page_url}. Mỗi bài một
request GET trang bài (spiders/facebook/features/post_metrics) qua proxy không chở tài khoản Facebook/Threads nào, rồi
publish message "kind": "metrics" (post_id + reactions_count/comments_count/shares_count) lên raw_posts - ingest chỉ cập
nhật số của bài đã có. Không giữ tài khoản nào nên đi hàng đợi rảnh crawl_idle:facebook (consumer/loop.py): chạy
khi vòng lặp không có việc khác hoặc trong giờ yên lặng Meta.

Proxy bị chặn (redirect login / trang không có số / lỗi HTTP) FB_LOGGED_OUT_BLOCK_STRIKES lần liền thì nghỉ
FB_LOGGED_OUT_COOLDOWN_MINUTES phút (khoá Redis riêng, không đụng cooldown chung của platform_proxies - TikTok vẫn dùng
proxy đó bình thường). Bài không lấy được số (bị chặn, hết proxy, FB_LOGGED_OUT_METRICS=0) rơi về đường cũ: job page_feed
có đăng nhập của page chứa bài, lấy từ request["fallback_page_feeds"] (cinemark-api dựng sẵn), đẩy vào làn ưu tiên.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from typing import Any

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CrawlJobFailed,
    CrawlJobSkipped,
    crawl_job_key,
    logger,
)
from social_crawler.consumer.process import _stopped_for
from social_crawler.db.proxies import ProxyRow, list_proxies_without_meta_accounts
from social_crawler.db.proxy_settings import get_setting
from social_crawler.runtime.pool import build_proxy_url
from social_crawler.spiders.facebook.features.post_metrics.post_metrics import (
    BLOCKED_STATUSES,
    FetchResult,
    fetch_post_metrics,
)

# Công tắc chính là nút "fb_logged_out_metrics_enabled" ở thẻ Cài đặt proxy của dashboard (proxy_settings); biến môi
# trường này là công tắc khẩn trên máy chạy (0 = tắt bất kể dashboard).
ENABLED = os.getenv("FB_LOGGED_OUT_METRICS", "1") not in ("0", "false", "False", "")
# Trống = mọi proxy không chở tài khoản Meta (list_proxies_without_meta_accounts); "10,11" = chỉ các id đó (vẫn lọc Meta).
PROXY_IDS = {int(x) for x in os.getenv("FB_LOGGED_OUT_PROXY_IDS", "").split(",") if x.strip()}
DELAY_SECONDS = float(os.getenv("FB_LOGGED_OUT_DELAY", "4"))
JITTER_SECONDS = float(os.getenv("FB_LOGGED_OUT_JITTER", "2"))
BLOCK_STRIKES = int(os.getenv("FB_LOGGED_OUT_BLOCK_STRIKES", "3"))
COOLDOWN_MINUTES = int(os.getenv("FB_LOGGED_OUT_COOLDOWN_MINUTES", "60"))

PROXY_COOLDOWN_KEY_TMPL = "fb_logged_out:proxy_cooldown:{proxy_id}"


class _ProxyRing:
    """Xoay vòng các proxy dùng được; proxy bị chặn BLOCK_STRIKES lần liền thì nghỉ (khoá Redis có TTL)."""

    def __init__(self, cache: RedisCache, proxies: list[ProxyRow]) -> None:
        self._cache = cache
        self._proxies = [p for p in proxies if not cache.get(PROXY_COOLDOWN_KEY_TMPL.format(proxy_id=p["id"]))]
        self._strikes: dict[int, int] = {}
        self._next = 0

    def __bool__(self) -> bool:
        return bool(self._proxies)

    def take(self) -> ProxyRow | None:
        if not self._proxies:
            return None
        proxy = self._proxies[self._next % len(self._proxies)]
        self._next += 1
        return proxy

    def report(self, proxy: ProxyRow, result: FetchResult) -> None:
        if result.status not in BLOCKED_STATUSES:
            self._strikes[proxy["id"]] = 0
            return
        strikes = self._strikes.get(proxy["id"], 0) + 1
        self._strikes[proxy["id"]] = strikes
        if strikes < BLOCK_STRIKES:
            return
        self._cache.set(
            PROXY_COOLDOWN_KEY_TMPL.format(proxy_id=proxy["id"]),
            {"status": result.status, "http_status": result.http_status, "at": int(time.time())},
            ttl_seconds=COOLDOWN_MINUTES * 60,
        )
        self._proxies = [p for p in self._proxies if p["id"] != proxy["id"]]
        logger.warning(
            "post_metrics_proxy_blocked",
            proxy_id=proxy["id"],
            status=result.status,
            http_status=result.http_status,
            cooldown_minutes=COOLDOWN_MINUTES,
            proxies_left=len(self._proxies),
        )


def _usable_proxies() -> list[ProxyRow]:
    proxies = list_proxies_without_meta_accounts()
    return [p for p in proxies if p["id"] in PROXY_IDS] if PROXY_IDS else proxies


def _queue_fallbacks(cache: RedisCache, request: dict[str, Any], failed: list[dict[str, Any]]) -> int:
    """Đẩy job page_feed (dựng sẵn bởi cinemark-api) của mỗi page có bài lỗi vào làn ưu tiên; trả về số job đã đẩy."""
    from social_crawler.consumer.loop import PRIORITY_QUEUE_KEY_TMPL  # loop -> dispatch -> job này: import vòng

    fallbacks = request.get("fallback_page_feeds") or {}
    pages = sorted({page for post in failed if (page := post.get("page_url")) and page in fallbacks})
    for page_url in pages:
        cache.lpush_json(
            PRIORITY_QUEUE_KEY_TMPL.format(platform="facebook"),
            {**fallbacks[page_url], "published_at": time.time()},
        )
    return len(pages)


def _metrics_message(post: dict[str, Any], result: FetchResult) -> dict[str, Any]:
    message: dict[str, Any] = {
        "platform": "facebook",
        "kind": "metrics",
        "source": "logged_out",
        "post_id": post["external_id"],
        "url": post["url"],
        "reactions_count": result.metrics["like"],
    }
    if result.metrics.get("comment") is not None:
        message["comments_count"] = result.metrics["comment"]
    if result.metrics.get("share") is not None:
        message["shares_count"] = result.metrics["share"]
    return message


async def _run_post_metrics(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    platform = request.get("platform") or "facebook"
    if platform != "facebook":
        raise CrawlJobFailed(f"post_metrics only supported on facebook, got {platform}")
    posts = [p for p in request.get("posts") or [] if p.get("external_id") and p.get("url")]
    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = crawl_job_key(platform)
    if run_id:
        cache.set(
            job_key,
            {"run_id": run_id, "type": "post_metrics", "keyword": f"{len(posts)} bài", "started_at": int(time.time())},
        )
    stats: dict[str, int] = {"posts": len(posts), "ok": 0, "unavailable": 0, "blocked": 0, "unsent": 0}
    failed: list[dict[str, Any]] = []
    try:
        if _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain):
            raise CrawlJobSkipped("post_metrics cancelled before start")
        enabled = ENABLED and bool(await asyncio.to_thread(get_setting, "fb_logged_out_metrics_enabled"))
        ring = _ProxyRing(cache, await asyncio.to_thread(_usable_proxies)) if enabled else None
        if not ring:
            logger.info("post_metrics_no_proxy_fallback", enabled=enabled, posts=len(posts))
            failed = posts
        else:
            publisher = KafkaPublisher()
            await publisher.start()
            try:
                for index, post in enumerate(posts):
                    if _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain):
                        raise CrawlJobSkipped("post_metrics cancelled")
                    proxy = ring.take()
                    if proxy is None:
                        # Mọi proxy đều đang nghỉ: phần còn lại đi đường page_feed.
                        failed.extend(posts[index:])
                        logger.warning("post_metrics_all_proxies_blocked", remaining=len(posts) - index)
                        break
                    result = await asyncio.to_thread(fetch_post_metrics, post["url"], build_proxy_url(proxy))
                    ring.report(proxy, result)
                    logger.info(
                        "post_metrics_fetched",
                        post_id=post["external_id"],
                        status=result.status,
                        http_status=result.http_status,
                        proxy_id=proxy["id"],
                        **{k: v for k, v in result.metrics.items() if v is not None},
                    )
                    if result.status == "ok":
                        sent = await publisher.publish(
                            RAW_POSTS_TOPIC, key=f"facebook:{post['external_id']}", value=_metrics_message(post, result)
                        )
                        stats["ok" if sent else "unsent"] += 1
                        if not sent:
                            failed.append(post)
                    else:
                        stats["unavailable" if result.status == "unavailable" else "blocked"] += 1
                        failed.append(post)
                    if index < len(posts) - 1:
                        await asyncio.sleep(max(0.0, DELAY_SECONDS + random.uniform(-JITTER_SECONDS, JITTER_SECONDS)))
            finally:
                await publisher.stop()
        stats["fallback_pages"] = await asyncio.to_thread(_queue_fallbacks, cache, request, failed)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))
    logger.info("post_metrics_finished", run_id=run_id, **stats)
