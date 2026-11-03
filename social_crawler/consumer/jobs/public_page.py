"""Job đọc một page Facebook không đăng nhập (type="public_page", 03/11/2026), chạy song song ở các worker riêng trong
consumer/loop.py (hàng đợi crawl_public_pages:facebook).

cinemark-api (source_pages.fill_public_pages_queue) thấy hàng đợi vơi là nạp thêm. Bài gửi lên raw_posts đúng dạng spider
page_feed, nên ingest và lịch giãn cách trang nguồn không phải đổi gì. Proxy chỉ lấy trong FB_PUBLIC_PAGES_PROXY_IDS, thuộc
pool public và không gán cho tài khoản nào; mỗi proxy chạy tối đa MAX_PER_PROXY job cùng lúc; bị chặn liền BLOCK_STRIKES lần thì nghỉ
COOLDOWN_MINUTES.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from typing import Any

from curl_cffi import requests as cffi_requests

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import SEEN_POSTS_KEY, SEEN_POSTS_TTL_SECONDS
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CrawlJobFailed,
    CrawlJobRetryable,
    CrawlJobSkipped,
    logger,
)
from social_crawler.consumer.process import _cancel_requested
from social_crawler.db.proxies import ProxyRow, list_public_page_proxies
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
# Proxy IP động (03/11/2026): lệnh chặn gắn với IP, nên IP đã đổi sang IP chưa bị chặn thì khỏi chờ hết cooldown. IP tĩnh không
# đổi nên vẫn nghỉ đủ COOLDOWN_MINUTES. IP từng bị chặn được nhớ riêng vì cổng xoay có thể quay lại IP cũ.
IP_CHECK_SECONDS = int(os.getenv("FB_PUBLIC_PAGES_IP_CHECK_SECONDS", "60"))
PROXY_BLOCKED_IP_KEY_TMPL = "facebook:public_pages:proxy_blocked_ip:{proxy_id}"
BLOCKED_IP_KEY_TMPL = "facebook:public_pages:blocked_ip:{ip}"
IP_CHECK_KEY_TMPL = "facebook:public_pages:ip_check:{proxy_id}"
# Nhịp theo phản hồi: dấu hiệu bị hạn chế làm chậm gấp đôi, job sạch giảm một nửa. IP đổi chưa chứng minh hạn chế đã hết, nên
# phải nghỉ tối thiểu MIN_REST_SECONDS x hệ số mới đo IP, và IP mới vẫn chạy với nhịp chậm đó.
BASE_INTERVAL_SECONDS = float(os.getenv("FB_PUBLIC_PAGES_REQUEST_INTERVAL", "5"))
MAX_PACE = int(os.getenv("FB_PUBLIC_PAGES_MAX_PACE", "8"))
MIN_REST_SECONDS = int(os.getenv("FB_PUBLIC_PAGES_MIN_REST_SECONDS", "300"))
PACE_KEY_TMPL = "facebook:public_pages:pace:{proxy_id}"
PACE_TTL_SECONDS = 86400
STATS_KEY_TMPL = "facebook:public_pages:stats:{hour}"
STATS_TTL_SECONDS = 7 * 86400
# Dòng "đang chạy" trên trang Hàng đợi; cinemark-api (crawl_jobs) đọc mọi crawl_job:facebook:public_page:*. Có TTL vì restart giữa
# job không chạy tới finally; lúc khởi động worker cũng xoá.
RUNNING_KEY_TMPL = "crawl_job:facebook:public_page:{proxy_id}"
RUNNING_TTL_SECONDS = 6 * 3600
BATCH_PUBLISH_TIMEOUT_SECONDS = 120
INFO_KEY_TMPL = "facebook:public_page_info:{url}"
INFO_TTL_SECONDS = 30 * 86400
# Hết proxy dùng được: báo cinemark-api ngừng xếp một lúc, không thì page bị tính là đã đọc và phải chờ tới hạn sau.
PAUSED_KEY = "facebook:public_pages:paused"
PAUSED_TTL_SECONDS = 600


_active: Counter[int] = Counter()
_pick_lock = asyncio.Lock()


class PublicPageProxyUnavailable(CrawlJobRetryable):
    """A queued job must wait for a proxy slot; it has not been crawled or completed."""


def _exit_ip(proxy: ProxyRow) -> str | None:
    try:
        url = build_proxy_url(proxy)
        response = cffi_requests.get("https://api.ipify.org", proxies={"http": url, "https": url}, timeout=10)
    except Exception:  # noqa: BLE001 - không đo được thì giữ nguyên cooldown
        return None
    ip = (response.text or "").strip()
    return ip if response.status_code == 200 and ip.replace(".", "").isdigit() and ip.count(".") == 3 else None


def _released_by_new_ip(cache: RedisCache, proxy: ProxyRow) -> bool:
    """Tối đa một lần đo mỗi IP_CHECK_SECONDS cho mỗi proxy. Lúc chặn không đo được IP thì lần đo này làm mốc."""
    proxy_id = proxy["id"]
    if not cache.set_nx(IP_CHECK_KEY_TMPL.format(proxy_id=proxy_id), 1, ttl_seconds=IP_CHECK_SECONDS):
        return False
    ip = _exit_ip(proxy)
    if not ip:
        return False
    blocked_ip = cache.get(PROXY_BLOCKED_IP_KEY_TMPL.format(proxy_id=proxy_id))
    if blocked_ip is None:
        _remember_blocked_ip(cache, proxy_id, ip)
        return False
    if ip == blocked_ip or cache.get(BLOCKED_IP_KEY_TMPL.format(ip=ip)):
        return False
    cache.delete(COOLDOWN_KEY_TMPL.format(proxy_id=proxy_id))
    cache.delete(STRIKES_KEY_TMPL.format(proxy_id=proxy_id))
    cache.delete(PROXY_BLOCKED_IP_KEY_TMPL.format(proxy_id=proxy_id))
    logger.info("public_pages_proxy_ip_changed", proxy_id=proxy_id, blocked_ip=blocked_ip, new_ip=ip)
    return True


def _remember_blocked_ip(cache: RedisCache, proxy_id: int, ip: str) -> None:
    ttl = COOLDOWN_MINUTES * 60
    cache.set(PROXY_BLOCKED_IP_KEY_TMPL.format(proxy_id=proxy_id), ip, ttl_seconds=ttl)
    cache.set(BLOCKED_IP_KEY_TMPL.format(ip=ip), 1, ttl_seconds=ttl)


def _pick_proxy(cache: RedisCache) -> ProxyRow | None:
    usable = {p["id"]: p for p in list_public_page_proxies()}
    for proxy_id in PROXY_IDS:
        if proxy_id not in usable or _active[proxy_id] >= MAX_PER_PROXY:
            continue
        if not cache.get(COOLDOWN_KEY_TMPL.format(proxy_id=proxy_id)) or _released_by_new_ip(cache, usable[proxy_id]):
            return usable[proxy_id]
    return None


def has_running_jobs() -> bool:
    return any(_active.values())


def _pace(cache: RedisCache, proxy_id: int) -> int:
    return max(1, int(cache.get(PACE_KEY_TMPL.format(proxy_id=proxy_id)) or 1))


def _report(cache: RedisCache, proxy: ProxyRow, status: str, http_status: int) -> None:
    key = STRIKES_KEY_TMPL.format(proxy_id=proxy["id"])
    pace = _pace(cache, proxy["id"])
    pace_key = PACE_KEY_TMPL.format(proxy_id=proxy["id"])
    if status not in BLOCKED_STATUSES:
        cache.delete(key)
        if pace > 1:
            cache.set(pace_key, pace // 2, ttl_seconds=PACE_TTL_SECONDS)
        return
    pace = min(MAX_PACE, pace * 2)
    cache.set(pace_key, pace, ttl_seconds=PACE_TTL_SECONDS)
    strikes = int(cache.get(key) or 0) + 1
    # An explicit server restriction is decisive; never send two more jobs to confirm a rate limit.
    if status not in {"rate_limited", "access_restricted"} and strikes < BLOCK_STRIKES:
        cache.set(key, strikes, ttl_seconds=COOLDOWN_MINUTES * 60)
        return
    cache.delete(key)
    cache.set(COOLDOWN_KEY_TMPL.format(proxy_id=proxy["id"]), status, ttl_seconds=COOLDOWN_MINUTES * 60)
    cache.set(
        IP_CHECK_KEY_TMPL.format(proxy_id=proxy["id"]), 1, ttl_seconds=max(IP_CHECK_SECONDS, MIN_REST_SECONDS * pace)
    )
    # Đo trước khi job kế tiếp được gửi: IP này là IP vừa bị chặn. Cổng vừa xoay thì lần sau đo ra IP mới và chạy tiếp ngay.
    if blocked_ip := _exit_ip(proxy):
        _remember_blocked_ip(cache, proxy["id"], blocked_ip)
    logger.warning(
        "public_pages_proxy_blocked",
        proxy_id=proxy["id"],
        status=status,
        http_status=http_status,
        cooldown_minutes=COOLDOWN_MINUTES,
        pace=pace,
    )


def _record_stats(cache: RedisCache, proxy_id: int, result: Any, new_posts: int) -> None:
    """Đếm theo giờ, proxy và endpoint: HGETALL social_crawler:facebook:public_pages:stats:<YYYYMMDDHH> (giờ UTC)."""
    counts = {
        "html_ok": 1 if result.status == "ok" else 0,
        "html_blocked": 1 if result.status in BLOCKED_STATUSES else 0,
        "feed_ok": max(0, result.pages_fetched - 1),
        "feed_blocked": 1 if result.pagination_status in BLOCKED_STATUSES else 0,
        "new_posts": new_posts,
    }
    key = STATS_KEY_TMPL.format(hour=time.strftime("%Y%m%d%H", time.gmtime()))
    for name, amount in counts.items():
        if amount:
            cache.hincrby(key, f"proxy{proxy_id}:{name}", amount, ttl_seconds=STATS_TTL_SECONDS)


async def _publish_new_posts(
    cache: RedisCache, request: dict[str, Any], posts: list[dict[str, Any]], kafka: Any
) -> int:
    fresh = [p for p in posts if not cache.get(f"{SEEN_POSTS_KEY}:{p['post_id']}")]
    if not fresh:
        return 0
    delivered = 0
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
            raise PublicPageProxyUnavailable("no public page proxy available")
        _active[proxy["id"]] += 1
    try:
        await _crawl_page(cache, request, url, proxy, max_pages, since_days)
    finally:
        _active[proxy["id"]] -= 1


async def _crawl_page(
    cache: RedisCache, request: dict[str, Any], url: str, proxy: ProxyRow, max_pages: int, since_days: int
) -> None:
    run_id = request.get("run_id")
    running_key = RUNNING_KEY_TMPL.format(proxy_id=proxy["id"])
    cache.set(
        running_key,
        {"run_id": run_id, "type": "public_page", "keyword": url, "started_at": int(time.time())},
        ttl_seconds=RUNNING_TTL_SECONDS,
    )
    loop = asyncio.get_running_loop()
    kafka = KafkaPublisher()
    await kafka.start()
    new_posts = 0

    def publish_batch(posts: list[dict[str, Any]]) -> bool:
        """Chạy trong luồng của fetch: gửi xong trang này rồi mới đọc trang sau, để job bị ngắt vẫn giữ được phần đã lấy."""
        nonlocal new_posts
        try:
            new_posts += asyncio.run_coroutine_threadsafe(
                _publish_new_posts(cache, request, posts, kafka), loop
            ).result(BATCH_PUBLISH_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - dừng job, cửa sổ này không được ghi là đã đọc
            logger.warning("public_page_batch_publish_failed", page_url=url, error=str(exc)[:200])
            return False
        return True

    try:
        result = await asyncio.to_thread(
            fetch_public_page,
            url,
            build_proxy_url(proxy),
            max_pages=max_pages,
            since_days=since_days,
            cache=cache,
            should_stop=lambda: is_platform_draining("facebook") or _cancel_requested(run_id=run_id),
            on_batch=publish_batch,
            request_interval=BASE_INTERVAL_SECONDS * _pace(cache, proxy["id"]),
        )
        # Lỗi gửi Kafka không nói gì về proxy: không tính vào chặn, cũng không được coi là job sạch để giảm nhịp.
        if result.pagination_status != "publish_failed":
            await asyncio.to_thread(
                _report, cache, proxy, result.pagination_status or result.status, result.http_status
            )
        if result.status != "ok":
            await asyncio.to_thread(_record_stats, cache, proxy["id"], result, 0)
            raise CrawlJobSkipped(f"public page {result.status}")
        # Bắt các bài chưa gửi được ở trên; bài đã gửi có dấu đã thấy nên không gửi lại.
        new_posts += await _publish_new_posts(cache, request, result.posts, kafka)
    finally:
        cache.delete(running_key)
        if run_id:
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))
        await kafka.stop()
    await asyncio.to_thread(_record_stats, cache, proxy["id"], result, new_posts)
    if result.pagination_status:
        logger.warning(
            "public_page_pagination_stopped",
            page_url=url,
            proxy_id=proxy["id"],
            pages_fetched=result.pages_fetched,
            stop_reason=result.stop_reason,
            status=result.pagination_status,
        )
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
            "windows_done": result.windows_done,
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
        windows_done=result.windows_done,
        since_days=since_days,
    )
