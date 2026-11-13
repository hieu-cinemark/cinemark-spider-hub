"""Job đọc kênh Threads không đăng nhập (type="threads_channel", 13/11/2026), chạy ở worker riêng trong consumer/loop.py
(hàng đợi crawl_public_pages:threads), cùng kiểu đọc page Facebook không đăng nhập (jobs/public_page.py).

Tìm kiếm và bình luận Threads vẫn dùng tài khoản; job này không đụng tài khoản nào. cinemark-api (source_pages) nạp hàng
đợi khi nó vơi. Bài gửi lên raw_posts với source="page" như page_feed, nên ingest gắn phim theo nội dung.

Lối ra: THREADS_CHANNEL_PROXY_IDS (proxy pool public, không gán tài khoản) lần lượt theo vòng; để trống thì đi thẳng IP máy.
Bị chặn (401/403/429/HTML) thì lối ra đó nghỉ THREADS_CHANNEL_COOLDOWN_MINUTES.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import time
from typing import Any

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache

# Cùng khoá "đã thấy" với spider tìm kiếm Threads: một bài đã gửi từ đường nào cũng không gửi lại trong TTL.
from social_crawler.constants.threads import SEEN_POSTS_KEY, SEEN_POSTS_TTL_SECONDS
from social_crawler.consumer.common import CrawlJobFailed, CrawlJobRetryable, logger
from social_crawler.consumer.process import _cancel_requested
from social_crawler.db.proxies import list_public_page_proxies
from social_crawler.runtime.pool import build_proxy_url
from social_crawler.runtime.task_queue import is_platform_draining
from social_crawler.spiders.threads.public.channel import fetch_channel, handle_from_url

QUEUE_KEY = "crawl_public_pages:threads"
PROCESSING_KEY = "crawl_public_pages:threads:processing"
PROXY_IDS = [int(x) for x in os.getenv("THREADS_CHANNEL_PROXY_IDS", "").split(",") if x.strip()]
WORKERS = max(0, int(os.getenv("THREADS_CHANNEL_WORKERS", "2")))
SINCE_DAYS = int(os.getenv("THREADS_CHANNEL_SINCE_DAYS", "30"))
MAX_PAGES = int(os.getenv("THREADS_CHANNEL_MAX_PAGES", "20"))
REQUEST_INTERVAL = float(os.getenv("THREADS_CHANNEL_REQUEST_INTERVAL", "3"))
COOLDOWN_MINUTES = int(os.getenv("THREADS_CHANNEL_COOLDOWN_MINUTES", "30"))
COOLDOWN_KEY_TMPL = "threads:channel:cooldown:{exit}"
PROFILE_KEY_TMPL = "threads:channel_profile:{handle}"
PROFILE_TTL_SECONDS = 30 * 86400
# cinemark-api đọc để giãn lịch kênh không có bài mới (quiet_runs) và thu hẹp khoảng ngày lần sau.
LAST_KEY_TMPL = "threads_channel_last:{url}"
LAST_TTL_SECONDS = 14 * 86400
RUNNING_KEY_TMPL = "crawl_job:threads:threads_channel:{slot}"
RUNNING_TTL_SECONDS = 3 * 3600
BATCH_PUBLISH_TIMEOUT_SECONDS = 120

_rotation = itertools.count()


class ThreadsChannelExitUnavailable(RuntimeError):
    """Mọi lối ra đang nghỉ: để job nằm lại hàng đợi."""


def _exits() -> list[tuple[str, str | None]]:
    """[(tên lối ra, proxy url hoặc None)]. Proxy trong danh sách mà không còn dùng được (tắt / đã gán tài khoản) bị bỏ."""
    if not PROXY_IDS:
        return [("direct", None)]
    usable = {p["id"]: p for p in list_public_page_proxies()}
    return [(f"proxy{pid}", build_proxy_url(usable[pid])) for pid in PROXY_IDS if pid in usable]


def pick_exit(cache: RedisCache) -> tuple[str, str | None] | None:
    exits = _exits()
    if not exits:
        return None
    start = next(_rotation)
    for i in range(len(exits)):
        name, url = exits[(start + i) % len(exits)]
        if not cache.get(COOLDOWN_KEY_TMPL.format(exit=name)):
            return name, url
    return None


async def _publish(
    cache: RedisCache, kafka: KafkaPublisher, request: dict[str, Any], posts: list[dict[str, Any]]
) -> int:
    delivered = 0
    for post in posts:
        post_id = str(post["post_id"])
        seen_key = f"{SEEN_POSTS_KEY}:{post_id}"
        if not cache.add_if_new(seen_key, SEEN_POSTS_TTL_SECONDS):
            continue
        try:
            sent = await kafka.publish(
                topic=RAW_POSTS_TOPIC,
                key=f"threads:{post_id}",
                value={
                    "platform": "threads",
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
            raise CrawlJobRetryable(f"threads channel post not delivered: {request['page_url']}")
    return delivered


async def run_threads_channel(request: dict[str, Any], *, slot: int = 0) -> None:
    url = request.get("page_url") or ""
    handle = handle_from_url(url)
    if not handle:
        raise CrawlJobFailed(f"threads_channel request has no valid handle: {url!r}")
    try:
        since_days = int(request.get("since_days", SINCE_DAYS))
        max_pages = int(request.get("max_pages", MAX_PAGES))
        if since_days <= 0 or max_pages < 0:
            raise ValueError
    except TypeError, ValueError:
        raise CrawlJobFailed("threads_channel requires since_days > 0 and max_pages >= 0") from None
    cache = RedisCache()
    chosen = await asyncio.to_thread(pick_exit, cache)
    if chosen is None:
        raise ThreadsChannelExitUnavailable("no Threads channel exit available")
    exit_name, proxy_url = chosen
    run_id = request.get("run_id")
    running_key = RUNNING_KEY_TMPL.format(slot=slot)
    cache.set(
        running_key,
        {"run_id": run_id, "type": "threads_channel", "keyword": url, "started_at": int(time.time())},
        ttl_seconds=RUNNING_TTL_SECONDS,
    )
    loop = asyncio.get_running_loop()
    kafka = KafkaPublisher()
    await kafka.start()
    new_posts = 0

    def on_batch(posts: list[dict[str, Any]]) -> bool:
        nonlocal new_posts
        try:
            new_posts += asyncio.run_coroutine_threadsafe(_publish(cache, kafka, request, posts), loop).result(
                BATCH_PUBLISH_TIMEOUT_SECONDS
            )
        except Exception as exc:  # noqa: BLE001 - dừng job, lượt sau đọc lại
            logger.warning("threads_channel_publish_failed", page_url=url, error=str(exc)[:200])
            return False
        return True

    try:
        result = await asyncio.to_thread(
            fetch_channel,
            handle,
            proxy_url,
            since_ts=int(time.time()) - since_days * 86400,
            max_pages=max_pages,
            request_interval=REQUEST_INTERVAL,
            on_batch=on_batch,
            should_stop=lambda: is_platform_draining("threads") or _cancel_requested(run_id=run_id),
        )
    finally:
        await kafka.stop()
        cache.delete(running_key)

    if result.profile:
        cache.set(PROFILE_KEY_TMPL.format(handle=handle), result.profile, ttl_seconds=PROFILE_TTL_SECONDS)
    cache.set(
        LAST_KEY_TMPL.format(url=url),
        {
            "at": int(time.time()),
            "new": new_posts,
            "status": result.status,
            "stop_reason": result.stop_reason,
            "user_id": result.user_id,
        },
        ttl_seconds=LAST_TTL_SECONDS,
    )
    logger.info(
        "threads_channel_done",
        page_url=url,
        exit=exit_name,
        status=result.status,
        error=result.error,
        pages=result.pages,
        posts_in_window=result.posts,
        new_posts=new_posts,
        stop_reason=result.stop_reason,
        followers=result.followers,
    )
    if result.status == "blocked":
        cache.set(COOLDOWN_KEY_TMPL.format(exit=exit_name), 1, ttl_seconds=COOLDOWN_MINUTES * 60)
        logger.warning("threads_channel_exit_blocked", exit=exit_name, error=result.error, telegram=True)
        if not result.pages:
            raise ThreadsChannelExitUnavailable(result.error or "blocked")
    elif result.status == "not_found":
        raise CrawlJobFailed(f"Threads channel not found: @{handle}")
    elif result.status == "error" and not result.pages:
        raise CrawlJobFailed(result.error or "threads channel error")
    if result.stop_reason == "batch_failed":
        raise CrawlJobRetryable(f"threads channel posts not delivered: {url}")
