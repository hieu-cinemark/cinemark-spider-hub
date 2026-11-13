"""Job YouTube (13/11/2026): tìm kiếm theo từ khoá, bình luận một video, video của một kênh - không tài khoản, chạy trong
tiến trình (InnerTube qua httpx, logic chép từ data-miner: spiders/youtube/).

Mục tiêu là gom nhiều video nhất có thể, cả video thường lẫn Shorts: mỗi từ khoá chạy qua nhiều bộ lọc của trang tìm kiếm
(YOUTUBE_SEARCH_MODES - liên quan, video tải lên trong tuần/tháng/năm, chỉ Shorts...) vì mỗi bộ lọc trả một tập kết quả khác
nhau; mỗi bộ lọc lật tới YOUTUBE_SEARCH_PAGES trang và chỉ dừng sớm khi YOUTUBE_SEEN_ONLY_PAGES trang liền không có video
mới. Video mới mới gọi API player (ngày đăng chính xác, like, view, mô tả đầy đủ); video đã thấy trong TTL thì bỏ qua.

Proxy: YOUTUBE_PROXY_URL (để trống = IP máy). Bài gửi lên raw_posts với platform="youtube"; bình luận lên raw_comments.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from social_crawler.clients.kafka import RAW_COMMENTS_TOPIC, RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import CrawlJobFailed, CrawlJobRetryable, crawl_job_key, logger
from social_crawler.consumer.process import _cancel_requested
from social_crawler.db.proxies import list_proxies
from social_crawler.runtime.lanes import worker_lane
from social_crawler.runtime.pool import build_proxy_url
from social_crawler.runtime.task_queue import is_platform_draining
from social_crawler.spiders.common.post_log import log_crawled_post
from social_crawler.spiders.youtube.channel.channel import get_channel_videos
from social_crawler.spiders.youtube.client import resolve_channel_id_from_handle
from social_crawler.spiders.youtube.comment.comment import get_video_comments
from social_crawler.spiders.youtube.detail.detail import get_video_meta
from social_crawler.spiders.youtube.search.search import search_pages
from social_crawler.spiders.youtube.search.search_constants import SEARCH_FILTERS
from social_crawler.spiders.youtube.utils import parse_relative_time, parse_view_count

SEARCH_MODES = [
    m.strip()
    for m in os.getenv(
        "YOUTUBE_SEARCH_MODES", "relevance,today,week,month,year,shorts,videos,under4,4to20,over20,view_count,newest"
    ).split(",")
    if m.strip()
]
SEARCH_PAGES = int(os.getenv("YOUTUBE_SEARCH_PAGES", "30"))
SEEN_ONLY_PAGES = int(os.getenv("YOUTUBE_SEEN_ONLY_PAGES", "5"))
META_CONCURRENCY = max(1, int(os.getenv("YOUTUBE_META_CONCURRENCY", "6")))
COMMENTS_MAX = int(os.getenv("YOUTUBE_COMMENTS_MAX", "1000"))
CHANNEL_MAX_VIDEOS = int(os.getenv("YOUTUBE_CHANNEL_MAX_VIDEOS", "200"))
REPLY_PAGES = int(os.getenv("YOUTUBE_REPLY_PAGES", "50"))
# Số bộ lọc tìm kiếm chạy cùng lúc trong một job từ khoá.
FILTER_CONCURRENCY = max(1, int(os.getenv("YOUTUBE_FILTER_CONCURRENCY", "4")))
PROXY_REFRESH_SECONDS = 300
# Làn YouTube (consumer/loop.py): YOUTUBE_LANES worker đọc hai hàng đợi Redis này - tìm kiếm/kênh trước, bình luận sau.
# Vòng Kafka của YouTube chỉ chuyển message vào đây (dispatch._handle_request), nên số làn không phụ thuộc số partition.
LANES = max(1, int(os.getenv("YOUTUBE_LANES", "8")))
SEARCH_QUEUE_KEY = "crawl_youtube:search"
COMMENTS_QUEUE_KEY = "crawl_youtube:comments"
PROCESSING_KEY = "crawl_youtube:processing"
SEEN_POSTS_KEY = "youtube:seen_video_ids"
# Như TikTok/Threads: video đã gửi được coi là mới lại sau TTL để view/like được cập nhật.
SEEN_POSTS_TTL_SECONDS = int(os.getenv("YOUTUBE_SEEN_POSTS_TTL_SECONDS", str(24 * 3600)))
SEEN_COMMENTS_KEY = "youtube:seen_comment_ids"
SEEN_COMMENTS_TTL_SECONDS = 30 * 86400


_proxy_cache: dict[str, Any] = {"at": 0.0, "urls": []}


def _proxy_urls() -> list[str]:
    """Proxy dành riêng cho YouTube: các dòng platform='youtube' trong platform_proxies (gán ở dashboard), đọc lại mỗi
    PROXY_REFRESH_SECONDS. Không có dòng nào thì YOUTUBE_PROXY_URL, không nữa thì IP máy."""
    now = time.monotonic()
    if now - _proxy_cache["at"] > PROXY_REFRESH_SECONDS:
        rows = [row for row in list_proxies("youtube") if row.get("platform") == "youtube"]
        _proxy_cache.update(at=now, urls=[build_proxy_url(row) for row in rows])
    return _proxy_cache["urls"] or ([os.environ["YOUTUBE_PROXY_URL"]] if os.getenv("YOUTUBE_PROXY_URL") else [])


_bad_proxies: dict[str, float] = {}
PROXY_REST_SECONDS = int(os.getenv("YOUTUBE_PROXY_REST_SECONDS", "600"))


def mark_proxy_bad(url: str | None) -> None:
    """Proxy từ chối kết nối / sai xác thực: nghỉ PROXY_REST_SECONDS; trong lúc đó làn dùng proxy khác hoặc IP máy."""
    if url:
        _bad_proxies[url] = time.monotonic() + PROXY_REST_SECONDS


def _proxy() -> str | None:
    """Mỗi làn một proxy (làn N dùng proxy thứ N chia dư) để các làn không dồn vào một cổng khi có nhiều proxy. Mọi proxy
    đang nghỉ thì đi IP máy - YouTube không được đứng vì một proxy hỏng."""
    now = time.monotonic()
    urls = [url for url in _proxy_urls() if _bad_proxies.get(url, 0) <= now]
    return urls[(worker_lane() or 0) % len(urls)] if urls else None


def current_proxy() -> str | None:
    return _proxy()


def _post_value(meta: dict[str, Any], stub: dict[str, Any], keyword_id: str | None, **extra: Any) -> dict[str, Any]:
    video_id = meta["video_id"]
    is_short = bool(stub.get("is_short") or meta.get("is_short"))
    handle = meta.get("channel_handle")
    return {
        "platform": "youtube",
        "keyword_id": keyword_id,
        "video_id": video_id,
        "url": f"https://www.youtube.com/shorts/{video_id}"
        if is_short
        else f"https://www.youtube.com/watch?v={video_id}",
        "title": meta.get("title") or stub.get("title"),
        "description": meta.get("description") or stub.get("description_snippet") or "",
        "channel_name": meta.get("channel_name") or stub.get("channel"),
        "channel_id": meta.get("channel_id") or stub.get("channel_id"),
        "channel_handle": handle,
        "channel_url": f"https://www.youtube.com/{handle}" if handle else None,
        "view_count": meta.get("view_count") or stub.get("view_count") or 0,
        "like_count": meta.get("like_count") or 0,
        "duration_seconds": meta.get("duration_seconds"),
        "create_time": meta.get("timestamp") or parse_relative_time(stub.get("published_time")),
        "thumbnail": meta.get("thumbnail"),
        "keywords": meta.get("keywords") or [],
        "category": meta.get("category"),
        "is_short": is_short,
        "is_live": meta.get("is_live"),
        **extra,
    }


async def _publish_videos(
    cache: RedisCache, kafka: KafkaPublisher, stubs: list[dict[str, Any]], keyword_id: str | None, **extra: Any
) -> int:
    """Gửi các video chưa thấy; trả số video đã gửi. Video không đọc được metadata vẫn gửi với dữ liệu từ trang tìm kiếm."""
    fresh = [
        s
        for s in stubs
        if s.get("video_id") and cache.add_if_new(f"{SEEN_POSTS_KEY}:{s['video_id']}", SEEN_POSTS_TTL_SECONDS)
    ]
    if not fresh:
        return 0
    sem = asyncio.Semaphore(META_CONCURRENCY)
    proxy = _proxy()

    async def meta_of(stub: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            try:
                return await get_video_meta(stub["video_id"], proxy=proxy) or {"video_id": stub["video_id"]}
            except Exception as exc:  # noqa: BLE001 - thiếu metadata vẫn giữ video
                logger.warning("youtube_meta_failed", video_id=stub["video_id"], error=str(exc)[:200])
                return {"video_id": stub["video_id"]}

    metas = await asyncio.gather(*(meta_of(s) for s in fresh))
    sent = 0
    for stub, meta in zip(fresh, metas):
        value = _post_value(meta, stub, keyword_id, **extra)
        key = f"{SEEN_POSTS_KEY}:{value['video_id']}"
        try:
            ok = await kafka.publish(topic=RAW_POSTS_TOPIC, key=f"youtube:{value['video_id']}", value=value)
        except Exception:
            cache.delete(key)
            raise
        if not ok:
            cache.delete(key)
            raise CrawlJobRetryable(f"youtube video not delivered: {value['video_id']}")
        sent += 1
        log_crawled_post(
            logger,
            platform="youtube",
            post_id=value["video_id"],
            text=value["title"],
            author=value["channel_name"],
            url=value["url"],
            views=value["view_count"],
            likes=value["like_count"],
        )
    return sent


def _stopped(run_id: str | None) -> bool:
    return is_platform_draining("youtube") or _cancel_requested(run_id=run_id)


async def run_youtube_search(request: dict[str, Any]) -> None:
    keyword = str(request.get("keyword") or "").strip()
    if not keyword:
        raise CrawlJobFailed("youtube search request missing keyword")
    keyword_id = request.get("keyword_id")
    run_id = request.get("run_id")
    max_pages = int(request.get("max_pages") or SEARCH_PAGES)
    modes = [m for m in (request.get("modes") or SEARCH_MODES) if m in SEARCH_FILTERS]
    cache = RedisCache()
    job_key = crawl_job_key("youtube")
    cache.set(
        job_key,
        {"run_id": run_id, "keyword": keyword, "keyword_id": keyword_id, "started_at": int(time.time())},
        ttl_seconds=3 * 3600,
    )
    kafka = KafkaPublisher()
    await kafka.start()
    stats: dict[str, dict[str, int]] = {}
    sem = asyncio.Semaphore(FILTER_CONCURRENCY)
    proxy = _proxy()

    async def run_mode(mode: str) -> None:
        async with sem:
            pages = new = found = seen_only = 0
            try:
                async for page, videos in search_pages(
                    keyword, filter_param=SEARCH_FILTERS[mode], max_pages=max_pages, proxy=proxy
                ):
                    pages = page
                    found += len(videos)
                    sent = await _publish_videos(cache, kafka, videos, keyword_id, search_mode=mode)
                    new += sent
                    seen_only = 0 if sent else seen_only + 1
                    if (SEEN_ONLY_PAGES and seen_only >= SEEN_ONLY_PAGES) or _stopped(run_id):
                        break
            finally:
                stats[mode] = {"pages": pages, "found": found, "new": new}

    try:
        results = await asyncio.gather(*(run_mode(mode) for mode in modes), return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        for error in errors:
            logger.warning(
                "youtube_search_mode_failed", keyword=keyword, error=f"{type(error).__name__}: {error}"[:200]
            )
        # Mọi bộ lọc đều hỏng (bị chặn / mất mạng): để worker xếp lại job; hỏng một phần thì giữ phần đã gom.
        if errors and len(errors) == len(modes):
            raise errors[0]
    finally:
        await kafka.stop()
        cache.delete(job_key)
    logger.info(
        "youtube_search_done",
        keyword=keyword,
        keyword_id=keyword_id,
        new=sum(s["new"] for s in stats.values()),
        modes=stats,
        telegram=True,
    )


async def run_youtube_comments(request: dict[str, Any]) -> None:
    video_id = str(request.get("post_id") or "").strip()
    if not video_id:
        raise CrawlJobFailed("youtube comments request missing post_id")
    limit = int(request.get("max_comments") or COMMENTS_MAX)
    cache = RedisCache()
    job_key = crawl_job_key("youtube")
    cache.set(
        job_key,
        {"run_id": request.get("run_id"), "type": "comments", "post_id": video_id, "started_at": int(time.time())},
        ttl_seconds=3600,
    )
    try:
        comments = await get_video_comments(
            video_id, proxy=_proxy(), max_comments=limit, sort="newest", reply_pages=REPLY_PAGES
        )
    finally:
        cache.delete(job_key)
    kafka = KafkaPublisher()
    await kafka.start()
    sent = 0
    now = time.time()
    try:
        rows: list[dict[str, Any]] = []
        for comment in comments:
            rows.append({**comment, "parent_comment_id": None})
            rows += [{**reply, "parent_comment_id": comment["comment_id"]} for reply in comment.get("replies") or []]
        for row in rows:
            comment_id = row.get("comment_id")
            if not comment_id or not cache.add_if_new(f"{SEEN_COMMENTS_KEY}:{comment_id}", SEEN_COMMENTS_TTL_SECONDS):
                continue
            value = {
                "platform": "youtube",
                "post_id": video_id,
                "comment_id": comment_id,
                "message": row.get("content"),
                "author_name": row.get("author"),
                "author_avatar_url": row.get("avatar"),
                "like_count": parse_view_count(row.get("likes")),
                "reply_count": parse_view_count(row.get("replies_count")),
                "parent_comment_id": row.get("parent_comment_id"),
                "timestamp": parse_relative_time(row.get("published_time"), now),
            }
            if not await kafka.publish(topic=RAW_COMMENTS_TOPIC, key=f"youtube:{comment_id}", value=value):
                cache.delete(f"{SEEN_COMMENTS_KEY}:{comment_id}")
                raise CrawlJobRetryable(f"youtube comment not delivered: {comment_id}")
            sent += 1
    finally:
        await kafka.stop()
    logger.info("youtube_comments_done", video_id=video_id, fetched=len(comments), new=sent)


async def run_youtube_channel(request: dict[str, Any]) -> None:
    """Video của một kênh (tab Video). channel = UC... / @handle / link kênh."""
    raw = str(request.get("channel") or request.get("page_url") or request.get("handle") or "").strip()
    if not raw:
        raise CrawlJobFailed("youtube channel request missing channel")
    channel_id = raw if raw.startswith("UC") and len(raw) == 24 else None
    if channel_id is None:
        handle = raw.rstrip("/").rsplit("/", 1)[-1].lstrip("@")
        channel_id = await resolve_channel_id_from_handle(handle)
    videos = await get_channel_videos(
        channel_id, proxy=_proxy(), max_results=int(request.get("max_videos") or CHANNEL_MAX_VIDEOS)
    )
    stubs = [
        {
            "video_id": v.get("videoId"),
            "title": v.get("title"),
            "published_time": v.get("public"),
            "view_count": parse_view_count(v.get("views")),
        }
        for v in videos
    ]
    cache = RedisCache()
    kafka = KafkaPublisher()
    await kafka.start()
    try:
        sent = await _publish_videos(cache, kafka, stubs, request.get("keyword_id"), source="page", source_page=raw)
    finally:
        await kafka.stop()
    logger.info("youtube_channel_done", channel=raw, channel_id=channel_id, found=len(videos), new=sent)
