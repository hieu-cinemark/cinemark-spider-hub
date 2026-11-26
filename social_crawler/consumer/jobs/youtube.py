"""Job YouTube (13/11/2026): tìm kiếm theo từ khoá, bình luận một video, video của một kênh - không tài khoản, chạy trong
tiến trình (InnerTube qua httpx, logic chép từ data-miner: spiders/youtube/).

Từ khoá bắt đầu bằng "#" (26/11/2026) là hashtag: đọc 2 tab của trang hashtag (spiders/youtube/hashtag/) thay cho tìm kiếm.

Mục tiêu là gom nhiều video nhất có thể, cả video thường lẫn Shorts: mỗi từ khoá chạy qua nhiều bộ lọc của trang tìm kiếm
(YOUTUBE_SEARCH_MODES - liên quan, video tải lên trong tuần/tháng/năm, chỉ Shorts...) vì mỗi bộ lọc trả một tập kết quả khác
nhau; mỗi bộ lọc lật tới youtube_search_pages (proxy_settings) trang và chỉ dừng sớm khi YOUTUBE_SEEN_ONLY_PAGES trang liền không có video
mới. Video mới mới gọi API player (ngày đăng chính xác, like, view, mô tả đầy đủ); video đã thấy trong TTL thì bỏ qua.

Transcript (16/11/2026, công tắc youtube_transcripts): video mới lấy thêm phụ đề (spiders/youtube/transcript/), mỗi video một
lần, gửi kèm trong trường "transcript" - cinemark-api lưu vào bảng post_transcripts và dùng khi lọc liên quan.

Proxy: YOUTUBE_PROXY_URL (để trống = IP máy). Bài gửi lên raw_posts với platform="youtube"; bình luận lên raw_comments.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx

from social_crawler.clients.kafka import RAW_COMMENTS_TOPIC, RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import CrawlJobFailed, CrawlJobRetryable, crawl_job_key, logger
from social_crawler.consumer.process import cancel_requested
from social_crawler.db.proxies import list_proxies
from social_crawler.db.proxy_settings import get_setting
from social_crawler.runtime import lane_budget
from social_crawler.runtime.job_queue import JobQueue
from social_crawler.runtime.lanes import worker_lane
from social_crawler.runtime.pool import build_proxy_url
from social_crawler.runtime.task_queue import is_platform_draining
from social_crawler.spiders.common.post_log import log_crawled_post
from social_crawler.spiders.youtube.channel.channel import get_channel_videos
from social_crawler.spiders.youtube.client import resolve_channel_id_from_handle
from social_crawler.spiders.youtube.comment.comment import get_video_comments
from social_crawler.spiders.youtube.detail.detail import get_video_meta
from social_crawler.spiders.youtube.hashtag.hashtag import TABS as HASHTAG_TABS
from social_crawler.spiders.youtube.hashtag.hashtag import hashtag_pages
from social_crawler.spiders.youtube.search.search import search_pages
from social_crawler.spiders.youtube.search.search_constants import SEARCH_FILTERS
from social_crawler.spiders.youtube.transcript.transcript import TranscriptBlocked, get_transcript
from social_crawler.spiders.youtube.utils import parse_relative_time, parse_view_count

SEARCH_MODES = [
    m.strip()
    for m in os.getenv(
        "YOUTUBE_SEARCH_MODES", "relevance,today,week,month,year,shorts,videos,under4,4to20,over20,view_count,newest"
    ).split(",")
    if m.strip()
]
SEEN_ONLY_PAGES = int(os.getenv("YOUTUBE_SEEN_ONLY_PAGES", "5"))
META_CONCURRENCY = max(1, int(os.getenv("YOUTUBE_META_CONCURRENCY", "6")))
COMMENTS_MAX = int(os.getenv("YOUTUBE_COMMENTS_MAX", "1000"))
CHANNEL_MAX_VIDEOS = int(os.getenv("YOUTUBE_CHANNEL_MAX_VIDEOS", "200"))
# Số bộ lọc tìm kiếm chạy cùng lúc trong một job từ khoá.
PROXY_REFRESH_SECONDS = 60
# Làn YouTube (consumer/queues.py): youtube_lanes (proxy_settings) worker đọc hai hàng đợi Redis này - tìm kiếm/kênh trước, bình luận sau.
# Vòng Kafka của YouTube chỉ chuyển message vào đây (dispatch.handle_request), nên số làn không phụ thuộc số partition.
# Số làn đang bật đọc từ proxy_settings (youtube_lanes, sửa trên dashboard); MAX_LANES làn được tạo sẵn, làn ngoài số đang
# bật thì đứng chờ - đổi số làn không cần restart.
MAX_LANES = 64


def active_lanes() -> int:
    return max(1, min(MAX_LANES, lane_budget.resolve(get_setting("youtube_lanes"), get_setting("lanes_total"))))


SEARCH_QUEUE_KEY = "crawl_youtube:search"
COMMENTS_QUEUE_KEY = "crawl_youtube:comments"
PROCESSING_KEY = "crawl_youtube:processing"
# Hai hàng chờ (tìm kiếm/kênh đi trước bình luận) dùng chung một khoá đang chạy.
SEARCH_QUEUE = JobQueue(SEARCH_QUEUE_KEY, PROCESSING_KEY)
COMMENTS_QUEUE = JobQueue(COMMENTS_QUEUE_KEY, PROCESSING_KEY)
SEEN_POSTS_KEY = "youtube:seen_video_ids"
# Như TikTok/Threads: video đã gửi được coi là mới lại sau TTL để view/like được cập nhật.
SEEN_POSTS_TTL_SECONDS = int(os.getenv("YOUTUBE_SEEN_POSTS_TTL_SECONDS", str(24 * 3600)))
SEEN_COMMENTS_KEY = "youtube:seen_comment_ids"
SEEN_COMMENTS_TTL_SECONDS = 30 * 86400
# Transcript chỉ lấy một lần mỗi video (nội dung không đổi); lỗi mạng / bị chặn thì xoá dấu để lần thấy sau lấy lại.
TRANSCRIPT_DONE_KEY = "youtube:transcript_done"
TRANSCRIPT_DONE_TTL_SECONDS = 180 * 86400
# 16/11/2026: YouTube trả IpBlocked cho transcript qua proxy làn (#28 - cổng "xoay vòng" giữ nguyên IP), trong khi IP máy VPS
# lấy được (thử 3/3). Thử proxy làn rồi tới IP máy; lối ra bị chặn nghỉ TRANSCRIPT_EXIT_REST_SECONDS; mọi lối ra đều bị chặn
# thì nghỉ lấy transcript (khoá Redis TRANSCRIPT_PAUSE_KEY) - video vẫn gửi, transcript lấy lần sau thấy video.
TRANSCRIPT_EXIT_REST_SECONDS = int(os.getenv("YOUTUBE_TRANSCRIPT_EXIT_REST_SECONDS", "1800"))
TRANSCRIPT_PAUSE_KEY = "youtube:transcript_paused"
TRANSCRIPT_CONCURRENCY = max(1, int(os.getenv("YOUTUBE_TRANSCRIPT_CONCURRENCY", "2")))
_transcript_exit_rest: dict[str, float] = {}


async def fetch_transcript(video_id: str, lane: str | None, cache: RedisCache) -> dict[str, Any] | None:
    """Transcript qua lối ra còn dùng được (proxy làn, rồi IP máy). Raise TranscriptBlocked khi mọi lối ra đều bị chặn (đã đặt
    nghỉ); lỗi khác (mạng...) nổi lên để chỗ gọi lấy lại lần sau."""
    now = time.monotonic()
    exits = list(dict.fromkeys([lane, None]))
    for exit_url in exits:
        name = exit_url or "direct"
        if _transcript_exit_rest.get(name, 0) > now:
            continue
        try:
            return await get_transcript(video_id, proxy=exit_url)
        except TranscriptBlocked:
            _transcript_exit_rest[name] = now + TRANSCRIPT_EXIT_REST_SECONDS
            logger.warning("youtube_transcript_exit_blocked", exit="proxy" if exit_url else "direct", video_id=video_id)
    cache.set(TRANSCRIPT_PAUSE_KEY, 1, ttl_seconds=TRANSCRIPT_EXIT_REST_SECONDS)
    logger.warning("youtube_transcripts_paused", minutes=TRANSCRIPT_EXIT_REST_SECONDS // 60, telegram=True)
    raise TranscriptBlocked("every exit blocked")


_proxy_cache: dict[str, Any] = {"at": None, "urls": []}


def _proxy_urls() -> list[str]:
    """Proxy gán cho YouTube (platform='youtube'). Đọc lại mỗi PROXY_REFRESH_SECONDS. Không có proxy nào thì IP máy."""
    now = time.monotonic()
    if _proxy_cache["at"] is None or now - _proxy_cache["at"] >= PROXY_REFRESH_SECONDS:
        _proxy_cache.update(at=now, urls=[build_proxy_url(row) for row in list_proxies("youtube")])
    return _proxy_cache["urls"]


_bad_proxies: dict[str, float] = {}


def cfg_proxy_rest_seconds() -> int:
    return int(get_setting("youtube_proxy_rest_seconds"))


def mark_proxy_bad(url: str | None) -> None:
    """Proxy từ chối kết nối / sai xác thực: nghỉ PROXY_REST_SECONDS; trong lúc đó làn dùng proxy khác hoặc IP máy."""
    if url:
        _bad_proxies[url] = time.monotonic() + cfg_proxy_rest_seconds()


_proxy_checked: dict[str, float] = {}
PROXY_CHECK_SECONDS = 300


async def lane_proxy() -> str | None:
    """Proxy của làn sau khi thử nhanh (generate_204, 6 giây), kết quả nhớ PROXY_CHECK_SECONDS. Proxy không qua thì nghỉ và
    làn dùng lối ra khác / IP máy, thay vì để mọi request của job chờ hết hạn qua một proxy chết."""
    for _ in range(len(_proxy_urls()) + 1):
        proxy = _proxy()
        checked = _proxy_checked.get(proxy) if proxy else None
        if proxy is None or (checked is not None and time.monotonic() - checked < PROXY_CHECK_SECONDS):
            return proxy
        try:
            async with httpx.AsyncClient(proxy=proxy, timeout=6) as client:
                ok = (await client.get("https://www.youtube.com/generate_204")).status_code == 204
        except Exception:  # noqa: BLE001 - mọi lỗi của proxy đều là "không dùng được"
            ok = False
        if ok:
            _proxy_checked[proxy] = time.monotonic()
            return proxy
        logger.warning("youtube_proxy_unusable", lane=worker_lane(), telegram=True)
        mark_proxy_bad(proxy)
    return None


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
    proxy = await lane_proxy()

    async def meta_of(stub: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            try:
                return await get_video_meta(stub["video_id"], proxy=proxy) or {"video_id": stub["video_id"]}
            except Exception as exc:  # noqa: BLE001 - thiếu metadata vẫn giữ video
                logger.warning("youtube_meta_failed", video_id=stub["video_id"], error=exc)
                return {"video_id": stub["video_id"]}

    want_transcripts = bool(get_setting("youtube_transcripts"))
    transcript_sem = asyncio.Semaphore(TRANSCRIPT_CONCURRENCY)

    async def transcript_of(stub: dict[str, Any]) -> dict[str, Any] | None:
        video_id = stub["video_id"]
        done = f"{TRANSCRIPT_DONE_KEY}:{video_id}"
        if not want_transcripts or cache.exists(TRANSCRIPT_PAUSE_KEY):
            return None
        if not cache.add_if_new(done, TRANSCRIPT_DONE_TTL_SECONDS):
            return None
        async with transcript_sem:
            if cache.exists(TRANSCRIPT_PAUSE_KEY):  # vừa có video khác làm nghỉ trong lúc chờ
                cache.delete(done)
                return None
            try:
                return await fetch_transcript(video_id, proxy, cache)
            except TranscriptBlocked:
                cache.delete(done)
                return None
            except Exception as exc:  # noqa: BLE001 - thiếu transcript vẫn gửi video
                cache.delete(done)
                logger.warning("youtube_transcript_failed", video_id=video_id, error=exc)
                return None

    metas, transcripts = await asyncio.gather(
        asyncio.gather(*(meta_of(s) for s in fresh)), asyncio.gather(*(transcript_of(s) for s in fresh))
    )
    sent = 0
    for stub, meta, transcript in zip(fresh, metas, transcripts):
        value = _post_value(meta, stub, keyword_id, **extra)
        if transcript:
            value["transcript"] = transcript
        key = f"{SEEN_POSTS_KEY}:{value['video_id']}"
        try:
            ok = await kafka.publish(topic=RAW_POSTS_TOPIC, key=f"youtube:{value['video_id']}", value=value)
        except Exception:
            cache.delete(key)
            cache.delete(f"{TRANSCRIPT_DONE_KEY}:{value['video_id']}")
            raise
        if not ok:
            cache.delete(key)
            cache.delete(f"{TRANSCRIPT_DONE_KEY}:{value['video_id']}")
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
    return is_platform_draining("youtube") or cancel_requested(run_id=run_id)


def search_sources(keyword: str, modes: list[str] | None, max_pages: int, proxy: str | None) -> dict[str, Any]:
    """Các nguồn video của một từ khoá: {tên: hàm tạo bộ lật trang}. "#..." là hashtag -> 2 tab trang hashtag (`modes` của
    tìm kiếm không áp dụng); còn lại là các bộ lọc tìm kiếm."""
    if keyword.startswith("#"):
        return {
            f"hashtag_{tab}": (lambda tab=tab: hashtag_pages(keyword, tab=tab, max_pages=max_pages, proxy=proxy))
            for tab in HASHTAG_TABS
        }
    return {
        mode: (
            lambda mode=mode: search_pages(keyword, filter_param=SEARCH_FILTERS[mode], max_pages=max_pages, proxy=proxy)
        )
        for mode in (modes or SEARCH_MODES)
        if mode in SEARCH_FILTERS
    }


async def run_youtube_search(request: dict[str, Any]) -> None:
    """Tìm YouTube theo từ khoá (hoặc hashtag) và gửi video mới lên `raw_posts`. Từ khoá thường chạy qua nhiều bộ lọc
    (`SEARCH_MODES`), hashtag chạy qua 2 tab; tối đa `youtube_filter_concurrency` nguồn đồng thời; mỗi nguồn lật tới
    `max_pages` trang và dừng sớm khi `SEEN_ONLY_PAGES` trang liền không có video mới. Một nguồn lỗi không dừng các nguồn
    khác; mọi nguồn đều lỗi thì ném lỗi đầu tiên để job được xếp lại."""
    keyword = str(request.get("keyword") or "").strip()
    if not keyword:
        raise CrawlJobFailed("youtube search request missing keyword")
    keyword_id = request.get("keyword_id")
    run_id = request.get("run_id")
    max_pages = int(request.get("max_pages") or get_setting("youtube_search_pages"))
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
    sem = asyncio.Semaphore(max(1, int(get_setting("youtube_filter_concurrency"))))
    sources = search_sources(keyword, request.get("modes"), max_pages, await lane_proxy())

    async def run_source(name: str) -> None:
        async with sem:
            pages = new = found = seen_only = 0
            try:
                async for page, videos in sources[name]():
                    pages = page
                    found += len(videos)
                    sent = await _publish_videos(cache, kafka, videos, keyword_id, search_mode=name)
                    new += sent
                    seen_only = 0 if sent else seen_only + 1
                    if (SEEN_ONLY_PAGES and seen_only >= SEEN_ONLY_PAGES) or _stopped(run_id):
                        break
            finally:
                stats[name] = {"pages": pages, "found": found, "new": new}

    try:
        results = await asyncio.gather(*(run_source(name) for name in sources), return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        for error in errors:
            logger.warning("youtube_search_mode_failed", keyword=keyword, error=error)
        # Mọi nguồn đều hỏng (bị chặn / mất mạng): để worker xếp lại job; hỏng một phần thì giữ phần đã gom.
        if errors and len(errors) == len(sources):
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
    """Cào bình luận và reply của một video YouTube rồi gửi những bình luận chưa thấy lên `raw_comments`. Đọc tối đa
    `max_comments` bình luận mới nhất kèm reply (`youtube_reply_pages` trang reply); bình luận đã gửi trong 30 ngày
    (`SEEN_COMMENTS_KEY`) bị bỏ qua; không gửi được lên Kafka thì ném `CrawlJobRetryable`."""
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
            video_id,
            proxy=await lane_proxy(),
            max_comments=limit,
            sort="newest",
            reply_pages=int(get_setting("youtube_reply_pages")),
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
        channel_id, proxy=await lane_proxy(), max_results=int(request.get("max_videos") or CHANNEL_MAX_VIDEOS)
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
