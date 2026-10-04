"""Kiểm tra sức khoẻ chủ động, lưu lượng thấp cho các đường crawl gốc - nhằm bắt một lần
sập toàn bộ *âm thầm* (mọi request đều trả HTTP 200 với header trông như thật nhưng dữ
liệu rỗng/không có) trước khi có người nhận ra dashboard cứ không có thêm dữ liệu. Đây
là một loại lỗi khác với thứ mà circuit breaker của services/pool.py vốn đã phủ: tài
khoản bị tắt hay proxy đang cooldown thì ồn ào (logger.error, tự cảnh báo Telegram)
ngay khi xảy ra, còn "mọi thứ *trông* khoẻ mà vẫn không trả về gì" tự nó không sinh ra
lỗi nào - đã xác nhận thực tế một cách đau đớn ngày 2026-09-17, khi endpoint item_list
chế độ khách của TikTok từ đang chạy chuyển sang âm thầm rỗng suốt nhiều giờ.

TikTok thăm dò bằng một danh tính khách synthetic (không tốn tài khoản). Facebook/Threads
thăm dò qua session đang cache (một trang tìm kiếm rẻ) - nếu chưa có session nào được
bootstrap thì bỏ qua (không tính là lỗi liên tiếp), để một máy mới khởi động không spam
Telegram.

Chạy định kỳ qua cron (không phải tiến trình sống lâu) - ví dụ mỗi 15-30 phút:

    */15 * * * * cd /path/to/spider-hub && .venv/bin/python -m social_crawler.health_check

Mã thoát luôn là 0 - lỗi được báo qua logger.error(telegram=True, ...).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from social_crawler.clients.redis import RedisCache
from social_crawler.logger import get_logger
from social_crawler.spiders.comet_graphql_client import SessionExpiredError
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokHashtagClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.hashtag_search.extract import (
    extract_response as extract_tiktok_hashtag,
)

logger = get_logger(__name__)

_PROBE_HASHTAG = "fyp"
# Các query kiểu phim tiếng Việt vô hại, thường trả về gì đó trên một session tìm kiếm
# GraphQL khoẻ - không phải một tên phim cụ thể (tránh báo sập nhầm kiểu "phim này hôm nay
# đơn giản là không có bài nào").
_PROBE_FACEBOOK_QUERY = "phim"
_PROBE_THREADS_QUERY = "phim"

_ALERT_AFTER_CONSECUTIVE_FAILURES = 2
_STREAK_TTL_SECONDS = 6 * 3600


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    skipped: bool = False
    detail: str = ""


def check_tiktok_item_list() -> ProbeResult:
    """Một lần tìm hashtag thật với _PROBE_HASHTAG qua một danh tính synthetic mới."""
    try:
        client = TikTokHashtagClient(synthetic=True)
        challenge_id = client.resolve_hashtag(_PROBE_HASHTAG)
        if not challenge_id:
            return ProbeResult(ok=False, detail="hashtag_not_found")
        response = client.search_hashtag(challenge_id, hashtag=_PROBE_HASHTAG)
    except (TikTokBlockedError, TikTokRateLimitedError, TikTokNetworkError) as exc:
        return ProbeResult(ok=False, detail=str(exc))
    videos = extract_tiktok_hashtag(response)
    return ProbeResult(ok=len(videos) > 0, detail=f"videos={len(videos)}")


def check_facebook_search() -> ProbeResult:
    """Một trang tìm kiếm GraphQL qua session Facebook đang cache."""
    try:
        from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient
        from social_crawler.spiders.facebook.features.search.extract import extract_response
    except Exception as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"import_failed:{exc}")

    try:
        client = FacebookGraphQLClient()
        response = client.search(_PROBE_FACEBOOK_QUERY, count=5)
        posts, _entities = extract_response(response)
        return ProbeResult(ok=len(posts) > 0, detail=f"posts={len(posts)}")
    except SessionExpiredError as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"no_session:{exc}")
    except Exception as exc:
        return ProbeResult(ok=False, detail=str(exc))


def check_threads_search() -> ProbeResult:
    """Một trang tìm kiếm GraphQL qua session Threads đang cache."""
    try:
        from social_crawler.spiders.threads.auth.graphql_client import ThreadsGraphQLClient
        from social_crawler.spiders.threads.features.search.extract import extract_response
    except Exception as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"import_failed:{exc}")

    try:
        client = ThreadsGraphQLClient()
        response = client.search(_PROBE_THREADS_QUERY, count=5)
        posts = extract_response(response)
        return ProbeResult(ok=len(posts) > 0, detail=f"posts={len(posts)}")
    except SessionExpiredError as exc:
        return ProbeResult(ok=False, skipped=True, detail=f"no_session:{exc}")
    except Exception as exc:
        return ProbeResult(ok=False, detail=str(exc))


def _streak_key(name: str) -> str:
    return f"health_check:{name}:consecutive_failures"


def _record_outcome(redis_cache: RedisCache, name: str, *, ok: bool) -> int:
    key = _streak_key(name)
    if ok:
        redis_cache.delete(key)
        return 0
    streak = redis_cache.incr(key)
    redis_cache.expire(key, _STREAK_TTL_SECONDS)
    return streak


def _probe_one(redis_cache: RedisCache, name: str, result: ProbeResult) -> None:
    if result.skipped:
        logger.info("health_check_skipped", check=name, detail=result.detail)
        return

    streak = _record_outcome(redis_cache, name, ok=result.ok)
    if result.ok:
        logger.info("health_check_ok", check=name, detail=result.detail)
        return

    logger.error(
        "health_check_failed",
        check=name,
        telegram=streak >= _ALERT_AFTER_CONSECUTIVE_FAILURES,
        detail=result.detail,
        consecutive_failures=streak,
        hint=(
            "native search returned no data (or errored) through the live session - "
            "check for a platform-side anti-bot / GraphQL change before assuming "
            "proxy/account alone"
        ),
    )


def run() -> None:
    redis_cache = RedisCache()
    _probe_one(redis_cache, "tiktok_item_list", check_tiktok_item_list())
    _probe_one(redis_cache, "facebook_search", check_facebook_search())
    _probe_one(redis_cache, "threads_search", check_threads_search())


if __name__ == "__main__":
    run()
    sys.exit(0)
