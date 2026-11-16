"""Nghỉ tìm từ khoá TikTok khi IP bị phạt (16/11/2026).

Đo thật trên proxy TikTok #27: 8 danh tính synthetic mới đều ra CÙNG một IP (cổng "xoay vòng" không đổi IP mỗi request).
Lượt tìm đầu trên IP đó có 12 video, các lượt sau 0 video, rồi body rỗng - TikTok phạt /api/search/general/full/ theo IP,
tăng dần, trong khi hashtag (challenge/item_list) và bình luận trên cùng IP vẫn chạy. Repo caixax/opentok cũng ghi: body rỗng
kéo dài ở search = TikTok đòi captcha cho thiết bị đó, không phải lỗi chữ ký. Thử lại 8 lần với danh tính mới (cách cũ) chỉ
dồn thêm request vào IP đang bị phạt.

Nên: body rỗng -> nghỉ tìm kiếm PAUSE_MINUTES phút; trang 1 ra 0 video ở ZERO_STREAK_LIMIT từ khoá liên tiếp -> cũng nghỉ (một
từ khoá lẻ có thể thật sự không có kết quả); mỗi lượt tìm cách nhau ít nhất MIN_GAP_SECONDS. Trong lúc nghỉ, spider thoát bằng
SEARCH_PAUSED_EXIT_CODE và consumer coi là bỏ qua (job gộp vẫn chạy phần hashtag).
"""

from __future__ import annotations

import os

from social_crawler.clients.redis import RedisCache

PAUSE_MINUTES = int(os.getenv("TIKTOK_SEARCH_PAUSE_MINUTES", "45"))
MIN_GAP_SECONDS = int(os.getenv("TIKTOK_SEARCH_MIN_GAP_SECONDS", "300"))
ZERO_STREAK_LIMIT = int(os.getenv("TIKTOK_SEARCH_ZERO_STREAK_LIMIT", "2"))

PAUSE_KEY = "tiktok:search:paused"
GAP_KEY = "tiktok:search:last_started"
ZERO_STREAK_KEY = "tiktok:search:zero_streak"


def is_paused(cache: RedisCache | None = None) -> bool:
    return (cache or RedisCache()).exists(PAUSE_KEY)


def pause(reason: str, cache: RedisCache | None = None) -> None:
    (cache or RedisCache()).set(PAUSE_KEY, reason, ttl_seconds=PAUSE_MINUTES * 60)


def claim_slot(cache: RedisCache | None = None) -> bool:
    """True khi được phép bắt đầu một lượt tìm (đã qua MIN_GAP_SECONDS từ lượt trước, nguyên tử)."""
    return MIN_GAP_SECONDS <= 0 or (cache or RedisCache()).add_if_new(GAP_KEY, MIN_GAP_SECONDS)


def note_page_one(videos: int, cache: RedisCache | None = None) -> bool:
    """Ghi kết quả trang 1. True khi chuỗi từ khoá ra 0 video chạm ZERO_STREAK_LIMIT (đã đặt nghỉ)."""
    cache = cache or RedisCache()
    if videos:
        cache.delete(ZERO_STREAK_KEY)
        return False
    streak = cache.incr(ZERO_STREAK_KEY)
    cache.expire(ZERO_STREAK_KEY, PAUSE_MINUTES * 60)
    if streak >= ZERO_STREAK_LIMIT:
        cache.delete(ZERO_STREAK_KEY)
        pause(f"page 1 empty on {streak} keywords in a row")
        return True
    return False
