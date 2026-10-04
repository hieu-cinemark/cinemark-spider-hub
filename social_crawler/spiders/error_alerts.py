"""Phát hiện đợt lỗi dồn dập theo cửa sổ trượt cho các lỗi spider tạm thời
(RateLimitedError/NetworkError trên Facebook/Threads/TikTok) - giống cách
ingest_consumer._note_drop của cinemark-api: một lần xảy ra đơn lẻ vốn đã được chính
spider log/cảnh báo (xem các khối except trong từng search.py), nên phần này chỉ leo
thang khi CÙNG một lý do cứ lặp lại trong một cửa sổ trượt - "1 lần thử lại xui" và "nền
tảng này dạo này bóp ngày càng nhiều" là hai tín hiệu khác nhau đáng phân biệt, và chỉ
cái thứ hai cần người để ý trước khi nó thành chặn cứng/checkpoint."""

from __future__ import annotations

from social_crawler.clients.redis import RedisCache
from social_crawler.logger import get_logger

logger = get_logger(__name__)

ALERT_THRESHOLD = 5
# Cửa sổ ngắn hơn 1 giờ của ingest_consumer - spider thử lại vài giây một lần, nên cùng
# một đợt dồn dập tích luỹ nhanh hơn nhiều so với số bài bị loại của ingest Kafka.
COUNTER_TTL_SECONDS = 1800


def note_transient_error(platform: str, reason: str, redis_cache: RedisCache | None = None) -> None:
    """Gọi một lần cho mỗi RateLimitedError/NetworkError mà bất kỳ spider nào raise. Cố gắng
    hết mức: không bao giờ raise, để một lần Redis trục trặc không thể làm sập một lượt crawl
    đang xử lý dở lỗi vì một lý do khác."""
    cache = redis_cache or RedisCache()
    key = f"error_burst:{platform}:{reason}"
    try:
        count = cache.incr(key)
        if count == 1:
            cache.expire(key, COUNTER_TTL_SECONDS)
    except Exception as exc:
        logger.warning("error_burst_tracking_failed", platform=platform, reason=reason, error=str(exc))
        return

    if count == ALERT_THRESHOLD:
        logger.error(
            "error_burst_detected",
            telegram=True,
            platform=platform,
            reason=reason,
            count=count,
            window_minutes=COUNTER_TTL_SECONDS // 60,
        )
