"""Hạn mức nuôi tài khoản mỗi ngày (giờ Việt Nam): đã nuôi hôm nay chưa, đánh dấu đã nuôi.

Tách từ nurture.py (17/11/2026); nurture.py vẫn re-export mọi tên.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger

logger = get_logger(__name__)


def _vn_today() -> str:
    return datetime.now(_VN_TZ).date().isoformat()


def _quota_key(platform: str, account_key: str, half_day: bool = False) -> str:
    key = f"nurture_day:{platform}:{account_key}:{_vn_today()}"
    if half_day:
        key += ":am" if datetime.now(_VN_TZ).hour < 12 else ":pm"
    return key


def _already_nurtured_today(redis_cache: RedisCache, platform: str, account_key: str, warming: bool = False) -> bool:
    try:
        return redis_cache.exists(_quota_key(platform, account_key, half_day=warming))
    except Exception as exc:
        # Âm thầm coi "không biết" là "chưa làm ấm" thay đổi hành vi thật (tài khoản này có thể bị
        # làm ấm hơn một lần mỗi ngày, phá đúng mục đích của hạn mức) - đáng biết khi nó xảy ra
        # thay vì trông giống hệt một ngày mới thật sự.
        logger.warning("nurture_quota_check_failed", platform=platform, account=account_key, error=str(exc))
        return False


def _mark_nurtured_today(redis_cache: RedisCache, platform: str, account_key: str) -> None:
    try:
        redis_cache.set(_quota_key(platform, account_key), {"ok": True}, ttl_seconds=_QUOTA_TTL_SECONDS)
        redis_cache.set(_quota_key(platform, account_key, half_day=True), {"ok": True}, ttl_seconds=_QUOTA_TTL_SECONDS)
    except Exception:
        logger.warning("nurture_quota_mark_failed", platform=platform, account=account_key)


# Ngày theo lịch Việt Nam - múi giờ của người vận hành. Mỗi tài khoản một lần làm ấm thành
# công mỗi ngày; bấm lần hai trên dashboard trong cùng ngày thì bỏ qua thay vì lướt lại cả
# pool.
_VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_QUOTA_TTL_SECONDS = 40 * 3600
