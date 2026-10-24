"""Hạn mức request Facebook mỗi tài khoản mỗi ngày (24/10/2026, chống checkpoint): một tài khoản gánh hàng nghìn request
/ ngày là hành vi không người thật nào có - tài khoản nào cũng dính checkpoint, chỉ sớm hay muộn.

- Mỗi request GraphQL/HTML của client cộng 1 (count_request); vượt hard_cap() thì spider dừng gọn.
- Trước mỗi job Facebook (consumer/jobs/search.py): tài khoản đã đủ FACEBOOK_DAILY_REQUEST_CAP hôm nay (giờ VN) thì cho
  nghỉ tới REST_UNTIL sáng hôm sau, gỡ con trỏ active / nhả khỏi làn để chọn tài khoản khác; không còn tài khoản nào
  dưới hạn mức thì bỏ qua job (keyword đó đợi lượt sau).
Bộ đếm trong Redis, key sống 2 ngày. FACEBOOK_DAILY_REQUEST_CAP=0 tắt.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY
from social_crawler.db.accounts import get_accounts, rest_account_until
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Đếm theo REQUEST GraphQL (24/10/2026; trước đó đếm job - 40 job = 4.000-16.000 request): mọi request Facebook của một
# tài khoản (tìm kiếm, page feed, bình luận) cộng vào cùng một bộ đếm trong ngày (CometGraphQLClient._count_request).
# Job chỉ bắt đầu khi còn dưới DAILY_CAP; đang chạy mà vượt HARD_CAP_FACTOR x DAILY_CAP thì spider dừng gọn.
DAILY_CAP = int(os.getenv("FACEBOOK_DAILY_REQUEST_CAP", "500"))
HARD_CAP_FACTOR = 1.2
REST_UNTIL = os.getenv("FACEBOOK_CAP_REST_UNTIL", "05:30")
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
_TTL_SECONDS = 2 * 86400


def _key(account: str, now: datetime) -> str:
    return f"fb_request_budget:{now.date().isoformat()}:{account.strip().lower()}"


def count_request(account: str, cache: RedisCache | None = None) -> int:
    """+1 request cho tài khoản hôm nay (giờ VN); trả số đã dùng."""
    cache = cache or RedisCache()
    key = _key(account, datetime.now(_VN))
    count = cache.incr(key)
    if count == 1:
        cache.expire(key, _TTL_SECONDS)
    return count


def hard_cap() -> int:
    return int(DAILY_CAP * HARD_CAP_FACTOR) if DAILY_CAP > 0 else 0


def _next_rest_end(now: datetime) -> datetime:
    end = now.replace(hour=int(REST_UNTIL[:2]), minute=int(REST_UNTIL[3:5]), second=0, microsecond=0)
    return end if end > now else end + timedelta(days=1)


def used_today(account: str, cache: RedisCache | None = None, now: datetime | None = None) -> int:
    now = now or datetime.now(_VN)
    try:
        return int((cache or RedisCache()).get(_key(account, now)) or 0)
    except TypeError, ValueError:
        return 0


def ensure_budget(cache: RedisCache | None = None, account: str | None = None) -> bool:
    """False = mọi tài khoản Facebook dùng được đã hết hạn mức hôm nay -> bỏ qua job. `account` (làn Facebook,
    services/lanes.py): chỉ xét đúng tài khoản đó - hết hạn mức thì cho nghỉ, nhả khỏi làn và trả False để làn lấy
    tài khoản khác."""
    if DAILY_CAP <= 0:
        return True
    cache = cache or RedisCache()
    now = datetime.now(_VN)
    if account:
        if used_today(account, cache, now) < DAILY_CAP:
            return True
        from social_crawler.services.lanes import release_account

        until = _next_rest_end(now)
        rest_account_until("facebook", account, until)
        release_account(account, cache)
        logger.warning(
            "facebook_daily_search_cap_reached", account=account, cap=DAILY_CAP, rest_until=until.isoformat()
        )
        return False
    for _ in range(10):  # mỗi vòng cho nghỉ một tài khoản hết hạn mức
        active = cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
        if active and used_today(str(active), cache, now) >= DAILY_CAP:
            until = _next_rest_end(now)
            rest_account_until("facebook", str(active), until)
            cache.delete(ACTIVE_ACCOUNT_REDIS_KEY)
            logger.warning(
                "facebook_daily_search_cap_reached", account=active, cap=DAILY_CAP, rest_until=until.isoformat()
            )
            continue
        if active:
            return True
        # Chưa có tài khoản active: còn tài khoản nào (không cooldown) chưa hết hạn mức thì để bootstrap chọn.
        ready = [a for a in get_accounts("facebook") if used_today(a.get("email") or a["id"], cache, now) < DAILY_CAP]
        if not ready:
            logger.warning("facebook_daily_search_cap_all_accounts", cap=DAILY_CAP)
        return bool(ready)
    return False


def record_search(cache: RedisCache | None = None, account: str | None = None) -> None:
    """Request đã được đếm ngay trong client - ở đây chỉ log mức đã dùng sau mỗi job."""
    if DAILY_CAP <= 0:
        return
    cache = cache or RedisCache()
    active = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
    if active:
        logger.info("facebook_request_budget", account=active, used=used_today(str(active), cache), cap=DAILY_CAP)
