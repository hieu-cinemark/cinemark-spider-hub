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
# Hết hạn mức là hành vi dự kiến, không phải sự cố: log mức info (warning tự gửi Telegram - mỗi tài khoản một tin mỗi ngày).

# Đếm theo REQUEST GraphQL (24/10/2026; trước đó đếm job - 40 job = 4.000-16.000 request): mọi request Facebook của một
# tài khoản (tìm kiếm, page feed, bình luận) cộng vào cùng một bộ đếm trong ngày (CometGraphQLClient._count_request).
# Job chỉ bắt đầu khi còn dưới DAILY_CAP; đang chạy mà vượt HARD_CAP_FACTOR x DAILY_CAP thì spider dừng gọn.
DAILY_CAP = int(os.getenv("FACEBOOK_DAILY_REQUEST_CAP", "500"))
HARD_CAP_FACTOR = 1.2
REST_UNTIL = os.getenv("FACEBOOK_CAP_REST_UNTIL", "05:30")
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
_TTL_SECONDS = 2 * 86400
# Chia hạn mức theo buổi (24/10/2026): tới đầu buổi thứ k (trong n buổi) mỗi tài khoản chỉ được dùng k/n DAILY_CAP. Trước
# đó hai làn đốt hết hạn mức của mọi tài khoản ngay sau 05:30, lượt chiều/tối (giờ bài đăng nhiều nhất là 19-21h) không còn
# request nào; mỗi tài khoản cũng chạy dồn 500 request một mạch thay vì rải ra cả ngày như người thật. Rỗng = không chia.
PERIODS = [p.strip() for p in os.getenv("FACEBOOK_BUDGET_PERIODS", "05:30,12:00,18:00").split(",") if p.strip()]


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


def _period_starts(now: datetime) -> list[datetime]:
    return sorted(now.replace(hour=int(p[:2]), minute=int(p[3:5]), second=0, microsecond=0) for p in PERIODS)


def allowed_now(now: datetime | None = None) -> int:
    """Số request mỗi tài khoản được dùng tính tới lúc này trong ngày (cộng dồn theo buổi, xem PERIODS)."""
    if DAILY_CAP <= 0 or not PERIODS:
        return DAILY_CAP
    now = now or datetime.now(_VN)
    started = sum(1 for start in _period_starts(now) if start <= now) or len(PERIODS)  # trước buổi đầu: giờ yên lặng
    return -(-DAILY_CAP * started // len(PERIODS))


def hard_cap() -> int:
    return int(allowed_now() * HARD_CAP_FACTOR) if DAILY_CAP > 0 else 0


def _rest_end(now: datetime, used: int) -> datetime:
    """Hết cả ngày -> nghỉ tới REST_UNTIL sáng mai; mới hết phần của buổi -> nghỉ tới đầu buổi kế tiếp."""
    if used < DAILY_CAP:
        upcoming = [start for start in _period_starts(now) if start > now]
        if upcoming:
            return upcoming[0]
    return _next_rest_end(now)


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
    allowed = allowed_now(now)
    if account:
        used = used_today(account, cache, now)
        if used < allowed:
            return True
        from social_crawler.services.lanes import release_account

        until = _rest_end(now, used)
        rest_account_until("facebook", account, until)
        release_account(account, cache)
        logger.info("facebook_daily_search_cap_reached", account=account, cap=allowed, rest_until=until.isoformat())
        return False
    for _ in range(10):  # mỗi vòng cho nghỉ một tài khoản hết hạn mức
        active = cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
        used = used_today(str(active), cache, now) if active else 0
        if active and used >= allowed:
            until = _rest_end(now, used)
            rest_account_until("facebook", str(active), until)
            cache.delete(ACTIVE_ACCOUNT_REDIS_KEY)
            logger.info("facebook_daily_search_cap_reached", account=active, cap=allowed, rest_until=until.isoformat())
            continue
        if active:
            return True
        # Chưa có tài khoản active: còn tài khoản nào (không cooldown) chưa hết hạn mức thì để bootstrap chọn.
        ready = [a for a in get_accounts("facebook") if used_today(a.get("email") or a["id"], cache, now) < allowed]
        if not ready:
            logger.info("facebook_daily_search_cap_all_accounts", cap=allowed)
        return bool(ready)
    return False


def record_search(cache: RedisCache | None = None, account: str | None = None) -> None:
    """Request đã được đếm ngay trong client - ở đây chỉ log mức đã dùng sau mỗi job."""
    if DAILY_CAP <= 0:
        return
    cache = cache or RedisCache()
    active = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
    if active:
        logger.info(
            "facebook_request_budget",
            account=active,
            used=used_today(str(active), cache),
            allowed_now=allowed_now(),
            cap=DAILY_CAP,
        )
