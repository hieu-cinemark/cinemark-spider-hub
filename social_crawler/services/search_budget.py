"""Hạn mức request Facebook mỗi tài khoản mỗi ngày (24/10/2026, chống checkpoint): một tài khoản gánh hàng nghìn request
/ ngày là hành vi không người thật nào có - tài khoản nào cũng dính checkpoint, chỉ sớm hay muộn.

- Mỗi request GraphQL/HTML của client cộng 1 (count_request); vượt hard_cap() thì spider dừng gọn.
- Trước mỗi job Facebook (consumer/jobs/search.py): tài khoản đã đủ FACEBOOK_DAILY_REQUEST_CAP hôm nay (giờ VN) thì cho
  nghỉ tới REST_UNTIL sáng hôm sau, gỡ con trỏ active / nhả khỏi làn để chọn tài khoản khác; không còn tài khoản nào
  dưới hạn mức thì bỏ qua job (keyword đó đợi lượt sau).
Bộ đếm trong Redis, key sống 2 ngày. FACEBOOK_DAILY_REQUEST_CAP=0 tắt.
"""

from __future__ import annotations

import math
import os
import time
from datetime import date, datetime, timedelta
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

# Chia hạn mức theo PHẦN (27/10/2026): job tìm kiếm mang lane "fresh" (bài mới: lượt "Bài viết gần đây" nông) hoặc "deep"
# (đào bài cũ lùi dần - services/deep_cursor.py). Mỗi phần có trần riêng trong ngày, nên phần đào không bao giờ ăn mất
# request của lượt bài mới (trước đó lượt quét sâu 05:30 dùng hết hạn mức, lượt "gần đây" 12:30/19:30 không còn gì).
# Job không có lane (chạy tay, page_feed, bình luận) chỉ tính vào tổng DAILY_CAP như cũ.
FRESH_SHARE = float(os.getenv("FACEBOOK_FRESH_SHARE", "0.4"))
SHARES = ("fresh", "deep")
# Trần request mỗi tài khoản mỗi GIỜ (Facebook và Threads): rải việc ra cả ngày thay vì một tài khoản bắn hàng trăm
# request trong một giờ. Làn chạm trần thì tạm dừng nhận job tới đầu giờ sau (consumer/loop.py). 0 = tắt.
HOURLY_CAP = int(os.getenv("FACEBOOK_HOURLY_REQUEST_CAP", "60"))
THREADS_HOURLY_CAP = int(os.getenv("THREADS_HOURLY_REQUEST_CAP", "60"))
_HOUR_TTL_SECONDS = 2 * 3600
# Tăng tải dần cho tài khoản mới (27/10/2026): tài khoản thêm từ warmup.WARMUP_STRICT_SINCE, ngày crawl thứ 1/2/3 (tính từ
# request đầu tiên) chỉ được RAMP[i] x mọi hạn mức (ngày, phần, giờ); từ ngày thứ len(RAMP)+1 đủ 100%. Rỗng = tắt.
RAMP = [float(x) for x in os.getenv("FACEBOOK_NEW_ACCOUNT_RAMP", "0.2,0.4,0.7").split(",") if x.strip()]
_FIRST_DAY_TTL_SECONDS = 30 * 86400
_new_accounts_cache: tuple[float, set[str]] | None = None


def _key(account: str, now: datetime) -> str:
    return f"fb_request_budget:{now.date().isoformat()}:{account.strip().lower()}"


def _share_key(account: str, share: str, now: datetime) -> str:
    return f"fb_request_budget:{now.date().isoformat()}:{account.strip().lower()}:{share}"


def _hour_key(platform: str, account: str, now: datetime) -> str:
    return f"{platform}_request_hour:{now.strftime('%Y-%m-%dT%H')}:{account.strip().lower()}"


def _new_account_keys() -> set[str]:
    """account_id + email (chữ thường) của tài khoản Facebook thêm từ WARMUP_STRICT_SINCE - nhớ 10 phút trong tiến trình
    (hàm này chạy ở mỗi request)."""
    global _new_accounts_cache
    now = time.monotonic()
    if _new_accounts_cache and now - _new_accounts_cache[0] < 600:
        return _new_accounts_cache[1]
    from social_crawler.db.accounts import accounts_created_since
    from social_crawler.services.warmup import WARMUP_STRICT_SINCE

    keys: set[str] = set()
    for account_id, email in accounts_created_since("facebook", WARMUP_STRICT_SINCE).items():
        keys.add(account_id.strip().lower())
        if email:
            keys.add(email.strip().lower())
    _new_accounts_cache = (now, keys)
    return keys


def _first_day_key(account: str) -> str:
    return f"fb_first_crawl_day:{account.strip().lower()}"


def ramp_factor(account: str | None, cache: RedisCache | None = None, now: datetime | None = None) -> float:
    """Hệ số hạn mức của tài khoản: < 1 trong len(RAMP) ngày crawl đầu của tài khoản mới, còn lại 1."""
    if not account or not RAMP or account.strip().lower() not in _new_account_keys():
        return 1.0
    cache = cache or RedisCache()
    today = (now or datetime.now(_VN)).date()
    first = cache.get(_first_day_key(account))
    if not first:
        return RAMP[0]
    days = (today - date.fromisoformat(str(first))).days
    return RAMP[days] if 0 <= days < len(RAMP) else 1.0


def _note_first_crawl_day(account: str, cache: RedisCache, now: datetime) -> None:
    if account.strip().lower() in _new_account_keys() and not cache.get(_first_day_key(account)):
        cache.set(_first_day_key(account), now.date().isoformat(), ttl_seconds=_FIRST_DAY_TTL_SECONDS)


def share_cap(share: str) -> int:
    """Trần trong ngày của một phần (fresh/deep) cho mỗi tài khoản; hai phần cộng lại đúng DAILY_CAP."""
    fresh = math.ceil(DAILY_CAP * FRESH_SHARE)
    return fresh if share == "fresh" else DAILY_CAP - fresh


def hourly_cap(platform: str) -> int:
    return HOURLY_CAP if platform == "facebook" else THREADS_HOURLY_CAP if platform == "threads" else 0


def _incr(cache: RedisCache, key: str, ttl: int) -> int:
    count = cache.incr(key)
    if count == 1:
        cache.expire(key, ttl)
    return count


def count_request(account: str, cache: RedisCache | None = None, share: str | None = None) -> int:
    """+1 request cho tài khoản hôm nay (giờ VN) - tổng, phần `share` nếu có, và giờ hiện tại; trả tổng đã dùng."""
    cache = cache or RedisCache()
    now = datetime.now(_VN)
    count = _incr(cache, _key(account, now), _TTL_SECONDS)
    if RAMP and count == 1:
        _note_first_crawl_day(account, cache, now)
    if share in SHARES:
        _incr(cache, _share_key(account, share, now), _TTL_SECONDS)
    if HOURLY_CAP > 0:
        _incr(cache, _hour_key("facebook", account, now), _HOUR_TTL_SECONDS)
    return count


def count_hour_request(platform: str, account: str, cache: RedisCache | None = None) -> int:
    """+1 request vào bộ đếm theo giờ (Threads - Facebook đếm trong count_request); trả số đã dùng giờ này."""
    return _incr(cache or RedisCache(), _hour_key(platform, account, datetime.now(_VN)), _HOUR_TTL_SECONDS)


def _read_int(cache: RedisCache, key: str) -> int:
    try:
        return int(cache.get(key) or 0)
    except TypeError, ValueError:
        return 0


def used_share(account: str, share: str, cache: RedisCache | None = None, now: datetime | None = None) -> int:
    return _read_int(cache or RedisCache(), _share_key(account, share, now or datetime.now(_VN)))


def used_this_hour(platform: str, account: str, cache: RedisCache | None = None, now: datetime | None = None) -> int:
    return _read_int(cache or RedisCache(), _hour_key(platform, account, now or datetime.now(_VN)))


def share_hard_cap(share: str) -> int:
    return int(share_cap(share) * HARD_CAP_FACTOR)


def hourly_hard_cap(platform: str) -> int:
    """Trong job: vượt mức này thì spider dừng gọn (job đã bắt đầu dưới trần được chạy nốt một chút, không quá xa)."""
    return int(hourly_cap(platform) * HARD_CAP_FACTOR)


def seconds_to_next_hour(now: datetime | None = None) -> float:
    now = now or datetime.now(_VN)
    return ((now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0) - now).total_seconds()


def hour_cap_reached(platform: str, account: str | None, cache: RedisCache | None = None) -> bool:
    cap = hourly_cap(platform)
    if not account or cap <= 0:
        return False
    if platform == "facebook":
        cap = int(cap * ramp_factor(str(account), cache))
    return used_this_hour(platform, str(account), cache) >= cap


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
        if used < int(allowed * ramp_factor(account, cache, now)):
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
        if active and used >= int(allowed * ramp_factor(str(active), cache, now)):
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


def share_available(share: str | None, cache: RedisCache | None = None, account: str | None = None) -> bool:
    """Phần `share` (fresh/deep) của tài khoản còn request hôm nay không. Hết phần thì KHÔNG cho tài khoản nghỉ - nó vẫn
    làm được phần kia; job này bị bỏ qua (lượt bài mới kế tiếp / nhịp đào giờ sau xếp lại)."""
    if DAILY_CAP <= 0 or share not in SHARES:
        return True
    cache = cache or RedisCache()
    account = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
    if not account:
        return True  # chưa có tài khoản active: bootstrap chọn, client vẫn chặn ở share_hard_cap
    used = used_share(str(account), share, cache)
    if used < int(share_cap(share) * ramp_factor(str(account), cache)):
        return True
    logger.info("facebook_share_cap_reached", account=account, share=share, used=used, cap=share_cap(share))
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
            used_fresh=used_share(str(active), "fresh", cache),
            used_deep=used_share(str(active), "deep", cache),
            used_this_hour=used_this_hour("facebook", str(active), cache),
            allowed_now=allowed_now(),
            cap=DAILY_CAP,
        )
