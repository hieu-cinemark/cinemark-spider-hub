"""Hạn mức request Facebook mỗi tài khoản mỗi ngày (24/10/2026, chống checkpoint): một tài khoản gánh hàng nghìn request
/ ngày là hành vi không người thật nào có - tài khoản nào cũng dính checkpoint, chỉ sớm hay muộn.

- Mỗi request GraphQL/HTML của client cộng 1 (count_request); vượt hard_cap() thì spider dừng gọn.
- Trước mỗi job Facebook (consumer/jobs/search.py): tài khoản đã đủ fb_daily_request_cap (cài đặt dashboard) hôm nay (giờ VN) thì cho
  nghỉ tới REST_UNTIL sáng hôm sau, gỡ con trỏ active / nhả khỏi làn để chọn tài khoản khác; không còn tài khoản nào
  dưới hạn mức thì bỏ qua job (keyword đó đợi lượt sau).
Bộ đếm trong Redis, key sống 2 ngày. fb_daily_request_cap (cài đặt dashboard)=0 tắt.
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY
from social_crawler.core.logger import get_logger
from social_crawler.db.accounts import get_accounts, rest_account_until
from social_crawler.db.proxy_settings import get_setting

logger = get_logger(__name__)


def _times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def cfg_daily_cap() -> int:
    return int(get_setting("fb_daily_request_cap"))


def cfg_rest_until() -> str:
    return str(get_setting("fb_cap_rest_until"))


def cfg_periods() -> list[str]:
    return _times(get_setting("fb_budget_periods"))


def cfg_fresh_share() -> float:
    return float(get_setting("fb_fresh_share"))


def cfg_hourly_cap() -> int:
    return int(get_setting("fb_hourly_request_cap"))


def cfg_threads_hourly_cap() -> int:
    return int(get_setting("threads_hourly_request_cap"))


def cfg_fresh_periods() -> list[str]:
    return _times(get_setting("fb_fresh_periods"))


def cfg_deep_periods() -> list[str]:
    return _times(get_setting("fb_deep_periods"))


def cfg_ramp() -> list[float]:
    return [float(x) for x in _times(get_setting("fb_new_account_ramp"))]


def cfg_new_account_daily_cap() -> int:
    return int(get_setting("fb_new_account_daily_cap"))


def cfg_restart_daily_caps() -> list[int]:
    return [int(x) for x in _times(get_setting("fb_restart_daily_caps"))]


def cfg_restart_idle_days() -> int:
    return int(get_setting("fb_restart_idle_days"))


# Hết hạn mức là hành vi dự kiến, không phải sự cố: log mức info (warning tự gửi Telegram - mỗi tài khoản một tin mỗi ngày).

# Đếm theo REQUEST GraphQL (24/10/2026; trước đó đếm job - 40 job = 4.000-16.000 request): mọi request Facebook của một
# tài khoản (tìm kiếm, page feed, bình luận) cộng vào cùng một bộ đếm trong ngày (CometGraphQLClient._count_request).
# Job chỉ bắt đầu khi còn dưới DAILY_CAP; đang chạy mà vượt HARD_CAP_FACTOR x DAILY_CAP thì spider dừng gọn.
# Hiện tại 400/tài khoản/ngày (31/10/2026, người vận hành chọn: chỉ còn vài tài khoản, không có kinh phí thay - giữ tài khoản
# sống lâu quan trọng hơn chạy hết tốc độ). DAILY_CAP=0 tắt hạn mức ngày, phần fresh/deep và RAMP (28-31/10 từng tắt; khi
# đó chỉ tài khoản mới còn trần NEW_ACCOUNT_DAILY_CAP).
HARD_CAP_FACTOR = 1.2
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
_TTL_SECONDS = 2 * 86400
# Chia hạn mức theo buổi (24/10/2026): tới đầu buổi thứ k (trong n buổi) mỗi tài khoản chỉ được dùng k/n DAILY_CAP. Trước
# đó hai làn đốt hết hạn mức của mọi tài khoản ngay sau 05:30, lượt chiều/tối (giờ bài đăng nhiều nhất là 19-21h) không còn
# request nào; mỗi tài khoản cũng chạy dồn 500 request một mạch thay vì rải ra cả ngày như người thật. Rỗng = không chia.

# Chia hạn mức theo PHẦN (27/10/2026): job tìm kiếm mang lane "fresh" (bài mới: lượt "Bài viết gần đây" nông) hoặc "deep"
# (đào bài cũ lùi dần - services/deep_cursor.py). Mỗi phần có trần riêng trong ngày, nên phần đào không bao giờ ăn mất
# request của lượt bài mới (trước đó lượt quét sâu 05:30 dùng hết hạn mức, lượt "gần đây" 12:30/19:30 không còn gì).
# Job không có lane (chạy tay, bình luận) chỉ tính vào tổng DAILY_CAP như cũ.
SHARES = ("fresh", "deep")
# Trần request mỗi tài khoản mỗi GIỜ (Facebook và Threads): rải việc ra cả ngày thay vì một tài khoản bắn hàng trăm
# request trong một giờ. Làn chạm trần thì tạm dừng nhận job tới đầu giờ sau (consumer/queues.py). 0 = tắt.
# Facebook mặc định TẮT từ 03/11/2026 (người vận hành chọn): chỉ quy định theo ngày, tăng dần bằng RESTART_DAILY_CAPS; việc được
# rải bởi các mốc FRESH_PERIODS / DEEP_PERIODS và khoảng nghỉ giữa job. Threads vẫn tắt.
_HOUR_TTL_SECONDS = 2 * 3600
# Mỗi phần được mở dần theo lịch riêng của nó (28/10/2026; trước đó theo buổi PERIODS 05:30/12:00/18:00 - lượt bài mới
# 06h dùng hết phần buổi sáng, lượt 09h/15h/21h rơi vào lúc chưa mở phần mới nên mọi job bị "bỏ qua"). Phần chưa dùng
# cộng dồn sang mốc sau. FRESH_PERIODS nên khớp giờ chạy Facebook trong lịch crawl (dashboard).
# Tăng tải dần cho tài khoản mới (27/10/2026): tài khoản thêm từ warmup.WARMUP_STRICT_SINCE, ngày crawl thứ 1/2/3 (tính từ
# request đầu tiên) chỉ được RAMP[i] x mọi hạn mức (ngày, phần, giờ); từ ngày thứ len(RAMP)+1 đủ 100%. Rỗng = tắt.
# Trần ngày RIÊNG cho tài khoản mới (thêm từ WARMUP_STRICT_SINCE) - CHỈ có hiệu lực khi hạn mức chung đang tắt (DAILY_CAP=0;
# khi DAILY_CAP > 0, tài khoản mới đã chịu DAILY_CAP x RAMP) - 30/10/2026.
# Tắt hạn mức chung từ 28/10 cũng làm RAMP mất tác dụng: tài khoản vừa nuôi xong chạy ngay hết tốc độ. Repo hopfian/fbk
# ghi nhận ~47 request đều đặn trong 2 phút đã đủ để tài khoản bị cảnh báo "hành vi tự động", và dùng trần 500/ngày.
# Tài khoản mới: NEW_ACCOUNT_DAILY_CAP x RAMP (100 / 200 / 350 rồi 500 mỗi ngày); tài khoản cũ vẫn không giới hạn. 0 = tắt.
# Tăng dần khi chạy lại (03/11/2026): mọi tài khoản Facebook, ngày crawl đầu tiên sau khi nghỉ từ RESTART_IDLE_DAYS ngày (hoặc chưa
# từng crawl) chỉ được RESTART_DAILY_CAPS[0] request, ngày sau [1]..., hết danh sách thì đủ DAILY_CAP. Không cần đọc công tắc lịch:
# lịch tắt thì tài khoản không có request, bật lại là tự bắt đầu từ đầu. Các số là điểm xuất phát, chưa chứng minh là an toàn. Rỗng = tắt.
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
    from social_crawler.accounts.warmup import WARMUP_STRICT_SINCE
    from social_crawler.db.accounts import accounts_created_since

    keys: set[str] = set()
    for account_id, email in accounts_created_since("facebook", WARMUP_STRICT_SINCE).items():
        keys.add(account_id.strip().lower())
        if email:
            keys.add(email.strip().lower())
    _new_accounts_cache = (now, keys)
    return keys


def is_new_account(account: str | None) -> bool:
    return bool(account) and str(account).strip().lower() in _new_account_keys()


def new_account_cap(account: str | None, cache: RedisCache | None = None, now: datetime | None = None) -> int:
    """Trần request hôm nay của một tài khoản mới khi hạn mức chung tắt; 0 = không áp (hạn mức chung đang bật - nó đã
    nhân RAMP - hoặc tài khoản cũ, hoặc NEW_ACCOUNT_DAILY_CAP=0)."""
    if cfg_daily_cap() > 0 or cfg_new_account_daily_cap() <= 0 or not is_new_account(account):
        return 0
    return max(1, int(cfg_new_account_daily_cap() * ramp_factor(str(account), cache, now)))


def _first_day_key(account: str) -> str:
    return f"fb_first_crawl_day:{account.strip().lower()}"


def _restart_key(account: str) -> str:
    return f"fb_restart_start_day:{account.strip().lower()}"


def _last_crawl_key(account: str) -> str:
    return f"fb_last_crawl_day:{account.strip().lower()}"


def _idle_since(last: object, today: date) -> bool:
    return not last or (today - date.fromisoformat(str(last))).days >= cfg_restart_idle_days()


def restart_daily_cap(account: str, cache: RedisCache, today: date) -> int | None:
    """Trần ngày trong giai đoạn tăng dần sau khi nghỉ; None khi đã qua giai đoạn này."""
    if not cfg_restart_daily_caps():
        return None
    start = cache.get(_restart_key(account))
    if not start or _idle_since(cache.get(_last_crawl_key(account)), today):
        return cfg_restart_daily_caps()[0]
    days = (today - date.fromisoformat(str(start))).days
    return cfg_restart_daily_caps()[days] if 0 <= days < len(cfg_restart_daily_caps()) else None


def _note_crawl_day(account: str, cache: RedisCache, now: datetime) -> None:
    today = now.date()
    if _idle_since(cache.get(_last_crawl_key(account)), today) or not cache.get(_restart_key(account)):
        cache.set(_restart_key(account), today.isoformat(), ttl_seconds=_FIRST_DAY_TTL_SECONDS)
        logger.info("facebook_restart_ramp_started", account=account, daily_caps=cfg_restart_daily_caps())
    cache.set(_last_crawl_key(account), today.isoformat(), ttl_seconds=_FIRST_DAY_TTL_SECONDS)


def ramp_factor(account: str | None, cache: RedisCache | None = None, now: datetime | None = None) -> float:
    """Hệ số hạn mức của tài khoản: bên thấp hơn giữa giai đoạn tài khoản mới và giai đoạn tăng dần sau khi nghỉ."""
    if not account:
        return 1.0
    cache = cache or RedisCache()
    now = now or datetime.now(_VN)
    factor = _new_account_factor(account, cache, now)
    cap = restart_daily_cap(account, cache, now.date()) if cfg_daily_cap() > 0 else None
    return min(factor, cap / cfg_daily_cap()) if cap else factor


def _new_account_factor(account: str, cache: RedisCache, now: datetime) -> float:
    """< 1 trong len(RAMP) ngày crawl đầu của tài khoản mới, còn lại 1."""
    if not cfg_ramp() or account.strip().lower() not in _new_account_keys():
        return 1.0
    today = now.date()
    first = cache.get(_first_day_key(account))
    if not first:
        return cfg_ramp()[0]
    days = (today - date.fromisoformat(str(first))).days
    return cfg_ramp()[days] if 0 <= days < len(cfg_ramp()) else 1.0


def _note_first_crawl_day(account: str, cache: RedisCache, now: datetime) -> None:
    if account.strip().lower() in _new_account_keys() and not cache.get(_first_day_key(account)):
        cache.set(_first_day_key(account), now.date().isoformat(), ttl_seconds=_FIRST_DAY_TTL_SECONDS)


def share_cap(share: str) -> int:
    """Trần trong ngày của một phần (fresh/deep) cho mỗi tài khoản; hai phần cộng lại đúng DAILY_CAP."""
    fresh = math.ceil(cfg_daily_cap() * cfg_fresh_share())
    return fresh if share == "fresh" else cfg_daily_cap() - fresh


def hourly_cap(platform: str) -> int:
    return cfg_hourly_cap() if platform == "facebook" else cfg_threads_hourly_cap() if platform == "threads" else 0


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
    if cfg_ramp() and count == 1:
        _note_first_crawl_day(account, cache, now)
    if cfg_restart_daily_caps() and count == 1:
        _note_crawl_day(account, cache, now)
    if share in SHARES:
        _incr(cache, _share_key(account, share, now), _TTL_SECONDS)
    if cfg_hourly_cap() > 0:
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
    return sorted(now.replace(hour=int(p[:2]), minute=int(p[3:5]), second=0, microsecond=0) for p in cfg_periods())


def allowed_now(now: datetime | None = None) -> int:
    """Số request mỗi tài khoản được dùng tính tới lúc này trong ngày (cộng dồn theo buổi, xem PERIODS). Khi đã chia theo
    phần (FRESH_SHARE > 0) tổng không chia theo buổi nữa: hai phần tự mở dần theo FRESH_PERIODS / DEEP_PERIODS và trần
    theo giờ đã rải việc ra cả ngày - chia thêm một lớp theo buổi chỉ chặn job hai lần."""
    if cfg_daily_cap() <= 0 or not cfg_periods() or cfg_fresh_share() > 0:
        return cfg_daily_cap()
    now = now or datetime.now(_VN)
    started = sum(1 for start in _period_starts(now) if start <= now) or len(
        cfg_periods()
    )  # trước buổi đầu: giờ yên lặng
    return -(-cfg_daily_cap() * started // len(cfg_periods()))


def hard_cap() -> int:
    return int(allowed_now() * HARD_CAP_FACTOR) if cfg_daily_cap() > 0 else 0


def _rest_end(now: datetime, used: int) -> datetime:
    """Hết cả ngày -> nghỉ tới REST_UNTIL sáng mai; mới hết phần của buổi -> nghỉ tới đầu buổi kế tiếp."""
    if used < cfg_daily_cap():
        upcoming = [start for start in _period_starts(now) if start > now]
        if upcoming:
            return upcoming[0]
    return _next_rest_end(now)


def _next_rest_end(now: datetime) -> datetime:
    end = now.replace(hour=int(cfg_rest_until()[:2]), minute=int(cfg_rest_until()[3:5]), second=0, microsecond=0)
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
    if cfg_daily_cap() <= 0:
        return _ensure_new_account_budget(cache or RedisCache(), account)
    cache = cache or RedisCache()
    now = datetime.now(_VN)
    allowed = allowed_now(now)
    if account:
        used = used_today(account, cache, now)
        if used < int(allowed * ramp_factor(account, cache, now)):
            return True
        from social_crawler.runtime.lanes import release_account

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


def _ensure_new_account_budget(cache: RedisCache, account: str | None) -> bool:
    """Hạn mức chung tắt: chỉ chặn tài khoản mới đã chạm new_account_cap - cho nghỉ tới REST_UNTIL sáng mai. Làn (có
    `account`) nhận False để đổi tài khoản; đường con trỏ active nhận True vì bootstrap sẽ chọn tài khoản khác."""
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
    if not target:
        return True
    now = datetime.now(_VN)
    cap = new_account_cap(str(target), cache, now)
    used = used_today(str(target), cache, now)
    if not cap or used < cap:
        return True
    until = _next_rest_end(now)
    rest_account_until("facebook", str(target), until)
    if account:
        from social_crawler.runtime.lanes import release_account

        release_account(account, cache)
    elif cache.get(ACTIVE_ACCOUNT_REDIS_KEY) == target:
        cache.delete(ACTIVE_ACCOUNT_REDIS_KEY)
    logger.info("facebook_new_account_cap_reached", account=target, used=used, cap=cap, rest_until=until.isoformat())
    return not account


def share_allowed_now(share: str, now: datetime | None = None) -> int:
    """Phần `share` được dùng tính tới lúc này: "fresh" mở 1/len(FRESH_PERIODS) ở mỗi lượt bài mới, "deep" mở mỗi giờ
    (DEEP_PERIODS); phần chưa dùng cộng dồn. Trước mốc đầu tiên trong ngày: 0 (giờ yên lặng / nuôi tài khoản)."""
    cap = share_cap(share)
    periods = cfg_fresh_periods() if share == "fresh" else cfg_deep_periods()
    if cfg_daily_cap() <= 0 or not periods:
        return cap
    now = now or datetime.now(_VN)
    starts = [now.replace(hour=int(p[:2]), minute=int(p[3:5]), second=0, microsecond=0) for p in periods]
    started = sum(1 for start in starts if start <= now)
    return -(-cap * started // len(periods))


def share_available(
    share: str | None, cache: RedisCache | None = None, account: str | None = None, *, quiet: bool = False
) -> bool:
    """Phần `share` (fresh/deep) của tài khoản còn request lúc này không. Hết phần thì KHÔNG cho tài khoản nghỉ - nó vẫn
    làm được phần kia; làn đổi sang tài khoản còn phần (lanes._ready_accounts) hoặc job bị bỏ qua."""
    if cfg_daily_cap() <= 0 or share not in SHARES:
        return True
    cache = cache or RedisCache()
    account = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY)
    if not account:
        return True  # chưa có tài khoản active: bootstrap chọn, client vẫn chặn ở share_hard_cap
    used = used_share(str(account), share, cache)
    cap = int(share_allowed_now(share) * ramp_factor(str(account), cache))
    if used < cap:
        return True
    if not quiet:
        logger.info("facebook_share_cap_reached", account=account, share=share, used=used, cap=cap)
    return False


def record_search(cache: RedisCache | None = None, account: str | None = None) -> None:
    """Request đã được đếm ngay trong client - ở đây chỉ log mức đã dùng sau mỗi job."""
    if cfg_daily_cap() <= 0:
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
            cap=cfg_daily_cap(),
        )
