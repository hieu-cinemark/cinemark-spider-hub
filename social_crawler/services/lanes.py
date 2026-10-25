"""Làn Facebook chạy song song (24/10/2026): FACEBOOK_LANES consumer Kafka cùng group (topic crawl_requests cần >= số
làn partition - message có key facebook:<keyword_id> nên tự rải đều), mỗi làn giữ CỐ ĐỊNH một tài khoản tới REST_UNTIL
sáng hôm sau, kéo theo proxy đã ghim của tài khoản đó. Hai làn không bao giờ dùng chung tài khoản (khoá SET NX).

Trước đây mọi job Facebook đi qua một con trỏ chung facebook:active_account - mỗi lúc một tài khoản làm việc, các tài
khoản khác ngồi chờ. Giờ N tài khoản (N IP khác nhau) làm việc cùng lúc, mỗi tài khoản vẫn chịu đúng hạn mức / nhịp
nghỉ / giờ yên lặng như cũ - Facebook không thấy thêm request nào trên mỗi tài khoản hay mỗi IP.

Tài khoản của làn được chọn khi: đang bật, không checkpoint, không cooldown (get_accounts), đã làm ấm xong
(services/warmup.py), còn hạn mức tìm kiếm hôm nay (services/search_budget.py), chưa làn nào giữ. Ưu tiên tài khoản
dùng ít nhất hôm nay. Tài khoản của làn không còn dùng được (bị từ chối phiên -> cooldown, hết hạn mức, checkpoint)
thì làn nhả nó ra và lấy tài khoản khác ở lần hỏi kế tiếp.
"""

from __future__ import annotations

import contextvars
import os
import random
from datetime import datetime
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.db.accounts import account_proxy_map, get_accounts, get_accounts_by_check_status
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.accounts import account_key as fb_account_key

logger = get_logger(__name__)

LANES = max(1, int(os.getenv("FACEBOOK_LANES", "2")))
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
_LANE_KEY = "facebook:lane:{lane}:account"
_LOCK_KEY = "facebook:lane_lock:{account}"

# Làn của task asyncio hiện tại (đặt ở đầu mỗi vòng consumer Facebook); None = không chạy theo làn (con trỏ chung cũ).
_current_lane: contextvars.ContextVar[int | None] = contextvars.ContextVar("facebook_lane", default=None)


def set_current_lane(lane: int | None) -> None:
    _current_lane.set(lane)


def current_lane() -> int | None:
    return _current_lane.get()


def _ttl_seconds() -> int:
    from social_crawler.services.search_budget import _next_rest_end

    now = datetime.now(_VN)
    return max(60, int((_next_rest_end(now) - now).total_seconds()))


def _ready_accounts(cache: RedisCache) -> list[str]:
    """Key tài khoản (email / id đã chuẩn hoá) đủ điều kiện làm việc lúc này, ít dùng hôm nay nhất trước. Loại tài khoản
    có phiên chết (lần kiểm tra gần nhất 'dead') - làn nhận nó chỉ tốn một lần bootstrap thất bại."""
    from social_crawler.services.search_budget import DAILY_CAP, allowed_now, used_today
    from social_crawler.services.warmup import unwarmed_account_ids

    warming = unwarmed_account_ids("facebook", cache)
    dead = {account["id"] for account in get_accounts_by_check_status("facebook", "dead")}
    allowed = allowed_now()
    ready = []
    for account in get_accounts("facebook"):
        if account["id"] in warming or account["id"] in dead:
            continue
        key = fb_account_key(account.get("email") or account["id"])
        used = used_today(key, cache)
        if DAILY_CAP > 0 and used >= allowed:
            continue
        ready.append((used, random.random(), key))
    return [key for _, _, key in sorted(ready)]


def release(lane: int, account: str | None = None, cache: RedisCache | None = None) -> None:
    cache = cache or RedisCache()
    account = account or cache.get(_LANE_KEY.format(lane=lane))
    cache.delete(_LANE_KEY.format(lane=lane))
    if account and cache.get(_LOCK_KEY.format(account=account)) == lane:
        cache.delete(_LOCK_KEY.format(account=account))


def release_account(account: str, cache: RedisCache | None = None) -> None:
    """Nhả tài khoản khỏi làn đang giữ nó (vd. phiên bị từ chối) - làn đó lấy tài khoản khác ở lần hỏi sau."""
    cache = cache or RedisCache()
    lane = cache.get(_LOCK_KEY.format(account=account))
    if isinstance(lane, int):
        release(lane, account, cache)


def lane_account(lane: int, cache: RedisCache | None = None) -> str | None:
    """Tài khoản làn `lane` đang giữ (còn dùng được), hoặc nhận một tài khoản mới; None = hiện không còn tài khoản nào."""
    cache = cache or RedisCache()
    ready = _ready_accounts(cache)
    held = cache.get(_LANE_KEY.format(lane=lane))
    if held and held in ready and cache.get(_LOCK_KEY.format(account=held)) == lane:
        return held
    if held:
        logger.info("facebook_lane_released", lane=lane, account=held)
        release(lane, held, cache)
    ttl = _ttl_seconds()
    # Hai làn không chạy cùng một IP: bỏ qua tài khoản ghim vào proxy mà làn khác đang dùng.
    proxies = account_proxy_map("facebook")
    busy_proxies = {
        proxies.get(str(other_account))
        for other in range(LANES)
        if other != lane and (other_account := cache.get(_LANE_KEY.format(lane=other)))
    } - {None}
    for key in ready:
        if proxies.get(key) in busy_proxies:
            continue
        if cache.set_nx(_LOCK_KEY.format(account=key), lane, ttl):
            cache.set(_LANE_KEY.format(lane=lane), key, ttl_seconds=ttl)
            logger.info("facebook_lane_account_claimed", lane=lane, account=key, until_seconds=ttl)
            return key
    # Không có tài khoản: job crawl của làn tự bỏ qua (CrawlJobSkipped), job khác (nuôi, kiểm tra cookie) vẫn chạy - trước
    # 25/10/2026 làn dừng hẳn việc nhận message, chặn luôn job nuôi tài khoản mới: không ai nuôi thì không có tài khoản
    # nào dùng được, kẹt vòng tròn. Báo một lần mỗi ngày.
    if cache.set_nx(f"facebook:lane_no_account_alerted:{datetime.now(_VN).date().isoformat()}", lane, 2 * 86400):
        logger.info(
            "facebook_lane_no_account",
            telegram=True,
            lane=lane,
            ready=len(ready),
            hint="every facebook account is warming up, resting, over its request share or dead - crawl jobs are skipped",
        )
    return None


def account_for_job() -> str | None:
    """Tài khoản cho job Facebook đang chạy trong làn hiện tại; None khi không chạy theo làn (dùng con trỏ chung)."""
    lane = current_lane()
    return lane_account(lane) if lane is not None else None
