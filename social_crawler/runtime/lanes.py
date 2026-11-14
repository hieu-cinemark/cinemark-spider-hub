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

Khoá theo máy (02/11/2026): giá trị khoá là "<instance>:<lane>", key tài khoản của làn cũng có instance. Trước đó chỉ có số
làn, nên làn 0 của local và làn 0 của VPS (nếu dùng chung Redis) đều coi khoá của nhau là của mình và cùng dùng một tài
khoản. INSTANCE_ID ổn định qua restart (không phải UUID mỗi lần chạy) để làn nhận lại đúng tài khoản sau khi khởi động lại
thay vì chờ khoá hết hạn tới sáng hôm sau, và để spider con (release_account) cùng instance với consumer. Hai consumer trên
cùng một máy thì phải đặt CRAWLER_INSTANCE_ID khác nhau.
"""

from __future__ import annotations

import contextvars
import os
import random
import socket
from datetime import datetime
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.accounts import get_accounts, get_accounts_by_check_status
from social_crawler.runtime import account_lease
from social_crawler.spiders.facebook.auth.accounts import account_key as fb_account_key

logger = get_logger(__name__)

LANES = max(1, int(os.getenv("FACEBOOK_LANES", "2")))
# Lane TikTok: TIKTOK_LANES consumer trong nhóm TikTok chạy job song song (crawl_requests cần ít nhất ngần ấy
# partition). Job TikTok dùng danh tính khách, không dùng tài khoản, nên lane chỉ giữ port proxy của nó (xem
# clients/proxy_provider.lease_proxy) - không chia sẻ trạng thái tài khoản với các lane Facebook ở trên.
TIKTOK_LANES = max(1, int(os.getenv("TIKTOK_LANES", "1")))
# Số lane chuyền cho tiến trình con cào (consumer/process.py) để spider biết mình chạy ở lane nào.
WORKER_LANE_ENV = "CRAWL_WORKER_LANE"
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
INSTANCE_ID = os.getenv("CRAWLER_INSTANCE_ID") or socket.gethostname()
_LANE_KEY = "facebook:lane:{instance}:{lane}:account"
_LOCK_KEY = "facebook:lane_lock:{account}"


def _lane_key(lane: int) -> str:
    return _LANE_KEY.format(instance=INSTANCE_ID, lane=lane)


def _owner(lane: int) -> str:
    return f"{INSTANCE_ID}:{lane}"


def _lane_of_lock(holder: object) -> int | None:
    """Làn của instance NÀY đang giữ khoá có giá trị `holder`; None = khoá của instance khác (hoặc không có khoá). Số trơn
    là khoá ghi trước khi có instance - còn sống tới khi hết TTL sau lần deploy đầu, coi như của mình."""
    if isinstance(holder, bool):
        return None
    if isinstance(holder, int):
        return holder
    if isinstance(holder, str) and holder.startswith(f"{INSTANCE_ID}:"):
        suffix = holder[len(INSTANCE_ID) + 1 :]
        return int(suffix) if suffix.isdigit() else None
    return None


# Làn của task asyncio hiện tại (đặt ở đầu mỗi vòng consumer Facebook); None = không chạy theo làn (con trỏ chung cũ).
_current_lane: contextvars.ContextVar[int | None] = contextvars.ContextVar("facebook_lane", default=None)


def set_current_lane(lane: int | None) -> None:
    _current_lane.set(lane)


def current_lane() -> int | None:
    return _current_lane.get()


# Lane của task consumer hiện tại cho MỌI nền tảng (Facebook và TikTok). Tách khỏi current_lane() mà logic
# tài khoản Facebook đọc, để lane TikTok không bao giờ bị coi là lane Facebook.
_worker_lane: contextvars.ContextVar[int | None] = contextvars.ContextVar("worker_lane", default=None)


def set_worker_lane(lane: int | None) -> None:
    _worker_lane.set(lane)


def worker_lane() -> int | None:
    """Lane của task consumer này, hoặc của consumer đã mở tiến trình con (CRAWL_WORKER_LANE); None = không chia lane."""
    lane = _worker_lane.get()
    if lane is not None:
        return lane
    raw = os.getenv(WORKER_LANE_ENV, "")
    return int(raw) if raw.isdigit() else None


def _ttl_seconds() -> int:
    from social_crawler.runtime.search_budget import _next_rest_end

    now = datetime.now(_VN)
    return max(60, int((_next_rest_end(now) - now).total_seconds()))


def _ready_accounts(cache: RedisCache, share: str | None = None, *, ignore_hour_cap: bool = False) -> list[str]:
    """Key tài khoản (email / id đã chuẩn hoá) đủ điều kiện làm việc lúc này, ít dùng hôm nay nhất trước. Loại tài khoản
    có phiên chết (lần kiểm tra gần nhất 'dead') - làn nhận nó chỉ tốn một lần bootstrap thất bại.

    27/10/2026: loại cả tài khoản đã chạm trần request của giờ này, và - khi job cần một phần hạn mức (`share`
    fresh/deep) - tài khoản đã hết phần đó. Trước đó làn giữ tài khoản tới khi hết TỔNG hạn mức ngày: hết phần "bài mới"
    là mọi job bài mới bị bỏ qua cả ngày dù tài khoản khác còn nguyên phần, chạm trần giờ là cả làn đứng một giờ."""
    from social_crawler.accounts.warmup import unwarmed_account_ids
    from social_crawler.runtime.search_budget import (
        DAILY_CAP,
        allowed_now,
        hour_cap_reached,
        new_account_cap,
        share_available,
        used_today,
    )

    warming = unwarmed_account_ids("facebook", cache)
    dead = {account["id"] for account in get_accounts_by_check_status("facebook", "dead")}
    allowed = allowed_now()
    ready = []
    for account in get_accounts("facebook"):
        if account["id"] in warming or account["id"] in dead:
            continue
        key = fb_account_key(account.get("email") or account["id"])
        if account_lease.holder("facebook", key, cache):  # đang đăng nhập tay / Restore / nuôi: làn dùng tài khoản khác
            continue
        used = used_today(key, cache)
        if DAILY_CAP > 0 and used >= allowed:
            continue
        new_cap = new_account_cap(key, cache)
        if new_cap and used >= new_cap:
            continue
        if not ignore_hour_cap and hour_cap_reached("facebook", key, cache):
            continue
        if not share_available(share, cache, key, quiet=True):
            continue
        ready.append((used, random.random(), key))
    return [key for _, _, key in sorted(ready)]


def only_hour_capped(cache: RedisCache | None = None) -> bool:
    """Không tài khoản nào làm việc được lúc này CHỈ vì trần request của giờ - làn nên tạm dừng tới giờ sau thay vì bỏ
    qua job (hết tổng hạn mức / đang nuôi / chết thì không phải lý do để chờ)."""
    cache = cache or RedisCache()
    return not _ready_accounts(cache) and bool(_ready_accounts(cache, ignore_hour_cap=True))


def release(lane: int, account: str | None = None, cache: RedisCache | None = None) -> None:
    cache = cache or RedisCache()
    account = account or cache.get(_lane_key(lane))
    cache.delete(_lane_key(lane))
    if account:
        lock = _LOCK_KEY.format(account=account)
        if not cache.delete_if_equals(lock, _owner(lane)):
            cache.delete_if_equals(lock, lane)  # khoá cũ, số trơn


def release_account(account: str, cache: RedisCache | None = None) -> None:
    """Nhả tài khoản khỏi làn của instance này đang giữ nó (vd. phiên bị từ chối) - làn đó lấy tài khoản khác ở lần hỏi
    sau. Tài khoản do instance khác giữ thì không đụng tới."""
    cache = cache or RedisCache()
    lane = _lane_of_lock(cache.get(_LOCK_KEY.format(account=account)))
    if lane is not None:
        release(lane, account, cache)


def lane_account(lane: int, cache: RedisCache | None = None, share: str | None = None) -> str | None:
    """Tài khoản làn `lane` đang giữ (còn dùng được - kể cả còn phần `share` và chưa chạm trần giờ), hoặc nhận một tài
    khoản mới; None = hiện không còn tài khoản nào."""
    cache = cache or RedisCache()
    ready = _ready_accounts(cache, share)
    held = cache.get(_lane_key(lane))
    if held and held in ready and _lane_of_lock(cache.get(_LOCK_KEY.format(account=held))) == lane:
        return held
    if held:
        logger.info("facebook_lane_released", lane=lane, account=held)
        release(lane, held, cache)
    ttl = _ttl_seconds()
    # Hai tài khoản không đi cùng lúc qua một proxy: proxy_lease giữ proxy cho tài khoản trong lúc job chạy.
    for key in ready:
        lock = _LOCK_KEY.format(account=key)
        claimed = cache.set_nx(lock, _owner(lane), ttl)
        if not claimed and _lane_of_lock(cache.get(lock)) == lane:
            # Khoá của chính làn này mà key tài khoản đã mất (khoá số trơn trước lần deploy này): nhận lại, đổi sang chủ mới.
            cache.set(lock, _owner(lane), ttl_seconds=ttl)
            claimed = True
        if claimed:
            cache.set(_lane_key(lane), key, ttl_seconds=ttl)
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


def other_lane_has_account(lane: int, cache: RedisCache | None = None) -> bool:
    """Có làn khác của instance này đang giữ tài khoản không."""
    cache = cache or RedisCache()
    for other in range(LANES):
        held = cache.get(_lane_key(other)) if other != lane else None
        if held and _lane_of_lock(cache.get(_LOCK_KEY.format(account=held))) == other:
            return True
    return False


def account_for_job(share: str | None = None) -> str | None:
    """Tài khoản cho job Facebook đang chạy trong làn hiện tại; None khi không chạy theo làn (dùng con trỏ chung)."""
    lane = current_lane()
    return lane_account(lane, share=share) if lane is not None else None
