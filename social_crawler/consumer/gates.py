"""Cổng chờ trước khi nhận job: giờ yên lặng, hạn mức giờ, tài khoản chưa sẵn sàng, giữ hàng đợi bình luận công khai, job cũ."""

from __future__ import annotations

import asyncio
import os
import random
import time
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from aiokafka import AIOKafkaConsumer

from social_crawler.clients.kafka import CRAWL_REQUESTS_TOPIC
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY as FACEBOOK_ACTIVE_ACCOUNT_KEY
from social_crawler.constants.threads import ACTIVE_ACCOUNT_REDIS_KEY as THREADS_ACTIVE_ACCOUNT_KEY
from social_crawler.consumer.common import (
    CrawlJobRetryable,
    crawl_job_key,
    logger,
)
from social_crawler.db.proxy_settings import get_setting
from social_crawler.runtime import lanes, pool, search_budget
from social_crawler.runtime.task_queue import finish_task, is_platform_draining
from social_crawler.spiders.facebook.auth.public_comments import queue_hold_reason

# Nhịp giữa hai job và giờ yên lặng (18/11/2026: từ env sang proxy_settings, sửa ở dashboard > Settings > Proxy > "Nhịp job
# Facebook/Threads", đọc mỗi job). Lịch sử các mức:
# - Nền tảng khác (other_job_pause_*): 5-20s -> 3-8s (19/10/2026) - giữa các trang đã có bóp nhịp riêng, khoảng này chỉ phá nhịp đều.
# - Facebook (facebook_job_pause_*): 20-60s (23/10/2026, sau nhiều tài khoản bị checkpoint / đăng xuất) -> 30-90s (27/10/2026: job
#   bài mới chỉ 1-2 trang, nên khoảng nghỉ giữa job là phần chính của nhịp tài khoản).
# - Threads (threads_job_pause_*): 10-30s (27/10/2026) - tài khoản Meta, nghỉ như Facebook nhưng ngắn hơn.
# - Bình luận Facebook (facebook_comments_pause_*): 15-30s (24/10/2026) - một lô 100 bài trước đây là một tràng request liền.
# - Giờ yên lặng (meta_quiet_hours, giờ VN, 24/10/2026): tài khoản hoạt động liên tục lúc 0-5 giờ sáng là dấu hiệu bot rõ nhất -
#   không chạy job tài khoản nào (kể cả làn ưu tiên); message Kafka nằm chờ tới sáng. "" = tắt.


def _pause_seconds(prefix: str) -> float:
    """Khoảng nghỉ ngẫu nhiên giữa hai job theo cặp proxy_settings `<prefix>_pause_min/max_seconds`."""
    low, high = float(get_setting(f"{prefix}_pause_min_seconds")), float(get_setting(f"{prefix}_pause_max_seconds"))
    return random.uniform(min(low, high), max(low, high))


def _quiet_hours() -> str:
    return str(get_setting("meta_quiet_hours") or "").strip()


QUIET_PLATFORMS = {"facebook", "threads"}
_VN = ZoneInfo("Asia/Ho_Chi_Minh")


def _quiet_seconds_left(platform: str, now: datetime | None = None) -> float:
    """Số giây còn lại của giờ yên lặng nếu `platform` đang trong đó, không thì 0."""
    window = _quiet_hours() if platform in QUIET_PLATFORMS else ""
    if "-" not in window:
        return 0.0
    now = now or datetime.now(_VN)
    start_s, end_s = (part.strip() for part in window.split("-", 1))
    start = now.replace(hour=int(start_s[:2]), minute=int(start_s[3:5]), second=0, microsecond=0)
    end = now.replace(hour=int(end_s[:2]), minute=int(end_s[3:5]), second=0, microsecond=0)
    if start <= end:
        inside = start <= now < end
    else:  # qua nửa đêm
        inside = now >= start or now < end
        if now >= start:
            end += timedelta(days=1)
    return (end - now).total_seconds() if inside else 0.0


# Job tìm kiếm Facebook theo lịch nằm hàng đợi lâu hơn chừng này thì bỏ (24/10/2026): mỗi lượt đặt nhiều việc hơn hạn mức request
# chạy nổi, job dư dồn sang hôm sau trong khi lượt mới vẫn đổ vào. Bỏ job cũ để lượt sau xếp lại từ đầu. Job có lane (27/10/2026):
# lượt bài mới mỗi 3 giờ, nhịp đào mỗi giờ. proxy_settings facebook_stale_job_hours / facebook_fresh_stale_job_hours /
# facebook_deep_stale_job_hours (18/11/2026, trước đây env), 0 = tắt.
_STALE_JOB_SETTING_BY_LANE = {"fresh": "facebook_fresh_stale_job_hours", "deep": "facebook_deep_stale_job_hours"}


def _stale_job_hours(lane: str | None = None) -> float:
    return float(get_setting(_STALE_JOB_SETTING_BY_LANE.get(lane or "", "facebook_stale_job_hours")))


def _retryable_failure(exc: BaseException) -> bool:
    """Lỗi hạ tầng, bản thân request không sao: cạn proxy hoặc item không publish được. NetworkError/TikTokNetworkError
    riêng của mọi nền tảng đều bọc pool.ProxyPoolExhaustedError mà nó được raise từ đó (xem ví dụ `raise ... from exc`
    trong comet_graphql_client.py/tiktok/client.py) - kiểm tra __cause__ ở đây giúp phần này không phụ thuộc nền tảng."""
    return isinstance(exc, (pool.ProxyPoolExhaustedError, CrawlJobRetryable)) or isinstance(
        exc.__cause__, pool.ProxyPoolExhaustedError
    )


def _finish_stale_job(request: dict[str, Any]) -> None:
    """Job hết hạn bị bỏ khỏi Kafka: dọn luôn mục pending của nó, không thì dashboard hiện chờ mãi. Redis lỗi không được
    giết vòng lặp - job vẫn bị bỏ theo đúng ý định."""
    try:
        finish_task(request, "skipped", error="stale")
    except Exception as exc:  # noqa: BLE001
        logger.warning("stale_job_finish_failed", run_id=request.get("run_id"), error=exc)


def _clear_leftover_job_key(platform: str) -> None:
    """Xoá key "job đang chạy" của vòng lặp này còn sót từ tiến trình trước. Tiến trình bị restart/kill giữa job thì
    khối finally (cache.delete(job_key)) không chạy - key không có TTL nên dashboard hiện mãi "Đang thu thập" và nút Dừng
    không xoá được (28/10/2026). Lúc khởi động, vòng lặp này chưa chạy job nào nên mọi thứ trong key đều là đồ cũ."""
    cache = RedisCache()
    key = crawl_job_key(platform)
    job = cache.get(key)
    if job:
        cache.delete(key)
        logger.info("leftover_running_job_cleared", platform=platform, key=key, run_id=(job or {}).get("run_id"))


def _stale_facebook_job(platform: str, request: dict[str, Any], timestamp_ms: int | None) -> bool:
    if platform != "facebook" or not timestamp_ms:
        return False
    if request.get("type") not in (None, "search"):
        return False
    hours = _stale_job_hours(request.get("lane"))
    return hours > 0 and time.time() * 1000 - timestamp_ms > hours * 3_600_000


async def _poll_paused(consumer: AIOKafkaConsumer, timeout_ms: int) -> None:
    """Poll trong lúc tạm dừng nhận job, chỉ để giữ chỗ trong group.

    Rebalance giữa chừng (một làn rút khỏi group khi hết tài khoản rồi vào lại, consumer khởi động lại) giao partition mới mà
    chưa bị pause - getmany trả message của chúng. Bỏ qua message đó là MẤT JOB: vị trí đọc đã tiến, lần commit() kế tiếp đẩy
    offset qua chúng (18/11/2026). Nên pause lại mọi partition đang giữ trước mỗi lần poll, và message lỡ đọc thì seek về đúng
    offset đầu để đọc lại sau khi hết dừng."""
    consumer.pause(*consumer.assignment())
    batch = await consumer.getmany(timeout_ms=timeout_ms)
    for partition, messages in batch.items():
        if messages:
            consumer.seek(partition, messages[0].offset)


def _hour_capped_account(platform: str) -> str | None:
    """Tài khoản active (không chạy theo làn) nếu nó đã chạm trần request của giờ hiện tại (services/search_budget.py)."""
    if platform not in ("facebook", "threads"):
        return None
    cache = RedisCache()
    account = cache.get(FACEBOOK_ACTIVE_ACCOUNT_KEY if platform == "facebook" else THREADS_ACTIVE_ACCOUNT_KEY)
    return str(account) if account and search_budget.hour_cap_reached(platform, str(account), cache) else None


# Làn Facebook không có tài khoản nào (28/10/2026): ít tài khoản dùng được hơn số làn (vd. 2 làn, 1 tài khoản - tài khoản
# kia đang nuôi) thì làn thừa bỏ qua mọi job rơi vào partition của nó - một nửa số job "Đã bỏ qua". Giờ làn thừa rút khỏi
# group để Kafka chuyển partition của nó sang làn có tài khoản, rồi cứ IDLE_LANE_CHECK_SECONDS hỏi lại. Làn 0 chỉ rút khi
# làn khác đang giữ tài khoản (05/11/2026: 1 tài khoản rơi vào làn 1, làn 0 ở lại bỏ qua 3/10 job "hết hạn mức"); không còn
# tài khoản nào thì làn 0 ở lại để luôn có làn đọc message.
IDLE_LANE_CHECK_SECONDS = float(os.getenv("FACEBOOK_IDLE_LANE_CHECK_SECONDS", "120"))
# Đọc qua message của nền tảng khác không chạy lại các bước kiểm tra trước mỗi message (05/11/2026: chúng gọi Redis/DB, qua
# tunnel ~10 giây/message nên ~2.000 message tồn mất nhiều giờ), nhưng vẫn kiểm tra ít nhất một lần sau chừng này giây.
FOREIGN_SKIP_MAX_SECONDS = float(os.getenv("CONSUMER_FOREIGN_SKIP_MAX_SECONDS", "30"))


async def _yield_partitions_while_no_account(consumer: AIOKafkaConsumer, platform: str) -> None:
    lane = lanes.current_lane()
    if platform != "facebook" or lane is None:
        return

    def should_idle() -> bool:
        return not lanes.lane_account(lane) and (lane != 0 or lanes.other_lane_has_account(lane))

    if not await asyncio.to_thread(should_idle):
        return
    logger.info("facebook_lane_idle_no_account", lane=lane, check_every_seconds=IDLE_LANE_CHECK_SECONDS)
    consumer.unsubscribe()
    try:
        while await asyncio.to_thread(should_idle):
            await asyncio.sleep(IDLE_LANE_CHECK_SECONDS)
    finally:
        consumer.subscribe([CRAWL_REQUESTS_TOPIC])
    logger.info("facebook_lane_resumed", lane=lane)


PUBLIC_HOLD_POLL_MS = 60_000
# Ưu tiên tìm kiếm từ khoá Facebook (03/11/2026): còn job search đang chờ thì chưa nhận thêm comment ẩn danh / đọc page; job đang
# chạy không bị ngắt. Mục đang chờ quá facebook_stale_job_hours không tính: consumer sẽ bỏ nó, và danh sách này không có TTL
# nên mục mồ côi không được chặn mãi. 0 = tắt.
# 13/11/2026 (người vận hành chọn): mặc định tắt - đọc page / comment ẩn danh dùng proxy riêng, không tài khoản, nên chạy liên tục.


def _facebook_search_waiting() -> bool:
    if not get_setting("fb_public_yield_to_search"):
        return False
    oldest = time.time() - _stale_job_hours() * 3600
    try:
        pending = RedisCache().lrange_json("task_pending:facebook")
    except Exception:  # noqa: BLE001 - không đọc được thì không chặn; phần còn lại tự xử lý Redis lỗi
        return False
    return any(
        isinstance(item, dict) and item.get("type") in (None, "search") and float(item.get("queued_at") or 0) >= oldest
        for item in pending
    )


def _public_hold_reason() -> str | None:
    return queue_hold_reason() or ("search_priority" if _facebook_search_waiting() else None)


async def _wait_out_public_hold(consumer: AIOKafkaConsumer, platform: str) -> None:
    """Comment ẩn danh (03/11/2026): proxy công khai đang cooldown / hết hạn mức ngày / không dùng được thì mọi job đều hỏng
    ngay - giữ nguyên hàng đợi tới khi hết, thay vì đốt cả hàng thành "failed". Vẫn poll để giữ chỗ trong group; rebalance có
    thể trả partition chưa pause nên message lỡ nhận được seek lại, không mất. Bấm Dừng thì nhả ra để job được bỏ qua."""
    if platform != "facebook_public":
        return
    reason = await asyncio.to_thread(_public_hold_reason)
    if not reason:
        return
    # warning trở lên tự gửi Telegram: chỉ proxy hỏng mới cần người xử lý, cooldown / hết hạn mức tự hết.
    (logger.error if reason == "proxy_unavailable" else logger.info)("public_comments_queue_held", reason=reason)
    consumer.pause(*consumer.assignment())
    try:
        while reason and not is_platform_draining("facebook"):
            await _poll_paused(consumer, PUBLIC_HOLD_POLL_MS)
            reason = await asyncio.to_thread(_public_hold_reason)
    finally:
        consumer.resume(*consumer.assignment())
    logger.info("public_comments_queue_resumed")
