"""Vòng lặp consumer Kafka theo từng nền tảng, làn ưu tiên Redis, khởi động lại khi lỗi."""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from social_crawler.clients.kafka import CRAWL_REQUESTS_TOPIC, FACEBOOK_PUBLIC_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY as FACEBOOK_ACTIVE_ACCOUNT_KEY
from social_crawler.constants.threads import ACTIVE_ACCOUNT_REDIS_KEY as THREADS_ACTIVE_ACCOUNT_KEY
from social_crawler.consumer.common import (
    PLATFORM_CONSUMER_GROUPS,
    CrawlJobRetryable,
    crawl_job_key,
    enabled_platforms,
    logger,
)
from social_crawler.consumer.dispatch import _handle_request, _request_platform
from social_crawler.consumer.jobs import public_page as public_page_job
from social_crawler.consumer.jobs import threads_channel as threads_channel_job
from social_crawler.consumer.process import _sleep_interruptible
from social_crawler.db.proxy_settings import get_proxy_settings
from social_crawler.runtime import lanes, pool, search_budget
from social_crawler.runtime.task_queue import finish_task, is_platform_draining
from social_crawler.spiders.facebook.auth.public_comments import queue_hold_reason

# Khi một lần kích hoạt theo lô (ví dụ cron theo lịch) xếp hàng nhiều từ khoá cùng lúc,
# việc xử lý đúng từng cái một của consumer này (xem docstring module) sẽ để chúng bắn
# liên tiếp không có khoảng nghỉ nào - cùng dấu hiệu bot "đều tăm tắp/liên tục" mà jitter
# theo từng request ở chỗ khác trong project đã chặn, chỉ là ở cấp giữa các lượt crawl thay
# vì giữa các trang. Cùng khoảng như sweep_pause_min/max của Facebook (search.py).
# 5-20s -> 3-8s (19/10/2026): 176 từ khoá FB x trung bình 12,5s = ~37 phút chỉ để nghỉ mỗi lượt. Giữa các trang vẫn có
# bóp nhịp riêng (MIN_REQUEST_INTERVAL_SECONDS + adaptive), nên khoảng này chỉ cần phá nhịp đều. Chỉnh qua env.
INTER_REQUEST_PAUSE_MIN_SECONDS = float(os.getenv("INTER_REQUEST_PAUSE_MIN_SECONDS", "3"))


INTER_REQUEST_PAUSE_MAX_SECONDS = float(os.getenv("INTER_REQUEST_PAUSE_MAX_SECONDS", "8"))
# Facebook nghỉ lâu hơn giữa các job (23/10/2026, sau nhiều tài khoản bị checkpoint / đăng xuất): nhịp dày từ một
# tài khoản là dấu hiệu bot. Đổi lại một lượt 176 từ khoá dài thêm ~2 giờ - chỉnh qua env.
# 20-60s -> 30-90s (27/10/2026): job bài mới chỉ 1-2 trang, nên khoảng nghỉ giữa job là phần chính của nhịp tài khoản.
FACEBOOK_PAUSE_MIN_SECONDS = float(os.getenv("FACEBOOK_PAUSE_MIN_SECONDS", "30"))
FACEBOOK_PAUSE_MAX_SECONDS = float(os.getenv("FACEBOOK_PAUSE_MAX_SECONDS", "90"))
# Threads giữa các job (27/10/2026; trước đó dùng chung 3-8s với TikTok): tài khoản Meta, nghỉ như Facebook nhưng ngắn hơn.
THREADS_PAUSE_MIN_SECONDS = float(os.getenv("THREADS_PAUSE_MIN_SECONDS", "10"))
THREADS_PAUSE_MAX_SECONDS = float(os.getenv("THREADS_PAUSE_MAX_SECONDS", "30"))
FACEBOOK_COMMENTS_PAUSE_MIN_SECONDS = float(os.getenv("FACEBOOK_COMMENTS_PAUSE_MIN_SECONDS", "15"))
FACEBOOK_COMMENTS_PAUSE_MAX_SECONDS = float(os.getenv("FACEBOOK_COMMENTS_PAUSE_MAX_SECONDS", "30"))

# Giờ yên lặng (giờ VN) cho tài khoản Meta (24/10/2026): tài khoản hoạt động liên tục lúc 0-5 giờ sáng là dấu hiệu bot
# rõ nhất - không chạy job nào (kể cả làn ưu tiên); message Kafka nằm chờ tới sáng. "" = tắt.
QUIET_HOURS = os.getenv("META_QUIET_HOURS", "23:30-05:30")
QUIET_PLATFORMS = {"facebook", "threads"}
_VN = ZoneInfo("Asia/Ho_Chi_Minh")


def _quiet_seconds_left(platform: str, now: datetime | None = None) -> float:
    """Số giây còn lại của giờ yên lặng nếu `platform` đang trong đó, không thì 0."""
    if platform not in QUIET_PLATFORMS or "-" not in QUIET_HOURS:
        return 0.0
    now = now or datetime.now(_VN)
    start_s, end_s = (part.strip() for part in QUIET_HOURS.split("-", 1))
    start = now.replace(hour=int(start_s[:2]), minute=int(start_s[3:5]), second=0, microsecond=0)
    end = now.replace(hour=int(end_s[:2]), minute=int(end_s[3:5]), second=0, microsecond=0)
    if start <= end:
        inside = start <= now < end
    else:  # qua nửa đêm
        inside = now >= start or now < end
        if now >= start:
            end += timedelta(days=1)
    return (end - now).total_seconds() if inside else 0.0


# Job tìm kiếm / page_feed Facebook theo lịch nằm hàng đợi lâu hơn chừng này thì bỏ (24/10/2026): mỗi lượt đặt nhiều việc hơn
# hạn mức request chạy nổi, job dư dồn sang hôm sau trong khi lượt mới vẫn đổ vào. Bỏ job cũ để lượt sau xếp lại từ đầu
# (từ khoá hiệu suất cao trước). 0 = tắt.
FACEBOOK_STALE_JOB_HOURS = float(os.getenv("FACEBOOK_STALE_JOB_HOURS", "6"))  # các lượt cách nhau 7h
# Job có lane (27/10/2026): lượt bài mới chạy mỗi 3 giờ, nhịp đào mỗi giờ - job sót lại quá lâu thì lượt sau xếp lại.
FACEBOOK_STALE_JOB_HOURS_BY_LANE = {
    "fresh": float(os.getenv("FACEBOOK_FRESH_STALE_JOB_HOURS", "3")),
    "deep": float(os.getenv("FACEBOOK_DEEP_STALE_JOB_HOURS", "2")),
}


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
        logger.warning("stale_job_finish_failed", run_id=request.get("run_id"), error=str(exc)[:200])


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
    if request.get("type") not in (None, "search", "page_feed"):
        return False
    hours = FACEBOOK_STALE_JOB_HOURS_BY_LANE.get(request.get("lane") or "", FACEBOOK_STALE_JOB_HOURS)
    return hours > 0 and time.time() * 1000 - timestamp_ms > hours * 3_600_000


async def _wait_out_quiet_hours(consumer: AIOKafkaConsumer, platform: str) -> None:
    """Tạm dừng nhận message (consumer.pause) nhưng vẫn poll để giữ chỗ trong group, tới khi hết giờ yên lặng."""
    left = _quiet_seconds_left(platform)
    if left <= 0:
        return
    logger.info("quiet_hours_pausing", platform=platform, minutes=round(left / 60), window=QUIET_HOURS)
    partitions = consumer.assignment()
    consumer.pause(*partitions)
    try:
        while _quiet_seconds_left(platform) > 0 and not is_platform_draining(platform):
            # Job không dùng tài khoản (hàng đợi rảnh) vẫn chạy được trong giờ yên lặng - giờ này là để che tài khoản.
            if not await _run_one_idle_job(platform):
                await consumer.getmany(timeout_ms=60_000)
    finally:
        consumer.resume(*partitions)
    logger.info("quiet_hours_resumed", platform=platform)


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


async def _wait_out_hourly_cap(consumer: AIOKafkaConsumer, platform: str) -> None:
    """Tài khoản chạm trần request của giờ: tạm dừng nhận job tới đầu giờ sau (cộng chút ngẫu nhiên), như giờ yên lặng -
    việc được rải ra cả ngày thay vì một tài khoản bắn hết hạn mức trong một hai giờ. Làn Facebook đổi sang tài khoản
    khác còn request trong giờ trước (lanes.lane_account bỏ qua tài khoản chạm trần), chỉ dừng khi mọi tài khoản dùng
    được đều đã chạm trần giờ."""
    lane = lanes.current_lane()
    if platform == "facebook" and lane is not None:
        account = await asyncio.to_thread(lanes.lane_account, lane)
        if account or not await asyncio.to_thread(lanes.only_hour_capped):
            return
        account = "all"
    else:
        account = await asyncio.to_thread(_hour_capped_account, platform)
        if not account:
            return
    wait = search_budget.seconds_to_next_hour() + random.uniform(60, 300)
    logger.info(
        "hourly_cap_pausing",
        platform=platform,
        account=account,
        lane=lane,
        cap=search_budget.hourly_cap(platform),
        minutes=round(wait / 60),
    )
    partitions = consumer.assignment()
    consumer.pause(*partitions)
    deadline = time.time() + wait
    try:
        while time.time() < deadline and not is_platform_draining(platform):
            if not await _run_one_idle_job(platform):
                await consumer.getmany(timeout_ms=int(min(60, max(1, deadline - time.time())) * 1000))
    finally:
        consumer.resume(*partitions)
    logger.info("hourly_cap_resumed", platform=platform, account=account)


PUBLIC_HOLD_POLL_MS = 60_000
# Ưu tiên tìm kiếm từ khoá Facebook (03/11/2026): còn job search đang chờ thì chưa nhận thêm comment ẩn danh / đọc page; job đang
# chạy không bị ngắt. Mục đang chờ quá FACEBOOK_STALE_JOB_HOURS không tính: consumer sẽ bỏ nó, và danh sách này không có TTL
# nên mục mồ côi không được chặn mãi. 0 = tắt.
PUBLIC_YIELD_TO_SEARCH = os.getenv("FB_PUBLIC_YIELD_TO_SEARCH", "1") not in ("0", "false", "False", "")


def _facebook_search_waiting() -> bool:
    if not PUBLIC_YIELD_TO_SEARCH:
        return False
    oldest = time.time() - FACEBOOK_STALE_JOB_HOURS * 3600
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
            consumer.pause(*consumer.assignment())
            batch = await consumer.getmany(timeout_ms=PUBLIC_HOLD_POLL_MS)
            for partition, messages in batch.items():
                if messages:
                    consumer.seek(partition, messages[0].offset)
            reason = await asyncio.to_thread(_public_hold_reason)
    finally:
        consumer.resume(*consumer.assignment())
    logger.info("public_comments_queue_resumed")


# Một request thất bại vì pool.acquire_proxy_for_account(required=True) thấy cả pool
# proxy của nền tảng đã cạn (xem docstring của lỗi đó - proxy của một tài khoản đã ghim
# đang cooldown và cũng không có proxy thay thế nào khoẻ) KHÔNG phải "một request lỗi" như
# payload sai định dạng hay tài khoản chết - mọi request *khác* của nền tảng này nằm sau
# nó trong topic đều đang lao vào đúng bức tường đó cho tới khi hết cooldown (khoảng thực
# tế đã thấy: 10 phút tới 2 giờ, xem lịch backoff của db.record_proxy_outcome). Trước khi
# có backoff này, vòng lặp nền tảng cứ commit qua từng cái hết tốc lực - đã xác nhận thực
# tế (2026-09-17) đốt sạch cả một hàng tồn comment trong vài giây, mỗi lần thử là một lần
# khởi động/dừng spider tức thì, chắc chắn thất bại, cho tới tận khi cooldown tự hết.
#
# Ngoài bản thân backoff, riêng loại lỗi này được publish lại lên crawl_requests (xem
# _requeue_after_proxy_exhaustion bên dưới) thay vì chỉ log rồi để bị commit-và-mất như mọi
# exception khác ở đây - đây là kiểu lỗi duy nhất mà consumer này xác định chắc chắn được là
# "bản thân request không sao, chỉ là nền tảng chưa phục vụ được *ngay lúc này*", nên một
# lần thử lại thật (không chỉ "ngừng dồn dập") là đáng làm. Giới hạn tối đa
# MAX_PROXY_EXHAUSTED_REQUEUES lần publish lại (đếm trong trường
# "_proxy_exhausted_retries" của chính request) để một nền tảng có pool proxy chết hẳn
# (không chỉ đang cooldown) không lặp mãi - cuối cùng nó bị bỏ thật, có báo rõ ràng. Mức
# backoff cơ sở/hệ số tăng/tối đa và trần số lần xếp hàng lại là các giá trị exhausted_*
# trong proxy_settings của dashboard (mặc định 30s / x2 / 300s / 3).

# Session Kafka của một nền tảng bị thu hồi (broker restart, mạng chập chờn, rebalance
# đua với consumer.commit() thành CommitFailedError/IllegalGenerationError - tất cả đều
# rơi vào handler KafkaError của _run_platform_consumer_once) là lỗi tạm thời và hồi phục
# được chỉ bằng cách vào lại group với một consumer mới. Thử lại ở đây trước thay vì rơi
# ngay xuống phản ứng cuối cùng của run(), vốn huỷ luôn vòng lặp của *hai* nền tảng kia và
# thoát cả tiến trình để một trình giám sát bên ngoài khởi động lại - quá tay với một trục
# trặc chỉ của một nền tảng, và trong môi trường dev không có trình giám sát như vậy thì nó
# chỉ để lại tình trạng không còn gì consume crawl_requests cho tới khi có người để ý. Chỉ
# khi broker vẫn không truy cập được quá _CONSUMER_RESTART_MAX_ATTEMPTS lần khởi động lại
# trong _CONSUMER_RESTART_WINDOW_SECONDS thì mới bỏ cuộc và rơi xuống phản ứng nặng hơn đó.
_CONSUMER_RESTART_BACKOFF_BASE_SECONDS = 5.0


_CONSUMER_RESTART_BACKOFF_GROWTH_FACTOR = 2.0


_CONSUMER_RESTART_BACKOFF_MAX_SECONDS = 120.0


_CONSUMER_RESTART_MAX_ATTEMPTS = 5


_CONSUMER_RESTART_WINDOW_SECONDS = 600.0


async def _requeue_after_proxy_exhaustion(
    requeue_publisher: KafkaPublisher, request: dict[str, Any], *, platform: str
) -> None:
    """Publish lại `request` lên crawl_requests với bộ đếm "_proxy_exhausted_retries" của nó
    tăng thêm - xem comment của setting exhausted_max_requeues ở trên để biết vì sao riêng
    loại lỗi này (và chỉ loại này) được thử lại thật thay vì bị log rồi mất như mọi exception
    khác mà _handle_request có thể raise. Bỏ cuộc (log rõ ràng, không publish lại) khi bộ đếm
    chạm trần - một nền tảng có pool proxy chết hẳn, không chỉ đang cooldown, không được lặp
    mãi. Cùng bộ đếm cho CrawlJobRetryable.

    True khi message gốc đã được xử lý xong và commit được (broker đã nhận bản xếp lại, hoặc đã bỏ cuộc); False khi publish
    lại hỏng - chỗ gọi phải giữ offset để message được giao lại."""
    retries = request.get("_proxy_exhausted_retries", 0)
    max_requeues = int((await asyncio.to_thread(get_proxy_settings))["exhausted_max_requeues"])
    if retries >= max_requeues:
        logger.error(
            "proxy_exhausted_requeue_gave_up",
            telegram=True,
            platform=platform,
            retries=retries,
            request=request,
        )
        return True
    requeued = {**request, "_proxy_exhausted_retries": retries + 1}
    if not requeue_publisher.started:
        # start() lúc khởi động vòng lặp có thể đã hỏng (Kafka chập chờn) và không tự thử lại.
        await requeue_publisher.start()
    delivered = await requeue_publisher.publish(
        topic=FACEBOOK_PUBLIC_COMMENTS_TOPIC if platform == "facebook_public" else CRAWL_REQUESTS_TOPIC,
        key=request.get("run_id") or platform,
        value=requeued,
    )
    if not delivered:
        logger.error("proxy_exhausted_requeue_failed", platform=platform, retries=retries, run_id=request.get("run_id"))
        return False
    logger.info("proxy_exhausted_requeued", platform=platform, retries=retries + 1, request=request)
    return True


# Làn ưu tiên (21/10/2026): danh sách Redis crawl_priority:<platform> - cinemark-api LPUSH job (vd. cào lại bài booking
# đang theo dõi mỗi ngày); vòng lặp nền tảng xả hết làn này TRƯỚC mỗi message Kafka, nên job ưu tiên luôn chạy trước hàng
# đợi thường. RPOP = vào trước ra trước trong làn.
PRIORITY_QUEUE_KEY_TMPL = "crawl_priority:{platform}"


# Kafka trống thì vẫn quay lại xem làn ưu tiên sau chừng này (không kẹt chờ message kế tiếp).
PRIORITY_POLL_SECONDS = 15


# Job đang chạy của làn ưu tiên nằm ở đây (LMOVE từ hàng đợi) tới khi xong mới xoá: restart / crash giữa chừng thì lần
# khởi động sau trả nó về hàng đợi thay vì mất (trước 22/10/2026 RPOP lấy hẳn ra - job dở là mất).
PRIORITY_PROCESSING_KEY_TMPL = "crawl_priority:{platform}:processing"


def _priority_keys(platform: str) -> tuple[str, str]:
    return PRIORITY_QUEUE_KEY_TMPL.format(platform=platform), PRIORITY_PROCESSING_KEY_TMPL.format(platform=platform)


def _recover_priority_queue(platform: str) -> None:
    key, processing = _priority_keys(platform)
    moved = RedisCache().requeue_processing(processing, key)
    if moved:
        logger.warning("priority_requests_recovered", platform=platform, count=moved)


async def _drain_priority_queue(platform: str) -> None:
    """Xả làn ưu tiên của nền tảng. Job lỗi cạn proxy được xếp lại cuối hàng (như đường Kafka, tối đa
    exhausted_max_requeues lần) rồi nghỉ - cả 3 nền tảng chạy cùng lúc dễ cạn proxy, trước đây job đó bị bỏ luôn."""
    cache = RedisCache()
    key, processing = _priority_keys(platform)
    while not is_platform_draining(platform):
        raw, request = await asyncio.to_thread(cache.claim_json, key, processing)
        if raw is None:
            return
        if not isinstance(request, dict):
            await asyncio.to_thread(cache.ack, processing, raw)
            continue
        logger.info(
            "priority_request_started", platform=platform, type=request.get("type"), run_id=request.get("run_id")
        )
        backoff = 0.0
        try:
            await _handle_request({**request, "platform": request.get("platform") or platform})
        except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn cả làn
            logger.error("priority_request_error", platform=platform, error=str(exc)[:300], request=request)
            if _retryable_failure(exc):
                proxy_settings = await asyncio.to_thread(get_proxy_settings)
                retries = int(request.get("_proxy_exhausted_retries", 0))
                if retries < int(proxy_settings["exhausted_max_requeues"]):
                    # LPUSH = cuối hàng (RPOP lấy từ đầu kia): các job ưu tiên khác được chạy trước.
                    await asyncio.to_thread(cache.lpush_json, key, {**request, "_proxy_exhausted_retries": retries + 1})
                    logger.info("priority_request_requeued", platform=platform, retries=retries + 1)
                else:
                    logger.error(
                        "priority_request_gave_up", telegram=True, platform=platform, retries=retries, request=request
                    )
                backoff = float(proxy_settings["exhausted_backoff_base_seconds"])
        # Không để trong finally: job bị huỷ giữa chừng (CancelledError) phải còn nằm trong processing để lần khởi động sau
        # trả về hàng đợi. Job đã xong mà chết trước dòng này thì chạy lại - run_id đã xong bị _handle_request bỏ qua.
        await asyncio.to_thread(cache.ack, processing, raw)
        if backoff:
            logger.warning("priority_lane_proxy_exhausted_pausing", platform=platform, backoff_seconds=backoff)
            await _sleep_interruptible(platform, backoff)
            return  # nhường cho vòng Kafka một lượt rồi mới xả tiếp


# Hàng đợi rảnh (25/10/2026): danh sách Redis crawl_idle:<platform> cho job KHÔNG dùng tài khoản (post_metrics - cập nhật
# số bài Facebook bằng trang không đăng nhập). Chỉ chạy khi vòng lặp không có việc gì khác: Kafka trống sau khi đã xả làn
# ưu tiên, hoặc trong giờ yên lặng Meta. Job loại khác lọt vào đây bị bỏ - chúng cần tài khoản, không được chạy lúc nghỉ.
IDLE_QUEUE_KEY_TMPL = "crawl_idle:{platform}"
IDLE_PROCESSING_KEY_TMPL = "crawl_idle:{platform}:processing"
IDLE_JOB_TYPES = {"post_metrics"}


def _recover_idle_queue(platform: str) -> None:
    moved = RedisCache().requeue_processing(
        IDLE_PROCESSING_KEY_TMPL.format(platform=platform), IDLE_QUEUE_KEY_TMPL.format(platform=platform)
    )
    if moved:
        logger.warning("idle_requests_recovered", platform=platform, count=moved)


async def _run_one_idle_job(platform: str) -> bool:
    """Chạy một job của hàng đợi rảnh (nếu có). True khi đã lấy ra một job (kể cả job bị bỏ / lỗi)."""
    if is_platform_draining(platform):
        return False
    cache = RedisCache()
    key, processing = IDLE_QUEUE_KEY_TMPL.format(platform=platform), IDLE_PROCESSING_KEY_TMPL.format(platform=platform)
    raw, request = await asyncio.to_thread(cache.claim_json, key, processing)
    if raw is None:
        return False
    try:
        if not isinstance(request, dict) or request.get("type") not in IDLE_JOB_TYPES:
            logger.warning("idle_request_rejected", platform=platform, request=request)
        else:
            logger.info("idle_request_started", platform=platform, type=request["type"], run_id=request.get("run_id"))
            await _handle_request({**request, "platform": request.get("platform") or platform})
    except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn vòng lặp
        logger.error("idle_request_error", platform=platform, error=str(exc)[:300], request=request)
    # Không để trong finally - xem _drain_priority_queue.
    await asyncio.to_thread(cache.ack, processing, raw)
    return True


# Đọc page không đăng nhập (03/11/2026): không dùng tài khoản và một job có thể chạy hàng chục phút, nên có worker song song
# riêng thay vì hàng đợi rảnh - job dài ở đó sẽ chiếm vòng lặp làn Facebook. cinemark-api nạp hàng đợi này khi nó vơi.
PUBLIC_PAGES_QUEUE_KEY = "crawl_public_pages:facebook"
PUBLIC_PAGES_PROCESSING_KEY = "crawl_public_pages:facebook:processing"
PUBLIC_PAGES_POLL_SECONDS = 15


def _recover_public_pages_queue() -> None:
    cache = RedisCache()
    for proxy_id in public_page_job.PROXY_IDS:
        cache.delete(public_page_job.RUNNING_KEY_TMPL.format(proxy_id=proxy_id))
    moved = cache.requeue_processing(PUBLIC_PAGES_PROCESSING_KEY, PUBLIC_PAGES_QUEUE_KEY)
    if moved:
        logger.warning("public_pages_requests_recovered", count=moved)


async def _run_one_public_page(cache: RedisCache) -> bool:
    """Một vòng của worker. False khi không có việc để làm ngay. Hết proxy thì để job nằm yên trong hàng đợi, không lấy ra rồi
    bỏ: page đó đã được tính là vừa xếp nên sẽ phải chờ tới lượt sau."""
    if is_platform_draining("facebook") or await asyncio.to_thread(_facebook_search_waiting):
        return False
    if await asyncio.to_thread(public_page_job._pick_proxy, cache) is None:
        if public_page_job.should_pause_filler(cache):
            cache.set(public_page_job.PAUSED_KEY, 1, ttl_seconds=public_page_job.PAUSED_TTL_SECONDS)
        return False
    cache.delete(public_page_job.PAUSED_KEY)
    raw, request = await asyncio.to_thread(cache.claim_json, PUBLIC_PAGES_QUEUE_KEY, PUBLIC_PAGES_PROCESSING_KEY)
    if raw is None:
        return False
    try:
        if isinstance(request, dict) and request.get("type") == "public_page":
            await _handle_request({**request, "platform": "facebook"})
        else:
            logger.warning("public_pages_request_rejected", request=request)
    except public_page_job.PublicPageProxyUnavailable:
        await asyncio.to_thread(cache.requeue_claim, PUBLIC_PAGES_PROCESSING_KEY, PUBLIC_PAGES_QUEUE_KEY, raw)
        return False
    except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn worker; page được đọc lại ở lượt sau
        logger.error("public_page_request_error", error=str(exc)[:300], page_url=(request or {}).get("page_url"))
    # Không để trong finally - xem _drain_priority_queue.
    await asyncio.to_thread(cache.ack, PUBLIC_PAGES_PROCESSING_KEY, raw)
    return True


async def _run_public_pages_worker(slot: int) -> None:
    cache = RedisCache()
    logger.info("public_pages_worker_started", slot=slot)
    while True:
        try:
            worked = await _run_one_public_page(cache)
        except Exception as exc:  # noqa: BLE001 - Redis / DB chập chờn: run() coi worker kết thúc là lỗi cả tiến trình
            logger.error("public_pages_worker_error", slot=slot, error=str(exc)[:300])
            worked = False
        if not worked:
            await asyncio.sleep(PUBLIC_PAGES_POLL_SECONDS)


# Kênh Threads không đăng nhập (13/11/2026): như page Facebook không đăng nhập - không tài khoản, job dài, nên worker riêng chạy
# cùng vòng Threads. cinemark-api nạp hàng đợi khi nó vơi.
def _recover_threads_channel_queue() -> None:
    cache = RedisCache()
    for slot in range(threads_channel_job.WORKERS):
        cache.delete(threads_channel_job.RUNNING_KEY_TMPL.format(slot=slot))
    moved = cache.requeue_processing(threads_channel_job.PROCESSING_KEY, threads_channel_job.QUEUE_KEY)
    if moved:
        logger.warning("threads_channel_requests_recovered", count=moved)


async def _run_one_threads_channel(cache: RedisCache, slot: int) -> bool:
    if is_platform_draining("threads"):
        return False
    if await asyncio.to_thread(threads_channel_job.pick_exit, cache) is None:
        return False
    raw, request = await asyncio.to_thread(
        cache.claim_json, threads_channel_job.QUEUE_KEY, threads_channel_job.PROCESSING_KEY
    )
    if raw is None:
        return False
    try:
        if isinstance(request, dict) and request.get("type") == "threads_channel":
            await _handle_request({**request, "platform": "threads", "_slot": slot})
        else:
            logger.warning("threads_channel_request_rejected", request=request)
    except threads_channel_job.ThreadsChannelExitUnavailable:
        await asyncio.to_thread(
            cache.requeue_claim, threads_channel_job.PROCESSING_KEY, threads_channel_job.QUEUE_KEY, raw
        )
        return False
    except Exception as exc:  # noqa: BLE001 - một kênh lỗi không được chặn worker; lượt sau đọc lại
        logger.error("threads_channel_request_error", error=str(exc)[:300], page_url=(request or {}).get("page_url"))
    await asyncio.to_thread(cache.ack, threads_channel_job.PROCESSING_KEY, raw)
    return True


async def _run_threads_channel_worker(slot: int) -> None:
    cache = RedisCache()
    logger.info("threads_channel_worker_started", slot=slot)
    while True:
        try:
            worked = await _run_one_threads_channel(cache, slot)
        except Exception as exc:  # noqa: BLE001
            logger.error("threads_channel_worker_error", slot=slot, error=str(exc)[:300])
            worked = False
        if not worked:
            await asyncio.sleep(PUBLIC_PAGES_POLL_SECONDS)


async def _run_platform_consumer_once(platform: str, group_id: str) -> bool:
    """Một lần chạy vòng lặp consumer của một nền tảng - xem docstring module để biết vì sao có
    mỗi nền tảng một vòng thay vì một vòng dùng chung. Instance nào cũng subscribe cùng topic
    và thấy mọi message; thứ gì không thuộc nền tảng này được commit qua ngay (không crawl,
    không nghỉ) để thông lượng của vòng lặp này không bao giờ bị ảnh hưởng bởi lưu lượng của
    các nền tảng khác.

    Trả về True khi _run_platform_consumer nên ngừng thử lại (KeyboardInterrupt có chủ đích;
    CancelledError thì được lan ra, không xử lý, vì đó là chỗ gọi bên ngoài - run(), hoặc lúc
    tắt - đang chủ động dừng task này), False khi kết thúc vì một KafkaError đáng thử lại với
    consumer mới (xem _run_platform_consumer)."""
    request_topic = FACEBOOK_PUBLIC_COMMENTS_TOPIC if platform == "facebook_public" else CRAWL_REQUESTS_TOPIC
    consumer = AIOKafkaConsumer(
        request_topic,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # "earliest": một request xếp hàng lúc consumer này tình cờ đang sập vẫn nên chạy khi nó
        # lên lại, không bị âm thầm bỏ - crawl request đủ hiếm (bấm nút, một lô hằng ngày) nên xử
        # lý một hàng tồn ngắn khi restart không bao giờ là vấn đề.
        auto_offset_reset="earliest",
        # Chỉ commit sau khi _handle_request trả về (xem lời gọi consumer.commit() rõ ràng bên
        # dưới), không theo bộ hẹn giờ 5s mặc định của aiokafka. Một lượt crawl có thể chạy nhiều
        # phút - bộ hẹn giờ mặc định sẽ commit offset của message gần như ngay sau khi nhận, rất
        # lâu trước khi lượt crawl nó kích hoạt thực sự xong. Nếu tiến trình chết giữa lúc crawl,
        # Kafka sẽ không bao giờ giao lại request đó - nó mất luôn, không còn dấu vết gì ngoài một
        # dòng DB "running" mồ côi.
        enable_auto_commit=False,
        # Vòng lặp này chờ trọn một lượt crawl (tới sweep_days=60 theo mặc định, xem
        # _sweep_days_for) rồi mới gọi lại consumer lấy message kế tiếp - max_poll_interval_ms mặc
        # định 5 phút của Kafka không đủ chút nào; vượt quá là broker âm thầm thu hồi tư cách thành
        # viên group giữa lúc crawl. 1 giờ phủ được mọi lượt quét đơn lẻ thực tế. Chỉ các lượt crawl
        # của chính nền tảng này mới tính vào đó - lượt quét dài của nền tảng khác chạy trên
        # consumer/vòng lặp riêng.
        # Authenticated jobs may rest while keeping their pagination cursor and lease.
        # Heartbeats continue; allow the bounded daily workload plus rests before a rebalance.
        max_poll_interval_ms=int(os.getenv("FACEBOOK_MAX_POLL_INTERVAL_MS", "14400000"))
        if platform == "facebook"
        else 3_600_000,
    )
    # Số lần liên tiếp dính pool.ProxyPoolExhaustedError cho vòng lặp nền tảng NÀY - xem
    # PROXY_EXHAUSTED_BACKOFF_* ở trên để biết vì sao cần khoảng nghỉ tăng dần riêng thay vì
    # lao thẳng vào message kế tiếp. Cố ý chỉ trong tiến trình (không Redis): nó chỉ cần sống
    # trong một vòng lặp đang chạy, và về 0 ngay khi có bất cứ thứ gì khác (thành công hoặc lỗi
    # khác) đi qua, y như sức khoẻ tài khoản/proxy mà nó theo dõi vốn tự reset khi thành công.
    proxy_exhausted_streak = 0
    # Chỉ dùng để publish lại một request bị cạn proxy lên crawl_requests (xem
    # _requeue_after_proxy_exhaustion) - một producer riêng, tách khỏi KafkaPublisher của bất
    # kỳ spider nào, vì vòng đời của nó gắn với chính vòng lặp consumer, không với một lượt
    # crawl.
    requeue_publisher = KafkaPublisher()
    try:
        await consumer.start()
        await requeue_publisher.start()
        await asyncio.to_thread(_clear_leftover_job_key, platform)
        # Nhiều làn (Facebook, TikTok): chỉ làn 0 trả job dở về hàng - làn khác khởi động sau sẽ trả cả job làn 0 đang chạy.
        if not lanes.worker_lane():
            await asyncio.to_thread(_recover_priority_queue, platform)
            await asyncio.to_thread(_recover_idle_queue, platform)
        logger.info(
            "crawl_request_consumer_started",
            topic=request_topic,
            platform=platform,
            group=group_id,
            lane=lanes.worker_lane(),
        )
        passing_foreign = False
        checked_at = 0.0
        while True:
            checked = not passing_foreign or time.monotonic() - checked_at >= FOREIGN_SKIP_MAX_SECONDS
            if checked:
                await _wait_out_quiet_hours(consumer, platform)
                await _yield_partitions_while_no_account(consumer, platform)
                await _wait_out_hourly_cap(consumer, platform)
                await _wait_out_public_hold(consumer, platform)
                await _drain_priority_queue(platform)
                checked_at = time.monotonic()
            passing_foreign = False
            batch = await consumer.getmany(timeout_ms=PRIORITY_POLL_SECONDS * 1000, max_records=1)
            if not any(batch.values()):
                if checked:
                    await _run_one_idle_job(platform)
                continue
            for message in [m for messages in batch.values() for m in messages]:
                request = message.value
                request_platform = _request_platform(request)
                if request_platform != platform:
                    # Vòng lặp nào cũng thấy mọi message trên topic dùng chung, nên một giá trị platform không
                    # khớp vòng nào sẽ hoàn toàn không được log (cả ba vòng đều âm thầm kết luận "không phải
                    # của mình"). Chỉ vòng facebook - chọn tuỳ ý, vòng nào cũng được - báo nó, để một nền tảng
                    # không nhận ra/gõ sai sinh ra đúng một cảnh báo thay vì ba, khôi phục khả năng quan sát
                    # mà _run_spider/_refresh_token từng tự cung cấp trước khi file này có các vòng lặp để
                    # định tuyến.
                    if platform == "facebook" and request_platform not in PLATFORM_CONSUMER_GROUPS:
                        logger.warning("unsupported_platform", platform=request_platform, request=request)
                    await consumer.commit()
                    passing_foreign = True
                    continue
                if not checked:
                    # Message của nền tảng này: đọc lại sau khi đã qua các bước kiểm tra.
                    consumer.seek(TopicPartition(message.topic, message.partition), message.offset)
                    continue
                if _stale_facebook_job(platform, request, message.timestamp):
                    logger.info(
                        "facebook_stale_job_dropped",
                        type=request.get("type") or "search",
                        keyword_id=request.get("keyword_id"),
                        age_hours=round((time.time() * 1000 - message.timestamp) / 3_600_000, 1),
                    )
                    await asyncio.to_thread(_finish_stale_job, request)
                    await consumer.commit()
                    continue
                if message.timestamp and "published_at" not in request:
                    # Message chưa mang thời điểm publish (hàng tồn từ bản cinemark-api cũ) vẫn so được với mốc Dừng.
                    request = {**request, "published_at": message.timestamp / 1000}
                skipped = False
                proxy_exhausted = False
                try:
                    skipped = await _handle_request(request)
                except Exception as exc:
                    # Một request lỗi không được giết cả consumer - log rồi chuyển sang cái kế tiếp.
                    logger.error("crawl_request_error", platform=platform, error=str(exc), request=request)
                    proxy_exhausted = _retryable_failure(exc)
                # Commit dù _handle_request thành công hay đã bị log rồi bỏ qua ở trên - dù thế nào message
                # này cũng đã xong, không được giao lại ở lần restart sau. Lỗi cạn proxy là ngoại lệ duy
                # nhất: nó được một cơ hội thử lại thật (xem _requeue_after_proxy_exhaustion) - publish lại
                # *trước* lần commit này, không phải sau, để crash ở giữa tệ nhất chỉ để lại một bản trùng
                # vô hại (cả bản gốc, chưa commit và sắp được giao lại khi restart, lẫn bản mới xếp hàng
                # lại) thay vì âm thầm mất request nếu crash rơi vào chiều ngược lại.
                if proxy_exhausted:
                    proxy_exhausted_streak += 1
                    requeued = await _requeue_after_proxy_exhaustion(requeue_publisher, request, platform=platform)
                    # to_thread: cache miss sẽ đọc DB đồng bộ.
                    proxy_settings = await asyncio.to_thread(get_proxy_settings)
                    backoff = min(
                        float(proxy_settings["exhausted_backoff_base_seconds"])
                        * (float(proxy_settings["exhausted_backoff_growth_factor"]) ** (proxy_exhausted_streak - 1)),
                        float(proxy_settings["exhausted_backoff_max_seconds"]),
                    )
                    logger.warning(
                        "platform_proxy_exhausted_pausing",
                        platform=platform,
                        consecutive_hits=proxy_exhausted_streak,
                        backoff_seconds=round(backoff, 1),
                    )
                    if requeued:
                        await consumer.commit()
                    else:
                        # Broker chưa nhận bản xếp lại: bản gốc là bản duy nhất còn lại - không commit, đọc lại đúng nó.
                        consumer.seek(TopicPartition(message.topic, message.partition), message.offset)
                    await _sleep_interruptible(platform, backoff)
                    continue
                await consumer.commit()
                proxy_exhausted_streak = 0
                if skipped or is_platform_draining(platform):
                    continue
                if request.get("type") == "comments" and platform == "facebook":
                    # Comment Facebook: nghỉ ngắn giữa các job (24/10/2026) - trước đây chạy liền nhau, một lô 100 bài là
                    # một tràng request liên tục từ một tài khoản.
                    pause = random.uniform(FACEBOOK_COMMENTS_PAUSE_MIN_SECONDS, FACEBOOK_COMMENTS_PAUSE_MAX_SECONDS)
                    await _sleep_interruptible(platform, pause)
                    continue
                if platform == "youtube":
                    # YouTube không dùng tài khoản; giãn cách nằm trong job (proxy xoay gắn sau) - không nghỉ giữa các job.
                    continue
                if request.get("type") == "comments" and platform not in ("tiktok", "facebook_public"):
                    # Comment Threads chỉ là curl_cffi thường - bỏ khoảng nghỉ giữa các request để một lô 100 bài không
                    # bị các lần chờ rảnh rỗi chiếm phần lớn thời gian. Comment TikTok mở một trình duyệt Patchright thật
                    # cho mỗi job (xem tiktok/features/comments/comments.py), nên chúng rơi xuống khoảng nghỉ bên dưới.
                    continue
                # Xem INTER_REQUEST_PAUSE_MIN/MAX_SECONDS ở trên - một khoảng nghỉ trước khi lấy thứ kế
                # tiếp trong hàng đợi *của nền tảng này*, không phải trước request đầu tiên của một lô mới
                # (chưa có gì để giãn cách).
                pause = (
                    random.uniform(FACEBOOK_PAUSE_MIN_SECONDS, FACEBOOK_PAUSE_MAX_SECONDS)
                    if platform == "facebook"
                    else random.uniform(THREADS_PAUSE_MIN_SECONDS, THREADS_PAUSE_MAX_SECONDS)
                    if platform == "threads"
                    else random.uniform(INTER_REQUEST_PAUSE_MIN_SECONDS, INTER_REQUEST_PAUSE_MAX_SECONDS)
                )
                logger.info("inter_request_pause", platform=platform, seconds=round(pause, 1))
                await _sleep_interruptible(platform, pause)
    except asyncio.CancelledError:
        logger.info("platform_consumer_cancelled", platform=platform)
        raise
    except KeyboardInterrupt:
        logger.info("platform_consumer_interrupted", platform=platform)
        return True
    except KafkaError as exc:
        logger.error("kafka_error", platform=platform, error=str(exc))
        return False
    finally:
        await consumer.stop()
        await requeue_publisher.stop()


async def _run_platform_consumer(platform: str, group_id: str, lane: int | None = None) -> None:
    """Khởi động lại _run_platform_consumer_once với consumer mới mỗi khi nó kết thúc vì
    KafkaError, thay vì để một trục trặc Kafka tạm thời của một nền tảng hạ luôn vòng lặp của
    hai nền tảng kia qua phản ứng cuối cùng nặng hơn của run() (xem comment của các hằng
    _CONSUMER_RESTART_*). Chỉ trả về - trao quyền lại cho run(), nơi áp phản ứng nặng hơn đó -
    sau một lần dừng có chủ đích (True) hoặc sau _CONSUMER_RESTART_MAX_ATTEMPTS lần khởi
    động lại trong _CONSUMER_RESTART_WINDOW_SECONDS mà không có trọn một khoảng chạy sạch ở
    giữa."""
    # contextvar riêng của task này - job trong làn Facebook dùng đúng tài khoản của làn. Làn TikTok chỉ đặt worker_lane.
    lanes.set_current_lane(lane if platform == "facebook" else None)
    lanes.set_worker_lane(lane)
    restart_times: list[float] = []
    while True:
        stop_cleanly = await _run_platform_consumer_once(platform, group_id)
        if stop_cleanly:
            return
        now = time.monotonic()
        restart_times = [t for t in restart_times if now - t < _CONSUMER_RESTART_WINDOW_SECONDS]
        restart_times.append(now)
        if len(restart_times) > _CONSUMER_RESTART_MAX_ATTEMPTS:
            logger.error(
                "platform_consumer_restart_giving_up",
                platform=platform,
                attempts=len(restart_times),
                window_seconds=_CONSUMER_RESTART_WINDOW_SECONDS,
            )
            return
        backoff = min(
            _CONSUMER_RESTART_BACKOFF_BASE_SECONDS
            * (_CONSUMER_RESTART_BACKOFF_GROWTH_FACTOR ** (len(restart_times) - 1)),
            _CONSUMER_RESTART_BACKOFF_MAX_SECONDS,
        )
        logger.warning(
            "platform_consumer_restarting",
            platform=platform,
            attempt=len(restart_times),
            backoff_seconds=round(backoff, 1),
        )
        await asyncio.sleep(backoff)


async def run() -> None:
    """Chạy song song vòng lặp consumer của mọi nền tảng trong cùng một tiến trình này - xem
    docstring module. _run_platform_consumer vốn đã tự thử lại KafkaError với consumer mới
    (rebalance/commit thất bại/broker chập chờn - xem docstring của nó), nên khi một task ở
    đây thực sự kết thúc thì đó hoặc là dừng có chủ đích, hoặc là một nền tảng đã dùng hết
    ngân sách thử lại với một broker không truy cập được một thời gian - không phải một trục
    trặc tạm thời đơn lẻ. Trước đây dùng asyncio.gather(..., return_exceptions=True), vốn
    chỉ log chuyện đó và để *hai* vòng kia chạy mãi: crawl của nền tảng đã chết âm thầm dừng
    vĩnh viễn trong khi tiến trình vẫn "đang chạy", và không có gì báo cho Restart=on-failure
    của systemd khởi động lại. Huỷ các vòng còn sống rồi raise lại ở đây khiến cả tiến trình
    thoát với mã khác 0, để systemd khởi động lại cả ba vòng gọn gàng - đúng bảo đảm mà bản
    một-consumer trước đây đã có."""
    tasks: dict[asyncio.Task, str] = {}
    enabled = enabled_platforms()
    logger.info("crawl_consumer_platforms", platforms=enabled)
    for platform, group_id in PLATFORM_CONSUMER_GROUPS.items():
        if platform not in enabled:
            continue
        lane_count = {"facebook": lanes.LANES, "tiktok": lanes.TIKTOK_LANES}.get(platform, 1)
        if platform == "facebook" or lane_count > 1:
            # Làn song song (runtime/lanes.py): N consumer cùng group. Facebook: mỗi làn một tài khoản; TikTok: mỗi làn một
            # cổng proxy riêng.
            for lane in range(lane_count):
                task = asyncio.create_task(
                    _run_platform_consumer(platform, group_id, lane), name=f"{platform}-lane-{lane}"
                )
                tasks[task] = f"{platform}-lane-{lane}"
        else:
            tasks[asyncio.create_task(_run_platform_consumer(platform, group_id), name=platform)] = platform
    if "facebook_public" in enabled:
        await asyncio.to_thread(_recover_public_pages_queue)
        for slot in range(public_page_job.WORKERS):
            name = f"public-pages-{slot}"
            tasks[asyncio.create_task(_run_public_pages_worker(slot), name=name)] = name
    if "threads" in enabled and threads_channel_job.WORKERS:
        await asyncio.to_thread(_recover_threads_channel_queue)
        for slot in range(threads_channel_job.WORKERS):
            name = f"threads-channel-{slot}"
            tasks[asyncio.create_task(_run_threads_channel_worker(slot), name=name)] = name
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError, KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("crawl_request_consumer_stopped")
        return

    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    finished = next(iter(done))
    platform = tasks[finished]
    if finished.cancelled():
        logger.info("crawl_request_consumer_stopped", platform=platform)
        return
    exc = finished.exception()
    if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
        logger.info("crawl_request_consumer_stopped", platform=platform)
        return
    logger.error("platform_consumer_stopped", platform=platform, error=str(exc) if exc else None)
    raise RuntimeError(f"{platform} consumer loop stopped unexpectedly") from exc
