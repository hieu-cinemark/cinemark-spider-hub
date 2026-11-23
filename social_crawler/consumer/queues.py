"""Hàng đợi Redis song song với Kafka: làn ưu tiên, hàng đợi rảnh, page công khai, kênh Threads, làn YouTube và các vòng chờ chạy job rảnh."""

from __future__ import annotations

import asyncio
import os
import random
import time

import httpx
from aiokafka import AIOKafkaConsumer

from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    logger,
)
from social_crawler.consumer.dispatch import _handle_request
from social_crawler.consumer.gates import (
    _facebook_search_waiting,
    _hour_capped_account,
    _poll_paused,
    _quiet_hours,
    _quiet_seconds_left,
    _retryable_failure,
)
from social_crawler.consumer.jobs import public_page as public_page_job
from social_crawler.consumer.jobs import threads_channel as threads_channel_job
from social_crawler.consumer.jobs import youtube as youtube_job
from social_crawler.consumer.process import _sleep_interruptible
from social_crawler.db.proxy_settings import get_proxy_settings
from social_crawler.runtime import lanes, search_budget
from social_crawler.runtime.job_queue import JobQueue
from social_crawler.runtime.task_queue import is_platform_draining

# Làn ưu tiên (21/10/2026): danh sách Redis crawl_priority:<platform> - cinemark-api LPUSH job (vd. cào lại bài booking
# đang theo dõi mỗi ngày); vòng lặp nền tảng xả hết làn này TRƯỚC mỗi message Kafka, nên job ưu tiên luôn chạy trước hàng
# đợi thường. RPOP = vào trước ra trước trong làn.
PRIORITY_QUEUE_KEY_TMPL = "crawl_priority:{platform}"


# Kafka trống thì vẫn quay lại xem làn ưu tiên sau chừng này (không kẹt chờ message kế tiếp).
PRIORITY_POLL_SECONDS = 15


# Job đang chạy của làn ưu tiên nằm ở đây (LMOVE từ hàng đợi) tới khi xong mới xoá: restart / crash giữa chừng thì lần
# khởi động sau trả nó về hàng đợi thay vì mất (trước 22/10/2026 RPOP lấy hẳn ra - job dở là mất).
PRIORITY_PROCESSING_KEY_TMPL = "crawl_priority:{platform}:processing"


def _priority_queue(platform: str) -> JobQueue:
    return JobQueue(
        PRIORITY_QUEUE_KEY_TMPL.format(platform=platform), PRIORITY_PROCESSING_KEY_TMPL.format(platform=platform)
    )


def _recover_priority_queue(platform: str) -> None:
    moved = _priority_queue(platform).recover(RedisCache())
    if moved:
        logger.warning("priority_requests_recovered", platform=platform, count=moved)


async def _drain_priority_queue(platform: str) -> None:
    """Xả làn ưu tiên của nền tảng. Job lỗi cạn proxy được xếp lại cuối hàng (như đường Kafka, tối đa
    exhausted_max_requeues lần) rồi nghỉ - cả 3 nền tảng chạy cùng lúc dễ cạn proxy, trước đây job đó bị bỏ luôn."""
    cache = RedisCache()
    queue = _priority_queue(platform)
    while not is_platform_draining(platform):
        raw, request = await asyncio.to_thread(queue.claim, cache)
        if raw is None:
            return
        if not isinstance(request, dict):
            await asyncio.to_thread(queue.ack, cache, raw)
            continue
        logger.info(
            "priority_request_started", platform=platform, type=request.get("type"), run_id=request.get("run_id")
        )
        backoff = 0.0
        try:
            await _handle_request({**request, "platform": request.get("platform") or platform})
        except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn cả làn
            logger.error("priority_request_error", platform=platform, error=exc, request=request)
            if _retryable_failure(exc):
                proxy_settings = await asyncio.to_thread(get_proxy_settings)
                retries = int(request.get("_proxy_exhausted_retries", 0))
                if retries < int(proxy_settings["exhausted_max_requeues"]):
                    # LPUSH = cuối hàng (RPOP lấy từ đầu kia): các job ưu tiên khác được chạy trước.
                    await asyncio.to_thread(
                        cache.lpush_json, queue.key, {**request, "_proxy_exhausted_retries": retries + 1}
                    )
                    logger.info("priority_request_requeued", platform=platform, retries=retries + 1)
                else:
                    logger.error(
                        "priority_request_gave_up", telegram=True, platform=platform, retries=retries, request=request
                    )
                backoff = float(proxy_settings["exhausted_backoff_base_seconds"])
        # Không để trong finally: job bị huỷ giữa chừng (CancelledError) phải còn nằm trong processing để lần khởi động sau
        # trả về hàng đợi. Job đã xong mà chết trước dòng này thì chạy lại - run_id đã xong bị _handle_request bỏ qua.
        await asyncio.to_thread(queue.ack, cache, raw)
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


def _idle_queue(platform: str) -> JobQueue:
    return JobQueue(IDLE_QUEUE_KEY_TMPL.format(platform=platform), IDLE_PROCESSING_KEY_TMPL.format(platform=platform))


def _recover_idle_queue(platform: str) -> None:
    moved = _idle_queue(platform).recover(RedisCache())
    if moved:
        logger.warning("idle_requests_recovered", platform=platform, count=moved)


async def _run_one_idle_job(platform: str) -> bool:
    """Chạy một job của hàng đợi rảnh (nếu có). True khi đã lấy ra một job (kể cả job bị bỏ / lỗi)."""
    if is_platform_draining(platform):
        return False
    cache = RedisCache()
    queue = _idle_queue(platform)
    raw, request = await asyncio.to_thread(queue.claim, cache)
    if raw is None:
        return False
    try:
        if not isinstance(request, dict) or request.get("type") not in IDLE_JOB_TYPES:
            logger.warning("idle_request_rejected", platform=platform, request=request)
        else:
            logger.info("idle_request_started", platform=platform, type=request["type"], run_id=request.get("run_id"))
            await _handle_request({**request, "platform": request.get("platform") or platform})
    except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn vòng lặp
        logger.error("idle_request_error", platform=platform, error=exc, request=request)
    # Không để trong finally - xem _drain_priority_queue.
    await asyncio.to_thread(queue.ack, cache, raw)
    return True


# Đọc page không đăng nhập (03/11/2026): không dùng tài khoản và một job có thể chạy hàng chục phút, nên có worker song song
# riêng thay vì hàng đợi rảnh - job dài ở đó sẽ chiếm vòng lặp làn Facebook. cinemark-api nạp hàng đợi này khi nó vơi.
PUBLIC_PAGES_QUEUE_KEY = "crawl_public_pages:facebook"
PUBLIC_PAGES_PROCESSING_KEY = "crawl_public_pages:facebook:processing"
PUBLIC_PAGES_QUEUE = JobQueue(PUBLIC_PAGES_QUEUE_KEY, PUBLIC_PAGES_PROCESSING_KEY)
PUBLIC_PAGES_POLL_SECONDS = 15


def _recover_public_pages_queue() -> None:
    cache = RedisCache()
    for proxy in public_page_job.list_public_page_proxies():
        cache.delete(public_page_job.RUNNING_KEY_TMPL.format(proxy_id=proxy["id"]))
    moved = PUBLIC_PAGES_QUEUE.recover(cache)
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
    raw, request = await asyncio.to_thread(PUBLIC_PAGES_QUEUE.claim, cache)
    if raw is None:
        return False
    try:
        if isinstance(request, dict) and request.get("type") == "public_page":
            await _handle_request({**request, "platform": "facebook"})
        else:
            logger.warning("public_pages_request_rejected", request=request)
    except public_page_job.PublicPageProxyUnavailable:
        await asyncio.to_thread(PUBLIC_PAGES_QUEUE.requeue, cache, raw)
        return False
    except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn worker; page được đọc lại ở lượt sau
        logger.error("public_page_request_error", error=exc, page_url=(request or {}).get("page_url"))
    # Không để trong finally - xem _drain_priority_queue.
    await asyncio.to_thread(PUBLIC_PAGES_QUEUE.ack, cache, raw)
    return True


async def _run_public_pages_worker(slot: int) -> None:
    cache = RedisCache()
    logger.info("public_pages_worker_started", slot=slot)
    while True:
        try:
            # Worker ngoài số proxy public x public_pages_per_proxy (proxy_settings) thì đứng chờ.
            worked = (slot < await asyncio.to_thread(public_page_job.active_workers)) and await _run_one_public_page(
                cache
            )
        except Exception as exc:  # noqa: BLE001 - Redis / DB chập chờn: run() coi worker kết thúc là lỗi cả tiến trình
            logger.error("public_pages_worker_error", slot=slot, error=exc)
            worked = False
        if not worked:
            await asyncio.sleep(PUBLIC_PAGES_POLL_SECONDS)


# Kênh Threads không đăng nhập (13/11/2026): như page Facebook không đăng nhập - không tài khoản, job dài, nên worker riêng chạy
# cùng vòng Threads. cinemark-api nạp hàng đợi khi nó vơi.
def _recover_threads_channel_queue() -> None:
    cache = RedisCache()
    for slot in range(threads_channel_job.MAX_WORKERS):
        cache.delete(threads_channel_job.RUNNING_KEY_TMPL.format(slot=slot))
    moved = threads_channel_job.QUEUE.recover(cache)
    if moved:
        logger.warning("threads_channel_requests_recovered", count=moved)


async def _run_one_threads_channel(cache: RedisCache, slot: int) -> bool:
    if is_platform_draining("threads"):
        return False
    if await asyncio.to_thread(threads_channel_job.pick_exit, cache) is None:
        return False
    raw, request = await asyncio.to_thread(threads_channel_job.QUEUE.claim, cache)
    if raw is None:
        return False
    try:
        if isinstance(request, dict) and request.get("type") == "threads_channel":
            await _handle_request({**request, "platform": "threads", "_slot": slot})
        else:
            logger.warning("threads_channel_request_rejected", request=request)
    except threads_channel_job.ThreadsChannelExitUnavailable:
        await asyncio.to_thread(threads_channel_job.QUEUE.requeue, cache, raw)
        return False
    except Exception as exc:  # noqa: BLE001 - một kênh lỗi không được chặn worker; lượt sau đọc lại
        logger.error("threads_channel_request_error", error=exc, page_url=(request or {}).get("page_url"))
    await asyncio.to_thread(threads_channel_job.QUEUE.ack, cache, raw)
    return True


async def _run_threads_channel_worker(slot: int) -> None:
    cache = RedisCache()
    logger.info("threads_channel_worker_started", slot=slot)
    while True:
        try:
            worked = slot < threads_channel_job.active_workers() and await _run_one_threads_channel(cache, slot)
        except Exception as exc:  # noqa: BLE001
            logger.error("threads_channel_worker_error", slot=slot, error=exc)
            worked = False
        if not worked:
            await asyncio.sleep(PUBLIC_PAGES_POLL_SECONDS)


# Làn YouTube (13/11/2026): không tài khoản, toàn HTTP nhẹ - youtube_lanes (proxy_settings) worker chạy song song trong tiến trình, đọc hàng
# đợi Redis mà vòng Kafka của YouTube chuyển sang (dispatch._handle_request). Tìm kiếm/kênh đi trước bình luận.
YOUTUBE_IDLE_POLL_SECONDS = 3
YOUTUBE_BLOCK_REST_SECONDS = float(os.getenv("YOUTUBE_BLOCK_REST_SECONDS", "60"))


def _recover_youtube_queue() -> None:
    cache = RedisCache()
    moved = 0
    # Job đang chạy dở lúc tiến trình trước dừng: trả về đúng hàng của nó (hai hàng chung một khoá đang chạy nên phải phân theo loại).
    scratch = JobQueue(youtube_job.PROCESSING_KEY, f"{youtube_job.PROCESSING_KEY}:recover")
    while True:
        raw, request = scratch.claim(cache)
        if raw is None:
            break
        kind = request.get("type") if isinstance(request, dict) else None
        queue = youtube_job.COMMENTS_QUEUE if kind == "comments" else youtube_job.SEARCH_QUEUE
        cache.requeue_claim(scratch.processing, queue.key, raw)
        moved += 1
    if moved:
        logger.warning("youtube_requests_recovered", count=moved)


async def _run_one_youtube_job(cache: RedisCache, lane: int) -> bool:
    if is_platform_draining("youtube"):
        return False
    queue = youtube_job.SEARCH_QUEUE
    raw, request = await asyncio.to_thread(queue.claim, cache)
    if raw is None:
        queue = youtube_job.COMMENTS_QUEUE
        raw, request = await asyncio.to_thread(queue.claim, cache)
    if raw is None:
        return False
    rest = 0.0
    try:
        if isinstance(request, dict):
            await _handle_request({**request, "platform": "youtube", "_lane": lane})
        else:
            logger.warning("youtube_request_rejected", request=request)
    except Exception as exc:  # noqa: BLE001 - một job lỗi không được chặn làn
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if isinstance(exc, (httpx.ProxyError, httpx.ConnectError, httpx.ConnectTimeout)) or status == 407:
            # Proxy của làn hỏng: cho proxy nghỉ, trả job về hàng và chạy lại ngay bằng lối ra khác.
            proxy = youtube_job.current_proxy()
            youtube_job.mark_proxy_bad(proxy)
            logger.warning(
                "youtube_proxy_failed", lane=lane, had_proxy=bool(proxy), error=type(exc).__name__, telegram=True
            )
            await asyncio.to_thread(queue.requeue, cache, raw)
            await asyncio.sleep(2 if proxy else YOUTUBE_BLOCK_REST_SECONDS)
            return True
        if status in (403, 429):
            # YouTube đang chặn lối ra của làn này: trả job về hàng, làn nghỉ một nhịp rồi thử lại.
            logger.warning("youtube_lane_blocked", lane=lane, status=status, telegram=True)
            await asyncio.to_thread(queue.requeue, cache, raw)
            await asyncio.sleep(YOUTUBE_BLOCK_REST_SECONDS)
            return True
        logger.error("youtube_request_error", lane=lane, error=exc)
        rest = 1.0
    await asyncio.to_thread(queue.ack, cache, raw)
    if rest:
        await asyncio.sleep(rest)
    return True


async def _run_youtube_lane(lane: int) -> None:
    lanes.set_worker_lane(lane)
    cache = RedisCache()
    logger.info("youtube_lane_started", lane=lane)
    while True:
        try:
            worked = lane < youtube_job.active_lanes() and await _run_one_youtube_job(cache, lane)
        except Exception as exc:  # noqa: BLE001 - Redis chập chờn: run() coi làn kết thúc là lỗi cả tiến trình
            logger.error("youtube_lane_error", lane=lane, error=exc)
            worked = False
        if not worked:
            await asyncio.sleep(YOUTUBE_IDLE_POLL_SECONDS)


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
    consumer.pause(*consumer.assignment())
    deadline = time.time() + wait
    try:
        while time.time() < deadline and not is_platform_draining(platform):
            if not await _run_one_idle_job(platform):
                await _poll_paused(consumer, int(min(60, max(1, deadline - time.time())) * 1000))
    finally:
        consumer.resume(*consumer.assignment())
    logger.info("hourly_cap_resumed", platform=platform, account=account)


async def _wait_out_quiet_hours(consumer: AIOKafkaConsumer, platform: str) -> None:
    """Tạm dừng nhận message (consumer.pause) nhưng vẫn poll để giữ chỗ trong group, tới khi hết giờ yên lặng."""
    left = _quiet_seconds_left(platform)
    if left <= 0:
        return
    logger.info("quiet_hours_pausing", platform=platform, minutes=round(left / 60), window=_quiet_hours())
    consumer.pause(*consumer.assignment())
    try:
        while _quiet_seconds_left(platform) > 0 and not is_platform_draining(platform):
            # Job không dùng tài khoản (hàng đợi rảnh) vẫn chạy được trong giờ yên lặng - giờ này là để che tài khoản.
            if not await _run_one_idle_job(platform):
                await _poll_paused(consumer, 60_000)
    finally:
        consumer.resume(*consumer.assignment())
    logger.info("quiet_hours_resumed", platform=platform)
