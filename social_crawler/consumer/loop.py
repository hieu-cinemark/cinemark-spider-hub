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

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.clients.kafka import CRAWL_REQUESTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import PLATFORM_CONSUMER_GROUPS, logger
from social_crawler.consumer.dispatch import _handle_request, _request_platform
from social_crawler.consumer.process import _sleep_interruptible
from social_crawler.db.proxy_settings import get_proxy_settings
from social_crawler.services import lanes, pool
from social_crawler.services.task_queue import is_platform_draining

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
FACEBOOK_PAUSE_MIN_SECONDS = float(os.getenv("FACEBOOK_PAUSE_MIN_SECONDS", "20"))
FACEBOOK_PAUSE_MAX_SECONDS = float(os.getenv("FACEBOOK_PAUSE_MAX_SECONDS", "60"))
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


async def _wait_for_lane_account(consumer: AIOKafkaConsumer, platform: str) -> None:
    """Làn Facebook (services/lanes.py) chưa có tài khoản nào rảnh: tạm dừng nhận message (vẫn poll để giữ chỗ trong
    group) và hỏi lại mỗi phút - không nhận job về rồi bỏ."""
    lane = lanes.current_lane()
    if platform != "facebook" or lane is None:
        return
    if await asyncio.to_thread(lanes.lane_account, lane):
        return
    # Thường là mọi tài khoản đã hết hạn mức / đang nghỉ - dự kiến vào cuối ngày. Telegram chỉ một tin mỗi ngày.
    today = datetime.now(_VN).date().isoformat()
    first_today = await asyncio.to_thread(RedisCache().set_nx, f"facebook:lane_wait_alerted:{today}", 1, 2 * 86400)
    logger.info("facebook_lane_waiting_for_account", lane=lane, telegram=first_today)
    partitions = consumer.assignment()
    consumer.pause(*partitions)
    try:
        while not is_platform_draining(platform):
            await consumer.getmany(timeout_ms=60_000)
            if _quiet_seconds_left(platform) <= 0 and await asyncio.to_thread(lanes.lane_account, lane):
                break
    finally:
        consumer.resume(*partitions)
    logger.info("facebook_lane_account_ready", lane=lane)


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
            await consumer.getmany(timeout_ms=60_000)
    finally:
        consumer.resume(*partitions)
    logger.info("quiet_hours_resumed", platform=platform)


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
    mãi."""
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
        return
    requeued = {**request, "_proxy_exhausted_retries": retries + 1}
    await requeue_publisher.publish(
        topic=CRAWL_REQUESTS_TOPIC,
        key=request.get("run_id") or platform,
        value=requeued,
    )
    logger.info("proxy_exhausted_requeued", platform=platform, retries=retries + 1, request=request)


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
            if isinstance(exc, pool.ProxyPoolExhaustedError) or isinstance(exc.__cause__, pool.ProxyPoolExhaustedError):
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
        finally:
            await asyncio.to_thread(cache.ack, processing, raw)
        if backoff:
            logger.warning("priority_lane_proxy_exhausted_pausing", platform=platform, backoff_seconds=backoff)
            await _sleep_interruptible(platform, backoff)
            return  # nhường cho vòng Kafka một lượt rồi mới xả tiếp


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
    consumer = AIOKafkaConsumer(
        CRAWL_REQUESTS_TOPIC,
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
        max_poll_interval_ms=3_600_000,
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
        # Nhiều làn Facebook: chỉ làn 0 trả job dở về hàng - làn khác khởi động sau sẽ trả cả job làn 0 đang chạy.
        if not lanes.current_lane():
            await asyncio.to_thread(_recover_priority_queue, platform)
        logger.info(
            "crawl_request_consumer_started",
            topic=CRAWL_REQUESTS_TOPIC,
            platform=platform,
            group=group_id,
            lane=lanes.current_lane(),
        )
        while True:
            await _wait_out_quiet_hours(consumer, platform)
            await _wait_for_lane_account(consumer, platform)
            await _drain_priority_queue(platform)
            batch = await consumer.getmany(timeout_ms=PRIORITY_POLL_SECONDS * 1000, max_records=1)
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
                    continue
                skipped = False
                proxy_exhausted = False
                try:
                    skipped = await _handle_request(request)
                except Exception as exc:
                    # Một request lỗi không được giết cả consumer - log rồi chuyển sang cái kế tiếp.
                    logger.error("crawl_request_error", platform=platform, error=str(exc), request=request)
                    # NetworkError/TikTokNetworkError riêng của mọi nền tảng đều bọc
                    # pool.ProxyPoolExhaustedError mà nó được raise từ đó (xem ví dụ
                    # `raise ... from exc` trong comet_graphql_client.py/tiktok/client.py) - kiểm tra
                    # __cause__ ở đây giúp phần này không phụ thuộc nền tảng, vòng lặp điều phối chung này
                    # không cần import exception riêng của từng nền tảng để nhận ra nó.
                    proxy_exhausted = isinstance(exc, pool.ProxyPoolExhaustedError) or isinstance(
                        exc.__cause__, pool.ProxyPoolExhaustedError
                    )
                # Commit dù _handle_request thành công hay đã bị log rồi bỏ qua ở trên - dù thế nào message
                # này cũng đã xong, không được giao lại ở lần restart sau. Lỗi cạn proxy là ngoại lệ duy
                # nhất: nó được một cơ hội thử lại thật (xem _requeue_after_proxy_exhaustion) - publish lại
                # *trước* lần commit này, không phải sau, để crash ở giữa tệ nhất chỉ để lại một bản trùng
                # vô hại (cả bản gốc, chưa commit và sắp được giao lại khi restart, lẫn bản mới xếp hàng
                # lại) thay vì âm thầm mất request nếu crash rơi vào chiều ngược lại.
                if proxy_exhausted:
                    proxy_exhausted_streak += 1
                    await _requeue_after_proxy_exhaustion(requeue_publisher, request, platform=platform)
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
                    await consumer.commit()
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
                if request.get("type") == "comments" and platform != "tiktok":
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
    return True


async def _run_platform_consumer(platform: str, group_id: str, lane: int | None = None) -> None:
    """Khởi động lại _run_platform_consumer_once với consumer mới mỗi khi nó kết thúc vì
    KafkaError, thay vì để một trục trặc Kafka tạm thời của một nền tảng hạ luôn vòng lặp của
    hai nền tảng kia qua phản ứng cuối cùng nặng hơn của run() (xem comment của các hằng
    _CONSUMER_RESTART_*). Chỉ trả về - trao quyền lại cho run(), nơi áp phản ứng nặng hơn đó -
    sau một lần dừng có chủ đích (True) hoặc sau _CONSUMER_RESTART_MAX_ATTEMPTS lần khởi
    động lại trong _CONSUMER_RESTART_WINDOW_SECONDS mà không có trọn một khoảng chạy sạch ở
    giữa."""
    lanes.set_current_lane(lane)  # contextvar riêng của task này - job trong làn dùng đúng tài khoản của làn
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
    for platform, group_id in PLATFORM_CONSUMER_GROUPS.items():
        if platform == "facebook" and lanes.LANES > 1:
            # Làn Facebook song song (services/lanes.py): FACEBOOK_LANES consumer cùng group, mỗi làn một tài khoản.
            for lane in range(lanes.LANES):
                task = asyncio.create_task(
                    _run_platform_consumer(platform, group_id, lane), name=f"facebook-lane-{lane}"
                )
                tasks[task] = f"facebook-lane-{lane}"
        else:
            tasks[asyncio.create_task(_run_platform_consumer(platform, group_id), name=platform)] = platform
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
