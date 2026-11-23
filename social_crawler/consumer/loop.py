"""Vòng lặp consumer Kafka theo từng nền tảng, làn ưu tiên Redis, khởi động lại khi lỗi."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from social_crawler.clients.kafka import CRAWL_REQUESTS_TOPIC, FACEBOOK_PUBLIC_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.consumer.common import (
    PLATFORM_CONSUMER_GROUPS,
    enabled_platforms,
    logger,
)
from social_crawler.consumer.dispatch import _handle_request, _request_platform
from social_crawler.consumer.gates import (
    FOREIGN_SKIP_MAX_SECONDS,
    _clear_leftover_job_key,
    _finish_stale_job,
    _pause_seconds,
    _retryable_failure,
    _stale_facebook_job,
    _wait_out_public_hold,
    _yield_partitions_while_no_account,
)
from social_crawler.consumer.jobs import public_page as public_page_job
from social_crawler.consumer.jobs import threads_channel as threads_channel_job
from social_crawler.consumer.jobs import youtube as youtube_job
from social_crawler.consumer.process import _sleep_interruptible
from social_crawler.consumer.queues import (
    PRIORITY_POLL_SECONDS,
    _drain_priority_queue,
    _recover_idle_queue,
    _recover_priority_queue,
    _recover_public_pages_queue,
    _recover_threads_channel_queue,
    _recover_youtube_queue,
    _run_one_idle_job,
    _run_public_pages_worker,
    _run_threads_channel_worker,
    _run_youtube_lane,
    _wait_out_hourly_cap,
    _wait_out_quiet_hours,
)
from social_crawler.db.proxy_settings import get_proxy_settings
from social_crawler.runtime import lanes
from social_crawler.runtime.task_queue import is_platform_draining

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
        # Job có đăng nhập có thể nghỉ mà vẫn giữ con trỏ phân trang và lease.
        # Heartbeat vẫn chạy; cho phép khối việc hằng ngày có giới hạn cộng thời gian nghỉ trước khi rebalance.
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
                except Exception as exc:  # noqa: BLE001 - một request lỗi không được giết cả consumer
                    # Một request lỗi không được giết cả consumer - log rồi chuyển sang cái kế tiếp.
                    logger.error("crawl_request_error", platform=platform, error=exc, request=request)
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
                    pause = await asyncio.to_thread(_pause_seconds, "facebook_comments")
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
                # Xem _pause_seconds ở trên - một khoảng nghỉ trước khi lấy thứ kế
                # tiếp trong hàng đợi *của nền tảng này*, không phải trước request đầu tiên của một lô mới
                # (chưa có gì để giãn cách).
                prefix = {"facebook": "facebook_job", "threads": "threads_job"}.get(platform, "other_job")
                pause = await asyncio.to_thread(_pause_seconds, prefix)
                logger.info("inter_request_pause", platform=platform, seconds=round(pause, 1))
                await _sleep_interruptible(platform, pause)
    except asyncio.CancelledError:
        logger.info("platform_consumer_cancelled", platform=platform)
        raise
    except KeyboardInterrupt:
        logger.info("platform_consumer_interrupted", platform=platform)
        return True
    except KafkaError as exc:
        logger.error("kafka_error", platform=platform, error=exc)
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
        for slot in range(public_page_job.MAX_WORKERS):
            name = f"public-pages-{slot}"
            tasks[asyncio.create_task(_run_public_pages_worker(slot), name=name)] = name
    if "youtube" in enabled:
        await asyncio.to_thread(_recover_youtube_queue)
        for lane in range(youtube_job.MAX_LANES):
            name = f"youtube-lane-{lane}"
            tasks[asyncio.create_task(_run_youtube_lane(lane), name=name)] = name
    if "threads" in enabled:
        await asyncio.to_thread(_recover_threads_channel_queue)
        for slot in range(threads_channel_job.MAX_WORKERS):
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
    logger.error("platform_consumer_stopped", platform=platform, error=exc)
    raise RuntimeError(f"{platform} consumer loop stopped unexpectedly") from exc
