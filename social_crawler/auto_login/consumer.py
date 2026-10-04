"""Lắng nghe topic auto_login_requests trên Kafka (xem
app/clients/kafka.py:publish_auto_login_request của cinemark-api) và chạy một lần thử
đăng nhập lại cho mỗi message. Phía consumer tương ứng với:

  * app/services/auto_login.py:run_auto_login_tick - publish mỗi tài khoản có cookie
    chết một message ở mỗi lượt theo lịch (hoặc mỗi lần bấm "Run now" trên dashboard).
  * app/services/scheduler.py:_auto_login_tick - vòng lặp lập lịch do dashboard điều
    khiển, publish những message ở trên.

Đây là cách deploy thay thế cho auto_login/scheduler.py riêng của spider-hub - giờ
dashboard nắm "auto-login có bật không?", danh sách nền tảng cần chạy, cờ dry-run và
chu kỳ chạy. Vòng lặp cũ điều khiển bằng env của spider-hub được giữ lại cho các quy
trình vận hành cũ nhưng không còn là thứ dashboard nói chuyện cùng.

Chạy bằng:
    python -m social_crawler.auto_login.consumer

Cùng kiểu mỗi nền tảng một group như crawl_request_consumer.py - hàng đợi đăng nhập
lại của Facebook không bao giờ chặn đầu hàng của Threads. Hai consumer group riêng
(spider-hub.auto-login.facebook / spider-hub.auto-login.threads), mỗi group đọc từ
auto_login_requests và lập tức bỏ qua (commit) các message của nền tảng khác.

Để mặc định `auto_offset_reset="earliest"` cũng được - một consumer group mới tạo trên
topic auto_login_requests hoàn toàn mới không có gì để phát lại, và phát lại một
message cũ đằng nào cũng vô hại: db/relogin.get_account_for_relogin kiểm tra lại tài
khoản còn đủ điều kiện (đang bật, không bị checkpoint, không bị gắn cờ cần người xử
lý, vẫn chết) trước khi đăng nhập. Nếu topic đã có hàng tồn lớn, người vận hành vẫn nên
`seek to latest` một lần trước lần deploy đầu tiên - cùng cảnh báo như docstring module
của crawl_request_consumer.py.

Mã thoát: 0 khi nhận SIGTERM/SIGINT hoặc AUTO_LOGIN_DRAIN, 1 khi một vòng lặp consumer
chết (lỗi khởi động Kafka, lỗi bất ngờ) để Restart=on-failure của systemd khởi động
lại tiến trình.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import signal
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.auto_login.orchestrator import attempt_auto_login
from social_crawler.db.relogin import get_account_for_relogin
from social_crawler.logger import get_logger

logger = get_logger(__name__)

AUTO_LOGIN_REQUESTS_TOPIC = "auto_login_requests"

# Giống mục auto_login trong CONSUMER_GROUPS của cinemark-api - mỗi nền tảng một vòng
# lặp consumer độc lập để hàng đợi đăng nhập lại của Facebook không bao giờ chặn đầu hàng
# của Threads. Cùng lý do như crawl_request_consumer.py:PLATFORM_CONSUMER_GROUPS, chỉ là
# áp dụng cho auto-login.
PLATFORM_CONSUMER_GROUPS = {
    "facebook": "spider-hub.auto-login.facebook",
    "threads": "spider-hub.auto-login.threads",
}

# Nghỉ 4-10s sau mỗi lần thử đăng nhập thật trên cùng nền tảng, cùng khoảng mà
# auto_login/scheduler.py đã dùng khi chạy cùng logic trong tiến trình. Dùng lại một giá
# trị đã kiểm chứng thay vì nghĩ ra cái thứ hai - và cố ý bỏ qua sau một lần dry run
# hoặc một message bị bỏ qua, để một lượt dry-run ("chuyện gì sẽ xảy ra?" của người vận
# hành) không phí vài phút chỉ để liệt kê ứng viên.
INTER_ATTEMPT_PAUSE_MIN_SECONDS = 4.0
INTER_ATTEMPT_PAUSE_MAX_SECONDS = 10.0


def _bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _should_drain() -> bool:
    """Công tắc ngắt do người vận hành điều khiển - cho nút Dừng của bản deploy cắt ngang
    công việc đang chờ. Đọc lại ở mỗi message để một SIGTERM lật file (hoặc nạp lại env) có
    hiệu lực ở lần poll kế tiếp, không cần cơ chế nạp lại trong tiến trình. Cùng dạng với
    crawl_request_consumer.py:is_platform_draining."""
    flag = (os.getenv("AUTO_LOGIN_DRAIN") or "").strip().lower()
    return flag in {"1", "true", "yes", "on"}


def _deserialize(raw: bytes | None) -> Any:
    """Giải mã JSON giá trị của một message; None nếu là tombstone hoặc payload không phải
    JSON. Raise ở đây sẽ lan ra khỏi getmany() và kết thúc cả vòng lặp consumer chỉ vì một
    message độc."""
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError, ValueError:
        return None


def _handle_message(platform: str, value: dict[str, Any]) -> bool:
    """Tra account_id của message thành dòng đầy đủ mà attempt_auto_login cần và chạy luồng
    đăng nhập (luồng đó tự ghi dòng audit). Trả về có thực sự thử đăng nhập không, để chỗ
    gọi chỉ nghỉ sau những lần đó.

    Cố ý chạy đồng bộ - vòng lặp consumer chạy nó qua asyncio.to_thread: attempt_auto_login
    điều khiển Playwright đồng bộ (vốn từ chối khởi động trên thread đang có event loop
    chạy) và chặn ở psycopg/IMAP, mà nếu chạy ngay trên loop thì còn làm treo consumer của
    nền tảng kia và heartbeat Kafka.

    account_id sai / thiếu, tài khoản không còn đủ điều kiện, hoặc Supabase trục trặc thì
    được log + bỏ qua - không bao giờ raise vào vòng lặp Kafka (raise sẽ làm kẹt partition
    cho tới khi người vận hành can thiệp).
    """
    account_id = value.get("account_id")
    if account_id is None:
        logger.warning("auto_login_message_missing_account_id", platform=platform, value_keys=list(value.keys()))
        return False
    dry_run = bool(value.get("dry_run"))

    account_row = get_account_for_relogin(platform, str(account_id))
    if account_row is None:
        # Đã bị xoá, tắt, checkpoint, gắn cờ needs_manual_login, hoặc đã sống lại từ khi lượt
        # lập lịch publish message này (hoặc đây là một message cũ được phát lại) -
        # get_account_for_relogin kiểm tra lại tất cả. Log lại; KHÔNG raise - message kế tiếp
        # trên partition này không liên quan gì tới message này.
        logger.warning(
            "auto_login_account_ineligible_at_consume",
            platform=platform,
            account_id=account_id,
            run_id=value.get("run_id"),
        )
        return False

    outcome = attempt_auto_login(platform, account_row, dry_run=dry_run)
    logger.info(
        "auto_login_consume_done",
        platform=platform,
        account_id=account_id,
        status=outcome.status,
        needs_manual_login=outcome.needs_manual_login,
        run_id=value.get("run_id"),
    )
    return not dry_run


async def _run_consumer(platform: str) -> None:
    """Chỉ trả về bình thường khi được drain (AUTO_LOGIN_DRAIN). Lỗi khởi động Kafka hoặc lỗi
    bất ngờ được lan ra, để main() thoát với mã khác 0."""
    group_id = PLATFORM_CONSUMER_GROUPS[platform]
    consumer = AIOKafkaConsumer(
        AUTO_LOGIN_REQUESTS_TOPIC,
        bootstrap_servers=_bootstrap_servers(),
        group_id=group_id,
        value_deserializer=_deserialize,
        key_deserializer=lambda raw: raw.decode("utf-8", errors="replace") if raw else None,
        enable_auto_commit=False,  # commit SAU KHI _handle_message trả về để crash giữa chừng thì được phát lại
        auto_offset_reset="earliest",
    )
    try:
        await consumer.start()
    except KafkaError as exc:
        logger.error("auto_login_kafka_start_failed", platform=platform, error=str(exc))
        raise
    logger.info("auto_login_consumer_started", platform=platform, group_id=group_id)
    try:
        while True:
            try:
                # Timeout ngắn để cờ drain được kiểm tra ít nhất vài giây một lần. max_records=1: mỗi
                # lần poll một lần đăng nhập, để khoảng nghỉ bên dưới rơi vào giữa các lần thử liên
                # tiếp, và hàng tồn không bao giờ giữ một lô quá max_poll_interval_ms (vượt quá sẽ khiến
                # consumer này bị đá khỏi group, mọi lần commit thất bại, và lô đó bị giao lại - đăng
                # nhập cùng các tài khoản hai lần).
                batches = await consumer.getmany(timeout_ms=2000, max_records=1)
            except KafkaError as exc:
                logger.error("auto_login_kafka_poll_failed", platform=platform, error=str(exc))
                await asyncio.sleep(2.0)
                continue

            if _should_drain():
                logger.warning("auto_login_consumer_draining", platform=platform)
                return

            for tp, msgs in batches.items():
                for msg in msgs:
                    attempted = False
                    if not isinstance(msg.value, dict):
                        logger.warning(
                            "auto_login_message_unparseable",
                            platform=platform,
                            partition=tp.partition,
                            offset=msg.offset,
                        )
                    elif msg.value.get("platform") == platform:
                        try:
                            attempted = await asyncio.to_thread(_handle_message, platform, msg.value)
                        except Exception:
                            # Phòng thủ: _handle_message vốn đã tự nuốt các lỗi đã biết của nó. Thứ gì lọt ra được
                            # là bug thật đáng log, nhưng consumer phải chạy tiếp để không làm kẹt partition - và
                            # nghỉ như thể vừa có một lần đăng nhập, cho an toàn.
                            logger.exception("auto_login_handle_unhandled_exception", platform=platform)
                            attempted = True
                    # Message không parse được và message của nền tảng khác cũng được commit: mỗi group đọc
                    # cả topic, và bỏ qua một message mà không commit sẽ hiện ra thành lag vĩnh viễn trong
                    # get_consumer_lag của cinemark-api.
                    try:
                        await consumer.commit({tp: msg.offset + 1})
                    except KafkaError as exc:
                        logger.warning("auto_login_commit_failed", platform=platform, error=str(exc))
                    if attempted and not _should_drain():
                        await asyncio.sleep(
                            random.uniform(INTER_ATTEMPT_PAUSE_MIN_SECONDS, INTER_ATTEMPT_PAUSE_MAX_SECONDS)
                        )
    finally:
        await consumer.stop()
        logger.info("auto_login_consumer_stopped", platform=platform)


async def main() -> int:
    """Khởi động mỗi nền tảng (facebook, threads) một vòng lặp consumer - cùng lựa chọn song
    song như crawl_request_consumer.py để cấu hình Playwright-trên-IP-riêng độc lập theo
    từng nền tảng, không dùng chung. Xem docstring module về mã thoát."""
    # Xử lý signal để tắt êm khi systemd / Docker dừng.
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop_handler(*_args: Any) -> None:
        logger.warning("auto_login_consumer_signal_received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _stop_handler)
        except NotImplementedError:
            # Một số môi trường (đặc biệt là Windows) không hỗ trợ add_signal_handler - quay về hành
            # vi mặc định.
            signal.signal(sig, lambda *_a: None)

    tasks = {platform: asyncio.create_task(_run_consumer(platform)) for platform in PLATFORM_CONSUMER_GROUPS}
    stop_task = asyncio.create_task(stop_event.wait())
    await asyncio.wait([*tasks.values(), stop_task], return_when=asyncio.FIRST_COMPLETED)
    for t in (*tasks.values(), stop_task):
        t.cancel()
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    crashed = False
    for platform, result in zip(tasks, results):
        if result is None or isinstance(result, asyncio.CancelledError):
            continue
        crashed = True
        logger.error("auto_login_consumer_crashed", platform=platform, error=repr(result))
    return 1 if crashed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
