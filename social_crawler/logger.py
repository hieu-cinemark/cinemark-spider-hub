"""Cấu hình structlog cho spider-hub.

Hợp đồng log dùng chung với cinemark-api (app/core/logging.py bên đó cài đặt cùng hợp
đồng này - giữ hai bên đồng bộ):

  - Mọi dòng đều có: timestamp (ISO 8601, UTC), level, event, service, logger (tên
    module), cộng với mọi context đã bind (run_id ở đây, request_id ở cinemark-api).
  - event là một tên tiếng Anh dạng snake_case cố định ("proxy_degraded"), không bao
    giờ là một câu được ghép chuỗi - phần thay đổi đặt vào các trường key=value.
  - Lỗi dùng cùng các key ở mọi nơi: error (nội dung thông báo), error_type (tên class
    exception), error_code (mã cấp app, khi có). Truyền chính object exception vào
    error= sẽ tự điền cả error lẫn error_type (xem _normalize_error_fields); các alias
    cũ exc=/err= cũng được gộp vào error= theo cách đó.
  - LOG_FORMAT=console (mặc định) in mỗi event một dòng dễ đọc; LOG_FORMAT=json in mỗi
    dòng một object JSON để chuyển log đi nơi khác. Chỉ có màu khi ghi ra terminal thật
    - file log không bao giờ có mã escape ANSI.
  - LOG_LEVEL (mặc định info) lọc bỏ các mức thấp hơn.

Phần riêng của spider-hub bên trên hợp đồng: trường platform (facebook/threads/tiktok/
system, suy ra từ đường dẫn module gọi log) và chuyển tiếp sang Telegram các event
warning/error/critical (hoặc telegram=True).
"""

from __future__ import annotations

import logging
import os
import queue
import sys
import threading

import structlog

import social_crawler.env  # noqa: F401 - LOG_LEVEL/LOG_FORMAT có thể nằm trong .env

SERVICE_NAME = "spider-hub"

_configured = False

_TELEGRAM_AUTO_LEVELS = ("warning", "error", "critical")
_STATUS_BY_LEVEL = {"critical": "FAILED", "error": "FAILED", "warning": "WARNING", "debug": "DEBUG"}
_TELEGRAM_SERVICE_MODULE = "social_crawler.clients.telegram"

# Một worker nền duy nhất (không phải mỗi event log một OS thread mới) xả hàng đợi này -
# không có nó, một loạt log thử lại/lỗi (ví dụ mọi lần thử trong lúc Facebook sập, trên
# nhiều request đang chạy) sẽ sinh ra nhiều thread đồng thời, mỗi cái giữ một lời gọi HTTP
# Telegram chặn, đúng lúc tiến trình đang chịu tải.
_telegram_queue: queue.Queue[str] = queue.Queue()
_telegram_worker_started = False
_telegram_worker_lock = threading.Lock()


def _telegram_worker() -> None:
    from social_crawler.clients.telegram import send_telegram_message

    while True:
        text = _telegram_queue.get()
        try:
            send_telegram_message(text)
        except Exception as exc:  # noqa: BLE001 - xem comment bên dưới: không gì được thoát khỏi worker này
            # Không gọi logger.* ở đây được - worker này chuyển mọi dòng log
            # warning/error/telegram=True trong cả hệ thống, nên đưa lỗi của chính nó quay lại cùng
            # pipeline đó có nguy cơ đệ quy vào chính hàng đợi đang xả. Một bug ở đây (bất cứ thứ gì
            # send_telegram_message chưa tự bắt, ví dụ payload text thật sự sai định dạng) nếu không
            # sẽ âm thầm tắt mọi cảnh báo Telegram mà không để lại dấu vết nào ở đâu, kể cả stdout -
            # print là kênh duy nhất không thể vòng lại vào đây.
            print(f"telegram_worker_crashed error={exc!r} text={text[:200]!r}")


def _ensure_telegram_worker() -> None:
    global _telegram_worker_started
    if _telegram_worker_started:
        return
    with _telegram_worker_lock:
        if _telegram_worker_started:
            return
        threading.Thread(target=_telegram_worker, daemon=True).start()
        _telegram_worker_started = True


def _platform(module_name: str) -> str:
    parts = module_name.split(".")
    if "spiders" in parts:
        idx = parts.index("spiders")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return "system"


def _add_service_fields(_logger, _method_name, event_dict):
    """Các trường service/logger/platform - xem hợp đồng trong docstring module. Đọc (nhưng
    không lấy đi) _module, thứ mà _telegram_processor vẫn cần sau đó."""
    module_name = event_dict.get("_module", "")
    event_dict.setdefault("service", SERVICE_NAME)
    if module_name:
        event_dict.setdefault("logger", module_name)
    event_dict.setdefault("platform", _platform(module_name))
    return event_dict


def _normalize_error_fields(_logger, _method_name, event_dict):
    """Gộp các alias cũ exc=/err= vào error=, và biến một object exception truyền vào error=
    thành error (text) + error_type (tên class) - để mọi dòng lỗi có cùng một dạng bất kể
    chỗ gọi viết thế nào."""
    for alias in ("exc", "err"):
        if alias in event_dict and "error" not in event_dict:
            event_dict["error"] = event_dict.pop(alias)
    error = event_dict.get("error")
    if isinstance(error, BaseException):
        event_dict.setdefault("error_type", type(error).__name__)
        event_dict["error"] = str(error) or type(error).__name__
    return event_dict


def _telegram_processor(_logger, method_name, event_dict):
    """Chuyển tiếp các event warning/error/critical, cộng mọi event được đánh dấu rõ
    telegram=True (ví dụ logger.info("crawl_finished", telegram=True, ...) cho một mốc hoàn
    thành), sang Telegram - xem clients/telegram.py. Tin nhắn chat giữ dòng tiêu đề
    "[PLATFORM] [STATUS] event" để một nhóm chat trộn crawler của nhiều nền tảng vẫn dễ
    lướt, dù bản thân dòng log giờ đã mang platform như một trường. Chạy lời gọi HTTP thật
    trên một thread nền để Telegram API chậm/không truy cập được không bao giờ chặn vòng lặp
    crawl đang chỉ muốn log một cảnh báo thử lại thường ngày. Không làm gì (kiểm tra bên
    trong send_telegram_message) nếu chưa cấu hình TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID."""
    module_name = event_dict.pop("_module", "")
    wants_telegram = event_dict.pop("telegram", False)
    is_auto_level = method_name in _TELEGRAM_AUTO_LEVELS

    if (is_auto_level or wants_telegram) and module_name != _TELEGRAM_SERVICE_MODULE:
        status = _STATUS_BY_LEVEL.get(method_name, "INFO")
        if method_name == "info" and wants_telegram:
            status = "SUCCESS"
        headline = f"[{str(event_dict.get('platform', 'system')).upper()}] [{status}] {event_dict.get('event', '')}"
        skip = {"event", "timestamp", "level", "service", "logger", "platform"}
        details = " | ".join(f"{k}={v}" for k, v in event_dict.items() if k not in skip)
        text = headline + (f"\n{details}" if details else "")
        _ensure_telegram_worker()
        _telegram_queue.put(text)
    return event_dict


def _configure_once() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    level = logging.getLevelNamesMapping().get(os.getenv("LOG_LEVEL", "info").upper(), logging.INFO)
    as_json = os.getenv("LOG_FORMAT", "console").lower() == "json"
    renderer = (
        structlog.processors.JSONRenderer(ensure_ascii=False)
        if as_json
        else structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_service_fields,
            _normalize_error_fields,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.dict_tracebacks if as_json else structlog.processors.format_exc_info,
            _telegram_processor,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    _configure_once()
    return structlog.get_logger(name).bind(_module=name)


def bind_run_id(run_id: str) -> None:
    """Bind run_id vào mọi dòng log tiếp theo của tiến trình này, bất kể bao nhiêu
    module/logger khác nhau gọi get_logger() - structlog.contextvars.merge_contextvars vốn
    đã là processor đầu tiên (xem _configure_once), nên không cần sửa gì ở chỗ khác. Gọi
    một lần, càng sớm càng tốt (ví dụ ngay sau argparse trong một entrypoint CLI được gọi
    làm tiến trình con cho một lượt chạy được theo dõi cụ thể) - xem phần xử lý --run-id
    trong auth/bootstrap.py của facebook/threads. Cho phép bên đọc output log của tiến trình
    này (refresh_tracker.py của cinemark-api) lọc đúng các dòng của lượt chạy này thay vì
    đoán chỉ từ tên nền tảng, vốn không chính xác khi tiến trình con của nhiều nền tảng có
    thể cùng ghi vào một file log dùng chung."""
    structlog.contextvars.bind_contextvars(run_id=run_id)
