from __future__ import annotations

import queue
import threading

import structlog

_configured = False

_TELEGRAM_AUTO_LEVELS = ("warning", "error", "critical")
_STATUS_BY_LEVEL = {"critical": "FAILED", "error": "FAILED", "warning": "WARNING", "debug": "DEBUG"}
_TELEGRAM_SERVICE_MODULE = "social_crawler.services.telegram"

# A single background worker (not one new OS thread per log event) drains
# this queue - without it, a burst of retry/error logs (e.g. every attempt
# during a Facebook outage, across several in-flight requests) spawns many
# concurrent threads each holding a blocking Telegram HTTP call open, right
# when the process is already under stress.
_telegram_queue: "queue.Queue[str]" = queue.Queue()
_telegram_worker_started = False
_telegram_worker_lock = threading.Lock()


def _telegram_worker() -> None:
    from social_crawler.services.telegram import send_telegram_message

    while True:
        text = _telegram_queue.get()
        try:
            send_telegram_message(text)
        except Exception:
            pass


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
            return parts[idx + 1].upper()
    return "SYSTEM"


def _platform_status_processor(_logger, method_name, event_dict):
    """Prefixes every log line's event text with "[PLATFORM] [STATUS]" -
    e.g. "[FACEBOOK] [SUCCESS] saved_token_cache" - so a console or Telegram
    chat mixing multiple platforms' crawlers stays scannable at a glance.
    Reads (but doesn't consume) the _module value get_logger() binds onto
    every logger, since _telegram_processor still needs it afterwards."""
    module_name = event_dict.get("_module", "")
    status = _STATUS_BY_LEVEL.get(method_name, "INFO")
    if method_name == "info" and event_dict.get("telegram"):
        status = "SUCCESS"
    event_dict["event"] = f"[{_platform(module_name)}] [{status}] {event_dict.get('event', '')}"
    return event_dict


def _telegram_processor(_logger, method_name, event_dict):
    """Forwards warning/error/critical events, plus any event explicitly
    marked telegram=True (e.g. logger.info("crawl_finished", telegram=True,
    ...) for a completion milestone), to Telegram - see services/telegram.py.
    Runs the actual HTTP call on a background thread so a slow/unreachable
    Telegram API never blocks the crawl loop that's just trying to log a
    routine retry warning. No-op (checked inside send_telegram_message) if
    TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID aren't configured."""
    module_name = event_dict.pop("_module", "")
    wants_telegram = event_dict.pop("telegram", False)
    is_auto_level = method_name in _TELEGRAM_AUTO_LEVELS

    if (is_auto_level or wants_telegram) and module_name != _TELEGRAM_SERVICE_MODULE:
        event = event_dict.get("event", "")
        details = " | ".join(f"{k}={v}" for k, v in event_dict.items() if k != "event")
        text = event + (f"\n{details}" if details else "")
        _ensure_telegram_worker()
        _telegram_queue.put(text)
    return event_dict


def _configure_once() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _platform_status_processor,
            _telegram_processor,
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    _configure_once()
    return structlog.get_logger(name).bind(_module=name)
