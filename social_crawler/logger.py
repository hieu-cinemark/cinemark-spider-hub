"""structlog setup for spider-hub.

Shared log contract with cinemark-api (app/core/logging.py implements the
same one - keep the two in sync):

  - Every line carries: timestamp (ISO 8601, UTC), level, event, service,
    logger (module name), plus whatever context was bound (run_id here,
    request_id in cinemark-api).
  - event is a static snake_case English name ("proxy_degraded"), never an
    interpolated sentence - variable parts go in key=value fields.
  - Errors use the same keys everywhere: error (message text), error_type
    (exception class name), error_code (an app-level code, when there is
    one). Passing the exception object itself as error= fills in both
    error and error_type automatically (see _normalize_error_fields); the
    legacy aliases exc=/err= are folded into error= the same way.
  - LOG_FORMAT=console (default) renders one human-readable line per event;
    LOG_FORMAT=json renders one JSON object per line for log shipping.
    Colors only when writing to a real terminal - log files never get ANSI
    escape codes.
  - LOG_LEVEL (default info) filters below that level.

spider-hub specifics on top of the contract: a platform field (facebook/
threads/tiktok/system, derived from the logging module's path) and
Telegram forwarding of warning/error/critical (or telegram=True) events.
"""

from __future__ import annotations

import logging
import os
import queue
import sys
import threading

import structlog

import social_crawler.env  # noqa: F401 - LOG_LEVEL/LOG_FORMAT may live in .env

SERVICE_NAME = "spider-hub"

_configured = False

_TELEGRAM_AUTO_LEVELS = ("warning", "error", "critical")
_STATUS_BY_LEVEL = {"critical": "FAILED", "error": "FAILED", "warning": "WARNING", "debug": "DEBUG"}
_TELEGRAM_SERVICE_MODULE = "social_crawler.clients.telegram"

# A single background worker (not one new OS thread per log event) drains
# this queue - without it, a burst of retry/error logs (e.g. every attempt
# during a Facebook outage, across several in-flight requests) spawns many
# concurrent threads each holding a blocking Telegram HTTP call open, right
# when the process is already under stress.
_telegram_queue: queue.Queue[str] = queue.Queue()
_telegram_worker_started = False
_telegram_worker_lock = threading.Lock()


def _telegram_worker() -> None:
    from social_crawler.clients.telegram import send_telegram_message

    while True:
        text = _telegram_queue.get()
        try:
            send_telegram_message(text)
        except Exception as exc:  # noqa: BLE001 - see comment below: nothing may escape this worker
            # Can't call logger.* here - this worker delivers every
            # warning/error/telegram=True log line in the whole system, so
            # routing its own failure back through that same pipeline risks
            # recursing into the queue it's draining. A bug here (anything
            # send_telegram_message doesn't already catch itself, e.g. a
            # genuinely malformed text payload) would otherwise silently
            # disable all Telegram alerting with zero trace anywhere,
            # including stdout - print is the one channel that can't loop
            # back into this.
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
    """service/logger/platform fields - see the module docstring's contract.
    Reads (but doesn't consume) _module, which _telegram_processor still
    needs afterwards."""
    module_name = event_dict.get("_module", "")
    event_dict.setdefault("service", SERVICE_NAME)
    if module_name:
        event_dict.setdefault("logger", module_name)
    event_dict.setdefault("platform", _platform(module_name))
    return event_dict


def _normalize_error_fields(_logger, _method_name, event_dict):
    """Folds the legacy exc=/err= aliases into error=, and turns an
    exception object passed as error= into error (text) + error_type
    (class name) - so every error line has the same shape whichever way the
    call site wrote it."""
    for alias in ("exc", "err"):
        if alias in event_dict and "error" not in event_dict:
            event_dict["error"] = event_dict.pop(alias)
    error = event_dict.get("error")
    if isinstance(error, BaseException):
        event_dict.setdefault("error_type", type(error).__name__)
        event_dict["error"] = str(error) or type(error).__name__
    return event_dict


def _telegram_processor(_logger, method_name, event_dict):
    """Forwards warning/error/critical events, plus any event explicitly
    marked telegram=True (e.g. logger.info("crawl_finished", telegram=True,
    ...) for a completion milestone), to Telegram - see clients/telegram.py.
    The chat message keeps the "[PLATFORM] [STATUS] event" headline so a
    chat mixing several platforms' crawlers stays scannable, even though
    the log line itself now carries platform as a field. Runs the actual
    HTTP call on a background thread so a slow/unreachable Telegram API
    never blocks the crawl loop that's just trying to log a routine retry
    warning. No-op (checked inside send_telegram_message) if
    TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID aren't configured."""
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
    """Binds run_id onto every subsequent log line from this process,
    however many different modules/loggers end up calling get_logger() -
    structlog.contextvars.merge_contextvars is already the first processor
    (see _configure_once), so this needs no changes anywhere else. Call
    once, as early as possible (e.g. right after argparse in a CLI
    entrypoint invoked as a subprocess for one specific tracked run) - see
    facebook/threads auth/bootstrap.py's --run-id handling. Lets a consumer
    of this process's log output (cinemark-api's refresh_tracker.py) filter
    down to exactly this run's own lines instead of guessing from platform
    name alone, which isn't precise when multiple platforms' subprocesses
    can be writing to the same shared log file at once."""
    structlog.contextvars.bind_contextvars(run_id=run_id)
