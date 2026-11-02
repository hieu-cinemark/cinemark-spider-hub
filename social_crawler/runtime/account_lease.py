"""Quyền dùng độc quyền một tài khoản Meta xuyên suốt đăng nhập / Restore / crawl / nuôi / kiểm tra cookie (02/11/2026).

Trước đó không có khoá chung: khoá làn chỉ giữa các làn với nhau, proxy_ip:active là quyền theo CỔNG (cùng tài khoản nhận lại
được), manual_login:session chỉ giữa các phiên đăng nhập tay - nên Restore và một job nền có thể mở cùng một tài khoản cùng
lúc.

Lease thuộc về một tiến trình (consumer chỉ điều phối, người giữ là tiến trình con mở trình duyệt / gửi request): nhận lại
trong cùng tiến trình thì đếm tham chiếu, một luồng nền gia hạn tới khi nhả hoặc tiến trình thoát, tiến trình bị SIGKILL thì
lease tự hết sau LEASE_TTL_SECONDS. Redis lỗi lúc nhận = coi như bận (không chạy khi không chứng minh được độc quyền).
AccountBusyError là một ProxyPoolExhaustedError: đi đúng đường "chưa phục vụ được lúc này" có sẵn (thoát 75, consumer xếp lại,
không phạt tài khoản).

Mất lease: crawl HTTP thấy cờ lost trước mỗi request (CometGraphQLClient._throttle). Thao tác có browser không có chỗ kiểm tra
như vậy và API đồng bộ của Playwright không gọi được từ luồng khác, nên luồng gia hạn kết thúc cả tiến trình (mã 75): driver thấy
pipe đóng và đóng Chromium theo (đo 02/11/2026: dưới 1 giây). Lỗi Redis thì dừng trước khi lease hết hạn thật, để không có lúc
hai phiên cùng chạy.
"""

from __future__ import annotations

import atexit
import os
import socket
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import redis

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.runtime import pool

logger = get_logger(__name__)

PLATFORMS = {"facebook", "threads"}
LEASE_TTL_SECONDS = 180
RENEW_EVERY_SECONDS = 60
_KEY_TMPL = "account_lease:{platform}:{account}"
# Slot đăng nhập tay không gắn dòng tài khoản nào (constants.*.DEFAULT_ACCOUNT_KEY).
_NO_ACCOUNT_KEY = "default"
# Thao tác chỉ gửi HTTP qua client có kiểm tra cờ lost; còn lại đều có browser đang chạy.
_HTTP_ONLY_OPERATIONS = {"crawl"}
# Cho cảnh báo Telegram kịp gửi trước khi tiến trình kết thúc.
_EXIT_FLUSH_SECONDS = 2.0
_exit = os._exit  # test thay bằng hàm không thoát thật


class AccountBusyError(pool.ProxyPoolExhaustedError):
    """Tài khoản đang được một thao tác khác dùng (hoặc không xác nhận được là không)."""


@dataclass
class _Held:
    cache: Any
    key: str
    value: dict[str, Any]
    count: int = 1
    lost: bool = False
    stop: threading.Event = field(default_factory=threading.Event)


_held: dict[tuple[str, str], _Held] = {}
_lock = threading.Lock()


def _ident(platform: str, account_key: str | None) -> tuple[str, str] | None:
    account = (account_key or "").strip().lower()
    if platform not in PLATFORMS or not account or account == _NO_ACCOUNT_KEY:
        return None
    return platform, account


def _renew_loop(held: _Held) -> None:
    last_ok = time.monotonic()
    while not held.stop.wait(RENEW_EVERY_SECONDS):
        try:
            renewed = held.cache.expire_if_equals(held.key, held.value, LEASE_TTL_SECONDS)
        except redis.RedisError as exc:
            # Trục trặc ngắn thì lease vẫn còn hạn; chỉ khi quá TTL mới không còn chứng minh được.
            logger.warning("account_lease_renew_failed", key=held.key, error=str(exc)[:200])
            renewed = time.monotonic() - last_ok < LEASE_TTL_SECONDS - RENEW_EVERY_SECONDS
        else:
            last_ok = time.monotonic() if renewed else last_ok
        if not renewed:
            held.lost = True
            operation = held.value["operation"]
            logger.error("account_lease_lost", telegram=True, key=held.key, operation=operation)
            if operation not in _HTTP_ONLY_OPERATIONS:
                sys.stdout.flush()
                time.sleep(_EXIT_FLUSH_SECONDS)
                _exit(pool.PROXY_EXHAUSTED_EXIT_CODE)
            return


def acquire(platform: str, account_key: str | None, operation: str, cache: RedisCache | None = None) -> None:
    ident = _ident(platform, account_key)
    if ident is None:
        return
    with _lock:
        held = _held.get(ident)
        if held is not None:
            if held.lost:
                raise AccountBusyError(f"{platform}: lost the lease on account {ident[1]!r}")
            held.count += 1
            return
        cache = cache or RedisCache()
        key = _KEY_TMPL.format(platform=ident[0], account=ident[1])
        value = {
            "token": uuid.uuid4().hex,
            "operation": operation,
            "host": socket.gethostname(),
            "pid": os.getpid(),
        }
        if not cache.set_nx(key, value, LEASE_TTL_SECONDS):
            try:
                current = cache.get(key)
            except redis.RedisError:
                current = None
            raise AccountBusyError(
                f"{platform}: account {ident[1]!r} is busy ({(current or {}).get('operation', 'unknown')}) - not opening it for {operation}."
            )
        held = _Held(cache=cache, key=key, value=value)
        _held[ident] = held
    threading.Thread(target=_renew_loop, args=(held,), daemon=True, name=f"lease-{ident[1]}").start()


def release(platform: str, account_key: str | None) -> None:
    ident = _ident(platform, account_key)
    if ident is None:
        return
    with _lock:
        held = _held.get(ident)
        if held is None:
            return
        held.count -= 1
        if held.count > 0:
            return
        del _held[ident]
    _drop(held)


def _drop(held: _Held) -> None:
    held.stop.set()
    # Chỉ xoá khi còn đúng là của mình: lease đã hết hạn và người khác nhận thì không đụng tới.
    held.cache.delete_if_equals(held.key, held.value)


def lost(platform: str, account_key: str | None) -> bool:
    ident = _ident(platform, account_key)
    held = _held.get(ident) if ident else None
    return bool(held and held.lost)


def holder(platform: str, account_key: str | None, cache: RedisCache | None = None) -> dict[str, Any] | None:
    ident = _ident(platform, account_key)
    if ident is None:
        return None
    return (cache or RedisCache()).get(_KEY_TMPL.format(platform=ident[0], account=ident[1]))


@contextmanager
def hold(platform: str, account_key: str | None, operation: str, cache: RedisCache | None = None) -> Iterator[None]:
    acquire(platform, account_key, operation, cache)
    try:
        yield
    finally:
        release(platform, account_key)


def _release_all() -> None:
    with _lock:
        held_now = list(_held.values())
        _held.clear()
    for held in held_now:
        try:
            _drop(held)
        except Exception:  # noqa: BLE001, S110 - đang thoát: lease tự hết hạn sau TTL
            pass


atexit.register(_release_all)
