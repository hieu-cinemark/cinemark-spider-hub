"""Connection Postgres (Supabase) cho mọi module trong package này - xem db/__init__.py để
biết vì sao mỗi lời gọi một connection mới thay vì dùng pool."""

from __future__ import annotations

import contextlib
import os
import threading
import time
from typing import Any

import psycopg
from psycopg.rows import dict_row

import social_crawler.core.env  # nạp .env đúng một lần, dù bao nhiêu module import nó  # noqa: F401


def _database_url() -> str:
    """DATABASE_URL (Supabase production) trừ khi APP_ENV=development, khi đó dùng
    LOCAL_DATABASE_URL (một Postgres local, xem scripts/dev_db_schema.sql) thay thế - xem
    .env.example. Mặc định là "production" khi chưa đặt để một .env đã deploy sẵn (service
    systemd, cron) không có dòng APP_ENV nào vẫn gọi Supabase y như trước giờ; chỉ máy dev
    chủ động bật APP_ENV=development mới nói chuyện với DB local."""
    if os.environ.get("APP_ENV", "production") == "development":
        try:
            return os.environ["LOCAL_DATABASE_URL"]
        except KeyError:
            raise RuntimeError("APP_ENV=development but LOCAL_DATABASE_URL is not set (see .env.example)") from None
    return os.environ["DATABASE_URL"]


# Tái dùng connection trong cùng tiến trình: trước đây mỗi lời gọi mở một connection mới tới pooler Supabase, và khi consumer chạy
# liên tục (hàng chục lượt/giây) chi phí bắt tay + xác thực của mỗi connection là phần lớn egress "Shared Pooler" (11,8 GB/chu kỳ,
# 20/11/2026). Connection nhàn rỗi được giữ lại ngắn hạn; DB_POOL_MAX_IDLE=0 tắt hẳn (về hành vi cũ).
_IDLE_TTL_SECONDS = 20.0
_MAX_IDLE = int(os.environ.get("DB_POOL_MAX_IDLE", "4"))

_lock = threading.Lock()
_idle: list[tuple[float, psycopg.Connection[Any]]] = []
_owner_pid = os.getpid()


def _open() -> psycopg.Connection[Any]:
    # prepare_threshold=None: pooler ở chế độ transaction (cổng 6543) không giữ prepared statement giữa các lượt, và connection
    # tái dùng sẽ vượt ngưỡng 5 lần execute mặc định của psycopg.
    return psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5, prepare_threshold=None)


def _acquire() -> psycopg.Connection[Any]:
    global _owner_pid
    now = time.monotonic()
    with _lock:
        if _owner_pid != os.getpid():  # tiến trình con (fork) không được dùng socket của cha
            _idle.clear()
            _owner_pid = os.getpid()
        while _idle:
            at, conn = _idle.pop()
            if (
                now - at < _IDLE_TTL_SECONDS
                and not conn.closed
                and conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            ):
                return conn
            _discard(conn)
    return _open()


def _discard(conn: psycopg.Connection[Any]) -> None:
    with contextlib.suppress(Exception):  # đóng connection hỏng không được che lỗi gốc
        conn.close()


def _release(conn: psycopg.Connection[Any]) -> None:
    if conn.closed or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        _discard(conn)
        return
    with _lock:
        if len(_idle) < _MAX_IDLE:
            _idle.append((time.monotonic(), conn))
            return
    _discard(conn)


class _Lease:
    """`with connect() as conn:` như trước (commit khi thành công, rollback khi lỗi) nhưng trả connection về kho thay vì đóng."""

    def __init__(self) -> None:
        self._conn: psycopg.Connection[Any] | None = None

    def __enter__(self) -> psycopg.Connection[Any]:
        self._conn = _acquire()
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            if exc_type is None:
                conn.commit()
            else:
                conn.rollback()
        except Exception:
            _discard(conn)
            if exc_type is None:
                raise
            return
        _release(conn)


def connect() -> Any:
    """Context manager trả một connection Postgres (psycopg, dict_row)."""
    if _MAX_IDLE <= 0:
        return psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5)
    return _Lease()
