"""Dòng thời gian theo tài khoản (04/11/2026): tài khoản chết thì đọc lại được chuyện gì đã xảy ra, theo thứ tự.

Không có khoá ngoại tới platform_accounts: dòng tài khoản bị xoá thì lịch sử vẫn còn. Ghi lỗi không bao giờ làm hỏng việc đang
chạy. Không bao giờ truyền giá trị cookie/token/mật khẩu vào `details`.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from social_crawler.core.logger import get_logger
from social_crawler.db.connection import connect

logger = get_logger(__name__)

_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS account_events ("
    "id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), platform text NOT NULL, "
    "account_key text NOT NULL, event text NOT NULL, details jsonb NOT NULL DEFAULT '{}'::jsonb)"
)
_INDEX_SQL = "CREATE INDEX IF NOT EXISTS account_events_account_at ON account_events (platform, account_key, at DESC)"
_table_ready = False


def _ensure_table(conn: Any) -> None:
    global _table_ready
    if not _table_ready:
        conn.execute(_TABLE_SQL)
        conn.execute(_INDEX_SQL)
        _table_ready = True


def record(platform: str, account_key: str | None, event: str, **details: Any) -> None:
    key = (account_key or "").strip().lower()
    if not key:
        return
    try:
        with connect() as conn:
            _ensure_table(conn)
            conn.execute(
                "INSERT INTO account_events (platform, account_key, event, details) VALUES (%s, %s, %s, %s)",
                (platform, key, event, Jsonb(details)),
            )
    except (psycopg.Error, TypeError) as exc:
        logger.warning("account_event_write_failed", event_name=event, error_type=type(exc).__name__)


def timeline(platform: str, keys: list[str], *, limit: int = 200) -> list[dict[str, Any]]:
    """Cũ trước, mới sau. Một tài khoản có thể được ghi theo email hoặc account_id nên nhận nhiều key."""
    with connect() as conn:
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT at, account_key, event, details FROM account_events "
            "WHERE platform = %s AND account_key = ANY(%s) ORDER BY at DESC, id DESC LIMIT %s",
            (platform, [key.strip().lower() for key in keys if key], limit),
        ).fetchall()
    return list(reversed(rows))
