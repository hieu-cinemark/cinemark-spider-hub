"""Đọc D1 cinemark-config-db qua HTTP query API của Cloudflare (đồng bộ). Chỉ dùng cho các bảng cấu hình đã chuyển khỏi Supabase
(ai_settings, ai_providers, proxy_settings) khi CONFIG_DB_BACKEND=d1; cinemark-api là bên ghi (app/services/accounts/config_db/d1_store.py).

Cần CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, CLOUDFLARE_D1_CONFIG_DATABASE_ID trong .env. Lỗi mạng/HTTP/SQL ném D1Error để chỗ gọi
áp dụng quy tắc "đọc cấu hình lỗi thì giữ bản tốt gần nhất" như với Postgres - module này không tự log."""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from typing import Any, Self

import httpx

import social_crawler.core.env  # nạp .env đúng một lần  # noqa: F401

_BASE_URL = "https://api.cloudflare.com/client/v4"
_TIMEOUT = httpx.Timeout(15.0, connect=8.0)
_client: httpx.Client | None = None


class D1Error(RuntimeError):
    """D1 không trả lời được hoặc từ chối câu SQL."""


def enabled() -> bool:
    return os.environ.get("CONFIG_DB_BACKEND", "postgres").strip().lower() == "d1"


def _http() -> httpx.Client:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.Client(timeout=_TIMEOUT, limits=httpx.Limits(max_connections=4, max_keepalive_connections=2))
    return _client


def query(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """Chạy một câu SQL trên cinemark-config-db, trả các dòng (list rỗng khi không có)."""
    try:
        account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        token = os.environ["CLOUDFLARE_API_TOKEN"]
        database = os.environ["CLOUDFLARE_D1_CONFIG_DATABASE_ID"]
    except KeyError as exc:
        raise D1Error(f"thiếu biến môi trường {exc.args[0]} (CONFIG_DB_BACKEND=d1)") from None
    try:
        resp = _http().post(
            f"{_BASE_URL}/accounts/{account}/d1/database/{database}/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql, "params": params or []},
        )
    except httpx.HTTPError as exc:
        raise D1Error(f"{type(exc).__name__}: {exc}") from exc
    if resp.status_code >= 400:
        raise D1Error(f"HTTP {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    if not data.get("success"):
        raise D1Error(f"D1 từ chối câu SQL: {str(data.get('errors'))[:200]}")
    results = data.get("result") or []
    return results[0].get("results", []) if results else []


def json_value(raw: Any) -> Any:
    """Cột JSON lưu dạng TEXT trong D1 -> dict/list (đã là dict thì giữ nguyên, chuỗi hỏng thì None)."""
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return None
    return raw


def iso(value: datetime | str | None = None) -> str:
    """Thời gian dạng chuỗi ISO-8601 UTC độ rộng cố định (microsecond, +00:00) - định dạng cột thời gian trong D1, so sánh chuỗi
    đúng thứ tự thời gian. Không truyền gì = bây giờ; chuỗi ISO (kể cả "Z") được chuẩn hoá lại."""
    if value is None:
        value = datetime.now(UTC)
    elif isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="microseconds")


# --- Bộ chuyển đổi kiểu connect() cho script vận hành ---------------------------------------------------------------
# Script trong scripts/ viết SQL đơn giản theo kiểu psycopg (`%s`, `= true`, conn.execute(...).fetchone()). Code chạy thật của crawler
# KHÔNG đi qua đây (nó dùng db/d1_pool.py với SQL SQLite viết riêng) - chỉ script dùng, để khỏi viết lại từng câu.
_BOOL = re.compile(r"(?<![\w'\"])(true|false)(?![\w'\"])", re.IGNORECASE)


def _to_sqlite(sql: str) -> str:
    return _BOOL.sub(lambda m: "1" if m.group(1).lower() == "true" else "0", sql.replace("%s", "?"))


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.rowcount = len(rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)


class _Connection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    @staticmethod
    def execute(sql: str, params: tuple[Any, ...] | list[Any] = ()) -> _Rows:
        return _Rows(query(_to_sqlite(sql), list(params)))


def connect() -> _Connection:
    return _Connection()
