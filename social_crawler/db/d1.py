"""Truy cập D1 cinemark-config-db qua HTTP query API của Cloudflare (đồng bộ): tài khoản, proxy, cấu hình AI/cào. cinemark-api là bên
tạo bảng và ghi cấu hình (app/services/accounts/config_db/).

Cần CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, CLOUDFLARE_D1_CONFIG_DATABASE_ID trong .env. Lỗi mạng/HTTP/SQL ném D1Error để chỗ gọi
áp dụng quy tắc "đọc cấu hình lỗi thì giữ bản tốt gần nhất" - module này không tự log."""

from __future__ import annotations

import json
import os
import random
import re
import threading
import time
from datetime import UTC, datetime
from typing import Any, Self

import httpx

import social_crawler.core.env  # nạp .env đúng một lần  # noqa: F401

_BASE_URL = "https://api.cloudflare.com/client/v4"
_TIMEOUT = httpx.Timeout(15.0, connect=8.0)
_client: httpx.Client | None = None

# Độ bền khi D1 chập chờn. Câu đọc được thử lại (ít tốn kém, kết quả không đổi); câu ghi chỉ thử lại khi chắc chắn CHƯA được thực thi
# (không kết nối được, hoặc bị từ chối 429) - thử lại sau một timeout đọc có thể nhận tài khoản/proxy hai lần.
_READ_PREFIXES = ("SELECT", "PRAGMA", "WITH")
_RETRY_DELAYS = (0.4, 1.2)  # giây, cộng nhiễu; tối đa 1 + len() lần gọi
# Ngắt mạch: _BREAKER_THRESHOLD lần lỗi hạ tầng liên tiếp thì _BREAKER_OPEN_SECONDS giây tới báo lỗi ngay, không chờ timeout 15 giây
# cho từng lời gọi (mỗi lượt claim_account/claim_proxy hỏi D1 vài lần nên một đợt D1 chết có thể treo cả consumer hàng phút).
_BREAKER_THRESHOLD = 5
_BREAKER_OPEN_SECONDS = 20.0
_breaker_lock = threading.Lock()
_consecutive_failures = 0
_open_until = 0.0
_sleep = time.sleep


class D1Error(RuntimeError):
    """D1 không trả lời được hoặc từ chối câu SQL."""


def _http() -> httpx.Client:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.Client(timeout=_TIMEOUT, limits=httpx.Limits(max_connections=4, max_keepalive_connections=2))
    return _client


def _breaker_guard() -> None:
    """Báo lỗi ngay khi mạch đang mở (D1 vừa lỗi liên tiếp)."""
    if time.monotonic() < _open_until:
        raise D1Error("D1 is temporarily unavailable (circuit open after repeated failures)")


def _note_transport(ok: bool) -> None:
    """Ghi nhận một lần gọi thành công/thất bại ở tầng hạ tầng (mạng, 5xx, 429); lỗi SQL hay 4xx khác không tính."""
    global _consecutive_failures, _open_until
    with _breaker_lock:
        if ok:
            _consecutive_failures = 0
            return
        _consecutive_failures += 1
        if _consecutive_failures >= _BREAKER_THRESHOLD:
            _open_until = time.monotonic() + _BREAKER_OPEN_SECONDS
            _consecutive_failures = 0


def reset_breaker() -> None:
    """Đóng mạch và xoá bộ đếm (dùng trong test hoặc sau khi sửa cấu hình)."""
    global _consecutive_failures, _open_until
    with _breaker_lock:
        _consecutive_failures = 0
        _open_until = 0.0


def query(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """Chạy một câu SQL trên cinemark-config-db, trả các dòng (list rỗng khi không có).

    Câu đọc được thử lại khi mạng/5xx/429; câu ghi chỉ thử lại khi chưa chắc chắn được thực thi (xem _RETRY_DELAYS). Lỗi hạ tầng liên
    tiếp làm mở ngắt mạch."""
    try:
        account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        token = os.environ["CLOUDFLARE_API_TOKEN"]
        database = os.environ["CLOUDFLARE_D1_CONFIG_DATABASE_ID"]
    except KeyError as exc:
        raise D1Error(f"Missing environment variable {exc.args[0]}") from None
    is_read = sql.lstrip().upper().startswith(_READ_PREFIXES)
    last_attempt = len(_RETRY_DELAYS)
    for attempt in range(last_attempt + 1):
        _breaker_guard()
        try:
            resp = _http().post(
                f"{_BASE_URL}/accounts/{account}/d1/database/{database}/query",
                headers={"Authorization": f"Bearer {token}"},
                json={"sql": sql, "params": params or []},
            )
        except httpx.HTTPError as exc:
            _note_transport(False)
            never_sent = isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout))
            if (is_read or never_sent) and attempt < last_attempt:
                _sleep(_RETRY_DELAYS[attempt] + random.uniform(0, 0.2))
                continue
            raise D1Error(f"{type(exc).__name__}: {exc}") from exc
        if resp.status_code == 429 or resp.status_code >= 500:
            _note_transport(False)
            # 429 là bị từ chối nên chưa thực thi; 5xx với câu ghi có thể đã thực thi nên không thử lại.
            if (is_read or resp.status_code == 429) and attempt < last_attempt:
                _sleep(_RETRY_DELAYS[attempt] + random.uniform(0, 0.2))
                continue
            raise D1Error(f"HTTP {resp.status_code}: {resp.text[:200]}")
        _note_transport(True)
        break
    if resp.status_code >= 400:
        raise D1Error(f"HTTP {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    if not data.get("success"):
        raise D1Error(f"D1 rejected the SQL statement: {str(data.get('errors'))[:200]}")
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
# Script trong scripts/ viết SQL đơn giản theo kiểu Postgres (`%s`, `= true`, conn.execute(...).fetchone()). Code chạy thật của crawler
# KHÔNG đi qua đây (nó dùng db/d1_pool.py với SQL SQLite viết riêng) - chỉ script dùng, để khỏi viết lại từng câu.
_BOOL = re.compile(r"(?<![\w'\"])(true|false)(?![\w'\"])", re.IGNORECASE)


_ANY = re.compile(r"=\s*ANY\(\s*%s\s*\)", re.IGNORECASE)


def _to_sqlite(sql: str) -> str:
    # `col = ANY(%s)` (tham số là danh sách) -> IN (SELECT value FROM json_each(?)); danh sách được mã hoá JSON ở _Connection.execute.
    sql = _ANY.sub("IN (SELECT value FROM json_each(%s))", sql)
    return _BOOL.sub(lambda m: "1" if m.group(1).lower() == "true" else "0", sql.replace("%s", "?"))


def _param(value: Any) -> Any:
    return json.dumps(list(value)) if isinstance(value, (list, tuple, set)) else value


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
        return _Rows(query(_to_sqlite(sql), [_param(value) for value in params]))


def connect() -> _Connection:
    return _Connection()
