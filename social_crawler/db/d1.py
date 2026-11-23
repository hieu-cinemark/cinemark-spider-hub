"""Đọc D1 cinemark-config-db qua HTTP query API của Cloudflare (đồng bộ). Chỉ dùng cho các bảng cấu hình đã chuyển khỏi Supabase
(ai_settings, ai_providers, proxy_settings) khi CONFIG_DB_BACKEND=d1; cinemark-api là bên ghi (app/services/accounts/config_db/d1_store.py).

Cần CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, CLOUDFLARE_D1_CONFIG_DATABASE_ID trong .env. Lỗi mạng/HTTP/SQL ném D1Error để chỗ gọi
áp dụng quy tắc "đọc cấu hình lỗi thì giữ bản tốt gần nhất" như với Postgres - module này không tự log."""

from __future__ import annotations

import json
import os
from typing import Any

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
