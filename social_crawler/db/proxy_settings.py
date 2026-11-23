"""Cấu hình cào đọc từ Postgres - CHỈ ĐỌC. Nguồn sự thật là cinemark-api: nó khai báo các khoá, giá trị mặc định và giới hạn
(`ProxySettings` trong app/schemas/settings.py), tạo bảng, và ghi đủ mọi khoá vào dòng `proxy_settings` khi khởi động
(`ensure_proxy_settings_complete`); dashboard (Settings › Proxy) sửa qua cinemark-api. Thêm khoá mới: chỉ thêm ở cinemark-api,
deploy cinemark-api trước, rồi mới dùng `get_setting("khoá")` ở đây.

  proxy_settings(id=1, settings jsonb) - một dòng chứa mọi tham số dạng số/chuỗi/bool.
  proxy_providers(key, api_url, token, last_used_at, last_status) - mỗi nguồn proxy một dòng (link đổi IP + key nhà cung cấp,
      xem clients/proxy_provider.py). spider-hub chỉ ghi `last_used_at`/`last_status` khi dùng gói.

Đọc được cache CACHE_TTL_SECONDS (sửa trên dashboard có hiệu lực trong vòng một phút, không cần restart). DB lỗi thì giữ bản
tốt gần nhất; tiến trình vừa khởi động mà chưa đọc được lần nào thì thử lại tới FIRST_LOAD_TIMEOUT_SECONDS rồi báo lỗi
`SettingsUnavailable` - không còn giá trị mặc định riêng ở đây để hai repo không lệch nhau.
"""

from __future__ import annotations

import time
from typing import Any, TypedDict

from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

CACHE_TTL_SECONDS = 60.0
FIRST_LOAD_TIMEOUT_SECONDS = 120.0
FIRST_LOAD_RETRY_SECONDS = 10.0


class SettingsUnavailable(RuntimeError):
    """Chưa đọc được proxy_settings lần nào (DB lỗi lúc khởi động) hoặc dòng thiếu khoá đang cần."""


class ProviderConfig(TypedDict):
    key: str
    # Link đổi IP của nguồn. Có thể chứa placeholder {token} / {proxy} / {username} /
    # {password} - xem clients/proxy_provider.py.
    api_url: str
    # Key của nhà cung cấp (bí mật - không bao giờ log nguyên văn).
    token: str | None


_settings_cache: tuple[float, dict[str, Any]] | None = None
_provider_cache: dict[str, tuple[float, ProviderConfig | None]] = {}


def _load_settings() -> dict[str, Any] | None:
    """Dòng proxy_settings đúng như cinemark-api đã ghi; None khi không đọc được DB."""
    from social_crawler.db import d1
    from social_crawler.db.connection import connect

    try:
        if d1.enabled():
            rows = d1.query("SELECT settings FROM proxy_settings WHERE id = 1")
            row = {"settings": d1.json_value(rows[0]["settings"])} if rows else None
        else:
            with connect() as conn:
                row = conn.execute("SELECT settings FROM proxy_settings WHERE id = 1").fetchone()
    except Exception as exc:  # noqa: BLE001 - DB sập, thiếu DATABASE_URL, lỗi quyền: chỗ gọi giữ bản tốt gần nhất
        logger.warning("proxy_settings_load_failed", error=str(exc))
        return None
    stored = row["settings"] if row and isinstance(row.get("settings"), dict) else None
    if not stored:
        logger.warning("proxy_settings_empty", hint="cinemark-api chưa ghi dòng proxy_settings - restart cinemark-api")
        return None
    return dict(stored)


def get_proxy_settings() -> dict[str, Any]:
    """Mọi tham số cào (có cache). DB lỗi: dùng bản tốt gần nhất; chưa có bản nào thì thử lại tới FIRST_LOAD_TIMEOUT_SECONDS."""
    global _settings_cache
    now = time.monotonic()
    if _settings_cache is not None and now - _settings_cache[0] <= CACHE_TTL_SECONDS:
        return _settings_cache[1]
    loaded = _load_settings()
    if loaded is None and _settings_cache is not None:
        # Không thử DB ở mọi lời gọi: giữ bản cũ thêm một chu kỳ cache.
        loaded = _settings_cache[1]
    deadline = now + FIRST_LOAD_TIMEOUT_SECONDS
    while loaded is None:
        if time.monotonic() >= deadline:
            raise SettingsUnavailable(
                "không đọc được proxy_settings từ Postgres (cinemark-api ghi dòng này khi khởi động)"
            )
        time.sleep(FIRST_LOAD_RETRY_SECONDS)
        loaded = _load_settings()
    _settings_cache = (time.monotonic(), loaded)
    return loaded


def get_setting(key: str) -> Any:
    """Một khoá của proxy_settings. Khoá thiếu nghĩa là cinemark-api đang chạy bản cũ hơn spider-hub: deploy cinemark-api."""
    settings = get_proxy_settings()
    if key not in settings:
        raise SettingsUnavailable(f"proxy_settings thiếu khoá {key!r} - deploy/restart cinemark-api để nó ghi khoá mới")
    return settings[key]


def get_provider(key: str) -> ProviderConfig | None:
    """Gói proxy xoay vòng `key` từ proxy_providers, hoặc None nếu không có dòng cho nó. token
    là None khi token của dòng để trống - chỗ gọi coi cả hai là "provider chưa được thiết
    lập". Đọc DB lỗi thì dùng giá trị cache tốt gần nhất (nếu có) thay vì cache luôn lỗi,
    để DB chập chờn không tắt một provider đang chạy suốt cả phút."""
    now = time.monotonic()
    hit = _provider_cache.get(key)
    if hit is not None and now - hit[0] <= CACHE_TTL_SECONDS:
        return hit[1]

    from social_crawler.db import d1, d1_pool
    from social_crawler.db.connection import connect

    row: dict[str, Any] | None = None
    try:
        if d1.enabled():
            row = d1_pool.get_provider_row(key)
        else:
            with connect() as conn:
                row = conn.execute("SELECT key, api_url, token FROM proxy_providers WHERE key = %s", (key,)).fetchone()
    except Exception as exc:  # noqa: BLE001 - cùng quy tắc không-bao-giờ-chết như _load_settings
        logger.warning("proxy_provider_load_failed", key=key, error=exc)
        # Nhớ kết quả (giá trị tốt gần nhất, hoặc None) thêm CACHE_TTL_SECONDS: không thử lại DB ở mọi lời gọi - 27/10/2026
        # VPS -> Supabase timeout, mỗi lần thử chặn 15 giây và log dồn liên tục.
        stale = hit[1] if hit is not None else None
        _provider_cache[key] = (now, stale)
        return stale

    config: ProviderConfig | None = (
        {
            "key": key,
            "api_url": row.get("api_url") or "",
            "token": row.get("token") or None,
        }
        if row is not None
        else None
    )
    _provider_cache[key] = (now, config)
    return config


def record_provider_use(key: str, status: str) -> None:
    """Ghi last_used_at/last_status của gói `key` (status: "ok" hoặc "error: <lý do ngắn>", không bao giờ chứa
    token). Best-effort - lỗi DB chỉ log, không làm hỏng lượt crawl đang lấy proxy."""
    from social_crawler.db import d1, d1_pool
    from social_crawler.db.connection import connect

    try:
        if d1.enabled():
            d1_pool.record_provider_use(key, status)
            return
        with connect() as conn:
            conn.execute(
                "UPDATE proxy_providers SET last_used_at = now(), last_status = %s WHERE key = %s", (status[:300], key)
            )
    except Exception as exc:  # noqa: BLE001 - cùng quy tắc không-bao-giờ-chết như _load_settings
        logger.warning("proxy_provider_record_use_failed", key=key, error=str(exc))
