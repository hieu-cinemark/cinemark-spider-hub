"""Hành vi proxy tinh chỉnh được, sửa từ trang Settings của dashboard (/settings/proxy và
/settings/proxy/providers của cinemark-api) thay vì gán cứng rải rác trong pool.py,
db.py, proxy_provider.py, proxy_health_check.py, crawl_request_consumer.py và các
spider TikTok.

Hai bảng, cùng nằm trong Postgres với platform_proxies:

  proxy_settings(id=1, settings jsonb, updated_at) - một dòng singleton chứa các tham
      số dạng số/chuỗi. Key thiếu trong dòng sẽ quay về DEFAULTS bên dưới, nên một dòng
      rỗng/không có hành xử y như các hằng gán cứng mà module này thay thế.
  proxy_providers(key, api_url, token, ip_allowlist, updated_at) - mỗi gói proxy xoay
      vòng của nhà cung cấp một dòng (xem proxy_provider.get_new_proxy). Nguồn duy nhất
      của token nhà cung cấp - các biến .env PROXIESTRUST_* cũ đã được chuyển về đây
      ngày 2026-09-28 và không còn được đọc.

cinemark-api giữ phía ghi và bản sao riêng của cùng các giá trị mặc định (ProxySettings
trong app/schemas/settings.py) - giữ hai bên đồng bộ khi thêm key. Lần đọc ở đây được
cache CACHE_TTL_SECONDS để một vòng lặp nóng (record_proxy_outcome chạy sau gần như mọi
request) không thêm một lượt gọi DB mỗi lần, trong khi lưu trên dashboard vẫn áp dụng
trong vòng một phút cho các tiến trình sống lâu như crawl_request_consumer.py.

Mọi lỗi khi đọc (thiếu bảng, DB sập, giá trị sai định dạng) đều hạ về DEFAULTS và log
cảnh báo - setting không đọc được tuyệt đối không được làm dừng việc crawl.
"""

from __future__ import annotations

import time
from typing import Any, TypedDict

from social_crawler.logger import get_logger

logger = get_logger(__name__)

CACHE_TTL_SECONDS = 60.0

# Mỗi giá trị ở đây là hằng mà nó thay thế - xem từng module dùng nó để biết lý do của con
# số ban đầu.
DEFAULTS: dict[str, Any] = {
    # pool.acquire_proxy_for_account - số lần lỗi trước khi một proxy đã ghim bị coi là chết
    # và tài khoản được ghim lại sang chỗ khác.
    "repin_after_consecutive_failures": 5,
    # db.record_proxy_outcome - cooldown = base * 2^(failures), có trần.
    "cooldown_base_minutes": 5.0,
    "cooldown_max_minutes": 120.0,
    # proxy_health_check.py
    "health_check_ping_url": "https://www.google.com/generate_204",
    "health_check_timeout_seconds": 10.0,
    "health_check_alert_after_failures": 2,
    "health_check_streak_ttl_hours": 6.0,
    # proxy_provider.get_new_proxy (API thuê proxy xoay vòng của nhà cung cấp)
    "provider_request_timeout_seconds": 10.0,
    "provider_min_get_new_interval_seconds": 60.0,
    "provider_max_cooldown_wait_seconds": 120.0,
    # crawl_request_consumer.py - backoff khi xếp hàng lại lúc mọi proxy đều sập
    "exhausted_backoff_base_seconds": 30.0,
    "exhausted_backoff_growth_factor": 2.0,
    "exhausted_backoff_max_seconds": 300.0,
    "exhausted_max_requeues": 3,
    # Danh tính khách synthetic của TikTok (spiders/tiktok/client.py) - dòng proxy_providers
    # nào tạo lease cho từng client, và một crawl_request được tiêu bao nhiêu lần lấy danh
    # tính+IP mới.
    "tiktok_synthetic_provider": "proxiestrust_tiktok_us",
    "tiktok_hashtag_max_attempts": 8,
    "tiktok_comments_max_attempts": 8,
}


class ProviderConfig(TypedDict):
    key: str
    api_url: str
    token: str | None
    ip_allowlist: bool


_settings_cache: tuple[float, dict[str, Any]] | None = None
_provider_cache: dict[str, tuple[float, ProviderConfig | None]] = {}


def _ensure_tables(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS proxy_settings (
            id integer PRIMARY KEY CHECK (id = 1),
            settings jsonb NOT NULL DEFAULT '{}'::jsonb,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS proxy_providers (
            key text PRIMARY KEY,
            api_url text NOT NULL DEFAULT '',
            token text NOT NULL DEFAULT '',
            ip_allowlist boolean NOT NULL DEFAULT false,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def _coerce(key: str, value: Any) -> Any:
    """Ép giá trị đã lưu về kiểu của giá trị mặc định; raise khi gặp rác để chỗ gọi quay về mặc
    định cho riêng key đó."""
    default = DEFAULTS[key]
    if isinstance(default, bool):
        return bool(value)
    if isinstance(default, int):
        return int(value)
    if isinstance(default, float):
        return float(value)
    text = str(value).strip()
    if not text:
        raise ValueError("blank")
    return text


def _load_settings() -> dict[str, Any]:
    from social_crawler.db.connection import connect

    merged = dict(DEFAULTS)
    try:
        with connect() as conn:
            _ensure_tables(conn)
            row = conn.execute("SELECT settings FROM proxy_settings WHERE id = 1").fetchone()
    except Exception as exc:  # noqa: BLE001 - DB sập, thiếu DATABASE_URL, lỗi quyền: không bao giờ gây lỗi chết
        logger.warning("proxy_settings_load_failed", error=str(exc))
        return merged
    stored = row["settings"] if row and isinstance(row.get("settings"), dict) else {}
    for key, value in stored.items():
        if key not in DEFAULTS:
            continue
        try:
            merged[key] = _coerce(key, value)
        except TypeError, ValueError:
            logger.warning("proxy_setting_invalid", key=key, value=str(value)[:80])
    return merged


def get_proxy_settings() -> dict[str, Any]:
    """Mọi tham số proxy, giá trị DB trộn lên trên DEFAULTS (có cache)."""
    global _settings_cache
    now = time.monotonic()
    if _settings_cache is None or now - _settings_cache[0] > CACHE_TTL_SECONDS:
        _settings_cache = (now, _load_settings())
    return _settings_cache[1]


def get_setting(key: str) -> Any:
    return get_proxy_settings()[key]


def get_provider(key: str) -> ProviderConfig | None:
    """Gói proxy xoay vòng `key` từ proxy_providers, hoặc None nếu không có dòng cho nó. token
    là None khi token của dòng để trống - chỗ gọi coi cả hai là "provider chưa được thiết
    lập". Đọc DB lỗi thì dùng giá trị cache tốt gần nhất (nếu có) thay vì cache luôn lỗi,
    để DB chập chờn không tắt một provider đang chạy suốt cả phút."""
    now = time.monotonic()
    hit = _provider_cache.get(key)
    if hit is not None and now - hit[0] <= CACHE_TTL_SECONDS:
        return hit[1]

    from social_crawler.db.connection import connect

    row: dict[str, Any] | None = None
    try:
        with connect() as conn:
            _ensure_tables(conn)
            row = conn.execute(
                "SELECT key, api_url, token, ip_allowlist FROM proxy_providers WHERE key = %s", (key,)
            ).fetchone()
    except Exception as exc:  # noqa: BLE001 - cùng quy tắc không-bao-giờ-chết như _load_settings
        logger.warning("proxy_provider_load_failed", key=key, error=exc)
        return hit[1] if hit is not None else None

    config: ProviderConfig | None = (
        {
            "key": key,
            "api_url": row.get("api_url") or "",
            "token": row.get("token") or None,
            "ip_allowlist": bool(row["ip_allowlist"]),
        }
        if row is not None
        else None
    )
    _provider_cache[key] = (now, config)
    return config
