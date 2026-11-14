"""Hành vi proxy tinh chỉnh được, sửa từ trang Settings của dashboard (/settings/proxy và
/settings/proxy/providers của cinemark-api) thay vì gán cứng rải rác trong pool.py,
db.py, proxy_provider.py, proxy_health_check.py, crawl_request_consumer.py và các
spider TikTok.

Hai bảng, cùng nằm trong Postgres với platform_proxies:

  proxy_settings(id=1, settings jsonb, updated_at) - một dòng singleton chứa các tham
      số dạng số/chuỗi. Key thiếu trong dòng sẽ quay về DEFAULTS bên dưới, nên một dòng
      rỗng/không có hành xử y như các hằng gán cứng mà module này thay thế.
  proxy_providers(key, api_url, token, updated_at) - mỗi nguồn proxy một dòng:
      `api_url` = link đổi IP (có thể chứa placeholder {token}/{proxy}/{username}/{password}),
      `token` = key của nhà cung cấp. Mọi nguồn dùng CHUNG một cơ chế - xem
      clients/proxy_provider.py. Nguồn duy nhất
    của token nhà cung cấp - các biến .env của nhà cung cấp cũ đã được chuyển về đây
      ngày 2026-09-28 và không còn được đọc.

platform_proxies có thêm cột `provider_key` (NULL = proxy nhập tay/tĩnh) trỏ về
proxy_providers.key - trước đó code phải đoán nguồn của một dòng proxy bằng cách so
proxy_url với host:port trong token của provider, nên không có cách nào biết chắc.

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

from social_crawler.core.logger import get_logger

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
    # clients/proxy_provider.py - các adapter của nhà cung cấp (xoay IP)
    "provider_request_timeout_seconds": 10.0,
    # Giãn cách tối thiểu giữa hai lần gọi cùng một gói - nhà cung cấp từ chối lời gọi
    # quá dồn dập (quan sát 2026-09-17: khoảng 90s; người vận hành xác nhận tối thiểu 60s).
    "provider_min_call_interval_seconds": 60.0,
    "provider_max_cooldown_wait_seconds": 120.0,
    # crawl_request_consumer.py - backoff khi xếp hàng lại lúc mọi proxy đều sập
    "exhausted_backoff_base_seconds": 30.0,
    "exhausted_backoff_growth_factor": 2.0,
    "exhausted_backoff_max_seconds": 300.0,
    "exhausted_max_requeues": 3,
    # Danh tính khách synthetic của TikTok (spiders/tiktok/client.py) - dòng proxy_providers
    # nào cấp IP mới cho từng client, và một crawl_request được tiêu bao nhiêu lần lấy danh
    # tính+IP mới. Tên gói trung tính (không mang tên nhà cung cấp) - scripts/migrate_proxy_providers.py
    # đổi tên các gói cũ sang dạng này.
    "tiktok_synthetic_provider": "tiktok_us",
    "tiktok_hashtag_max_attempts": 8,
    "tiktok_comments_max_attempts": 8,
    # consumer/jobs/post_metrics.py - cập nhật số bài booking Facebook bằng trang không đăng nhập qua proxy không chở
    # tài khoản Meta. Tắt (nút trên dashboard) thì job đẩy hết bài sang page_feed có đăng nhập như trước 25/10/2026.
    "fb_logged_out_metrics_enabled": True,
    # Hiệu năng cào (13/11/2026) - sửa ở Settings › Proxy của dashboard thay vì .env. Số làn/worker áp dụng trong vòng một
    # phút, không cần restart (mỗi làn tự kiểm tra mình có nằm trong số làn đang bật không).
    # consumer/jobs/youtube.py
    "youtube_lanes": 16,
    "youtube_filter_concurrency": 6,
    "youtube_search_pages": 30,
    "youtube_reply_pages": 50,
    # consumer/jobs/public_page.py - đọc page Facebook không tài khoản trên mọi proxy nền tảng "public"
    "public_pages_per_proxy": 3,
    "public_pages_gap_seconds": 3.0,
    # consumer/jobs/threads_channel.py - kênh Threads không tài khoản, cũng qua proxy "public"
    "threads_channel_workers": 4,
    # consumer/jobs/page_feed.py + scheduler của cinemark-api - đọc page Facebook bằng tài khoản (tắt từ 13/11/2026)
    "facebook_account_page_feed": False,
    # Phía cinemark-api (scheduler): số job giữ sẵn trong hàng đợi / số video được xếp cào bình luận mỗi giờ
    "public_pages_queue_target": 20,
    "threads_channels_queue_target": 8,
    "youtube_comments_per_round": 1000,
}


# Key đã đổi tên (tên cũ -> tên mới). Giá trị đã lưu dưới tên cũ vẫn được dùng cho tới khi dashboard lưu lại dưới tên mới.
RENAMED_KEYS = {"provider_min_get_new_interval_seconds": "provider_min_call_interval_seconds"}


def _with_renamed_keys(stored: dict[str, Any]) -> dict[str, Any]:
    out = dict(stored)
    for old, new in RENAMED_KEYS.items():
        if old in out and new not in out:
            out[new] = out[old]
    return out


class ProviderConfig(TypedDict):
    key: str
    # Link đổi IP của nguồn. Có thể chứa placeholder {token} / {proxy} / {username} /
    # {password} - xem clients/proxy_provider.py.
    api_url: str
    # Key của nhà cung cấp (bí mật - không bao giờ log nguyên văn).
    token: str | None


_settings_cache: tuple[float, dict[str, Any]] | None = None
_provider_cache: dict[str, tuple[float, ProviderConfig | None]] = {}


_tables_ensured = False


def _ensure_tables(conn: Any) -> None:
    """CREATE/ALTER IF NOT EXISTS - chỉ một lần mỗi tiến trình (27/10/2026: trước đó mỗi lần đọc cache hết hạn là 4 câu
    DDL qua đường VPS -> Supabase Sydney, mỗi câu một vòng mạng)."""
    global _tables_ensured
    if _tables_ensured:
        return
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
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    # Lần gần nhất code thực sự gọi gói này (xoay/reset/dùng cổng) và kết quả - để dashboard hiện "chạy lúc nào,
    # lỗi gì" thay vì updated_at (chỉ là lần sửa cấu hình). Bảng cũ không có hai cột này.
    conn.execute("ALTER TABLE proxy_providers ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
    conn.execute("ALTER TABLE proxy_providers ADD COLUMN IF NOT EXISTS last_status text")
    # Cột `ip_allowlist` của chế độ "cấp quyền theo IP nguồn" đã bị bỏ cùng API lease của
    # nhà cung cấp cũ - không còn code nào đọc nó. Cột vẫn còn trong DB cho tới khi
    # scripts/migrate_proxy_providers.py được chạy với --drop-ip-allowlist (phía ghi
    # cinemark-api cũng phải bỏ cột này trước).
    # `provider_key` = nguồn nào sở hữu dòng proxy này (NULL = nhập tay/tĩnh). Trước đó code đoán
    # bằng cách so proxy_url với host:port trong token của từng gói.
    conn.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_key text")
    # `provider_ref` = mã riêng của cổng này trong hệ thống nhà cung cấp (vd proxy_id của
    # webtui, key của cổng ở zingproxy) - điền vào placeholder {ref} của link reset.
    conn.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_ref text")
    _tables_ensured = True


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


def _load_settings() -> dict[str, Any] | None:
    """Giá trị DB trộn lên DEFAULTS; None khi không đọc được DB (chỗ gọi giữ giá trị tốt gần nhất)."""
    from social_crawler.db.connection import connect

    merged = dict(DEFAULTS)
    try:
        with connect() as conn:
            _ensure_tables(conn)
            row = conn.execute("SELECT settings FROM proxy_settings WHERE id = 1").fetchone()
    except Exception as exc:  # noqa: BLE001 - DB sập, thiếu DATABASE_URL, lỗi quyền: không bao giờ gây lỗi chết
        logger.warning("proxy_settings_load_failed", error=str(exc))
        return None
    stored = row["settings"] if row and isinstance(row.get("settings"), dict) else {}
    stored = _with_renamed_keys(stored)
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
        loaded = _load_settings()
        # DB lỗi: giữ giá trị tốt gần nhất (lần đầu thì DEFAULTS) và chờ hết CACHE_TTL_SECONDS mới thử lại - không thử DB
        # ở mọi lời gọi (mỗi lần thử hỏng chặn tới 3 x connect_timeout giây và ghi một dòng cảnh báo).
        if loaded is None:
            loaded = _settings_cache[1] if _settings_cache is not None else dict(DEFAULTS)
        _settings_cache = (now, loaded)
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
    from social_crawler.db.connection import connect

    try:
        with connect() as conn:
            _ensure_tables(conn)
            conn.execute(
                "UPDATE proxy_providers SET last_used_at = now(), last_status = %s WHERE key = %s", (status[:300], key)
            )
    except Exception as exc:  # noqa: BLE001 - cùng quy tắc không-bao-giờ-chết như _load_settings
        logger.warning("proxy_provider_record_use_failed", key=key, error=str(exc))
