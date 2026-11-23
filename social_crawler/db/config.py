"""Các bảng cấu hình do dashboard quản lý mà phía này chỉ đọc: filter_keywords,
ai_providers và ai_settings (cinemark-api giữ phía ghi). Tham số proxy có bộ đọc có
cache riêng ở db/proxy_settings.py."""

from __future__ import annotations

import time
from typing import Any

from social_crawler.core.logger import get_logger
from social_crawler.db import d1

logger = get_logger(__name__)

# Cache trong tiến trình: cấu hình AI đổi hiếm (người dùng sửa ở dashboard) nhưng bị đọc cho từng lời gọi AI, mỗi lần kéo cả
# prompts (~11 kB). Trước khi có cache, ~170k lượt đọc/88 ngày thành hàng GB egress.
_CACHE_TTL_SECONDS = 60.0
_cache: dict[str, tuple[float, Any]] = {}


def _cached(key: str) -> Any | None:
    hit = _cache.get(key)
    return hit[1] if hit and time.monotonic() - hit[0] < _CACHE_TTL_SECONDS else None


def _remember(key: str, value: Any) -> Any:
    _cache[key] = (time.monotonic(), value)
    return value


def get_ai_provider(key: str) -> dict[str, Any] | None:
    """Một dòng ai_providers (base_url/api_key/model) - do cinemark-api sở hữu và ghi (tab AI
    trong Settings, app/services/platform_config_db.py), cùng thông tin đăng nhập mà
    app/ai/client.py của nó dùng. None khi dòng hoặc bảng không tồn tại, hoặc không kết nối
    được DB - chỗ gọi coi đó là "provider chưa được cấu hình"."""
    hit = _cached(f"provider:{key}")
    if hit is not None:
        return hit
    try:
        rows = d1.query(
            "SELECT key, base_url, api_key, model, reasoning_effort, updated_at FROM ai_providers WHERE key = ?",
            [key],
        )
        row = rows[0] if rows else None
        return _remember(f"provider:{key}", row) if row is not None else None
    except d1.D1Error as exc:
        logger.warning("ai_provider_load_failed", provider=key, error=exc)
        return None


def get_ai_settings() -> dict[str, Any]:
    """Dòng singleton ai_settings (tab AI trong Settings của dashboard). Thiếu DB hoặc bảng
    không gây lỗi chết - chỗ gọi coi như enabled=False và dùng prompt mặc định trong code.
    Cột model là di sản cũ: model giờ nằm trên dòng ai_providers (get_ai_provider)."""
    hit = _cached("ai_settings")
    if hit is not None:
        return hit
    try:
        # Chỉ đọc đúng cột cần (không SELECT *): schema do cinemark-api tạo/migrate, bên này không chạy DDL mỗi lượt.
        rows = d1.query("SELECT enabled, active_provider, model, prompts FROM ai_settings WHERE id = 1")
        row = {**rows[0], "prompts": d1.json_value(rows[0]["prompts"])} if rows else None
    except Exception as exc:  # noqa: BLE001 - đọc cài đặt AI lỗi thì trả cấu hình tắt, không dừng lượt cào
        logger.warning("ai_settings_load_failed", error=exc)
        return {"enabled": False, "model": "", "prompts": {}}
    if not row:
        return {"enabled": False, "model": "", "prompts": {}}
    prompts = row.get("prompts") if isinstance(row.get("prompts"), dict) else {}
    return _remember(
        "ai_settings",
        {
            "enabled": bool(row.get("enabled")),
            # Nhà cung cấp đang chọn cho mọi lời gọi AI (tab AI của dashboard, 2026-10-22): "beeknoee" (Bee).
            "active_provider": (row.get("active_provider") or "").strip().lower(),
            "model": (row.get("model") or "").strip(),
            "prompts": {k: v for k, v in prompts.items() if isinstance(k, str) and isinstance(v, str) and v.strip()},
        },
    )
