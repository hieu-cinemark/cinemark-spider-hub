"""Các bảng cấu hình do dashboard quản lý mà phía này chỉ đọc: filter_keywords,
ai_providers và ai_settings (cinemark-api giữ phía ghi). Tham số proxy có bộ đọc có
cache riêng ở db/proxy_settings.py."""

from __future__ import annotations

from typing import Any

import psycopg

from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)


def get_ai_provider(key: str) -> dict[str, Any] | None:
    """Một dòng ai_providers (base_url/api_key/model) - do cinemark-api sở hữu và ghi (tab AI
    trong Settings, app/services/platform_config_db.py), cùng thông tin đăng nhập mà
    app/ai/client.py của nó dùng. None khi dòng hoặc bảng không tồn tại, hoặc không kết nối
    được DB - chỗ gọi coi đó là "provider chưa được cấu hình"."""
    try:
        with connect() as conn:
            return conn.execute("SELECT base_url, api_key, model FROM ai_providers WHERE key = %s", (key,)).fetchone()
    except psycopg.Error as exc:
        logger.warning("ai_provider_load_failed", provider=key, error=exc)
        return None


def get_ai_settings() -> dict[str, Any]:
    """Dòng singleton ai_settings (tab AI trong Settings của dashboard). Thiếu DB hoặc bảng
    không gây lỗi chết - chỗ gọi coi như enabled=False và dùng prompt mặc định trong code.
    Cột model là di sản cũ: model giờ nằm trên dòng ai_providers (get_ai_provider)."""
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_settings (
                    id integer PRIMARY KEY CHECK (id = 1),
                    enabled boolean NOT NULL DEFAULT false,
                    model text NOT NULL DEFAULT '',
                    prompts jsonb NOT NULL DEFAULT '{}'::jsonb,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO ai_settings (id, enabled, model, prompts)
                VALUES (1, false, '', '{}'::jsonb)
                ON CONFLICT (id) DO NOTHING
                """
            )
            cur.execute("SELECT enabled, model, prompts FROM ai_settings WHERE id = 1")
            row = cur.fetchone()
            conn.commit()
    except Exception as exc:
        logger.warning("ai_settings_load_failed", error=str(exc))
        return {"enabled": False, "model": "", "prompts": {}}
    if not row:
        return {"enabled": False, "model": "", "prompts": {}}
    prompts = row.get("prompts") if isinstance(row.get("prompts"), dict) else {}
    return {
        "enabled": bool(row.get("enabled")),
        "model": (row.get("model") or "").strip(),
        "prompts": {k: v for k, v in prompts.items() if isinstance(k, str) and isinstance(v, str) and v.strip()},
    }
