"""Dashboard-managed config tables this side only reads: filter_keywords,
ai_providers and ai_settings (cinemark-api owns the write side). Proxy
tunables have their own cached reader in db/proxy_settings.py."""

from __future__ import annotations

from typing import Any

import psycopg

from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)


def get_filter_keywords(category: str | None = None) -> list[dict[str, Any]]:
    """Enabled filter_keywords rows - 'movie_relevant'/'spam_offtopic'
    keywords maintained from the dashboard (see module docstring). Read-only
    from this side; not yet called by any extraction pipeline - a future
    content filter (deciding whether a scraped post/comment is worth
    keeping) would call this rather than querying filter_keywords directly,
    same as every other table in this module."""
    try:
        with connect() as conn:
            if category:
                rows = conn.execute(
                    "SELECT keyword, category FROM filter_keywords WHERE enabled = true AND category = %s",
                    (category,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT keyword, category FROM filter_keywords WHERE enabled = true").fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_filter_keywords_failed", category=category, error=str(exc))
        return []
    return rows


def get_ai_provider(key: str) -> dict[str, Any] | None:
    """One ai_providers row (base_url/api_key/model) - owned and written by
    cinemark-api (Settings AI tab, app/services/platform_config_db.py), the
    same credentials its own app/ai/client.py uses. None when the row or
    table doesn't exist, or the DB can't be reached - callers treat that as
    "provider not configured"."""
    try:
        with connect() as conn:
            return conn.execute("SELECT base_url, api_key, model FROM ai_providers WHERE key = %s", (key,)).fetchone()
    except psycopg.Error as exc:
        logger.warning("ai_provider_load_failed", provider=key, error=exc)
        return None


def get_ai_settings() -> dict[str, Any]:
    """Singleton ai_settings row (dashboard Settings AI tab). Missing DB
    or table is not fatal - callers treat enabled=False and use code
    default prompts. The model column is legacy: the model now lives on the
    ai_providers row (get_ai_provider)."""
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
