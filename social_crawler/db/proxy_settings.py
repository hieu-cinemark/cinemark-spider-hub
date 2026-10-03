"""Tunable proxy behavior, editable from the dashboard's Settings page
(cinemark-api's /settings/proxy and /settings/proxy/providers) instead of
being hardcoded across pool.py, db.py, proxy_provider.py,
proxy_health_check.py, crawl_request_consumer.py and the TikTok spiders.

Two tables, both in the same Postgres as platform_proxies:

  proxy_settings(id=1, settings jsonb, updated_at) - one singleton row of
      numeric/text knobs. Keys missing from the row fall back to DEFAULTS
      below, so an empty/absent row behaves exactly like the hardcoded
      constants this module replaced.
  proxy_providers(key, api_url, token, ip_allowlist, updated_at) - one row
      per rotating-proxy vendor plan (see proxy_provider.get_new_proxy).
      The only source of vendor tokens - the old PROXIESTRUST_* .env vars
      were moved here on 2026-09-28 and are no longer read.

cinemark-api owns the write side and its own copy of the same defaults
(app/schemas/settings.py's ProxySettings) - keep the two in sync when a
key is added. Reads here are cached for CACHE_TTL_SECONDS so a hot loop
(record_proxy_outcome runs after nearly every request) doesn't add a DB
round trip per call, while a dashboard save still applies within a minute
to long-lived processes like crawl_request_consumer.py.

Any failure to read (table missing, DB down, malformed value) degrades to
DEFAULTS and logs a warning - a settings outage must never stop crawling.
"""

from __future__ import annotations

import time
from typing import Any, TypedDict

from social_crawler.logger import get_logger

logger = get_logger(__name__)

CACHE_TTL_SECONDS = 60.0

# Every value here is the constant it replaced - see each consumer module
# for the reasoning behind the original number.
DEFAULTS: dict[str, Any] = {
    # pool.acquire_proxy_for_account - failures before a pinned proxy is
    # treated as dead and the account is re-pinned elsewhere.
    "repin_after_consecutive_failures": 5,
    # db.record_proxy_outcome - cooldown = base * 2^(failures), capped.
    "cooldown_base_minutes": 5.0,
    "cooldown_max_minutes": 120.0,
    # proxy_health_check.py
    "health_check_ping_url": "https://www.google.com/generate_204",
    "health_check_timeout_seconds": 10.0,
    "health_check_alert_after_failures": 2,
    "health_check_streak_ttl_hours": 6.0,
    # proxy_provider.get_new_proxy (rotating-lease vendor API)
    "provider_request_timeout_seconds": 10.0,
    "provider_min_get_new_interval_seconds": 60.0,
    "provider_max_cooldown_wait_seconds": 120.0,
    # crawl_request_consumer.py - requeue backoff when every proxy is down
    "exhausted_backoff_base_seconds": 30.0,
    "exhausted_backoff_growth_factor": 2.0,
    "exhausted_backoff_max_seconds": 300.0,
    "exhausted_max_requeues": 3,
    # TikTok synthetic guest identities (spiders/tiktok/client.py) - which
    # proxy_providers row mints their per-client lease, and how many fresh
    # identity+IP draws one crawl_request may spend.
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
    """Cast a stored value to its default's type; raises on garbage so the
    caller can fall back to the default for just that key."""
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
    except Exception as exc:  # noqa: BLE001 - DB down, missing DATABASE_URL, permissions: never fatal
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
    """All proxy knobs, DB values merged over DEFAULTS (cached)."""
    global _settings_cache
    now = time.monotonic()
    if _settings_cache is None or now - _settings_cache[0] > CACHE_TTL_SECONDS:
        _settings_cache = (now, _load_settings())
    return _settings_cache[1]


def get_setting(key: str) -> Any:
    return get_proxy_settings()[key]


def get_provider(key: str) -> ProviderConfig | None:
    """The rotating-proxy vendor plan `key` from proxy_providers, or None
    if there's no row for it. token is None when the row's token is blank -
    callers treat both as "provider not set up". A failed DB read serves
    the last good cached value (if any) instead of caching the failure, so
    a DB blip doesn't switch a working provider off for a whole minute."""
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
    except Exception as exc:  # noqa: BLE001 - same never-fatal rule as _load_settings
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
