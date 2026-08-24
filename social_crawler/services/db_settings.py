"""Reads operational config (Facebook proxy, accounts, apidirect.io
credentials) from cinemark-api's Postgres `settings` table instead of
spider-hub's own .env - one shared source of truth for config both services
need, instead of the same values drifting apart across two .env files.

Only DATABASE_URL itself has to stay in spider-hub's .env - nothing else
can bootstrap the connection that reads everything else. Uses psycopg
(sync), not the async driver cinemark-api's SQLAlchemy engine uses: this is
a one-shot read at import time (settings.py/accounts.py are still plain
module-level constants, not something worth threading an event loop
through just for this)."""

from __future__ import annotations

import os

import psycopg

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it


def load_settings(keys: list[str]) -> dict[str, str]:
    """One connection, one query, for whichever keys the caller needs at
    import time. Returns {} (never raises) if DATABASE_URL isn't set or the
    DB is unreachable - every caller here falls back to its own .env value
    in that case, so a server without DB access yet still works."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return {}

    dsn = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)

    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute("SELECT key, value FROM settings WHERE key = ANY(%s)", (keys,))
            return dict(cur.fetchall())
    except psycopg.Error as exc:
        print(f"[db_settings] could not load settings from DB, falling back to .env: {exc}")
        return {}
