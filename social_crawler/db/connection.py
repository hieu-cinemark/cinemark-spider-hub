"""Postgres (Supabase) connection for every module in this package - see
db/__init__.py for why it's a fresh connection per call rather than a pool."""

from __future__ import annotations

import os
from typing import Any

import psycopg
from psycopg.rows import dict_row

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it


def _database_url() -> str:
    """DATABASE_URL (prod Supabase) unless APP_ENV=development, in which
    case LOCAL_DATABASE_URL (a local Postgres, see scripts/dev_db_schema.sql)
    is used instead - see .env.example. Defaults to "production" when unset
    so an existing deployed .env (systemd service, cron) with no APP_ENV
    line at all keeps hitting Supabase exactly as it always has; only a
    dev machine that explicitly opts in with APP_ENV=development ever talks
    to a local DB."""
    if os.environ.get("APP_ENV", "production") == "development":
        try:
            return os.environ["LOCAL_DATABASE_URL"]
        except KeyError:
            raise RuntimeError("APP_ENV=development but LOCAL_DATABASE_URL is not set (see .env.example)") from None
    return os.environ["DATABASE_URL"]


def connect() -> psycopg.Connection[Any]:
    return psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5)
