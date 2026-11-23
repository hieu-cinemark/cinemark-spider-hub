"""connect() cho script vận hành: Postgres (mặc định) hoặc D1 khi CONFIG_DB_BACKEND=d1 (xem db/d1.py)."""

from __future__ import annotations

from typing import Any

from social_crawler.db import d1
from social_crawler.db.connection import connect as _postgres_connect


def connect() -> Any:
    return d1.connect() if d1.enabled() else _postgres_connect()
