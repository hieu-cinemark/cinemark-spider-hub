"""Connection Postgres (Supabase) cho mọi module trong package này - xem db/__init__.py để
biết vì sao mỗi lời gọi một connection mới thay vì dùng pool."""

from __future__ import annotations

import os
from typing import Any

import psycopg
from psycopg.rows import dict_row

import social_crawler.env  # noqa: F401 # nạp .env đúng một lần, dù bao nhiêu module import nó


def _database_url() -> str:
    """DATABASE_URL (Supabase production) trừ khi APP_ENV=development, khi đó dùng
    LOCAL_DATABASE_URL (một Postgres local, xem scripts/dev_db_schema.sql) thay thế - xem
    .env.example. Mặc định là "production" khi chưa đặt để một .env đã deploy sẵn (service
    systemd, cron) không có dòng APP_ENV nào vẫn gọi Supabase y như trước giờ; chỉ máy dev
    chủ động bật APP_ENV=development mới nói chuyện với DB local."""
    if os.environ.get("APP_ENV", "production") == "development":
        try:
            return os.environ["LOCAL_DATABASE_URL"]
        except KeyError:
            raise RuntimeError("APP_ENV=development but LOCAL_DATABASE_URL is not set (see .env.example)") from None
    return os.environ["DATABASE_URL"]


def connect() -> psycopg.Connection[Any]:
    return psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5)
