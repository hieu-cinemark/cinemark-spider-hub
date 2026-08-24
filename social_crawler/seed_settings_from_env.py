#!/usr/bin/env python3
"""One-time migration: pushes the Facebook proxy/account and apidirect.io
config currently in .env into cinemark-api's `settings` table in Postgres,
so social_crawler.settings/accounts read them from there going forward (see
social_crawler/services/db_settings.py) instead of two separately
maintained .env files drifting apart.

Safe to re-run - upserts (ON CONFLICT DO UPDATE), so it always ends with the
DB holding whatever this .env currently has.

Run:
    python -m social_crawler.seed_settings_from_env
"""

from __future__ import annotations

import os

import psycopg

import social_crawler.env  # noqa: F401  # loads .env exactly once

KEYS_FROM_ENV = {
    "facebook_proxy_url": "FACEBOOK_PROXY_URL",
    "facebook_proxy_username": "FACEBOOK_PROXY_USERNAME",
    "facebook_proxy_password": "FACEBOOK_PROXY_PASSWORD",
    "facebook_login_use_proxy": "FACEBOOK_LOGIN_USE_PROXY",
    "facebook_accounts": "FACEBOOK_ACCOUNTS",
    "api_direct_url": "API_DIRECT_URL",
    "api_direct_token": "API_DIRECT_TOKEN",
}


def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not set - add it to .env first (same value cinemark-api uses).")

    dsn = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)

    rows = []
    for db_key, env_key in KEYS_FROM_ENV.items():
        value = os.environ.get(env_key)
        if value is None:
            print(f"skip {env_key} (not set in .env)")
            continue
        rows.append((db_key, value))

    if not rows:
        print("Nothing to seed - none of the expected env vars are set.")
        return

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for key, value in rows:
            cur.execute(
                """
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )
        conn.commit()

    print(f"Seeded {len(rows)} setting(s): {', '.join(k for k, _ in rows)}")


if __name__ == "__main__":
    main()
