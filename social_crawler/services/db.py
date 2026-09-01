"""Postgres client (Supabase) for account/proxy config - these change often
enough (accounts get disabled/swapped, proxies get rotated) that editing
.env and restarting every process that reads it (bootstrap.py's subprocess,
`scrapy crawl`'s subprocess, crawl_request_consumer.py's long-lived service)
stopped being acceptable. A change to the platform_accounts/platform_proxies
tables takes effect on the very next call, no restart needed.

A fresh connection per call, not a pool: every caller here either runs
inside a short-lived subprocess (bootstrap.py runs once per refresh, a
`scrapy crawl` process runs once per crawl) or calls this rarely enough
(once at crawl/session start) that pool lifecycle management would add
complexity for no real benefit.

Schema (see the migration this was built against - no ORM/migration tool
here, just two tables):

  platform_accounts(id, platform, account_id, password, totp_secret,
                     cookie, token, email, email_password, enabled,
                     created_at, updated_at)
  platform_proxies(id, platform ['all' = shared across every platform],
                    proxy_url, username, password, login_use_proxy, enabled,
                    created_at, updated_at)
"""

from __future__ import annotations

import os
from typing import Any, TypedDict

import psycopg
from psycopg.rows import dict_row

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it
from social_crawler.logger import get_logger

logger = get_logger(__name__)

Account = dict[str, str]


class ProxyRow(TypedDict):
    url: str
    username: str
    password: str
    login_use_proxy: bool


def _connect() -> psycopg.Connection[Any]:
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row, connect_timeout=5)


def get_accounts(platform: str) -> list[Account]:
    try:
        with _connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true "
                "ORDER BY id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_accounts_failed", platform=platform, error=str(exc))
        return []

    return [
        {
            "id": row["account_id"],
            "password": row["password"],
            "2fa": row["totp_secret"],
            "cookie": row["cookie"],
            "token": row["token"],
            "email": row["email"],
            # The recovery email's own password (not the platform account's)
            # - needed to log into that inbox for a verification code, not
            # used by any login flow yet, just carried through for now.
            "email_password": row["email_password"],
        }
        for row in rows
    ]


def disable_account(platform: str, account_id: str, reason: str) -> bool:
    """Flips one platform_accounts row to enabled=false - called when a
    real login attempt with this account's own stored credentials comes
    back without a logged-in cookie, which is the clearest signal available
    that Facebook/Instagram has thrown up a checkpoint/2FA prompt auto-login
    can't click through (see bootstrap.py's own c_user/ds_user_id check).
    Doesn't raise on a DB error - the caller is already mid-failure-handling
    for the checkpoint itself; a disable that couldn't be recorded shouldn't
    mask that original error, it just means this account gets retried (and
    probably fails the same way) next rotation instead of being skipped.
    Returns whether the update actually went through, so the caller knows
    whether to still alert about it."""
    try:
        with _connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET enabled = false WHERE platform = %s AND account_id = %s",
                (platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_disable_account_failed", platform=platform, account_id=account_id, reason=reason, error=str(exc)
        )
        return False
    return True


def get_proxy(platform: str) -> ProxyRow | None:
    """The best-matching enabled proxy for this platform - a
    platform-specific row if one exists, otherwise the shared row
    (platform = 'all'). None (not an error) if none is configured or the
    DB is unreachable - every caller already treats "no proxy" as valid
    (proxy is opt-in everywhere it's used)."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT proxy_url, username, password, login_use_proxy "
                "FROM platform_proxies WHERE enabled = true AND platform IN (%s, 'all') "
                "ORDER BY (platform = 'all') ASC LIMIT 1",
                (platform,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_failed", platform=platform, error=str(exc))
        return None

    if row is None:
        return None
    return {
        "url": row["proxy_url"],
        "username": row["username"],
        "password": row["password"],
        "login_use_proxy": row["login_use_proxy"],
    }
