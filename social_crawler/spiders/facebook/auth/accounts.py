"""Picks which platform_accounts row a bootstrap run acts as - queried fresh
from Supabase on every call (see social_crawler/services/db.py), not cached
at import time the way the old FACEBOOK_ACCOUNTS env var was. Accounts get
added/disabled/rotated out often enough that a stale in-memory list would
mean editing the table doesn't take effect until every long-lived process
restarts."""

from __future__ import annotations

from social_crawler.constants.facebook import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.services.db import get_accounts
from social_crawler.services.redis import RedisCache


def account_key(user: str) -> str:
    """Redis key suffix identifying an account - the login email, normalized,
    so the same account always maps to the same storage_state/token cache
    regardless of casing/whitespace in how it's stored."""
    return user.strip().lower()


def next_account(redis_cache: RedisCache) -> dict[str, str] | None:
    """None if no enabled facebook row exists in platform_accounts - callers
    treat that as "fall back to manual login / a single default slot", same
    as an empty FACEBOOK_ACCOUNTS used to mean."""
    accounts = get_accounts("facebook")
    if not accounts:
        return None

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(accounts)
    return accounts[index]
