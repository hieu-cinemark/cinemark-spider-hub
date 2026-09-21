"""Picks which platform_accounts row a bootstrap run acts as - queried fresh
from Supabase/local dev DB on every call (see social_crawler/services/db.py,
services/pool.py), not cached at import time the way the old
FACEBOOK_ACCOUNTS env var was. Accounts get added/disabled/rotated out often
enough that a stale in-memory list would mean editing the table doesn't take
effect until every long-lived process restarts."""

from __future__ import annotations

from social_crawler.services import pool


def account_key(user: str) -> str:
    """Redis key suffix identifying an account - the login email, normalized,
    so the same account always maps to the same storage_state/token cache
    regardless of casing/whitespace in how it's stored."""
    return user.strip().lower()


def next_account() -> dict[str, str] | None:
    """The next account for a bootstrap run to act as - see
    services/pool.acquire_account for the selection rule (least-recently-
    used among healthy accounts, skipping anything mid-cooldown or
    checkpointed). None if no enabled facebook row is currently usable -
    callers treat that as "fall back to manual login / a single default
    slot", same as an empty FACEBOOK_ACCOUNTS used to mean.

    Bootstrap.py must call pool.release_account() with the outcome once the
    login attempt this account was picked for actually finishes - acquiring
    here only marks it "in use" (last_used_at), it doesn't yet know whether
    the attempt will succeed."""
    return pool.acquire_account("facebook")
