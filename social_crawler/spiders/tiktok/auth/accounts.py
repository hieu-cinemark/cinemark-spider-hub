"""
Picks which platform_accounts row (platform='tiktok') a client run acts as -
queried fresh from Supabase on every call (see social_crawler/services/db.py),
same pattern as facebook/threads' own auth/accounts.py.

Unlike Facebook/Instagram, TikTok never logs in at all - see
constants/tiktok.py's module docstring for why a browser is never touched
after the account's identity has been captured once from a real,
already-trusted browser session. platform_accounts has no device_id/odin_id
columns of its own, so this repurposes two existing generic ones instead of
a schema change:

  - account_id -> device_id
  - token      -> odin_id
  - cookie     -> the raw `Cookie:` header string (ttwid/msToken/s_v_web_id)

password/totp_secret/email are unused for this platform (empty string).
"""

from __future__ import annotations

from social_crawler.constants.tiktok import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.services.db import get_accounts
from social_crawler.services.redis import RedisCache

def next_account(redis_cache: RedisCache) -> dict[str, str] | None:
    """None if no enabled tiktok row exists in platform_accounts."""
    accounts = get_accounts("tiktok")
    if not accounts:
        return None

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(accounts)
    return accounts[index]
