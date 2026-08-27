"""Picks which platform_accounts row (platform='threads') a bootstrap run
acts as - threads.com logs in through Instagram's own account system, so
this mirrors social_crawler.spiders.facebook.auth.accounts field for field
(see that module for the "why" behind querying fresh on every call instead
of caching at import time)."""

from __future__ import annotations

from social_crawler.constants.threads import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.services.db import get_accounts
from social_crawler.services.redis import RedisCache


def account_key(user: str) -> str:
    """Redis key suffix identifying an account - the login id, normalized,
    so the same account always maps to the same storage_state/token cache
    regardless of casing/whitespace in how it's stored."""
    return user.strip().lower()


def next_account(redis_cache: RedisCache) -> dict[str, str] | None:
    """None if no enabled threads row exists in platform_accounts - callers
    treat that as "fall back to manual login / a single default slot", same
    as an empty INSTAGRAM_ACCOUNTS used to mean."""
    accounts = get_accounts("threads")
    if not accounts:
        return None

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(accounts)
    account = dict(accounts[index])
    # Some account-export formats append extra data after the TOTP secret
    # with a "|"-delimited suffix (e.g. recovery codes) - pyotp.TOTP()
    # otherwise rejects it outright with "Non-base32 digit found" since "|"
    # isn't a valid base32 character. Stripped defensively here (not just
    # at migration time) in case a future row gets pasted in with the same
    # quirk still attached.
    account["2fa"] = account["2fa"].split("|", 1)[0]
    return account
