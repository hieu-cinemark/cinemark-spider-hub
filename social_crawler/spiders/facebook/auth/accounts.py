"""
Picks which FACEBOOK_ACCOUNTS entry a bootstrap run acts as.
"""

from __future__ import annotations
import json
import os

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it
from social_crawler.constants.facebook import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.services.redis import RedisCache

# Named keys instead of a single "|"-joined string - a positional format is
# too easy to get a field out of order (confirmed: one account's raw entry
# had cookie/token/email shuffled relative to another's, silently corrupting
# the login for whichever account guessed the order wrong).
REQUIRED_ACCOUNT_FIELDS = ("id", "password", "2fa", "cookie", "token", "email")


def parse_facebook_accounts(raw: str | None = None) -> list[dict[str, str]]:
    """
    Parse FACEBOOK_ACCOUNTS.
    Expected format:
    [
        {
            "id": "...",
            "password": "...",
            "2fa": "...",
            "cookie": "...",
            "token": "...",
            "email": "..."
        }
    ]

    "id" is the login identifier (email/phone/username/uid). "2fa" is a TOTP
    secret used to auto-submit a code if Facebook shows a 2FA prompt - leave
    "" if the account doesn't have 2FA enabled. "cookie", if set, is a raw
    `Cookie:` header string from an already-logged-in session - when
    present, bootstrap.py imports it directly instead of driving a browser
    through the login form. "token" is reserved, not currently used. Every
    key must be present (use "" for anything the account doesn't have).

    Sourced from the FACEBOOK_ACCOUNTS env var.
    """

    if raw is None:
        raw = os.getenv("FACEBOOK_ACCOUNTS", "[]")

    if not raw.strip():
        return []

    try:
        accounts = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "FACEBOOK_ACCOUNTS is not valid JSON"
        ) from exc

    if not isinstance(accounts, list):
        raise ValueError(
            "FACEBOOK_ACCOUNTS must be a JSON array"
        )

    parsed: list[dict[str, str]] = []

    for index, account in enumerate(accounts):
        if not isinstance(account, dict):
            raise ValueError(
                f"FACEBOOK_ACCOUNTS[{index}] must be an object"
            )

        missing = [field for field in REQUIRED_ACCOUNT_FIELDS if field not in account]
        if missing:
            raise ValueError(
                f"FACEBOOK_ACCOUNTS[{index}] is missing field(s) {missing}. "
                f"Expected keys: {REQUIRED_ACCOUNT_FIELDS}"
            )

        parsed.append({field: str(account[field]).strip() for field in REQUIRED_ACCOUNT_FIELDS})

    return parsed


FACEBOOK_ACCOUNTS = parse_facebook_accounts()


def account_key(user: str) -> str:
    """Redis key suffix identifying an account - the login email, normalized,
    so the same account always maps to the same storage_state/token cache
    regardless of casing/whitespace in how it's written in FACEBOOK_ACCOUNTS."""
    return user.strip().lower()


def next_account(redis_cache: RedisCache) -> dict[str, str]:
    if not FACEBOOK_ACCOUNTS:
        raise RuntimeError("FACEBOOK_ACCOUNTS is empty")

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(FACEBOOK_ACCOUNTS)

    return FACEBOOK_ACCOUNTS[index]