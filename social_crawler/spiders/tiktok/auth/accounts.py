"""
Picks which platform_accounts row (platform='tiktok') a client run acts as -
queried fresh from Supabase on every call (see social_crawler/services/db.py),
same pattern as facebook/threads' own auth/accounts.py.

Unlike Facebook/Instagram, TikTok never automates a password login - see
constants/tiktok.py's module docstring for why a browser is never touched
after the account's identity has been captured once from a real,
already-trusted browser session. A logged-in session is imported the same
way Facebook/Threads are: a human pastes the Cookie header (must include
sessionid + ttwid), then bootstrap recaptures device_id/odin_id. The
`cookie` field holds that header; password/totp_secret/email are unused.
platform_accounts has no device_id/odin_id columns of its own, so this
repurposes two existing generic ones instead of a schema change:

  - account_id -> device_id
  - token      -> odin_id
  - cookie     -> the raw `Cookie:` header string (ttwid/msToken/s_v_web_id,
                  plus sessionid/sid_tt/etc. for a real logged-in account)
"""

from __future__ import annotations

from social_crawler.constants.tiktok import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.db import get_accounts, list_enabled_accounts
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.tiktok.auth.cookies import cookie_map

logger = get_logger(__name__)


def cookie_names(cookie: str | None) -> list[str]:
    return sorted(cookie_map(cookie or ""))


def is_logged_in_cookie(cookie: str | None) -> bool:
    """Guest ttwid-only rows can resolve a hashtag then get an empty 200 on
    item_list. A real web login carries sessionid (and often sid_tt)."""
    names = {name.lower() for name in cookie_map(cookie or "")}
    return bool(names & {"sessionid", "sid_tt"})


def next_account(
    redis_cache: RedisCache,
    *,
    exclude_ids: set[str] | None = None,
    require_login: bool = True,
    require_usable_proxy: bool = False,
) -> dict[str, str] | None:
    """None if no enabled tiktok row matches. Prefers logged-in cookies so
    a crawl does not rotate onto a guest row after a restore of a different
    account. `exclude_ids` skips device_ids that already empty-200'd this run.

    `require_usable_proxy`: when True (TikTok comments / browser spiders),
    skip accounts whose sticky-pinned proxy is mid-cooldown - otherwise
    rotation keeps handing out "ghim'd" accounts that immediately fail
    acquire_proxy_for_account. If every remaining account is pinned to a
    cooling proxy, raises ProxyPoolExhaustedError so the spider can exit
    with PROXY_EXHAUSTED_EXIT_CODE and the consumer requeues.

    Also raises ProxyPoolExhaustedError when get_accounts() is empty but
    enabled rows still exist mid-account-cooldown - otherwise a single
    soft-failure cooldown left the spider logging tiktok_account_unusable
    and the consumer marking comments_crawl_finished (quiet success)."""
    accounts = get_accounts("tiktok")
    if not accounts:
        cooling = list_enabled_accounts("tiktok")
        if cooling:
            ids = [row["id"] for row in cooling]
            logger.warning("tiktok_all_accounts_cooling", device_ids=ids)
            raise pool.ProxyPoolExhaustedError(
                f"tiktok: every enabled account is mid-cooldown (device_ids={ids})"
            )
        return None

    skip = exclude_ids or set()
    remaining = [row for row in accounts if row["id"] not in skip]
    logged_in = [row for row in remaining if is_logged_in_cookie(row.get("cookie"))]
    candidates = logged_in if require_login else (logged_in or remaining)
    if not candidates:
        if require_login and remaining:
            logger.warning(
                "tiktok_no_logged_in_account",
                guest_count=len(remaining),
                guest_device_ids=[row["id"] for row in remaining],
                guest_cookie_names=[cookie_names(row.get("cookie")) for row in remaining],
            )
            return None
        # Every currently-usable account was already tried this run, but
        # others may still be cooling - requeue instead of "no accounts".
        cooling = [
            row for row in list_enabled_accounts("tiktok") if row["id"] not in skip
        ]
        if cooling:
            ids = [row["id"] for row in cooling]
            logger.warning(
                "tiktok_remaining_accounts_cooling",
                tried=sorted(skip),
                cooling=ids,
            )
            raise pool.ProxyPoolExhaustedError(
                f"tiktok: tried all currently usable accounts; others mid-cooldown (device_ids={ids})"
            )
        return None

    if require_usable_proxy:
        usable = [row for row in candidates if pool.account_pinned_proxy_usable("tiktok", row["id"])]
        if not usable:
            cooling_ids = [row["id"] for row in candidates]
            logger.warning(
                "tiktok_all_pinned_proxies_cooling",
                device_ids=cooling_ids,
                note="every remaining account is sticky-pinned to a cooling proxy",
            )
            raise pool.ProxyPoolExhaustedError(
                "tiktok: every remaining account is pinned to a proxy that is cooling down "
                f"(device_ids={cooling_ids})"
            )
        if len(usable) < len(candidates):
            logger.info(
                "tiktok_skipped_cooling_pinned_accounts",
                skipped=len(candidates) - len(usable),
                usable=len(usable),
            )
        candidates = usable

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(candidates)
    return candidates[index]
