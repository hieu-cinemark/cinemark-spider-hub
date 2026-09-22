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
  filter_keywords(id, keyword, category ['movie_relevant'|'spam_offtopic'],
                   enabled, created_at, updated_at) - CRUD'd from the
                   dashboard (cinemark-api's /settings/filter-keywords, see
                   its own app/services/platform_config_db.py), read-only
                   here. Not yet wired into any extraction pipeline - see
                   get_filter_keywords below.
"""

from __future__ import annotations

import os
from typing import Any, TypedDict

import psycopg
from psycopg.rows import dict_row

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it
from social_crawler.logger import get_logger
from social_crawler.services.kira import diagnose_account_failure

logger = get_logger(__name__)

Account = dict[str, str]


class ProxyRow(TypedDict):
    id: int
    url: str
    username: str
    password: str
    login_use_proxy: bool
    # The row's own platform column ('facebook'/'threads'/... or the shared
    # 'all') - NOT necessarily equal to whatever platform get_proxy() was
    # called with, since a shared row can back multiple platforms. Callers
    # recording an outcome (record_proxy_outcome/mark_proxy_used) must key
    # on this value, not the platform they searched with, or the UPDATE
    # silently matches zero rows for any proxy that's platform='all'.
    platform: str


def _database_url() -> str:
    """DATABASE_URL (prod Supabase) unless APP_ENV=development, in which
    case LOCAL_DATABASE_URL (a local Postgres, see scripts/dev_db_schema.sql)
    is used instead - see .env.example. Defaults to "production" when unset
    so an existing deployed .env (systemd service, cron) with no APP_ENV
    line at all keeps hitting Supabase exactly as it always has; only a
    dev machine that explicitly opts in with APP_ENV=development ever talks
    to a local DB."""
    if os.environ.get("APP_ENV", "production") == "development":
        try:
            return os.environ["LOCAL_DATABASE_URL"]
        except KeyError:
            raise RuntimeError("APP_ENV=development but LOCAL_DATABASE_URL is not set (see .env.example)") from None
    return os.environ["DATABASE_URL"]


def _connect() -> psycopg.Connection[Any]:
    return psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5)


def _account_dict(row: dict[str, Any]) -> Account:
    return {
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


def get_accounts(platform: str) -> list[Account]:
    """Enabled accounts for platform that the pool (see services/pool.py)
    can currently hand out - excludes anything mid-cooldown (a transient
    failure's backoff hasn't elapsed yet) or flagged 'checkpoint' (a hard
    failure - enabled is already false for those too, but the explicit
    status check documents why, rather than relying on enabled alone).
    Ordered least-recently-used first (NULLs - never used yet - first) so
    the pool can just take accounts[0] instead of needing its own rotation
    counter."""
    try:
        with _connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true "
                "AND status != 'checkpoint' AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                "ORDER BY last_used_at ASC NULLS FIRST, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_accounts_failed", platform=platform, error=str(exc))
        return []

    return [_account_dict(row) for row in rows]


def list_enabled_accounts(platform: str) -> list[Account]:
    """Enabled, non-checkpointed accounts for platform, including rows
    currently mid-cooldown. Crawl acquire still uses get_accounts() (which
    skips cooldown); the feed-nurture script uses this so a cooling-down
    account can still get a light browse session without waiting out the
    breaker."""
    try:
        with _connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true "
                "AND status != 'checkpoint' "
                "ORDER BY last_used_at ASC NULLS FIRST, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_enabled_accounts_failed", platform=platform, error=str(exc))
        return []
    return [_account_dict(row) for row in rows]


def get_accounts_by_check_status(platform: str, status: str) -> list[Account]:
    """Enabled accounts for platform whose last recorded check (see
    record_cookie_check / scripts/check_facebook_cookies.py) came back as
    `status` - e.g. "dead", to find exactly which accounts a one-time
    re-login pass (scripts/relogin_facebook_accounts.py) needs to touch,
    without re-probing every account's cookie again first."""
    try:
        with _connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true AND last_check_status = %s "
                "ORDER BY id ASC",
                (platform, status),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_accounts_by_check_status_failed", platform=platform, status=status, error=str(exc))
        return []
    return [_account_dict(row) for row in rows]


def claim_account(platform: str) -> Account | None:
    """Atomically pick the LRU healthy account for platform and stamp
    last_used_at in the same transaction (SELECT ... FOR UPDATE SKIP LOCKED
    then UPDATE). Two concurrent acquire_account() callers therefore cannot
    both see the same stale last_used_at and walk off with the same row -
    the second skips the locked row and takes the next LRU instead.

    get_accounts() stays a plain read (Threads/TikTok rotation still lists
    the pool); only the Facebook-style acquire path needs the claim."""
    try:
        with _connect() as conn:
            picked = conn.execute(
                "SELECT id FROM platform_accounts WHERE platform = %s AND enabled = true "
                "AND status != 'checkpoint' AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                "ORDER BY last_used_at ASC NULLS FIRST, id ASC "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_accounts SET last_used_at = now() WHERE id = %s "
                "RETURNING account_id, password, totp_secret, cookie, token, email, email_password",
                (picked["id"],),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_claim_account_failed", platform=platform, error=str(exc))
        return None
    return _account_dict(row) if row is not None else None


def get_account_by_row_id(platform: str, row_id: int) -> Account | None:
    """Same shape as get_accounts()'s rows, but a single row by its numeric
    primary key regardless of enabled - used by tiktok/auth/bootstrap.py to
    target one specific account for a manual identity refresh, which should
    still work on an account that got disabled after its odin_id went
    stale."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND id = %s",
                (platform, row_id),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_by_row_id_failed", platform=platform, row_id=row_id, error=str(exc))
        return None

    if row is None:
        return None
    return _account_dict(row)


def get_account_by_key(platform: str, key: str) -> Account | None:
    """Look up one platform_accounts row by email or account_id, including
    disabled/checkpointed rows. Dashboard restore and a pinned --account
    refresh need the saved cookie/session even when the pool will not
    hand the row out for a normal crawl."""
    needle = (key or "").strip()
    if not needle:
        return None
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s "
                "AND (lower(coalesce(email, '')) = lower(%s) OR lower(account_id) = lower(%s)) "
                "ORDER BY id ASC LIMIT 1",
                (platform, needle, needle),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_by_key_failed", platform=platform, error=str(exc))
        return None
    return _account_dict(row) if row is not None else None


def get_account_pk(platform: str, key: str) -> int | None:
    """Numeric platform_accounts.id for email or account_id, including
    disabled/checkpointed rows. TikTok cookie-import / restore pin a row
    this way because identity writes (update_tiktok_identity) key on PK,
    not the account_id column (which bootstrap overwrites with device_id)."""
    needle = (key or "").strip()
    if not needle:
        return None
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT id FROM platform_accounts WHERE platform = %s "
                "AND (lower(coalesce(email, '')) = lower(%s) OR lower(account_id) = lower(%s)) "
                "ORDER BY id ASC LIMIT 1",
                (platform, needle, needle),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_pk_failed", platform=platform, error=str(exc))
        return None
    return int(row["id"]) if row is not None else None


def update_account_cookie(platform: str, row_id: int, cookie: str) -> bool:
    """Overwrite only the cookie column on one row - used by TikTok cookie
    import before the follow-up identity capture fills device_id/odin_id."""
    try:
        with _connect() as conn:
            cur = conn.execute(
                "UPDATE platform_accounts SET cookie = %s WHERE platform = %s AND id = %s",
                (cookie, platform, row_id),
            )
            updated = cur.rowcount > 0
    except psycopg.Error as exc:
        logger.error("db_update_account_cookie_failed", platform=platform, row_id=row_id, error=str(exc))
        return False
    if updated:
        logger.info("account_cookie_updated", platform=platform, row_id=row_id)
    else:
        logger.warning("account_cookie_update_no_match", platform=platform, row_id=row_id)
    return updated


def reactivate_account(platform: str, account_id: str) -> None:
    """Clear checkpoint/disable after a restore (or cookie import) actually
    recaptured GraphQL tokens with a live session. Distinct from
    record_account_outcome(success=True), which does not flip enabled back
    on - a human-disabled healthy row should stay off until someone turns
    it on, but a checkpoint that the saved cookies just proved wrong
    should not stay terminal."""
    try:
        with _connect() as conn:
            cur = conn.execute(
                "UPDATE platform_accounts SET enabled = true, status = 'active', "
                "consecutive_failures = 0, cooldown_until = NULL, last_check_note = NULL, "
                "last_checked_at = now() WHERE platform = %s AND account_id = %s",
                (platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error("db_reactivate_account_failed", platform=platform, account_id=account_id, error=str(exc))
        return
    if cur.rowcount > 0:
        # The single source-of-truth log for this state transition - some
        # call sites (e.g. tiktok/auth/bootstrap.py) used to log their own
        # differently-named event on top of this; prefer this one so
        # "was this account reactivated" has exactly one event name to
        # search for regardless of caller.
        logger.info("account_reactivated", platform=platform, account_id=account_id)
    else:
        logger.warning("account_reactivate_no_match", platform=platform, account_id=account_id)


def update_tiktok_identity(
    row_id: int, *, device_id: str, odin_id: str, cookie: str, lookup_key: str | None = None
) -> bool:
    """Writes a freshly-captured identity bundle back to one platform_accounts
    row (platform='tiktok') - see tiktok/auth/accounts.py for why this reuses
    account_id/token/cookie rather than dedicated columns (account_id ->
    device_id, token -> odin_id, cookie -> raw Cookie header). Called by
    tiktok/auth/bootstrap.py after a browser capture succeeds. Doesn't raise
    on a DB error, same rationale as disable_account: the capture itself
    already succeeded, a write failure here shouldn't be conflated with
    that.

    lookup_key: human pin (email / pending-tiktok) so a follow-up
    --account refresh still finds this row after account_id becomes device_id.
    """
    try:
        with _connect() as conn:
            if lookup_key:
                conn.execute(
                    "UPDATE platform_accounts SET account_id = %s, token = %s, cookie = %s, "
                    "email = CASE WHEN coalesce(email, '') = '' THEN %s ELSE email END "
                    "WHERE platform = 'tiktok' AND id = %s",
                    (device_id, odin_id, cookie, lookup_key, row_id),
                )
            else:
                conn.execute(
                    "UPDATE platform_accounts SET account_id = %s, token = %s, cookie = %s "
                    "WHERE platform = 'tiktok' AND id = %s",
                    (device_id, odin_id, cookie, row_id),
                )
    except psycopg.Error as exc:
        logger.error("db_update_tiktok_identity_failed", row_id=row_id, error=str(exc))
        return False
    return True


def get_filter_keywords(category: str | None = None) -> list[dict[str, Any]]:
    """Enabled filter_keywords rows - 'movie_relevant'/'spam_offtopic'
    keywords maintained from the dashboard (see module docstring). Read-only
    from this side; not yet called by any extraction pipeline - a future
    content filter (deciding whether a scraped post/comment is worth
    keeping) would call this rather than querying filter_keywords directly,
    same as every other table in this module."""
    try:
        with _connect() as conn:
            if category:
                rows = conn.execute(
                    "SELECT keyword, category FROM filter_keywords WHERE enabled = true AND category = %s",
                    (category,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT keyword, category FROM filter_keywords WHERE enabled = true").fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_filter_keywords_failed", category=category, error=str(exc))
        return []
    return rows


def mark_account_used(platform: str, account_id: str) -> None:
    """Stamps last_used_at = now() the moment the pool (services/pool.py)
    hands this account out - not on release - so two acquire_account() calls
    made back-to-back (before either has had a chance to succeed/fail and
    release) don't both see the same stale last_used_at and pick the same
    "least recently used" account twice. Doesn't raise on a DB error - a
    failed timestamp write shouldn't block the login attempt that's about to
    use this account, it just means the LRU ordering is briefly less
    accurate next call."""
    try:
        with _connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET last_used_at = now() WHERE platform = %s AND account_id = %s",
                (platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error("db_mark_account_used_failed", platform=platform, account_id=account_id, error=str(exc))


def record_account_outcome(
    platform: str, account_id: str, *, success: bool, hard_failure: bool = False, reason: str | None = None
) -> None:
    """Circuit-breaker update after a login/replay attempt - see
    services/pool.py for the acquire/release API this backs.

    - success: clears any cooldown/failure streak - a clean login proves
      the account is fine again, whatever happened before. Also clears any
      stale last_check_note from a previous checkpoint - it no longer
      describes this account's current state.
    - hard_failure (checkpoint/2FA/no c_user after a real login attempt):
      same terminal action disable_account() already took for this exact
      signal - flips enabled=false so a human has to clear it, no
      self-expiring cooldown (a checkpoint doesn't heal on its own on a
      timer the way rate-limiting does). reason, when given, is the raw
      technical failure text (e.g. the RuntimeError bootstrap.py was about
      to raise) - sent to Kira for a short human-readable diagnosis stored
      in last_check_note (see services/kira.diagnose_account_failure),
      surfaced by cinemark-api/the dashboard next to the account. Best
      effort: a missing/failed diagnosis still lets the disable go through,
      it just leaves last_check_note NULL.
    - otherwise (soft failure - network blip, timeout, an automation gap):
      short exponential backoff (5m, 10m, 20m, ... capped at 2h) keyed off
      consecutive_failures, so a flaky run doesn't take the account out
      for good but also isn't retried again a second later.
    """
    try:
        with _connect() as conn:
            if success:
                conn.execute(
                    "UPDATE platform_accounts SET status = 'active', consecutive_failures = 0, "
                    "cooldown_until = NULL, last_check_note = NULL, last_checked_at = now() "
                    "WHERE platform = %s AND account_id = %s",
                    (platform, account_id),
                )
            elif hard_failure:
                note = diagnose_account_failure(reason) if reason else None
                conn.execute(
                    "UPDATE platform_accounts SET status = 'checkpoint', enabled = false, "
                    "consecutive_failures = consecutive_failures + 1, "
                    "last_check_note = %s, last_checked_at = now() "
                    "WHERE platform = %s AND account_id = %s",
                    (note, platform, account_id),
                )
                # error level - see logger.py's _telegram_processor - always
                # alerts: a checkpointed account is disabled and stays that
                # way until a human clears it, unlike a soft failure's
                # self-expiring cooldown.
                logger.error(
                    "account_checkpointed",
                    platform=platform,
                    account_id=account_id,
                    note=note or "account disabled - needs manual re-login/cookie-import before it's usable again",
                )
            else:
                cur = conn.execute(
                    "UPDATE platform_accounts SET consecutive_failures = consecutive_failures + 1, "
                    "cooldown_until = now() + LEAST("
                    "  power(2, consecutive_failures + 1) * interval '5 minutes', interval '2 hours'"
                    ") WHERE platform = %s AND account_id = %s "
                    "RETURNING consecutive_failures, cooldown_until",
                    (platform, account_id),
                )
                row = cur.fetchone()
                if row is not None:
                    logger.warning(
                        "account_soft_failure",
                        platform=platform,
                        account_id=account_id,
                        consecutive_failures=row["consecutive_failures"],
                        cooldown_until=str(row["cooldown_until"]),
                    )
    except psycopg.Error as exc:
        logger.error(
            "db_record_account_outcome_failed",
            platform=platform,
            account_id=account_id,
            success=success,
            hard_failure=hard_failure,
            error=str(exc),
        )


def record_cookie_check(platform: str, account_id: str, *, status: str, note: str | None = None) -> None:
    """Records the outcome of a passive cookie-liveness check (see
    scripts/check_facebook_cookies.py) - reusing a cached cookie against a
    real page load with no actual login/replay attempt behind it. Only
    touches last_checked_at/last_check_status/last_check_note (the same
    manual-check columns cinemark-api's dashboard "Check" button writes via
    its own update_account_check_result) - deliberately NOT
    record_account_outcome/pool.release_account, whose own docstring
    warns against calling it for a run that "just reused an already-cached
    session with no fresh check", which would incorrectly reset or
    increment a real failure streak this check never actually exercised.
    Best-effort like disable_account - a health check that can't record
    its own result shouldn't crash the whole batch."""
    try:
        with _connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET last_checked_at = now(), last_check_status = %s, last_check_note = %s "
                "WHERE platform = %s AND account_id = %s",
                (status, note, platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_record_cookie_check_failed", platform=platform, account_id=account_id, status=status, error=str(exc)
        )


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
    whether to still alert about it. reason is sent to Kira for a short
    human-readable diagnosis stored in last_check_note (see services/
    kira.diagnose_account_failure) - same best-effort contract as
    record_account_outcome's hard_failure branch: a missing/failed
    diagnosis still lets the disable go through, just with no note."""
    note = diagnose_account_failure(reason)
    try:
        with _connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET enabled = false, last_check_note = %s, last_checked_at = now() "
                "WHERE platform = %s AND account_id = %s",
                (note, platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_disable_account_failed", platform=platform, account_id=account_id, reason=reason, error=str(exc)
        )
        return False
    logger.error("account_disabled", platform=platform, account_id=account_id, reason=reason, note=note)
    return True


def get_proxy(platform: str) -> ProxyRow | None:
    """The best-matching enabled, not-cooling-down proxy for this platform -
    a platform-specific row if one exists, otherwise the shared row
    (platform = 'all'), least-recently-used first. None (not an error) if
    none is configured, every enabled one is mid-cooldown, or the DB is
    unreachable - every caller already treats "no proxy" as valid (proxy is
    opt-in everywhere it's used).

    Doesn't consider account↔proxy pinning at all - this is the
    unpinned/legacy picker, still used as the fallback for a run with no
    real account context (manual login, the DEFAULT_ACCOUNT_KEY slot). See
    get_least_loaded_proxy + services/pool.acquire_proxy_for_account for the
    sticky-pinned picker every real account goes through instead."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy "
                "FROM platform_proxies WHERE enabled = true AND platform IN (%s, 'all') "
                "AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                "ORDER BY (platform = 'all') ASC, last_used_at ASC NULLS FIRST LIMIT 1",
                (platform,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_failed", platform=platform, error=str(exc))
        return None

    if row is None:
        return None
    return _proxy_row(row)


def list_proxies(platform: str) -> list[ProxyRow]:
    """Every enabled proxy row for platform (platform-specific + shared
    'all'), regardless of cooldown state - unlike get_proxy/claim_proxy,
    which deliberately hide a cooling-down row since they're picking one to
    actually use right now. For a periodic health ping (see
    services/proxy_health.py) that wants to test every configured proxy,
    including ones currently cooling down, so a real recovery clears the
    cooldown immediately via record_proxy_outcome(success=True) instead of
    waiting out the timer with no evidence it's actually back."""
    try:
        with _connect() as conn:
            rows = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy "
                "FROM platform_proxies WHERE enabled = true AND platform IN (%s, 'all') "
                "ORDER BY (platform = 'all') ASC, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_proxies_failed", platform=platform, error=str(exc))
        return []
    return [_proxy_row(row) for row in rows]


def claim_proxy(platform: str) -> ProxyRow | None:
    """Atomically pick the LRU usable proxy for platform (platform-specific
    row preferred over shared 'all') and stamp last_used_at in the same
    transaction. Same SKIP LOCKED rationale as claim_account - the unpinned
    acquire_proxy() fallback used to SELECT then UPDATE on two connections,
    so two callers could both take the same proxy."""
    try:
        with _connect() as conn:
            picked = conn.execute(
                "SELECT id FROM platform_proxies WHERE enabled = true AND platform IN (%s, 'all') "
                "AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                "ORDER BY (platform = 'all') ASC, last_used_at ASC NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE id = %s "
                "RETURNING id, platform, proxy_url, username, password, login_use_proxy",
                (picked["id"],),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_claim_proxy_failed", platform=platform, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def get_proxy_by_id(proxy_id: int) -> ProxyRow | None:
    """One specific proxy row, but only if it's currently usable (enabled,
    not mid-cooldown) - same "None means not usable right now" contract as
    get_proxy(). Used to resolve an account's pinned assigned_proxy_id; a
    pinned proxy that's currently unhealthy deliberately returns None here
    rather than falling back to a different proxy, so a pinned account never
    silently ends up on a different IP than the one it's known-associated
    with - see services/pool.acquire_proxy_for_account."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy "
                "FROM platform_proxies WHERE id = %s AND enabled = true "
                "AND (cooldown_until IS NULL OR cooldown_until <= now())",
                (proxy_id,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_by_id_failed", proxy_id=proxy_id, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def get_least_loaded_proxy(platform: str) -> ProxyRow | None:
    """The usable proxy for platform currently pinned to the fewest *live*
    accounts (enabled, not checkpointed). Disabled/checkpointed pins still
    occupy an IP identity in the fraud-detection sense, but they must not
    make a proxy look "full" so new accounts pile onto a quieter IP that
    is actually free. Cooldown accounts still count - they are coming back
    to this pin. Ties broken by last_used_at. None if nothing usable is
    configured.

    Prefer pin_account_to_least_loaded_proxy() at acquire time so the pick
    and the pin share one transaction; this read is the non-locking view
    of the same rule."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy "
                "FROM platform_proxies pp "
                "LEFT JOIN platform_accounts pa ON pa.assigned_proxy_id = pp.id "
                "AND pa.enabled = true AND pa.status != 'checkpoint' "
                "WHERE pp.enabled = true AND pp.platform IN (%s, 'all') "
                "AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= now()) "
                "GROUP BY pp.id "
                "ORDER BY (pp.platform = 'all') ASC, count(pa.id) ASC, pp.last_used_at ASC NULLS FIRST LIMIT 1",
                (platform,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_least_loaded_proxy_failed", platform=platform, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def pin_account_to_least_loaded_proxy(platform: str, account_row_id: int) -> ProxyRow | None:
    """Lock the current least-loaded live proxy, pin this account to it, and
    stamp last_used_at in one transaction. Two first-time acquires cannot
    both read load=0 and land on the same proxy while a emptier one sits
    unlocked - SKIP LOCKED sends the second caller to the next candidate."""
    try:
        with _connect() as conn:
            picked = conn.execute(
                "SELECT pp.id FROM platform_proxies pp "
                "WHERE pp.enabled = true AND pp.platform IN (%s, 'all') "
                "AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= now()) "
                "ORDER BY (pp.platform = 'all') ASC, "
                "(SELECT count(*) FROM platform_accounts pa "
                " WHERE pa.assigned_proxy_id = pp.id AND pa.enabled = true "
                " AND pa.status != 'checkpoint') ASC, "
                "pp.last_used_at ASC NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE id = %s "
                "RETURNING id, platform, proxy_url, username, password, login_use_proxy",
                (picked["id"],),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE platform_accounts SET assigned_proxy_id = %s WHERE id = %s",
                (row["id"], account_row_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_pin_account_to_least_loaded_proxy_failed",
            platform=platform,
            account_row_id=account_row_id,
            error=str(exc),
        )
        return None
    return _proxy_row(row)


def _proxy_row(row: dict[str, Any]) -> ProxyRow:
    return {
        "id": row["id"],
        "url": row["proxy_url"],
        "username": row["username"],
        "password": row["password"],
        "login_use_proxy": row["login_use_proxy"],
        "platform": row["platform"],
    }


def get_account_proxy_assignment(platform: str, account_key: str) -> tuple[int, int | None] | None:
    """(row id, assigned_proxy_id) for one platform_accounts row, or None if
    no such row exists (the DEFAULT_ACCOUNT_KEY manual-login slot, or a key
    that doesn't match any row). account_key is looked up case-insensitively
    against *both* account_id and email, matching facebook/auth/bootstrap.py's
    own normalize_account_key(account.get("email") or account["id"]) - the
    Redis-cached "active account" key callers like comet_graphql_client.py
    pass in here can be either field depending on which one that account has
    set, so matching only account_id would silently miss any account whose
    key came from its email field instead.

    assigned_proxy_id is None when this account hasn't been sticky-pinned to
    a proxy yet - see services/pool.acquire_proxy_for_account, which
    auto-pins on first use."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT id, assigned_proxy_id FROM platform_accounts "
                "WHERE platform = %s AND (lower(account_id) = lower(%s) OR lower(email) = lower(%s))",
                (platform, account_key, account_key),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_proxy_assignment_failed", platform=platform, account_key=account_key, error=str(exc))
        return None
    if row is None:
        return None
    return row["id"], row["assigned_proxy_id"]


def assign_proxy(account_row_id: int, proxy_id: int) -> None:
    """Sticky-pins one platform_accounts row to one proxy - permanent until
    someone clears it (e.g. the dashboard's "reset proxy" action) or the
    proxy row itself is deleted (assigned_proxy_id then goes back to NULL
    via ON DELETE SET NULL, see scripts/dev_db_schema.sql). Doesn't raise on
    a DB error - same rationale as mark_account_used: a failed write here
    just means this account gets re-evaluated for pinning again next call
    instead of blocking the run that's already in progress."""
    try:
        with _connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET assigned_proxy_id = %s WHERE id = %s",
                (proxy_id, account_row_id),
            )
    except psycopg.Error as exc:
        logger.error("db_assign_proxy_failed", account_row_id=account_row_id, proxy_id=proxy_id, error=str(exc))
        return
    # This pinning is called out in this function's own docstring as one of
    # the strongest multi-accounting signals FB/Threads/TikTok's fraud
    # detection looks for - worth its own log line rather than relying on
    # pool.py's acquire_proxy_for_account to have logged the pick (that
    # function only logs its own first-pin/re-pin branches, not a direct
    # assign_proxy call from elsewhere).
    logger.info("proxy_assigned", account_row_id=account_row_id, proxy_id=proxy_id)


def get_proxy_raw_status(proxy_id: int) -> dict[str, Any] | None:
    """enabled/consecutive_failures for one proxy row regardless of current
    health (unlike get_proxy_by_id, which returns None for anything not
    currently usable) - lets services/pool.acquire_proxy_for_account tell a
    proxy that's just transiently cooling down (stay pinned, skip this one
    run) apart from one that's deliberately disabled or has failed enough
    times in a row to be treated as dead (re-pin the account elsewhere
    instead). None if the row no longer exists."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT enabled, consecutive_failures FROM platform_proxies WHERE id = %s",
                (proxy_id,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_raw_status_failed", proxy_id=proxy_id, error=str(exc))
        return None
    return dict(row) if row is not None else None


def platform_has_any_proxy(platform: str) -> bool:
    """Whether at least one platform_proxies row (platform-specific or the
    shared 'all') exists for platform at all, regardless of health - lets
    services/pool.acquire_proxy_for_account(required=True) tell "proxies are
    configured for this platform but none are currently usable" (should
    fail loudly rather than silently run unproxied - see that function) from
    "this platform has just never used a proxy" (fine to run unproxied,
    unchanged from before sticky pinning existed)."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM platform_proxies WHERE platform IN (%s, 'all') LIMIT 1", (platform,)
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_platform_has_any_proxy_failed", platform=platform, error=str(exc))
        return False
    return row is not None


def has_enabled_accounts(platform: str) -> bool:
    """Whether platform has at least one enabled platform_accounts row at
    all, regardless of its current cooldown/checkpoint state - lets
    services/pool.acquire_account tell "no accounts configured for this
    platform at all" (normal - falls back to the manual/default slot) apart
    from "accounts are configured but every single one is currently
    checkpointed or cooling down" (a real incident worth alerting on)."""
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM platform_accounts WHERE platform = %s AND enabled = true LIMIT 1", (platform,)
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_has_enabled_accounts_failed", platform=platform, error=str(exc))
        return False
    return row is not None


def mark_proxy_used(platform: str, proxy_url: str) -> None:
    """Same rationale as mark_account_used - stamped on acquire, not
    release, so back-to-back acquire_proxy() calls don't both see the same
    stale last_used_at."""
    try:
        with _connect() as conn:
            conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE platform = %s AND proxy_url = %s",
                (platform, proxy_url),
            )
    except psycopg.Error as exc:
        logger.error("db_mark_proxy_used_failed", platform=platform, proxy_url=proxy_url, error=str(exc))


def record_proxy_outcome(platform: str, proxy_url: str, *, success: bool) -> None:
    """Circuit-breaker update after a request/login attempt through this
    proxy - see services/pool.py. Proxies only ever get the soft-failure
    treatment (no "checkpoint" concept applies to a proxy) - a bad proxy is
    still a proxy, just one worth backing off from for a while rather than
    disabling outright, since a residential/mobile IP that's flaky right now
    may well recover once the underlying rotation on the provider's side
    moves on."""
    try:
        with _connect() as conn:
            if success:
                # Not logging unconditionally - this runs after essentially
                # every request, so an unconditional log line here would
                # just be noise. Read the pre-update value first (a tiny,
                # log-only race against a concurrent caller is fine here)
                # so this can report only the case actually worth seeing: a
                # proxy that was degraded just recovered, symmetric with the
                # "proxy_degraded" warning below for the failure branch.
                before = conn.execute(
                    "SELECT consecutive_failures FROM platform_proxies WHERE platform = %s AND proxy_url = %s",
                    (platform, proxy_url),
                ).fetchone()
                conn.execute(
                    "UPDATE platform_proxies SET status = 'active', consecutive_failures = 0, "
                    "cooldown_until = NULL WHERE platform = %s AND proxy_url = %s",
                    (platform, proxy_url),
                )
                if before is not None and before["consecutive_failures"] > 0:
                    logger.info(
                        "proxy_recovered",
                        platform=platform,
                        proxy_url=proxy_url,
                        previous_consecutive_failures=before["consecutive_failures"],
                    )
            else:
                cur = conn.execute(
                    "UPDATE platform_proxies SET status = 'degraded', consecutive_failures = consecutive_failures + 1, "
                    "cooldown_until = now() + LEAST("
                    "  power(2, consecutive_failures + 1) * interval '5 minutes', interval '2 hours'"
                    ") WHERE platform = %s AND proxy_url = %s "
                    "RETURNING id, consecutive_failures, cooldown_until",
                    (platform, proxy_url),
                )
                row = cur.fetchone()
                if row is not None:
                    log_fields: dict[str, Any] = {
                        "platform": platform,
                        "proxy_url": proxy_url,
                        "consecutive_failures": row["consecutive_failures"],
                        "cooldown_until": str(row["cooldown_until"]),
                    }
                    if platform == "all":
                        # A shared proxy's own failure doesn't just cool
                        # down whatever caller happened to hit it - it cools
                        # down every enabled account on every platform
                        # pinned here. Surfacing the real blast radius here
                        # (not just "platform=all", which reads as "no
                        # specific platform" rather than "every platform")
                        # is what would have made today's TikTok/Facebook
                        # cross-contamination (both hit the same degraded
                        # 139.99.83.20 the same day) obvious from one log
                        # line instead of two separate, seemingly-unrelated
                        # incidents.
                        affected = conn.execute(
                            "SELECT platform, count(*) AS accounts FROM platform_accounts "
                            "WHERE assigned_proxy_id = %s AND enabled = true GROUP BY platform",
                            (row["id"],),
                        ).fetchall()
                        log_fields["affected_platforms"] = {r["platform"]: r["accounts"] for r in affected}
                    logger.warning("proxy_degraded", **log_fields)
    except psycopg.Error as exc:
        logger.error(
            "db_record_proxy_outcome_failed", platform=platform, proxy_url=proxy_url, success=success, error=str(exc)
        )


def get_ai_settings() -> dict[str, Any]:
    """Singleton ai_settings row (dashboard Settings AI tab). Missing DB
    or table is not fatal - callers treat enabled=False and use code
    default prompts/model."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_settings (
                    id integer PRIMARY KEY CHECK (id = 1),
                    enabled boolean NOT NULL DEFAULT false,
                    model text NOT NULL DEFAULT 'qwen3.8-flash',
                    prompts jsonb NOT NULL DEFAULT '{}'::jsonb,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO ai_settings (id, enabled, model, prompts)
                VALUES (1, false, 'qwen3.8-flash', '{}'::jsonb)
                ON CONFLICT (id) DO NOTHING
                """
            )
            cur.execute("SELECT enabled, model, prompts FROM ai_settings WHERE id = 1")
            row = cur.fetchone()
            conn.commit()
    except Exception as exc:
        logger.warning("ai_settings_load_failed", error=str(exc))
        return {"enabled": False, "model": "qwen3.8-flash", "prompts": {}}
    if not row:
        return {"enabled": False, "model": "qwen3.8-flash", "prompts": {}}
    prompts = row.get("prompts") if isinstance(row.get("prompts"), dict) else {}
    return {
        "enabled": bool(row.get("enabled")),
        "model": (row.get("model") or "qwen3.8-flash").strip() or "qwen3.8-flash",
        "prompts": {k: v for k, v in prompts.items() if isinstance(k, str) and isinstance(v, str) and v.strip()},
    }