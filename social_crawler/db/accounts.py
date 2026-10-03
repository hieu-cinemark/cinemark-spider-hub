"""platform_accounts: the account pool's everyday CRUD - get/claim/
update/disable rows, record login/replay outcomes and cookie checks (see
services/pool.py for the acquire/release API on top). The auto-login
flow's own slice of this table lives in db/relogin.py."""

from __future__ import annotations

from typing import Any

import psycopg

from social_crawler.clients.kira import diagnose_account_failure
from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)


Account = dict[str, str]


def account_dict(row: dict[str, Any]) -> Account:
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
        with connect() as conn:
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

    return [account_dict(row) for row in rows]


def list_enabled_accounts(platform: str) -> list[Account]:
    """Enabled, non-checkpointed accounts for platform, including rows
    currently mid-cooldown. Crawl acquire still uses get_accounts() (which
    skips cooldown); the feed-nurture script uses this so a cooling-down
    account can still get a light browse session without waiting out the
    breaker."""
    try:
        with connect() as conn:
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
    return [account_dict(row) for row in rows]


def get_accounts_by_check_status(platform: str, status: str) -> list[Account]:
    """Enabled accounts for platform whose last recorded check (see
    record_cookie_check / scripts/check_facebook_cookies.py) came back as
    `status` - e.g. "dead", to find exactly which accounts a one-time
    re-login pass (scripts/relogin_facebook_accounts.py) needs to touch,
    without re-probing every account's cookie again first."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true AND last_check_status = %s "
                "ORDER BY id ASC",
                (platform, status),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_accounts_by_check_status_failed", platform=platform, status=status, error=str(exc))
        return []
    return [account_dict(row) for row in rows]


def claim_account(platform: str) -> Account | None:
    """Atomically pick the LRU healthy account for platform and stamp
    last_used_at in the same transaction (SELECT ... FOR UPDATE SKIP LOCKED
    then UPDATE). Two concurrent acquire_account() callers therefore cannot
    both see the same stale last_used_at and walk off with the same row -
    the second skips the locked row and takes the next LRU instead.

    get_accounts() stays a plain read (Threads/TikTok rotation still lists
    the pool); only the Facebook-style acquire path needs the claim."""
    try:
        with connect() as conn:
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
    return account_dict(row) if row is not None else None


def get_account_by_row_id(platform: str, row_id: int) -> Account | None:
    """Same shape as get_accounts()'s rows, but a single row by its numeric
    primary key regardless of enabled - used by tiktok/auth/bootstrap.py to
    target one specific account for a manual identity refresh, which should
    still work on an account that got disabled after its odin_id went
    stale."""
    try:
        with connect() as conn:
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
    return account_dict(row)


def get_account_by_key(platform: str, key: str) -> Account | None:
    """Look up one platform_accounts row by email or account_id, including
    disabled/checkpointed rows. Dashboard restore and a pinned --account
    refresh need the saved cookie/session even when the pool will not
    hand the row out for a normal crawl."""
    needle = (key or "").strip()
    if not needle:
        return None
    try:
        with connect() as conn:
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
    return account_dict(row) if row is not None else None


def get_account_pk(platform: str, key: str) -> int | None:
    """Numeric platform_accounts.id for email or account_id, including
    disabled/checkpointed rows. TikTok cookie-import / restore pin a row
    this way because identity writes (update_tiktok_identity) key on PK,
    not the account_id column (which bootstrap overwrites with device_id)."""
    needle = (key or "").strip()
    if not needle:
        return None
    try:
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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


def has_enabled_accounts(platform: str) -> bool:
    """Whether platform has at least one enabled platform_accounts row at
    all, regardless of its current cooldown/checkpoint state - lets
    services/pool.acquire_account tell "no accounts configured for this
    platform at all" (normal - falls back to the manual/default slot) apart
    from "accounts are configured but every single one is currently
    checkpointed or cooling down" (a real incident worth alerting on)."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM platform_accounts WHERE platform = %s AND enabled = true LIMIT 1", (platform,)
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_has_enabled_accounts_failed", platform=platform, error=str(exc))
        return False
    return row is not None
