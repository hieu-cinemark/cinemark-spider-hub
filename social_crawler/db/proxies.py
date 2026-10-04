"""platform_proxies: proxy selection, health/cooldown bookkeeping, and
the sticky account<->proxy pinning (platform_accounts.assigned_proxy_id)
that services/pool.acquire_proxy_for_account builds on."""

from __future__ import annotations

from typing import Any, TypedDict

import psycopg

from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)


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
        with connect() as conn:
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
    proxy_health_check.py) that wants to test every configured proxy,
    including ones currently cooling down, so a real recovery clears the
    cooldown immediately via record_proxy_outcome(success=True) instead of
    waiting out the timer with no evidence it's actually back."""
    try:
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
            row = conn.execute(
                "SELECT id, assigned_proxy_id FROM platform_accounts "
                "WHERE platform = %s AND (lower(account_id) = lower(%s) OR lower(email) = lower(%s))",
                (platform, account_key, account_key),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error(
            "db_get_account_proxy_assignment_failed", platform=platform, account_key=account_key, error=str(exc)
        )
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
        with connect() as conn:
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
        with connect() as conn:
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
        with connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM platform_proxies WHERE platform IN (%s, 'all') LIMIT 1", (platform,)
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_platform_has_any_proxy_failed", platform=platform, error=str(exc))
        return False
    return row is not None


def mark_proxy_used(platform: str, proxy_url: str) -> None:
    """Same rationale as mark_account_used - stamped on acquire, not
    release, so back-to-back acquire_proxy() calls don't both see the same
    stale last_used_at."""
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE platform = %s AND proxy_url = %s",
                (platform, proxy_url),
            )
    except psycopg.Error as exc:
        logger.error("db_mark_proxy_used_failed", platform=platform, proxy_url=proxy_url, error=str(exc))


# 2^20 x even a large base (e.g. 60 min) stays far inside Postgres'
# interval range, and far above any sane cooldown_max_minutes cap - so the
# cap below is always what actually bounds the cooldown.
_COOLDOWN_MAX_EXPONENT = 20


def record_proxy_outcome(platform: str, proxy_url: str, *, success: bool) -> None:
    """Circuit-breaker update after a request/login attempt through this
    proxy - see services/pool.py. Proxies only ever get the soft-failure
    treatment (no "checkpoint" concept applies to a proxy) - a bad proxy is
    still a proxy, just one worth backing off from for a while rather than
    disabling outright, since a residential/mobile IP that's flaky right now
    may well recover once the underlying rotation on the provider's side
    moves on."""
    try:
        with connect() as conn:
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
                # Base/cap come from the dashboard (proxy_settings). The
                # exponent is capped at _COOLDOWN_MAX_EXPONENT before the
                # multiply: uncapped, power(2, n) * interval overflows
                # Postgres' interval range at n=35 ("interval out of range",
                # confirmed 2026-09-28 on a proxy stuck at 34 failures) -
                # every later UPDATE then failed, so cooldown_until froze in
                # the past, get_proxy_by_id kept handing the dead proxy out
                # as "usable", and acquire_proxy_for_account's repin (which
                # only triggers while the proxy is cooling) never fired.
                from social_crawler.db.proxy_settings import get_setting

                base_minutes = float(get_setting("cooldown_base_minutes"))
                max_minutes = float(get_setting("cooldown_max_minutes"))
                cur = conn.execute(
                    "UPDATE platform_proxies SET status = 'degraded', consecutive_failures = consecutive_failures + 1, "
                    "cooldown_until = now() + LEAST("
                    "  power(2, LEAST(consecutive_failures + 1, %s)) * (%s * interval '1 minute'), "
                    "  %s * interval '1 minute'"
                    ") WHERE platform = %s AND proxy_url = %s "
                    "RETURNING id, consecutive_failures, cooldown_until",
                    (_COOLDOWN_MAX_EXPONENT, base_minutes, max_minutes, platform, proxy_url),
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
