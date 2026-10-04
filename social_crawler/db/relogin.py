"""Helpers used by the auto-login scheduler / consumer / orchestrator.

Stays out of db/accounts.py on purpose: that module is the single
source of truth for the platform_accounts table's everyday CRUD
(get/update/claim/disable), and the auto-login flow uses a different
slice of that table (rows where the cookie is dead, plus rows that
explicitly need a human-supervised re-login after a checkpoint). Mixing
both into one file has historically hidden which queries are
"every crawl cares about this" vs. "only the relogin scheduler cares
about this".

All functions are read-mostly - the only writes are to `last_check_*`,
`needs_manual_login`, and `last_relogin_at`, the same manual-check
columns check_facebook_cookies.py already populates. The actual
cookie/totp/password columns are left alone by every helper here; that's
auto_login/facebook.py's / bootstrap.py's own job.
"""

from __future__ import annotations

from typing import Any

import psycopg

from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Which rows the auto-login flow may touch at all - shared by the
# scheduler's list query and the consumer's per-message lookup, so an
# account disabled, checkpointed, flagged for a human, or already marked
# alive after a tick published it is never logged in on a stale message.
_RELOGIN_ELIGIBLE = (
    "platform = %s AND enabled = true AND status != 'checkpoint' "
    "AND last_check_status = 'dead' AND needs_manual_login = false"
)
_RELOGIN_COLUMNS = "id, account_id, password, totp_secret, cookie, token, email, email_password"


def list_accounts_needing_relogin(platform: str) -> list[dict[str, Any]]:
    """Enabled, non-checkpointed accounts whose last cookie liveness check
    came back as `dead` (see scripts/check_facebook_cookies.py /
    check_threads_cookies.py) AND that are NOT already flagged for human
    intervention. The auto-login scheduler iterates this list every tick.

    Rows in cooldown are deliberately included: a previous soft failure
    shouldn't block re-login attempts on the next hourly tick, since
    "dead cookie" is a hard failure that needs a full relogin regardless
    of any transient backoff - the relogin itself succeeds or fails on
    its own merits (record_account_outcome handles the resulting state).
    """
    try:
        with connect() as conn:
            rows = conn.execute(
                f"SELECT {_RELOGIN_COLUMNS} FROM platform_accounts WHERE {_RELOGIN_ELIGIBLE} "
                "ORDER BY last_checked_at ASC NULLS LAST, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_accounts_needing_relogin_failed", platform=platform, error=str(exc))
        return []
    return list(rows)


def mark_needs_manual_login(account_row_id: int, reason: str) -> None:
    """Flip needs_manual_login=true on one platform_accounts row, with the
    short reason text in last_check_note so the dashboard's "needs
    attention" column can show it directly. Idempotent (a second call
    just overwrites the note).

    Called from the auto-login scheduler when a re-login attempt hit
    something only a human can resolve (Facebook photo checkpoint,
    email 2FA with no email_password on file, unknown 2FA prompt
    markup, ...). The row is excluded from list_accounts_needing_relogin
    once the flag is set, so the scheduler won't retry the same dead-end
    every hour; a human clears the flag via the dashboard after they've
    done the manual login.
    """
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET needs_manual_login = true, "
                "last_check_note = %s, last_checked_at = now() WHERE id = %s",
                (reason, account_row_id),
            )
    except psycopg.Error as exc:
        logger.error("db_mark_needs_manual_login_failed", account_row_id=account_row_id, error=str(exc))


def clear_needs_manual_login(account_row_id: int) -> None:
    """Reset the dashboard's "needs manual intervention" flag after a
    human has done the work (or after a successful auto-login proves the
    prior failure was transient, e.g. Facebook briefly hit a checkpoint
    and recovered on its own). Mirrors mark_needs_manual_login's
    idempotency contract.
    """
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET needs_manual_login = false, last_check_note = NULL WHERE id = %s",
                (account_row_id,),
            )
    except psycopg.Error as exc:
        logger.error("db_clear_needs_manual_login_failed", account_row_id=account_row_id, error=str(exc))


def stamp_last_relogin(account_row_id: int, *, status: str) -> None:
    """Audit trail for the auto-login scheduler: last_relogin_at +
    last_relogin_status, surfaced by the dashboard alongside last_check_*
    so an operator can tell "when was this account last attempted, and
    what happened". `status` is one of: "relogged_in" | "needs_human" |
    "failed" | "error" - matching the tuple auto_login/facebook.py's
    relogin_one() already returns. Never called for a dry run, so a
    preview tick doesn't overwrite the last real attempt's status.

    Does NOT touch cookie/totp/enabled/disabled - the actual relogin
    flow is responsible for that. This is purely an audit row.
    """
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET last_relogin_at = now(), last_relogin_status = %s WHERE id = %s",
                (status, account_row_id),
            )
    except psycopg.Error as exc:
        logger.error("db_stamp_last_relogin_failed", account_row_id=account_row_id, status=status, error=str(exc))


def account_count_needing_manual(platform: str) -> int:
    """Used by the auto-login scheduler to decide whether to alert
    (Telegram). Any account flagged "needs_manual_login=true" is
    operator-actionable; the scheduler logs an aggregate count every
    tick so a stranded batch shows up in one line rather than per-row.
    """
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM platform_accounts WHERE platform = %s "
                "AND enabled = true AND needs_manual_login = true",
                (platform,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_account_count_needing_manual_failed", platform=platform, error=str(exc))
        return 0
    return int(row["n"]) if row else 0


def get_account_for_relogin(platform: str, account_id: str) -> dict[str, Any] | None:
    """Look up a single platform_accounts row by (platform, account_id)
    with the same column set list_accounts_needing_relogin returns, so
    the auto_login/consumer.py / Kafka-side flow can resolve an
    account_id from a published message back into the row
    attempt_auto_login needs (id + password + totp_secret + cookie +
    token + email + email_password).

    Re-applies list_accounts_needing_relogin's eligibility filter at
    consume time, so it returns None not only when the row is gone, but
    also when it was disabled, checkpointed, flagged needs_manual_login, or
    marked alive after the message was published (or on a replayed old
    message) - and when Supabase is briefly unhappy. The consumer logs the
    miss and moves on, never throws into the Kafka loop.
    """
    try:
        with connect() as conn:
            row = conn.execute(
                f"SELECT {_RELOGIN_COLUMNS} FROM platform_accounts WHERE {_RELOGIN_ELIGIBLE} "
                "AND account_id = %s ORDER BY id ASC LIMIT 1",
                (platform, account_id),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_for_relogin_failed", platform=platform, account_id=account_id, error=str(exc))
        return None
    return dict(row) if row is not None else None
