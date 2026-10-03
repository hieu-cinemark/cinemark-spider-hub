"""Auto-login orchestrator for Facebook + threads accounts.

Drives one re-login pass per account, picking the right 2FA strategy
(TOTP if the row has a totp_secret, IMAP email if it doesn't but has
email + email_password, otherwise just attempt the login - most accounts
never get a 2FA prompt at all). The actual login form filling / cookie
writing still lives in the platform's own
`spiders/<platform>/auth/triggers.py` + this package's per-platform
`<platform>.py` (relogin_one) - this orchestrator just glues those
together with a 2FA-aware caller.

SAFETY: every login here goes through pool.pinned_login_proxy (the
account's own sticky-pinned proxy, never unproxied - see
facebook.py:relogin_one), the same policy bootstrap.py's auto-login uses,
since an account got flagged for "suspected automated behavior"
(2026-09-11) from unattended logins that went out unpinned. The callers
are opt-in on top of that: the env-driven scheduler only runs with
AUTO_LOGIN_ENABLED=true (see scheduler.py), the Kafka consumer
only acts on what the dashboard publishes, and AUTO_LOGIN_KILL_SWITCH=true
refuses every unattended login at once.

Public surface:
  attempt_auto_login(platform, account_row, *, dry_run=False) -> Outcome
  AutoLoginOutcome dataclass (status, note, needs_manual_login)

Statuses returned (mirroring relogin_one's tuple so the caller can log
the same shape):
  - "relogged_in": fresh cookies written, account is back in service
    (its last check is recorded "alive", so it leaves the dead list).
  - "needs_human": can't auto-solve (no password stored, OR a 2FA prompt
    with neither a totp_secret nor an emailed code, OR Facebook
    photo-checkpoint, OR unknown 2FA markup variant). needs_manual_login=
    true is set on the row so the scheduler skips it on subsequent ticks
    until a human clears the flag.
  - "failed": login flow ran end-to-end but Facebook rejected the
    credentials (wrong password / locked account / real checkpoint).
    A separate mark_needs_manual_login is NOT set here - the underlying
    platform's record_account_outcome(hard_failure=True) path already
    flips enabled=false for these (caller is responsible for calling
    that after we return).
  - "error": transient infrastructure failure (proxy down, IMAP
    unreachable, browser crash). The row's last_check_status stays
    'dead' so the next hourly tick will retry; only the audit row is
    stamped.
  - "skipped_dry_run": dry_run=True, nothing was attempted and nothing is
    written to the row.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from social_crawler.auto_login.email_2fa import fetch_email_2fa_code
from social_crawler.db.accounts import account_dict
from social_crawler.db.relogin import mark_needs_manual_login, stamp_last_relogin
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Status literals re-exported for callers that don't want to import the
# Literal type itself.
Status = Literal["relogged_in", "needs_human", "failed", "error", "skipped_dry_run"]

CodeProvider = Callable[[], str | None]

# Slack for clock skew between this host and the mail server's
# INTERNALDATE when only accepting codes mailed after the attempt started.
_EMAIL_CLOCK_SKEW_SECONDS = 60


@dataclass
class AutoLoginOutcome:
    status: Status
    note: str | None = None
    needs_manual_login: bool = False


def _run_facebook_relogin(account_row: dict[str, Any], code_provider: CodeProvider | None) -> AutoLoginOutcome:
    """Thin wrapper around facebook.py's relogin_one - the same one-account
    login flow scripts/relogin_facebook_accounts.py runs, so there's one
    version of "how to log Facebook in" rather than two drifting apart.

    account_row is a raw platform_accounts row (db/relogin.py's column
    set: numeric `id`, login in `account_id`, `totp_secret`) - converted
    to the db.accounts.Account shape relogin_one/auto_login read (login in
    `id`, TOTP secret in `2fa`) before the call.

    Uses the sync Playwright API, which refuses to start on a thread with
    a running asyncio loop - the Kafka consumer calls attempt_auto_login
    via asyncio.to_thread for exactly that reason.

    Returns AutoLoginOutcome instead of a tuple so callers don't have
    to remember positional ordering. "needs_human" maps to
    needs_manual_login=True; everything else to False.
    """
    # Imported lazily so the email-2fa path doesn't pull Playwright in
    # just to read a code from IMAP.
    from patchright.sync_api import sync_playwright

    from social_crawler.auto_login.facebook import relogin_one
    from social_crawler.clients.redis import RedisCache

    with sync_playwright() as pw:
        status, note = relogin_one(pw, RedisCache(), account_dict(account_row), code_provider=code_provider)

    return AutoLoginOutcome(
        status=status,  # type: ignore[arg-type]
        note=note,
        needs_manual_login=status == "needs_human",
    )


def _run_threads_relogin(account_row: dict[str, Any], code_provider: CodeProvider | None) -> AutoLoginOutcome:
    """Mirror of _run_facebook_relogin for threads. scripts/relogin_threads_accounts.py
    doesn't exist yet (only the FB one does - the threads equivalent is on
    the Phase 2 roadmap). Until it lands, we surface that as a clear
    "needs_human" rather than silently no-op'ing, so an operator running
    the scheduler against threads sees the gap immediately instead of
    every tick silently failing."""
    return AutoLoginOutcome(
        status="needs_human",
        note="threads auto-relogin not implemented yet (scripts/relogin_threads_accounts.py missing)",
        needs_manual_login=False,
    )


def _resolve_2fa(account_row: dict[str, Any], attempt_started_at: float) -> tuple[CodeProvider | None, str]:
    """Returns (code_provider, strategy_label) for one account:
      - (None, "totp") if a totp_secret is on the row - auto_login
        generates the code from it itself
      - (email provider, "email_imap") otherwise, if email + email_password
        are set - only accepts a code mailed after attempt_started_at
      - (None, "none") if neither is configured - the login is still
        attempted; a 2FA prompt then surfaces as needs_human

    An unreachable inbox raises Email2FAUnreachableError out of the
    provider, which relogin_one reports as a transient "error" (retried
    next tick) rather than flagging the account for a human.
    Keeping the resolution here (rather than inline in attempt_auto_login)
    makes the "what 2FA channel did this account use" decision unit-
    testable without spinning up Playwright.
    """
    if (account_row.get("totp_secret") or "").strip():
        return None, "totp"

    if (account_row.get("email") or "").strip() and (account_row.get("email_password") or ""):

        def _email_code() -> str | None:
            return fetch_email_2fa_code(account_row, since_unix=attempt_started_at - _EMAIL_CLOCK_SKEW_SECONDS)

        return _email_code, "email_imap"

    return None, "none"


def attempt_auto_login(
    platform: str,
    account_row: dict[str, Any],
    *,
    dry_run: bool = False,
) -> AutoLoginOutcome:
    """One re-login attempt for one account row. See module docstring
    for status semantics.

    `account_row` must be the dict shape returned by
    services/relogin.list_accounts_needing_relogin (or compatible) - in
    particular it must carry the `id` (numeric PK) and at minimum
    account_id / password / totp_secret / cookie / token / email /
    email_password columns. The 2FA code provider is resolved inside; the
    actual login form fill + cookie write stays inside the platform's own
    relogin flow.
    """
    row_id = account_row.get("id")
    account_label = account_row.get("account_id") or row_id or "?"
    logger.info("auto_login_attempt_start", platform=platform, account_id=account_label, dry_run=dry_run)

    if dry_run:
        # Nothing attempted, so nothing written - a preview tick must not
        # overwrite the last real attempt's last_relogin_* audit columns.
        return AutoLoginOutcome(status="skipped_dry_run", note="dry-run mode - no login attempted")

    code_provider, strategy = _resolve_2fa(account_row, time.time())

    # Dispatch to the platform-specific runner. New platforms plug in
    # by adding an elif branch here.
    runner: Callable[[dict[str, Any], CodeProvider | None], AutoLoginOutcome] | None
    if platform == "facebook":
        runner = _run_facebook_relogin
    elif platform == "threads":
        runner = _run_threads_relogin
    else:
        runner = None

    if runner is None:
        outcome = AutoLoginOutcome(
            status="needs_human",
            note=f"unsupported platform {platform!r} - only facebook and threads are wired in",
            needs_manual_login=True,
        )
    else:
        try:
            outcome = runner(account_row, code_provider)
        except Exception as exc:  # last-resort catch-all so a runner bug doesn't kill the scheduler tick
            logger.exception("auto_login_runner_crashed", platform=platform, account_id=account_label)
            outcome = AutoLoginOutcome(status="error", note=f"runner crashed: {exc}")

    if row_id is not None:
        stamp_last_relogin(int(row_id), status=outcome.status)
        if outcome.needs_manual_login:
            mark_needs_manual_login(int(row_id), outcome.note or "")

    logger.info(
        "auto_login_attempt_end",
        platform=platform,
        account_id=account_label,
        status=outcome.status,
        strategy=strategy,
        note=outcome.note,
    )
    return outcome
