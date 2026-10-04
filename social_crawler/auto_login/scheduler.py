"""Hourly auto-login scheduler.

OPT-IN ONLY. No background loop starts unless AUTO_LOGIN_ENABLED=true
is in the environment. This is intentional - an account got flagged for
"suspected automated behavior" (2026-09-11) from unattended logins that
went out unpinned; every login here now goes through the account's own
pinned proxy (pool.pinned_login_proxy), but a whole tick of credential
logins is still something an operator should turn on deliberately.
Operators who want it on must:

    export AUTO_LOGIN_ENABLED=true
    python -m social_crawler.auto_login.scheduler

after confirming the proxy pool, browser fingerprint, and at least one
manual `bootstrap.py --show-browser --manual` run all look healthy.
AUTO_LOGIN_KILL_SWITCH=true refuses every unattended login (this
scheduler, the Kafka consumer, bootstrap.py) regardless.

One-shot runs:

    python -m social_crawler.auto_login.scheduler --once [--dry-run | --no-dry-run] [--force]

--once honors AUTO_LOGIN_DRY_RUN unless --dry-run/--no-dry-run is given.
--force runs even without AUTO_LOGIN_ENABLED, and then defaults to a dry
run - pass --no-dry-run to really log in.

Wire format:

    AUTO_LOGIN_ENABLED=true                # gate
    AUTO_LOGIN_INTERVAL_SECONDS=3600       # default 1h
    AUTO_LOGIN_PLATFORMS=facebook,threads  # default both
    AUTO_LOGIN_DRY_RUN=false               # default false; operators
                                           # are encouraged to start
                                           # with true for the first
                                           # tick to confirm the run
                                           # plan before any real login
                                           # fires.

Each tick:

  1. Reads the env vars above (re-read every tick so an operator can
     change them via SIGHUP-style `kill -HUP $pid` -> just restart,
     no in-process reload).
  2. Calls list_accounts_needing_relogin for each enabled platform.
  3. Iterates accounts sequentially (NOT parallel - one bot per IP at
     a time, same pacing scripts/relogin_facebook_accounts.py already uses).
  4. Records the outcome to last_relogin_*, and if status == "needs_human"
     flips needs_manual_login=true.
  5. Logs a per-tick summary to the platform log + Telegram alert if
     ANY account landed on needs_human OR a platform-wide
     account_count_needing_manual grew.

The scheduler is a no-op when AUTO_LOGIN_ENABLED is unset, false, or
the env var is the empty string - any other value (true / 1 / yes,
case-insensitive) enables it.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

from social_crawler.auto_login.orchestrator import attempt_auto_login
from social_crawler.db.relogin import account_count_needing_manual, list_accounts_needing_relogin
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_INTERVAL_SECONDS = 3600
_DEFAULT_PLATFORMS = ("facebook", "threads")

_TRUTHY = {"true", "1", "yes", "y", "on"}


def _is_enabled() -> bool:
    return (os.environ.get("AUTO_LOGIN_ENABLED") or "").strip().lower() in _TRUTHY


def _interval_seconds() -> int:
    raw = os.environ.get("AUTO_LOGIN_INTERVAL_SECONDS") or ""
    try:
        return max(60, int(raw)) if raw else _DEFAULT_INTERVAL_SECONDS
    except ValueError:
        return _DEFAULT_INTERVAL_SECONDS


def _platforms() -> tuple[str, ...]:
    raw = os.environ.get("AUTO_LOGIN_PLATFORMS") or ""
    if not raw.strip():
        return _DEFAULT_PLATFORMS
    out = tuple(p.strip().lower() for p in raw.split(",") if p.strip())
    return out or _DEFAULT_PLATFORMS


def _dry_run() -> bool:
    raw = (os.environ.get("AUTO_LOGIN_DRY_RUN") or "").strip().lower()
    if not raw:
        return False
    return raw in _TRUTHY


def _run_one_tick(dry_run: bool | None = None) -> dict[str, Any]:
    """One scheduler tick. Returns a small summary dict the caller
    (CLI wrapper / tests) can inspect; nothing is printed here - all
    output is structured log lines. dry_run=None means AUTO_LOGIN_DRY_RUN."""
    summary: dict[str, Any] = {"platforms": {}, "dry_run": _dry_run() if dry_run is None else dry_run}
    for platform in _platforms():
        rows = list_accounts_needing_relogin(platform)
        logger.info(
            "auto_login_tick_start",
            platform=platform,
            candidate_count=len(rows),
            dry_run=summary["dry_run"],
        )
        per_platform: dict[str, int] = {}
        for row in rows:
            outcome = attempt_auto_login(platform, row, dry_run=summary["dry_run"])
            per_platform[outcome.status] = per_platform.get(outcome.status, 0) + 1
            # Throttle between accounts - the same minimum pause the
            # standalone scripts use (4-10s for FB). Done here (rather
            # than inside attempt_auto_login) so a single dry-run tick
            # isn't slowed down by the throttle.
            if not summary["dry_run"]:
                time.sleep(7)
        pending_manual = account_count_needing_manual(platform)
        summary["platforms"][platform] = {
            "attempted": len(rows),
            "by_status": per_platform,
            "needs_manual_total": pending_manual,
        }
        logger.info(
            "auto_login_tick_end",
            platform=platform,
            **summary["platforms"][platform],
        )
    return summary


def run_forever() -> None:
    """Blocking loop: tick, sleep AUTO_LOGIN_INTERVAL_SECONDS, tick.
    Catches every exception inside the tick so a single bad row never
    kills the scheduler process - the next tick still runs.

    For daemon-style deployment wrap this in a process supervisor
    (systemd / supervisord). The scheduler itself does NOT daemonize.
    """
    if not _is_enabled():
        print(
            "AUTO_LOGIN_ENABLED is not set to a truthy value. Refusing to start.\n"
            "Set AUTO_LOGIN_ENABLED=true (and ideally AUTO_LOGIN_DRY_RUN=true for the first tick) "
            "to enable.",
            file=sys.stderr,
        )
        sys.exit(2)

    interval = _interval_seconds()
    logger.info(
        "auto_login_scheduler_started", interval_seconds=interval, platforms=list(_platforms()), dry_run=_dry_run()
    )
    while True:
        try:
            _run_one_tick()
        except Exception:
            logger.exception("auto_login_tick_crashed")
        time.sleep(interval)


# Test/dry-run helper: run exactly one tick and exit, regardless of
# whether AUTO_LOGIN_ENABLED is set. Useful for "let me see what would
# happen right now" without flipping the env var.
def run_once(*, force: bool = False, dry_run: bool | None = None) -> dict[str, Any]:
    """dry_run=None defers to AUTO_LOGIN_DRY_RUN - except when force is what
    let this run past a disabled AUTO_LOGIN_ENABLED, where it defaults to a
    dry run; only an explicit dry_run=False logs in for real then."""
    enabled = _is_enabled()
    if not force and not enabled:
        print(
            "run_once(force=False) requires AUTO_LOGIN_ENABLED=true. "
            "Pass force=True to override (one-shot, dry run unless dry_run=False).",
            file=sys.stderr,
        )
        sys.exit(2)
    if dry_run is None and not enabled:
        dry_run = True
    return _run_one_tick(dry_run)


if __name__ == "__main__":
    # CLI entry: `python -m social_crawler.auto_login.scheduler`
    # runs forever; `... --once [--dry-run | --no-dry-run] [--force]` runs
    # a single tick. The --once form is what an operator uses during the
    # first deploy to confirm the candidate list looks right. Neither
    # dry-run flag -> None, so AUTO_LOGIN_DRY_RUN=true is honored.
    if "--once" in sys.argv:
        force = "--force" in sys.argv
        if "--dry-run" in sys.argv:
            dry: bool | None = True
        elif "--no-dry-run" in sys.argv:
            dry = False
        else:
            dry = None
        run_once(force=force, dry_run=dry)
    else:
        run_forever()
