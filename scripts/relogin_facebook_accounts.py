"""One-time, sequential re-login pass for facebook accounts whose cached
cookie has gone dead (see scripts/check_facebook_cookies.py) - each one goes
through social_crawler/auto_login/facebook.py's relogin_one (fills the
login form, solves TOTP 2FA, writes the fresh cookies back to the row and
its Redis storage_state cache).

Deliberately sequential, one account at a time (never parallel within this
script) - each account logs in through its own sticky-pinned proxy
(pool.pinned_login_proxy, never unproxied), so back-to-back logins never
share an IP in a tight loop even though they run one after another with a
paced delay.

This script doesn't solve an email-based verification code itself (the
auto-login orchestrator passes relogin_one a code_provider for that, see
social_crawler/auto_login/orchestrator.py) - an account whose row has no
totp_secret and gets shown a 2FA prompt just gets reported as needing a
human (`bootstrap.py --show-browser --manual`), never auto-disabled (see
MissingTotpSecretError's own docstring for why guessing that's a checkpoint
would be wrong).

Usage:
    # Every facebook account last recorded as "dead" by check_facebook_cookies.py
    python -m scripts.relogin_facebook_accounts

    # Just specific accounts
    python -m scripts.relogin_facebook_accounts --account 61570510702486 --account someone@example.com
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from patchright.sync_api import sync_playwright

from social_crawler.auto_login.facebook import PLATFORM, relogin_one
from social_crawler.clients.redis import RedisCache
from social_crawler.db.accounts import get_accounts_by_check_status
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_MIN_PAUSE_SECONDS = 20.0
_MAX_PAUSE_SECONDS = 60.0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--account",
        action="append",
        help="account_id/email to re-login (repeatable). Default: every facebook account last checked 'dead'.",
    )
    args = parser.parse_args()

    # Piping this to a file/tee (as any background run does) switches
    # stdout to block-buffered, so `print()`'s progress lines can sit in a
    # buffer for minutes with nothing actually written to the log file yet
    # - confirmed happening for real: a run that was quietly succeeding
    # account after account looked identical, from the log file alone, to
    # one stuck on the very first account, and got killed as a false
    # "hung" diagnosis. Line-buffering here means what's in the log file is
    # always what has actually happened so far.
    sys.stdout.reconfigure(line_buffering=True)

    # A slow/flaky network (VPN reconnecting, wifi handoff) can make the
    # very first Supabase connection stall well past _connect()'s own
    # connect_timeout - psycopg/libpq's timeout doesn't reliably cover a
    # stuck DNS resolution on every platform. Printing before the call (not
    # after) means a stall shows up as "still on this line" instead of a
    # script that looks frozen/dead with zero output, like it did in
    # practice - see the KeyboardInterrupt while this project's user was
    # waiting on this exact line with nothing printed yet.
    print("Connecting to Supabase to find accounts marked 'dead'...")
    if args.account:
        all_dead = get_accounts_by_check_status(PLATFORM, "dead")
        needles = {a.strip().lower() for a in args.account}
        accounts = [a for a in all_dead if a["id"].lower() in needles or (a.get("email") or "").lower() in needles]
        missing = needles - {a["id"].lower() for a in accounts} - {(a.get("email") or "").lower() for a in accounts}
        if missing:
            print(f"Not found among accounts last checked 'dead': {sorted(missing)}")
    else:
        accounts = get_accounts_by_check_status(PLATFORM, "dead")

    if not accounts:
        print("No facebook accounts to re-login (none currently recorded as 'dead').")
        return

    print(f"Re-logging in {len(accounts)} facebook account(s), one at a time...\n")
    redis_cache = RedisCache()
    results: list[tuple[str, str, str | None]] = []
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            label = account.get("email") or account["id"]
            try:
                status, note = relogin_one(pw, redis_cache, account)
            except Exception as exc:
                status, note = "error", str(exc)
                logger.error("relogin_crashed", account=label, error=str(exc))
            # relogin_one already recorded "alive" on success;
            # "needs_human"/"error" leave the existing "dead" status alone -
            # it's still dead until someone fixes it.
            results.append((label, status, note))
            print(f"  {label:45s} {status:14s} {note or ''}")
            if i < len(accounts) - 1:
                time.sleep(random.uniform(_MIN_PAUSE_SECONDS, _MAX_PAUSE_SECONDS))

    ok = sum(1 for _, s, _ in results if s == "relogged_in")
    needs_human = sum(1 for _, s, _ in results if s == "needs_human")
    other = len(results) - ok - needs_human
    print(f"\n{ok} re-logged in, {needs_human} need a human, {other} error (out of {len(results)}).")
    if needs_human:
        print("These need a real human-supervised login instead:")
        print(
            "  python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual --account <email_or_id>"
        )


if __name__ == "__main__":
    main()
