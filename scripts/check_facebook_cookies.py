"""Read recorded Facebook session observations. Never opens a browser or contacts Facebook.

An observation is not proof of current login health. Stale/missing data does not mark an
account dead, update its last-checked time or enqueue login. Existing CLI flags remain.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime, timedelta

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.accounts import (
    get_accounts,
    last_checked_at_map,
)
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
)

logger = get_logger(__name__)

PLATFORM = "facebook"
# Giữa các tài khoản - cố ý không chạy liên tục, xem docstring module.
_MIN_PAUSE_SECONDS = 4.0
_MAX_PAUSE_SECONDS = 10.0


def _check_one(pw, account: dict, redis_cache: RedisCache) -> tuple[str, str | None]:
    """Read the latest observation only. No browser, proxy acquisition or fallback cookie."""
    from social_crawler.constants.facebook import CACHE_MAX_AGE_SECONDS, CACHE_REDIS_KEY_TMPL

    account_key = (account.get("email") or account["id"]).strip().lower()
    recipe = redis_cache.get(CACHE_REDIS_KEY_TMPL.format(account=account_key))
    if not isinstance(recipe, dict):
        return "skipped", "no recorded session observation; explicit setup may be needed"
    cookies = recipe.get("cookies") or {}
    if not all(cookies.get(name) not in (None, "", "deleted") for name in REQUIRED_LOGIN_COOKIES):
        return "skipped", "session metadata is incomplete; no network check was performed"
    try:
        observed = float(recipe.get("http_refreshed_at", recipe.get("captured_at", 0)))
    except TypeError, ValueError:
        return "error", "invalid session observation time"
    if observed <= 0 or not 0 <= time.time() - observed < CACHE_MAX_AGE_SECONDS:
        return "skipped", "last session observation is stale; no network check was performed"
    return "observed", "recent session observation exists; not a live cookie validation"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--account", help="Only check this one account_id/email instead of every enabled facebook account"
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=None,
        help="Skip accounts whose cookie was checked less than this many hours ago (used by the scheduled run)",
    )
    args = parser.parse_args()

    # Xem bản sửa giống hệt trong relogin_facebook_accounts.py để biết vì sao điều này quan
    # trọng với mọi lượt chạy nền/qua pipe: không có nó, một lượt đang chạy bình thường và
    # một lượt thật sự kẹt ở tài khoản đầu tiên trông giống hệt nhau trong file log suốt
    # nhiều phút.
    sys.stdout.reconfigure(line_buffering=True)

    accounts = get_accounts(PLATFORM)
    if args.account:
        needle = args.account.strip().lower()
        accounts = [a for a in accounts if a["id"] == args.account or (a.get("email") or "").lower() == needle]

    if args.stale_hours:
        cutoff = datetime.now(tz=UTC) - timedelta(hours=args.stale_hours)
        checked = last_checked_at_map(PLATFORM)
        fresh = [a for a in accounts if checked.get(a["id"]) and checked[a["id"]] > cutoff]
        accounts = [a for a in accounts if a not in fresh]
        if fresh:
            print(f"Skipping {len(fresh)} account(s) checked within the last {args.stale_hours:g}h.")

    if not accounts:
        print("No enabled facebook accounts to check.")
        return

    print(f"Reading recorded observations for {len(accounts)} Facebook account(s); no network requests.\n")
    results: list[tuple[str, str, str | None]] = []
    redis_cache = RedisCache()
    for account in accounts:
        label = account.get("email") or account["id"]
        try:
            status, note = _check_one(None, account, redis_cache)
        except Exception as exc:  # noqa: BLE001 - a read failure is not a dead session
            status, note = "error", type(exc).__name__
        results.append((label, status, note))
        print(f"  {label:45s} {status:8s} {note or ''}")
    observed = sum(1 for _, status, _ in results if status == "observed")
    print(f"\n{observed} recently observed, {len(results) - observed} skipped/error. No live validation performed.")


if __name__ == "__main__":
    main()
