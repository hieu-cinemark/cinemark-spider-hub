"""Account and proxy pool: circuit-breaker-aware selection and outcome
recording on top of the platform_accounts/platform_proxies tables (see
services/db.py). Platform-agnostic by construction, but wired up against
Facebook first (facebook/auth/accounts.py, spiders/comet_graphql_client.py)
- Threads/TikTok can adopt the same acquire_*/release_* calls the same way
once tested there.

The pool replaces two things that used to be separate and weaker:
  - next_account()'s plain round-robin (facebook/auth/accounts.py, before
    this existed) - blind to whether an account is actually healthy right
    now, so a checkpointed/rate-limited account got retried on schedule
    regardless.
  - get_proxy()'s single fixed row with no failure tracking at all - a bad
    proxy was invisible; nothing ever backed off from it or even logged
    that it might be the problem.

Both pools use the same shape: acquire (health-aware pick + stamp
last_used_at) / release (record success or a soft/hard failure, updating
cooldown_until and consecutive_failures - see db.record_account_outcome /
db.record_proxy_outcome for the exact backoff schedule).

acquire_proxy_for_account() adds sticky account↔proxy pinning on top of
acquire_proxy() - see its own docstring below. Every real call site
(comet_graphql_client.py, tiktok/client.py, each platform's bootstrap.py)
should go through it rather than calling acquire_proxy()/get_proxy()
directly, or the pinning guarantee doesn't actually hold. It also
self-heals a dead pinned proxy (re-pins after REPIN_AFTER_CONSECUTIVE_
FAILURES) and, for callers that pass required=True, refuses to silently
fall back to running unproxied by raising ProxyPoolExhaustedError instead.
"""

from __future__ import annotations

from social_crawler.logger import get_logger
from social_crawler.services.db import (
    Account,
    ProxyRow,
    claim_account,
    claim_proxy,
    get_account_proxy_assignment,
    get_proxy_by_id,
    get_proxy_raw_status,
    has_enabled_accounts,
    mark_proxy_used,
    pin_account_to_least_loaded_proxy,
    platform_has_any_proxy,
    record_account_outcome,
    record_proxy_outcome,
)

logger = get_logger(__name__)

# How many consecutive failures a pinned proxy tolerates before it's treated
# as dead rather than just transiently cooling down - see
# acquire_proxy_for_account. Deliberately not 1: a single blip (a timeout, a
# provider-side IP rotation mid-request) shouldn't burn the account's whole
# IP identity; only a proxy that keeps failing across several separate
# cooldown cycles gets replaced.
REPIN_AFTER_CONSECUTIVE_FAILURES = 5

# scrapy spiders exit with this when acquire_proxy_for_account(required=True)
# failed. crawl_request_consumer maps it back to ProxyPoolExhaustedError so
# the Kafka message can be requeued with backoff instead of being committed
# as a quiet success (exit 0).
PROXY_EXHAUSTED_EXIT_CODE = 75


class ProxyPoolExhaustedError(RuntimeError):
    """Raised by acquire_proxy_for_account(required=True) when this platform
    has proxies configured but none are currently usable for this specific
    account (its pinned proxy is down and no healthy replacement exists
    either). Callers that need a proxy for their steady-state traffic - the
    ongoing GraphQL/signed-request replay clients, not the one-time login
    browser - should let this propagate and abort the run rather than
    catching it and proceeding unproxied: doing so would both expose the
    real server IP and, for an already-pinned account, silently swap its
    established IP identity out from under it - exactly what sticky pinning
    exists to prevent. See comet_graphql_client.py/tiktok/client.py, which
    catch this and re-raise it as their own NetworkError/TikTokNetworkError
    so it flows through the retry/alert handling every spider already has
    for a dead proxy."""


def abort_spider_for_network_error(exc: BaseException) -> None:
    """Never returns. Proxy-pool exhaustion → PROXY_EXHAUSTED_EXIT_CODE so
    the consumer requeues; any other network failure → exit 1 (job failed,
    not a successful empty crawl)."""
    import sys

    exhausted = isinstance(exc, ProxyPoolExhaustedError) or isinstance(
        getattr(exc, "__cause__", None), ProxyPoolExhaustedError
    )
    if exhausted:
        logger.error(
            "spider_aborted_proxy_exhausted",
            error=str(exc),
            exit_code=PROXY_EXHAUSTED_EXIT_CODE,
        )
        sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
    logger.error("spider_aborted_network_error", error=str(exc), exit_code=1)
    sys.exit(1)


def account_pinned_proxy_usable(platform: str, account_key: str) -> bool:
    """Whether this account can run right now without hitting a cooling
    sticky pin. True when unpinned (first use will pin a healthy proxy) or
    when the pinned proxy is currently claimable via get_proxy_by_id.
    False when pinned to a proxy mid-cooldown - callers like TikTok
    next_account skip those so rotation doesn't waste attempts on accounts
    that will immediately raise ProxyPoolExhaustedError."""
    assignment = get_account_proxy_assignment(platform, account_key)
    if assignment is None:
        return True
    _, assigned_proxy_id = assignment
    if assigned_proxy_id is None:
        return True
    return get_proxy_by_id(assigned_proxy_id) is not None


def acquire_account(platform: str) -> Account | None:
    """The healthiest available account for platform - least-recently-used
    among enabled, non-cooling-down, non-checkpointed rows (see
    db.claim_account). None means no account is currently usable.

    Alerts (logger.error, not just returns None) when accounts are actually
    configured for this platform but every single one is currently
    unusable - that's a real incident (the whole platform's collection is
    stalled), distinct from a fresh setup that's never had any accounts."""
    account = claim_account(platform)
    if account is None:
        if has_enabled_accounts(platform):
            logger.error(
                "account_pool_exhausted",
                platform=platform,
                note="every enabled account is checkpointed or cooling down - collection is stalled for this platform",
            )
        return None
    logger.debug("account_acquired", platform=platform, account_id=account["id"])
    return account


def release_account(
    platform: str, account_id: str, *, success: bool, hard_failure: bool = False, reason: str | None = None
) -> None:
    """Record what happened with the account acquire_account() handed out.
    Only call this after an actual login/replay attempt was made against
    Facebook/Threads/etc - not for a run that just reused an already-cached
    session with no fresh check, which would otherwise reset a real failure
    streak on a run that never actually verified anything. reason (the raw
    technical failure text) is only used when hard_failure=True - see
    db.record_account_outcome, which sends it to Kira for a short
    human-readable diagnosis stored on the account."""
    record_account_outcome(platform, account_id, success=success, hard_failure=hard_failure, reason=reason)


def acquire_proxy(platform: str) -> ProxyRow | None:
    """The best available, not-cooling-down proxy for platform - see
    db.claim_proxy. None means no proxy is currently usable (none configured,
    or the only one(s) available are mid-cooldown) - every caller already
    treats "no proxy" as a valid, opt-in-only outcome."""
    proxy = claim_proxy(platform)
    if proxy is not None:
        logger.debug("proxy_acquired", platform=platform, proxy_url=proxy["url"])
    return proxy


def release_proxy(proxy: ProxyRow, *, success: bool) -> None:
    """Record what happened using the proxy acquire_proxy() handed out.
    Takes the whole ProxyRow (not just a url) because a shared row's own
    platform column ('all') can differ from whatever platform was searched
    with - see ProxyRow's own docstring in services/db.py."""
    record_proxy_outcome(proxy["platform"], proxy["url"], success=success)


def acquire_proxy_for_account(platform: str, account_key: str | None, *, required: bool = False) -> ProxyRow | None:
    """The sticky-pinned proxy for this account - every real crawl/bootstrap
    call site should use this instead of acquire_proxy() directly, so the
    same account always presents as the same IP instead of fanning out
    across the whole proxy pool at random (see platform_accounts.
    assigned_proxy_id's docstring in scripts/dev_db_schema.sql for why that
    matters - it's one of the strongest multi-accounting signals FB/Threads/
    TikTok's fraud detection looks for). account_key is whatever identifies
    the account to the caller - account_id, email, or the same normalized
    Redis "active account" key comet_graphql_client.py already tracks - see
    db.get_account_proxy_assignment for how it's matched against a row.

    - account_key is None (no real account context - the DEFAULT_ACCOUNT_KEY
      manual-login slot) -> falls back to acquire_proxy()'s old unpinned
      behavior.
    - account has no assigned_proxy_id yet (first time it's ever acquired a
      proxy) -> auto-pins it to whichever usable proxy currently has the
      fewest live accounts pinned (enabled, not checkpointed), so a growing
      account pool spreads evenly instead of piling onto one proxy - and
      disabled/checkpointed pins don't make a dead-stack proxy look "full".
    - account is already pinned but that specific proxy is currently
      unusable -> stays pinned and returns None (skip this run) if the
      proxy is just transiently cooling down from a single blip; but if
      it's been disabled outright or has failed REPIN_AFTER_CONSECUTIVE_
      FAILURES times in a row, it's treated as dead and the account is
      re-pinned to a fresh least-loaded proxy instead (self-healing, logged
      loudly since it's an IP-identity change worth a human noticing).

    required=True raises ProxyPoolExhaustedError instead of returning None
    when this platform has proxies configured but none are usable right
    now for this account - see that exception's own docstring for why. Pass
    this from steady-state crawl clients (comet_graphql_client.py, tiktok/
    client.py); leave it False (the default) for the one-time, opt-in-only
    login-browser proxy in each platform's bootstrap.py, which is meant to
    tolerate "no proxy" as a normal outcome.
    """
    if not account_key:
        return acquire_proxy(platform)

    assignment = get_account_proxy_assignment(platform, account_key)
    if assignment is None:
        return acquire_proxy(platform)
    account_row_id, assigned_proxy_id = assignment

    if assigned_proxy_id is not None:
        proxy = get_proxy_by_id(assigned_proxy_id)
        if proxy is not None:
            mark_proxy_used(proxy["platform"], proxy["url"])
            return proxy

        raw = get_proxy_raw_status(assigned_proxy_id)
        proxy_is_dead = raw is None or not raw["enabled"] or raw["consecutive_failures"] >= REPIN_AFTER_CONSECUTIVE_FAILURES
        if not proxy_is_dead:
            logger.warning(
                "assigned_proxy_cooling_down",
                platform=platform,
                account_key=account_key,
                assigned_proxy_id=assigned_proxy_id,
                consecutive_failures=raw["consecutive_failures"] if raw else None,
            )
            return _unavailable(platform, required)

        replacement = pin_account_to_least_loaded_proxy(platform, account_row_id)
        if replacement is None:
            logger.error(
                "account_proxy_repin_failed_pool_exhausted",
                platform=platform,
                account_key=account_key,
                previous_proxy_id=assigned_proxy_id,
                note="pinned proxy is dead and no healthy replacement exists - whole platform proxy pool may be down",
            )
            return _unavailable(platform, required)

        logger.error(
            "account_proxy_repinned",
            platform=platform,
            account_key=account_key,
            previous_proxy_id=assigned_proxy_id,
            new_proxy_url=replacement["url"],
            reason="disabled" if (raw is None or not raw["enabled"]) else "too_many_failures",
        )
        return replacement

    proxy = pin_account_to_least_loaded_proxy(platform, account_row_id)
    if proxy is None:
        return _unavailable(platform, required)
    logger.info("account_pinned_to_proxy", platform=platform, account_key=account_key, proxy_url=proxy["url"])
    return proxy


def _unavailable(platform: str, required: bool) -> ProxyRow | None:
    """Shared exit path for acquire_proxy_for_account whenever it can't get
    a usable proxy for an account that has (or is establishing) a proxy
    identity. Raises only when required=True *and* this platform actually
    has proxies configured (platform_has_any_proxy) - a platform that's
    simply never used a proxy at all still returns None either way, same as
    before sticky pinning existed."""
    if required and platform_has_any_proxy(platform):
        raise ProxyPoolExhaustedError(
            f"{platform}: no usable proxy available for this account, and running it unproxied would expose "
            "the real server IP / break its pinned-IP identity - refusing rather than doing that silently."
        )
    return None
