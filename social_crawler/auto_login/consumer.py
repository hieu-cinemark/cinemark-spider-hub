"""Listens on Kafka's auto_login_requests topic (see
cinemark-api's app/clients/kafka.py:publish_auto_login_request) and
runs one re-login attempt per message. Consumer counterpart to:

  * app/services/auto_login.py:run_auto_login_tick - publishes one
    message per dead-cookie account per scheduled tick (or per
    dashboard "Run now" click).
  * app/services/scheduler.py:_auto_login_tick - the dashboard-driven
    scheduler loop that publishes the above.

This is the deployment shape that supersedes spider-hub's own
auto_login/scheduler.py - the dashboard now owns "is auto-login on?",
which platform list to run, the dry-run flag, and the run interval.
spider-hub's older env-var-driven loop is left in place for legacy
operator workflows but is no longer the one the dashboard talks to.

Run with:
    python -m social_crawler.auto_login.consumer

Same one-group-per-platform pattern as crawl_request_consumer.py - a
Facebook relogin backlog never head-of-line-blocks Threads. Two
separate consumer groups (spider-hub.auto-login.facebook /
spider-hub.auto-login.threads), each reading from auto_login_requests
and immediately skipping (committing) messages for the wrong platform.

`auto_offset_reset="earliest"` is OK to leave default - a freshly-
created consumer group on a brand-new auto_login_requests topic has
nothing to replay, and a replayed old message is harmless anyway:
services/relogin.get_account_for_relogin re-checks that the account is
still eligible (enabled, not checkpointed, not flagged for a human,
still dead) before anything logs in. If a topic already had a large
backlog, the operator should still `seek to latest` once before first
deploy - same warning as the module docstring on
crawl_request_consumer.py.

Exit code: 0 on SIGTERM/SIGINT or AUTO_LOGIN_DRAIN, 1 when a consumer
loop dies (Kafka start failure, unexpected error) so systemd's
Restart=on-failure brings the process back.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import signal
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.auto_login.orchestrator import attempt_auto_login
from social_crawler.db.relogin import get_account_for_relogin
from social_crawler.logger import get_logger

logger = get_logger(__name__)

AUTO_LOGIN_REQUESTS_TOPIC = "auto_login_requests"

# Mirrors cinemark-api's CONSUMER_GROUPS entry for auto_login - one
# independent consumer loop per platform so a Facebook relogin backlog
# never head-of-line-blocks Threads. Same reasoning as
# crawl_request_consumer.py:PLATFORM_CONSUMER_GROUPS, just applied
# to auto-login.
PLATFORM_CONSUMER_GROUPS = {
    "facebook": "spider-hub.auto-login.facebook",
    "threads": "spider-hub.auto-login.threads",
}

# 4-10s gap after every real login attempt on the same platform, same
# range auto_login/scheduler.py already used when it ran the same
# logic in-process. One proven value reused rather than inventing a
# second one - and deliberately skipped after a dry run or a skipped
# message, so a dry-run tick (operator's "what would happen?") doesn't
# waste minutes enumerating candidates.
INTER_ATTEMPT_PAUSE_MIN_SECONDS = 4.0
INTER_ATTEMPT_PAUSE_MAX_SECONDS = 10.0


def _bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _should_drain() -> bool:
    """Operator-driven kill switch - lets the deployment's Stop button
    short-circuit pending work. We re-read on every message so a SIGTERM
    that flips the file (or an env reload) takes effect on the next
    poll, with no in-process reload magic. Same shape as
    crawl_request_consumer.py:is_platform_draining."""
    flag = (os.getenv("AUTO_LOGIN_DRAIN") or "").strip().lower()
    return flag in {"1", "true", "yes", "on"}


def _deserialize(raw: bytes | None) -> Any:
    """JSON-decode one message value; None for a tombstone or a payload
    that isn't JSON. Raising here would surface out of getmany() and end
    the whole consumer loop on a single poison message."""
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError, ValueError:
        return None


def _handle_message(platform: str, value: dict[str, Any]) -> bool:
    """Resolves the message's account_id into the full row attempt_auto_login
    needs and runs the login flow (which stamps the audit row itself).
    Returns whether a real login was attempted, so the caller only paces
    after those.

    Synchronous on purpose - the consumer loop runs it via
    asyncio.to_thread: attempt_auto_login drives sync Playwright (which
    refuses to start on a thread with a running event loop) and blocks on
    psycopg/IMAP, which on the loop itself would also stall the other
    platform's consumer and Kafka heartbeats.

    A bad / missing account_id, an account that's no longer eligible, or
    a Supabase hiccup is logged + skipped - never raised into the Kafka
    loop (raising would stall the partition until the operator
    intervened).
    """
    account_id = value.get("account_id")
    if account_id is None:
        logger.warning("auto_login_message_missing_account_id", platform=platform, value_keys=list(value.keys()))
        return False
    dry_run = bool(value.get("dry_run"))

    account_row = get_account_for_relogin(platform, str(account_id))
    if account_row is None:
        # Deleted, disabled, checkpointed, flagged needs_manual_login, or
        # already alive again since the tick published this (or this is a
        # replayed old message) - get_account_for_relogin re-checks all of
        # that. Log it; do NOT raise - the next message on this partition
        # has nothing to do with this one.
        logger.warning(
            "auto_login_account_ineligible_at_consume",
            platform=platform,
            account_id=account_id,
            run_id=value.get("run_id"),
        )
        return False

    outcome = attempt_auto_login(platform, account_row, dry_run=dry_run)
    logger.info(
        "auto_login_consume_done",
        platform=platform,
        account_id=account_id,
        status=outcome.status,
        needs_manual_login=outcome.needs_manual_login,
        run_id=value.get("run_id"),
    )
    return not dry_run


async def _run_consumer(platform: str) -> None:
    """Returns normally only when drained (AUTO_LOGIN_DRAIN). A Kafka start
    failure or an unexpected error propagates, so main() exits non-zero."""
    group_id = PLATFORM_CONSUMER_GROUPS[platform]
    consumer = AIOKafkaConsumer(
        AUTO_LOGIN_REQUESTS_TOPIC,
        bootstrap_servers=_bootstrap_servers(),
        group_id=group_id,
        value_deserializer=_deserialize,
        key_deserializer=lambda raw: raw.decode("utf-8", errors="replace") if raw else None,
        enable_auto_commit=False,  # commit AFTER _handle_message returns so a crash mid-handle replays
        auto_offset_reset="earliest",
    )
    try:
        await consumer.start()
    except KafkaError as exc:
        logger.error("auto_login_kafka_start_failed", platform=platform, error=str(exc))
        raise
    logger.info("auto_login_consumer_started", platform=platform, group_id=group_id)
    try:
        while True:
            try:
                # Short timeout so the drain flag gets polled at least every
                # few seconds. max_records=1: one login per poll, so the pause
                # below lands between consecutive attempts, and a backlog never
                # holds one batch past max_poll_interval_ms (that would get this
                # consumer evicted from its group, fail every commit, and
                # redeliver the batch - logging the same accounts in twice).
                batches = await consumer.getmany(timeout_ms=2000, max_records=1)
            except KafkaError as exc:
                logger.error("auto_login_kafka_poll_failed", platform=platform, error=str(exc))
                await asyncio.sleep(2.0)
                continue

            if _should_drain():
                logger.warning("auto_login_consumer_draining", platform=platform)
                return

            for tp, msgs in batches.items():
                for msg in msgs:
                    attempted = False
                    if not isinstance(msg.value, dict):
                        logger.warning(
                            "auto_login_message_unparseable",
                            platform=platform,
                            partition=tp.partition,
                            offset=msg.offset,
                        )
                    elif msg.value.get("platform") == platform:
                        try:
                            attempted = await asyncio.to_thread(_handle_message, platform, msg.value)
                        except Exception:
                            # Defensive: _handle_message already swallows its
                            # own known errors. Anything that escapes is a
                            # true bug worth logging, but the consumer must
                            # keep going so we don't stall the partition -
                            # and pace as if a login ran, to be safe.
                            logger.exception("auto_login_handle_unhandled_exception", platform=platform)
                            attempted = True
                    # Unparseable and other-platform messages are committed
                    # too: each group reads the whole topic, and skipping a
                    # message without committing it shows up as permanent
                    # lag in cinemark-api's get_consumer_lag.
                    try:
                        await consumer.commit({tp: msg.offset + 1})
                    except KafkaError as exc:
                        logger.warning("auto_login_commit_failed", platform=platform, error=str(exc))
                    if attempted and not _should_drain():
                        await asyncio.sleep(
                            random.uniform(INTER_ATTEMPT_PAUSE_MIN_SECONDS, INTER_ATTEMPT_PAUSE_MAX_SECONDS)
                        )
    finally:
        await consumer.stop()
        logger.info("auto_login_consumer_stopped", platform=platform)


async def main() -> int:
    """Starts one consumer loop per platform (facebook, threads) - same
    parallelism choice as crawl_request_consumer.py so an independent
    Playwright-on-its-own-IP setup is per-platform, not shared. See the
    module docstring for the exit code."""
    # Signal handling for graceful shutdown on systemd / Docker stop.
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop_handler(*_args: Any) -> None:
        logger.warning("auto_login_consumer_signal_received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _stop_handler)
        except NotImplementedError:
            # Some environments (notably Windows) don't support
            # add_signal_handler - fall back to default behavior.
            signal.signal(sig, lambda *_a: None)

    tasks = {platform: asyncio.create_task(_run_consumer(platform)) for platform in PLATFORM_CONSUMER_GROUPS}
    stop_task = asyncio.create_task(stop_event.wait())
    await asyncio.wait([*tasks.values(), stop_task], return_when=asyncio.FIRST_COMPLETED)
    for t in (*tasks.values(), stop_task):
        t.cancel()
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    crashed = False
    for platform, result in zip(tasks, results):
        if result is None or isinstance(result, asyncio.CancelledError):
            continue
        crashed = True
        logger.error("auto_login_consumer_crashed", platform=platform, error=repr(result))
    return 1 if crashed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
