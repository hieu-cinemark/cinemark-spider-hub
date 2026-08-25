"""Listens on Kafka's crawl_requests topic and launches the matching
subprocess for each request - the consumer side of cinemark-api's manual
"run crawl"/"refresh token" endpoints and the daily scheduled job (see
cinemark-api's app/services/kafka.py + app/api/routes/scraper.py).

Requests are processed one at a time, not concurrently - firing several
subprocesses at once against the same Facebook session/account is exactly
the kind of burst this project's throttling/jitter elsewhere is designed to
avoid.

Run with:
    python -m social_crawler.crawl_request_consumer
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.logger import get_logger
from social_crawler.services.run_logs import RunLogWriter, finish_run

logger = get_logger(__name__)

CRAWL_REQUESTS_TOPIC = "crawl_requests"
CONSUMER_GROUP = "spider-hub.crawl-requests"

SCRAPY_BIN = str(Path(sys.executable).parent / "scrapy")
PYTHON_BIN = sys.executable

REPO_ROOT = Path(__file__).resolve().parent.parent

SPIDER_BY_PLATFORM = {"facebook": "facebook_search"}

TOKEN_REFRESH_QUERY = "tin tức hôm nay"

DEFAULT_SWEEP_DAYS = 30


def _sweep_days_for(request: dict[str, Any]) -> int:
    """facebook_search's sweep_days always covers the last N days *from
    today* (see _date_windows in the spider) - it has no concept of an
    arbitrary historical end_date. So a user-picked range doesn't map to
    literal start_date/end_date args (a single unfiltered query, capped by
    Facebook at N results total); it maps to *how many days* to sweep,
    using the range's length as that day count. No range picked at all -
    DEFAULT_SWEEP_DAYS, same as before."""
    start_raw, end_raw = request.get("start_date"), request.get("end_date")
    if start_raw and end_raw:
        start, end = date.fromisoformat(start_raw), date.fromisoformat(end_raw)
        return max((end - start).days + 1, 1)
    if start_raw:
        return max((date.today() - date.fromisoformat(start_raw)).days + 1, 1)
    return DEFAULT_SWEEP_DAYS


async def _stream_subprocess_to_run_log(args: list[str], run_id: str | None) -> int:
    """Runs args as a subprocess, writing each stdout/stderr line to
    scrape_run_logs as it arrives (if run_id is set), and returns the exit
    code. Shared by both request types below so the "capture output live,
    not just at exit" behavior stays identical between them."""
    # stderr merged into stdout: cinemark-web's Platforms page tails this as
    # one feed, and structlog's console renderer already writes to stdout
    # anyway - nothing meaningful would show up on a separate stderr stream.
    process = await asyncio.create_subprocess_exec(
        *args, cwd=REPO_ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    assert process.stdout is not None
    # One connection reused for every line of this run instead of connecting
    # fresh per line (see RunLogWriter's docstring) - opening it is itself a
    # blocking network call, so it goes through to_thread same as writes.
    log_writer = await asyncio.to_thread(RunLogWriter) if run_id else None
    try:
        async for raw_line in process.stdout:
            line = raw_line.decode(errors="replace").rstrip("\n")
            if line and log_writer is not None:
                await asyncio.to_thread(log_writer.write_line, run_id, line)
        return await process.wait()
    finally:
        if log_writer is not None:
            await asyncio.to_thread(log_writer.close)


async def _run_spider(request: dict[str, Any]) -> None:
    platform = request.get("platform")
    spider_name = SPIDER_BY_PLATFORM.get(platform)
    if spider_name is None:
        logger.warning("unsupported_platform", platform=platform, request=request)
        return

    keyword = request.get("keyword")
    if not keyword:
        logger.warning("crawl_request_missing_keyword", request=request)
        return

    run_id = request.get("run_id")

    args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"query={keyword}", "-a", "include_entities=false"]
    if request.get("keyword_id"):
        args += ["-a", f"keyword_id={request['keyword_id']}"]
    if request.get("max_pages"):
        args += ["-a", f"max_pages={request['max_pages']}"]
    # end_date anchors the sweep - without it the spider always sweeps the
    # last sweep_days days ending *today*, regardless of the picked range.
    # start_date is passed for symmetry/log readability only; sweep_days
    # (derived from the range's length, below) is what actually controls
    # how far back the sweep goes.
    if request.get("start_date"):
        args += ["-a", f"start_date={request['start_date']}"]
    if request.get("end_date"):
        args += ["-a", f"end_date={request['end_date']}"]
    # sweep_window_days deliberately omitted - the spider sizes it
    # automatically from sweep_days (see _auto_window_days in search.py)
    # instead of every caller having to pick a fixed width.
    args += ["-a", f"sweep_days={_sweep_days_for(request)}"]

    logger.info("crawl_request_started", platform=platform, keyword=keyword, keyword_id=request.get("keyword_id"))
    returncode = await _stream_subprocess_to_run_log(args, run_id)

    if returncode != 0:
        logger.error("crawl_request_failed", platform=platform, keyword=keyword, returncode=returncode)
        if run_id:
            await asyncio.to_thread(finish_run, run_id, status="failed", error=f"scrapy exited with code {returncode}")
    else:
        logger.info("crawl_request_finished", platform=platform, keyword=keyword)
        if run_id:
            await asyncio.to_thread(finish_run, run_id, status="completed")


async def _refresh_token(request: dict[str, Any]) -> None:
    """Same command scripts/refresh_token.sh's 4h cron already runs - just
    triggered on demand instead of waiting for the next tick."""
    run_id = request.get("run_id")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]

    logger.info("token_refresh_started", run_id=run_id)
    returncode = await _stream_subprocess_to_run_log(args, run_id)

    if returncode != 0:
        logger.error("token_refresh_failed", returncode=returncode)
        if run_id:
            await asyncio.to_thread(finish_run, run_id, status="failed", error=f"bootstrap exited with code {returncode}")
    else:
        logger.info("token_refresh_finished")
        if run_id:
            await asyncio.to_thread(finish_run, run_id, status="completed")


async def _handle_request(request: dict[str, Any]) -> None:
    if request.get("type") == "refresh_token":
        await _refresh_token(request)
    else:
        await _run_spider(request)


async def run() -> None:
    consumer = AIOKafkaConsumer(
        CRAWL_REQUESTS_TOPIC,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # "earliest": a request queued while this consumer happened to be
        # down should still run once it's back up, not be silently dropped -
        # crawl requests are rare enough (button clicks, one daily batch)
        # that processing a short backlog on restart is never a problem.
        auto_offset_reset="earliest",
        # Commit only after _handle_request returns (see the explicit
        # consumer.commit() call below), not on aiokafka's default 5s timer.
        # A single crawl can run for many minutes - the default timer would
        # commit a message's offset almost immediately after it's received,
        # long before the crawl it triggered actually finishes. If this
        # process then died mid-crawl, Kafka would never redeliver that
        # request - it's just gone, with no trace beyond an orphaned
        # "running" DB row.
        enable_auto_commit=False,
        # This loop awaits one whole crawl (up to sweep_days=30 by default,
        # see _sweep_days_for) before calling back into the consumer for the
        # next message - Kafka's default 5-minute max_poll_interval_ms is
        # nowhere near enough for that; once exceeded, the broker silently
        # revokes group membership mid-crawl. 1h covers any realistic single
        # sweep.
        max_poll_interval_ms=3_600_000,
    )
    await consumer.start()
    logger.info("crawl_request_consumer_started", topic=CRAWL_REQUESTS_TOPIC)
    try:
        async for message in consumer:
            try:
                await _handle_request(message.value)
            except Exception as exc:
                # One bad request must not kill the whole consumer - log and
                # move on to the next one.
                logger.error("crawl_request_error", error=str(exc), request=message.value)
            # Committed whether _handle_request succeeded or was logged and
            # skipped above - either way this message is done, not to be
            # redelivered on the next restart.
            await consumer.commit()
    except KafkaError as exc:
        logger.error("kafka_error", error=str(exc))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())
