"""Writes crawl_request_consumer.py's subprocess output straight to
cinemark-api's scrape_run_logs/scrape_runs tables - see db_settings.py for
why psycopg (sync) + the shared DATABASE_URL, not a second Kafka topic.
Every write is best-effort: a log line or a final status update that fails
to land must never take down the consumer or the crawl it's watching."""

from __future__ import annotations

import os
import re
import uuid

import psycopg

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it

# structlog's console renderer colors its output for a real terminal - captured
# as plain stdout text (see crawl_request_consumer.py's _stream_subprocess_to_run_log),
# those escape codes show up as literal "[2m", "[0m", etc. garbage instead of
# color. Nothing downstream wants them, so strip at write time rather than
# re-cleaning on every read.
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _strip_ansi(line: str) -> str:
    return _ANSI_ESCAPE_RE.sub("", line)


def _dsn() -> str | None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return None
    # psycopg (sync) needs a plain postgresql:// URL - cinemark-api's .env
    # uses +asyncpg for SQLAlchemy's async engine.
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


class RunLogWriter:
    """Holds one psycopg connection open for an entire subprocess run instead
    of connecting from scratch per line - measured at ~1-1.5s of connect/TLS/
    auth round-trip *per line* against Supabase's pooler, which dominated
    total crawl wall-clock time (a 698-line run spent ~807s almost entirely
    on this, dwarfing the crawler's own ~1.5-2.5s-per-request throttle) and
    stalled the subprocess itself once its stdout pipe buffer filled up
    waiting on writes queued behind those connects."""

    def __init__(self) -> None:
        self._dsn = _dsn()
        self._conn: psycopg.Connection | None = None
        if self._dsn is not None:
            self._connect()

    def _connect(self) -> None:
        try:
            self._conn = psycopg.connect(self._dsn, connect_timeout=5, autocommit=True)
        except psycopg.Error as exc:
            print(f"[run_logs] failed to open log connection: {exc}")
            self._conn = None

    def write_line(self, run_id: str, line: str) -> None:
        if self._dsn is None:
            return
        if self._conn is None or self._conn.closed:
            self._connect()
            if self._conn is None:
                return
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scrape_run_logs (id, run_id, line, created_at) VALUES (%s, %s, %s, now())",
                    (str(uuid.uuid4()), run_id, _strip_ansi(line)),
                )
        except psycopg.Error as exc:
            print(f"[run_logs] failed to write log line for run {run_id}: {exc}")
            # Connection may be broken (dropped by the pooler, network blip) -
            # drop it so the next write_line reconnects instead of retrying
            # the same dead connection for the rest of the run.
            try:
                self._conn.close()
            except psycopg.Error:
                pass
            self._conn = None

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except psycopg.Error:
                pass
            self._conn = None


def finish_run(run_id: str, *, status: str, error: str | None = None) -> None:
    dsn = _dsn()
    if dsn is None:
        return
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status = %s, finished_at = now(), error = %s WHERE id = %s",
                (status, error, run_id),
            )
    except psycopg.Error as exc:
        print(f"[run_logs] failed to finish run {run_id}: {exc}")
