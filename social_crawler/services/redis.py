from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from social_crawler.logger import get_logger

logger = get_logger(__name__)


class RedisCache:
    """Thin wrapper around redis-py: JSON-serializes values and prefixes
    every key so this project's keys don't collide with others on a shared
    Redis instance."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: int | None = None,
        password: str | None = None,
        prefix: str = "social_crawler:",
    ):
        self._prefix = prefix
        self._client = redis.Redis(
            host=host or os.environ.get("REDIS_HOST", "localhost"),
            port=port or int(os.environ.get("REDIS_PORT", "6379")),
            db=db if db is not None else int(os.environ.get("REDIS_DB", "0")),
            password=password or os.environ.get("REDIS_PASSWORD") or None,
            decode_responses=True,
        )

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def get(self, key: str) -> Any:
        raw = self._client.get(self._key(key))
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        """Retries a few times on transient Redis errors before giving up -
        without this, a brief blip right after a fresh browser login (which
        may have needed a human to solve a captcha/2FA) would crash with an
        unhandled redis.RedisError and silently discard that session, since
        it's only ever stored in Redis, never on disk."""
        raw = json.dumps(value, ensure_ascii=False)
        last_exc: redis.RedisError | None = None
        for attempt in range(1, 4):
            try:
                self._client.set(self._key(key), raw, ex=ttl_seconds)
                return
            except redis.RedisError as exc:
                last_exc = exc
                logger.warning("redis_set_failed", key=key, attempt=attempt, error=str(exc))
                if attempt < 3:
                    time.sleep(0.5 * attempt)
        logger.error("redis_set_failed_permanently", key=key, error=str(last_exc))
        raise last_exc

    def _log_op_failed(self, op: str, key: str, exc: redis.RedisError) -> None:
        """Shared warning for every write op below - unlike set() (which
        retries and is worth its own dedicated failure events), these are
        simpler fire-and-forget writes where the main gap was having *any*
        logged context (op, key) at all before the bare redis.RedisError
        propagated up to whatever generic except-and-continue caught it
        several frames away."""
        logger.warning("redis_op_failed", op=op, key=key, error=str(exc))

    def delete(self, key: str) -> None:
        try:
            self._client.delete(self._key(key))
        except redis.RedisError as exc:
            self._log_op_failed("delete", key, exc)
            raise

    def lrem_by_id(self, key: str, run_id: str) -> None:
        """Drop one queued task JSON whose id/run_id matches."""
        full = self._key(key)
        try:
            raw_items = self._client.lrange(full, 0, -1) or []
        except redis.RedisError as exc:
            self._log_op_failed("lrem_by_id", key, exc)
            raise
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if item.get("id") == run_id:
                try:
                    self._client.lrem(full, 1, raw)
                except redis.RedisError as exc:
                    self._log_op_failed("lrem_by_id", key, exc)
                    raise

    def lpush_capped(self, key: str, value: Any, cap: int) -> None:
        full = self._key(key)
        try:
            self._client.lpush(full, json.dumps(value, ensure_ascii=False))
            self._client.ltrim(full, 0, cap - 1)
        except redis.RedisError as exc:
            self._log_op_failed("lpush_capped", key, exc)
            raise

    def incr(self, key: str, amount: int = 1) -> int:
        """Atomically increment an integer counter (e.g. a rotation index) and
        return the new value - unlike a get()-then-set() round trip, this is
        safe under concurrent callers (e.g. an overlapping cron + manual run)
        since Redis's INCRBY is a single atomic operation."""
        try:
            return self._client.incrby(self._key(key), amount)
        except redis.RedisError as exc:
            self._log_op_failed("incr", key, exc)
            raise

    def expire(self, key: str, ttl_seconds: int) -> None:
        """Sets/refreshes a key's TTL without touching its value - used
        after incr() to arm a rolling window on a fresh counter's first
        increment, since incr() alone never sets an expiry (an untouched
        counter would otherwise live forever)."""
        try:
            self._client.expire(self._key(key), ttl_seconds)
        except redis.RedisError as exc:
            self._log_op_failed("expire", key, exc)
            raise

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(self._key(key)))

    def sadd(self, key: str, *members: str) -> int:
        """Add members to a set (e.g. crawled ids) - returns how many were newly added."""
        if not members:
            return 0
        try:
            return self._client.sadd(self._key(key), *members)
        except redis.RedisError as exc:
            self._log_op_failed("sadd", key, exc)
            raise

    def sismember(self, key: str, member: str) -> bool:
        try:
            return bool(self._client.sismember(self._key(key), member))
        except redis.RedisError as exc:
            self._log_op_failed("sismember", key, exc)
            raise

    def add_if_new(self, key: str, ttl_seconds: int) -> bool:
        """Atomically marks `key` as seen for ttl_seconds - True the first
        time (key didn't already exist), False if it's still within a
        previous call's TTL window. Same "was this new" contract as
        sadd(), but per-key TTL instead of one set's TTL covering every
        member alike - lets a caller re-treat the *same* id as new again
        after roughly ttl_seconds, instead of remembering it forever (see
        e.g. SEEN_POSTS_TTL_SECONDS's own comment for why permanent memory
        is wrong for content whose stats keep changing)."""
        try:
            return bool(self._client.set(self._key(key), "1", nx=True, ex=ttl_seconds))
        except redis.RedisError as exc:
            self._log_op_failed("add_if_new", key, exc)
            raise

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.RedisError as exc:
            logger.error("redis_connection_failed", error=str(exc))
            return False


def enable_dedupe_cache(spider_logger: Any) -> RedisCache | None:
    """Shared by every spider's start(): probe Redis and hand back a
    RedisCache to dedupe against if it's reachable, or None to silently fall
    back to in-run-only dedupe otherwise - callers just do
    `self._cache = enable_dedupe_cache(logger)` when their `dedupe` arg is
    enabled."""
    cache = RedisCache()
    if cache.ping():
        spider_logger.info("cross_run_dedupe_enabled")
        return cache
    spider_logger.warning("cross_run_dedupe_disabled", reason="redis_not_reachable")
    return None
