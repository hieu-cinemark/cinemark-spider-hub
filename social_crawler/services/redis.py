
from __future__ import annotations

import json
import os
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
        raw = json.dumps(value, ensure_ascii=False)
        self._client.set(self._key(key), raw, ex=ttl_seconds)

    def delete(self, key: str) -> None:
        self._client.delete(self._key(key))

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(self._key(key)))

    def sadd(self, key: str, *members: str) -> int:
        """Add members to a set (e.g. crawled ids) - returns how many were newly added."""
        if not members:
            return 0
        return self._client.sadd(self._key(key), *members)

    def sismember(self, key: str, member: str) -> bool:
        return bool(self._client.sismember(self._key(key), member))

    def smembers(self, key: str) -> set[str]:
        return self._client.smembers(self._key(key))

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
