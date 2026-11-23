"""Chọn proxy cho job có tài khoản: mọi proxy là proxy xoay, chia theo danh sách của từng nền tảng, luân phiên.

Thay cho ghim cố định tài khoản <-> proxy, "một proxy một tài khoản" và khoá cổng/xoay IP khi đổi tài khoản (ip_switch) - người
vận hành chốt 13/11/2026: proxy tĩnh vẫn bị ban, dùng proxy xoay cho mọi việc. Từ 18/11/2026 dùng loại proxy tự đổi IP mỗi 60
giây: IP thoát không cố định, nên không có gì để ghim theo IP.

Quy tắc:
  1. Danh sách của nền tảng = mọi proxy đang bật, không cooldown, có platform = nền tảng đó. Proxy gán cho nền tảng nào thì
     chỉ nền tảng đó dùng, không dùng chung, không mượn.
  2. Luân phiên: lấy proxy dùng lâu nhất chưa dùng lại (last_used_at cũ nhất) trong danh sách.
  3. Trong lúc job chạy, ENDPOINT của proxy (host:port) chỉ phục vụ tài khoản của job đó (giữ chỗ trong Redis LEASE_SECONDS,
     client gia hạn mỗi request bằng touch(), job xong thì release()). Giữ chỗ theo endpoint chứ không theo id dòng: hai dòng
     platform_proxies trỏ cùng một cổng (nhân bản theo nền tảng, nhập trùng) là cùng một đường ra, cùng một IP tại mỗi thời
     điểm - trước 18/11/2026 khoá theo id nên hai tài khoản có thể cùng lúc đi qua một cổng.
  4. Hết proxy trống thì job chờ (xếp lại), không đi bằng proxy của nền tảng khác.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import redis

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db import d1, d1_pool
from social_crawler.db.proxies import ProxyRow, _proxy_row

logger = get_logger(__name__)

# Đủ dài để không mất chỗ giữa hai request của một job; client gọi touch() mỗi request nên job dài vẫn giữ được.
LEASE_SECONDS = 600
LEASE_KEY_TMPL = "proxy_lease:{endpoint}"


def endpoint(proxy_url: str | None) -> str:
    """host:port của proxy, chữ thường, bỏ scheme và thông tin đăng nhập - khoá giữ chỗ và cách nhận ra hai dòng cùng cổng."""
    raw = str(proxy_url or "").strip()
    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    host = (parsed.hostname or raw).lower()
    try:
        port = parsed.port
    except ValueError:
        port = None
    return f"{host}:{port}" if port else host


def _lease_key(proxy_url: str | None) -> str:
    return LEASE_KEY_TMPL.format(endpoint=endpoint(proxy_url))


def _owner(account_key: str) -> str:
    return str(account_key).strip().lower()


def _candidates(platform: str) -> list[dict[str, Any]]:
    """Proxy dùng được của nền tảng (đang bật, không cooldown); proxy dùng lâu nhất chưa dùng lại đứng trước."""
    return d1_pool.lease_candidates(platform)


def _mark_used(proxy_id: int) -> None:
    try:
        d1_pool.mark_proxy_used(proxy_id)
    except d1.D1Error as exc:
        logger.warning("proxy_mark_used_failed", proxy_id=proxy_id, error=exc)


def claim(platform: str, account_key: str, cache: RedisCache | None = None) -> ProxyRow | None:
    """Proxy trống tốt nhất cho tài khoản này, đã giữ chỗ cho nó; None khi mọi proxy đang được tài khoản khác dùng / cooldown.
    Tài khoản đang giữ sẵn một proxy (job trước chưa nhả) thì nhận lại đúng proxy đó."""
    owner = _owner(account_key)
    cache = cache or RedisCache()
    try:
        rows = _candidates(platform)
    except d1.D1Error as exc:
        logger.error("proxy_candidates_failed", platform=platform, error=exc)
        return None
    try:
        for row in rows:
            key = _lease_key(row["proxy_url"])
            holder = cache.get(key)
            if holder == owner or (holder is None and cache.set_nx(key, owner, LEASE_SECONDS)):
                cache.expire(key, LEASE_SECONDS)
                _mark_used(row["id"])
                return _proxy_row(row)
    except redis.RedisError as exc:
        # Không giữ được chỗ thì không giao: hai tài khoản có thể rơi vào cùng một IP.
        logger.error("proxy_lease_failed", platform=platform, error=exc)
        return None
    logger.info("proxy_all_busy", platform=platform, account=owner, candidates=len(rows))
    return None


def touch(proxy: dict | None, account_key: str | None, cache: RedisCache | None = None) -> None:
    """Gia hạn chỗ giữ sau mỗi request (chỉ khi tài khoản này vẫn đang giữ)."""
    if not proxy or not proxy.get("url") or not account_key:
        return
    try:
        (cache or RedisCache()).expire_if_equals(_lease_key(proxy["url"]), _owner(account_key), LEASE_SECONDS)
    except redis.RedisError:
        pass  # nhả lease thất bại: lease tự hết hạn sau LEASE_SECONDS  # gia hạn lease thất bại: lease tự hết hạn, job vẫn chạy


def release(proxy: dict | None, account_key: str | None = None, cache: RedisCache | None = None) -> None:
    """Nhả proxy cho job kế tiếp. Có account_key thì chỉ nhả khi chính tài khoản đó đang giữ."""
    if not proxy or not proxy.get("url"):
        return
    key = _lease_key(proxy["url"])
    try:
        cache = cache or RedisCache()
        if account_key:
            cache.delete_if_equals(key, _owner(account_key))
        else:
            cache.delete(key)
    except redis.RedisError:
        pass  # nhả lease thất bại: lease tự hết hạn sau LEASE_SECONDS
