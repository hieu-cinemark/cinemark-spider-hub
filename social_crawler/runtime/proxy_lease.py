"""Chọn proxy cho job có tài khoản (13/11/2026): mọi proxy là proxy xoay, chia theo danh sách của từng nền tảng, luân phiên.

Thay cho ghim cố định tài khoản <-> proxy, "một proxy một tài khoản" và khoá cổng/xoay IP khi đổi tài khoản (ip_switch) - người
vận hành chốt 13/11/2026: proxy tĩnh vẫn bị ban, dùng proxy xoay cho mọi việc.

Quy tắc:
  1. Danh sách của nền tảng = mọi proxy đang bật, không cooldown, có platform = nền tảng đó; rồi các proxy 'all' (kho chung).
  2. Luân phiên: lấy proxy dùng lâu nhất chưa dùng lại (last_used_at cũ nhất) trong danh sách.
  3. Trong lúc job chạy, proxy chỉ phục vụ tài khoản của job đó (giữ chỗ trong Redis LEASE_SECONDS, client gia hạn mỗi
     request bằng touch(), job xong thì release()) - hai tài khoản không đi cùng lúc qua một IP.
  4. Danh sách của nền tảng hết proxy trống thì mượn proxy đang rảnh của nền tảng khác: nền tảng nào xong việc trước thì proxy
     của nó được job đang chờ của nền tảng khác dùng ngay.
"""

from __future__ import annotations

from typing import Any

import psycopg
import redis

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.connection import connect
from social_crawler.db.proxies import ProxyRow, _proxy_row

logger = get_logger(__name__)

# Đủ dài để không mất chỗ giữa hai request của một job; client gọi touch() mỗi request nên job dài vẫn giữ được.
LEASE_SECONDS = 600
LEASE_KEY_TMPL = "proxy_lease:{proxy_id}"


def _owner(account_key: str) -> str:
    return str(account_key).strip().lower()


def _candidates(platform: str) -> list[dict[str, Any]]:
    """Proxy dùng được, theo thứ tự ưu tiên: của nền tảng, rồi kho chung 'all', rồi của nền tảng khác (mượn); trong mỗi nhóm thì
    proxy dùng lâu nhất chưa dùng lại đứng trước."""
    with connect() as conn:
        return conn.execute(
            "SELECT id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref "
            "FROM platform_proxies WHERE enabled = true AND (cooldown_until IS NULL OR cooldown_until <= now()) "
            "ORDER BY CASE WHEN platform = %s THEN 0 WHEN platform = 'all' THEN 1 ELSE 2 END, last_used_at ASC NULLS FIRST",
            (platform,),
        ).fetchall()


def _mark_used(proxy_id: int) -> None:
    try:
        with connect() as conn:
            conn.execute("UPDATE platform_proxies SET last_used_at = now() WHERE id = %s", (proxy_id,))
    except psycopg.Error as exc:
        logger.warning("proxy_mark_used_failed", proxy_id=proxy_id, error=str(exc)[:200])


def claim(platform: str, account_key: str, cache: RedisCache | None = None) -> ProxyRow | None:
    """Proxy trống tốt nhất cho tài khoản này, đã giữ chỗ cho nó; None khi mọi proxy đang được tài khoản khác dùng / cooldown.
    Tài khoản đang giữ sẵn một proxy (job trước chưa nhả) thì nhận lại đúng proxy đó."""
    owner = _owner(account_key)
    cache = cache or RedisCache()
    try:
        rows = _candidates(platform)
    except psycopg.Error as exc:
        logger.error("proxy_candidates_failed", platform=platform, error=str(exc)[:200])
        return None
    try:
        for row in rows:
            key = LEASE_KEY_TMPL.format(proxy_id=row["id"])
            holder = cache.get(key)
            if holder == owner or (holder is None and cache.set_nx(key, owner, LEASE_SECONDS)):
                cache.expire(key, LEASE_SECONDS)
                _mark_used(row["id"])
                proxy = _proxy_row(row)
                if row["platform"] not in (platform, "all"):
                    logger.info(
                        "proxy_borrowed",
                        platform=platform,
                        account=owner,
                        proxy_id=row["id"],
                        owner_platform=row["platform"],
                    )
                return proxy
    except redis.RedisError as exc:
        # Không giữ được chỗ thì không giao: hai tài khoản có thể rơi vào cùng một IP.
        logger.error("proxy_lease_failed", platform=platform, error=str(exc)[:200])
        return None
    logger.info("proxy_all_busy", platform=platform, account=owner, candidates=len(rows))
    return None


def touch(proxy: dict | None, account_key: str | None, cache: RedisCache | None = None) -> None:
    """Gia hạn chỗ giữ sau mỗi request (chỉ khi tài khoản này vẫn đang giữ)."""
    if not proxy or not proxy.get("id") or not account_key:
        return
    try:
        (cache or RedisCache()).expire_if_equals(
            LEASE_KEY_TMPL.format(proxy_id=proxy["id"]), _owner(account_key), LEASE_SECONDS
        )
    except redis.RedisError:
        pass


def release(proxy: dict | None, account_key: str | None = None, cache: RedisCache | None = None) -> None:
    """Nhả proxy cho job kế tiếp. Có account_key thì chỉ nhả khi chính tài khoản đó đang giữ."""
    if not proxy or not proxy.get("id"):
        return
    key = LEASE_KEY_TMPL.format(proxy_id=proxy["id"])
    try:
        cache = cache or RedisCache()
        if account_key:
            cache.delete_if_equals(key, _owner(account_key))
        else:
            cache.delete(key)
    except redis.RedisError:
        pass


def busy_proxy_ids(cache: RedisCache | None = None) -> set[int]:
    """Proxy đang được một tài khoản giữ - cho việc không tài khoản (YouTube...) tránh khi mượn proxy của nền tảng khác."""
    cache = cache or RedisCache()
    try:
        with connect() as conn:
            ids = [r["id"] for r in conn.execute("SELECT id FROM platform_proxies WHERE enabled = true").fetchall()]
    except psycopg.Error:
        return set()
    return {pid for pid in ids if cache.get(LEASE_KEY_TMPL.format(proxy_id=pid))}
