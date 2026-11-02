"""Đổi IP cổng ZingProxy đã cấu hình, xác nhận bằng IP đầu ra thật (không gọi Facebook).

proxy_providers.api_url = link /open/change-ip/<key>; token = host:port:user:pass.
Không nhúng link/key thật vào source. Response API chỉ là ACK: phải đo được IP khác qua
chính cổng proxy mới thành công. Redis lỗi, cổng bận hoặc không đo được đều từ chối.
"""

from __future__ import annotations

import hashlib
import ipaddress
import math
import re
import time
import uuid
from urllib.parse import quote, urlsplit

import redis
import requests

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.proxy_settings import get_setting, record_provider_use

logger = get_logger(__name__)
SETTLE_SECONDS = 120.0
POLL_SECONDS = 5.0
IP_ECHO_URL = "https://api.ipify.org"
_WAIT = re.compile(r"(\d+(?:\.\d+)?)\s*(?:seconds?|giây|s)\b", re.IGNORECASE)


def is_provider(provider: dict) -> bool:
    try:
        return urlsplit(provider.get("api_url") or "").hostname == "api.zingproxy.com"
    except ValueError:
        return False


def _valid_url(url: str) -> bool:
    parts = urlsplit(url)
    return (
        parts.scheme == "https"
        and parts.hostname == "api.zingproxy.com"
        and parts.port in (None, 443)
        and not (parts.username or parts.password or parts.query or parts.fragment)
        and re.fullmatch(r"/open/change-ip/[A-Za-z0-9_-]+", parts.path) is not None
    )


def _egress(proxy: dict, timeout: float) -> str | None:
    auth = f"{quote(proxy['username'], safe='')}:{quote(proxy['password'], safe='')}@"
    endpoint = f"http://{auth}{proxy['host']}:{proxy['port']}"
    try:
        response = requests.get(
            IP_ECHO_URL, proxies={"http": endpoint, "https": endpoint}, timeout=timeout, allow_redirects=False
        )
        if response.status_code != 200:
            return None
        ip = ipaddress.ip_address(response.text.strip())
        return str(ip) if ip.version == 4 and ip.is_global else None
    except requests.RequestException, ValueError:
        # Exception request có thể chứa username/password; không log exception hoặc URL.
        return None


def _cooldown(response: requests.Response, body: dict) -> float | None:
    for value in (response.headers.get("Retry-After"), body.get("retry_after"), body.get("retryAfter")):
        if isinstance(value, (int, float, str)):
            try:
                seconds = float(value)
            except ValueError:
                continue
            if math.isfinite(seconds) and seconds > 0:
                return seconds + 1
    message = str(body.get("message") or body.get("error") or body.get("content") or "")
    match = _WAIT.search(message)
    return float(match.group(1)) + 1 if match else None


def rotate(provider_key: str, provider: dict, proxy: dict) -> tuple[bool, str | None]:
    """Chỉ gọi từ cổng đã được ip_switch.claim giữ cho account mới; không reset cổng đang dùng.
    Thêm khoá theo link (hash, không chứa key) để các tiến trình không gọi cùng link đồng thời.
    """
    reset_url = provider.get("api_url") or ""
    try:
        valid = _valid_url(reset_url)
    except ValueError:
        valid = False
    if not valid:
        logger.error("zingproxy_invalid_reset_url", provider=provider_key)
        return False, None
    timeout = float(get_setting("provider_request_timeout_seconds"))
    max_wait = float(get_setting("provider_max_cooldown_wait_seconds"))
    lock_key = f"zingproxy:rotate:{hashlib.sha256(reset_url.encode()).hexdigest()}"
    owner = uuid.uuid4().hex
    ttl = math.ceil(max_wait + SETTLE_SECONDS + timeout * 4 + 60)
    cache = RedisCache()
    try:
        if not cache.set_nx(lock_key, owner, ttl):
            logger.info("zingproxy_rotation_busy", provider=provider_key)
            return False, None
    except redis.RedisError:
        logger.warning("zingproxy_rotation_lock_unavailable", provider=provider_key)
        return False, None
    try:
        old_ip = _egress(proxy, timeout)
        if old_ip is None:
            record_provider_use(provider_key, "error: không đo được IP cũ, chưa gọi đổi IP")
            return False, None
        for attempt in range(2):
            try:
                response = requests.get(reset_url, timeout=timeout, allow_redirects=False)
                try:
                    body = response.json()
                except ValueError:
                    body = {}
                if not isinstance(body, dict):
                    body = {}
            except requests.RequestException:
                # Kết quả có thể không rõ (server nhận reset nhưng response timeout): không gọi lại mù quáng.
                record_provider_use(provider_key, "error: đổi IP lỗi mạng, chưa xác nhận kết quả")
                return False, None
            wait = _cooldown(response, body)
            rejected = (
                not 200 <= response.status_code < 300
                or body.get("success") is False
                or str(body.get("status") or "").lower() in {"error", "failed", "fail"}
            )
            if wait is not None and (rejected or response.status_code == 429):
                if attempt == 0 and wait <= max_wait and response.status_code not in (401, 403):
                    logger.info("zingproxy_waiting_cooldown", provider=provider_key, seconds=wait)
                    time.sleep(wait)
                    continue
                record_provider_use(provider_key, "error: đang chờ cooldown nhà cung cấp")
                return False, None
            if rejected:
                record_provider_use(provider_key, "error: nhà cung cấp từ chối đổi IP")
                return False, None
            break
        deadline = time.monotonic() + SETTLE_SECONDS
        while (remaining := deadline - time.monotonic()) > 0:
            time.sleep(min(POLL_SECONDS, remaining))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if cache.get(lock_key) != owner:
                return False, None
            ip = _egress(proxy, min(timeout, remaining))
            if ip is not None and ip != old_ip:
                record_provider_use(provider_key, f"ok: {old_ip} -> {ip}")
                logger.info("zingproxy_rotated", provider=provider_key, old_ip=old_ip, new_ip=ip)
                return True, ip
        record_provider_use(provider_key, "error: chưa xác nhận IP đầu ra mới")
        return False, None
    except redis.RedisError:
        logger.warning("zingproxy_rotation_lock_lost", provider=provider_key)
        return False, None
    finally:
        try:
            cache.delete_if_equals(lock_key, owner)
        except redis.RedisError:
            pass  # TTL sẽ nhả, không xoá khoá có thể đã thuộc tiến trình khác.
