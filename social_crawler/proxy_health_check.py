"""Kiểm tra kết nối proxy chủ động - ping trực tiếp mọi dòng platform_proxies đang bật (một
lần thăm dò kết nối đơn giản, không phải request đầy đủ ở cấp tính năng - xem
health_check.py cho loại lỗi khác đó: "trông khoẻ nhưng không trả dữ liệu"). Cấp dữ liệu
cho đúng circuit breaker mà services/pool.acquire_proxy_for_account vốn đọc
(record_proxy_outcome trong db/proxies.py) - proxy lỗi ở đây bị cooldown y như proxy lỗi
một request crawl thật, và proxy hồi phục ở đây được xoá cooldown ngay (nhánh thành công
của record_proxy_outcome), đối xứng với cách một lượt crawl thành công thật vốn xoá nó.

Script này chỉ giúp phát hiện nhanh proxy chết - tự nó không tạm dừng gì. Khi mọi proxy
của một nền tảng đều xuống cấp, vòng lặp của crawl_request_consumer.py dính
ProxyPoolExhaustedError ở ngay lần acquire_proxy_for_account(required=True) kế tiếp và
vốn đã tự backoff + xếp hàng lại (xem các hằng PROXY_EXHAUSTED_BACKOFF_* của module đó) -
đó là nửa "phản ứng" của cùng cơ chế; script này là nửa "chủ động", bắt sự cố trước khi
một job thật phải tự phát hiện theo cách đau đớn.

Một dòng proxy dùng chung giữa các nền tảng (platform='all') được ping một lần mỗi lượt
chạy, không phải một lần cho mỗi nền tảng tham chiếu nó - ping thừa chỉ phí lời gọi mạng
và tăng trùng consecutive_failures cho cùng một IP vật lý.

Chạy định kỳ qua cron (không phải tiến trình sống lâu) - ví dụ mỗi 5 phút:

    */5 * * * * cd /path/to/spider-hub && .venv/bin/python -m social_crawler.proxy_health_check

Mã thoát luôn là 0 - lỗi được báo qua logger.error(telegram=True, ...), cùng quy ước
với health_check.py.
"""

from __future__ import annotations

import sys

from curl_cffi import requests as curl_requests

from social_crawler.clients.redis import RedisCache
from social_crawler.db import proxies
from social_crawler.db.proxy_settings import get_setting
from social_crawler.logger import get_logger
from social_crawler.services.pool import build_proxy_url

logger = get_logger(__name__)

PLATFORMS = ("facebook", "threads", "tiktok")

# URL ping/timeout/ngưỡng cảnh báo/TTL chuỗi lỗi là các giá trị health_check_* trong
# proxy_settings của dashboard (mặc định bên dưới phản ánh các hằng ban đầu).
#
# generate_204 (URL ping mặc định) - một endpoint 204-No-Content mà nhiều trình duyệt/hệ
# điều hành vốn dùng cho đúng phép kiểm tra "đường mạng có thực sự dùng được không" này:
# payload tối thiểu, không chuyển hướng, không có bộ phát hiện bot nào để kích hoạt. Có bất
# kỳ response nào (status code không quan trọng) là chứng minh đường hầm proxy + bắt tay
# TLS đã chạy; chỉ lỗi ở cấp kết nối (không nhận được response nào) mới có nghĩa là bản
# thân proxy sập - xem health_check_timeout_seconds và phần xử lý RequestsError bên dưới,
# cùng cách phân biệt "resp is None -> vấn đề mạng" mà
# comet_graphql_client.py/tiktok/client.py vốn dùng cho request thật.


def ping_proxy(proxy: proxies.ProxyRow) -> bool:
    """True nếu một request thực sự nhận được response qua proxy này - xem docstring module để
    biết vì sao bản thân status code không quan trọng."""
    proxy_url = build_proxy_url(proxy)
    try:
        curl_requests.get(
            str(get_setting("health_check_ping_url")),
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=float(get_setting("health_check_timeout_seconds")),
            impersonate="chrome",
        )
        return True
    except curl_requests.RequestsError as exc:
        logger.warning("proxy_ping_failed", proxy_url=proxy["url"], platform=proxy["platform"], error=str(exc))
        return False


def _streak_key(proxy_url: str) -> str:
    return f"proxy_health_check:{proxy_url}:consecutive_failures"


def _record_outcome(redis_cache: RedisCache, proxy: proxies.ProxyRow, *, ok: bool) -> int:
    key = _streak_key(proxy["url"])
    proxies.record_proxy_outcome(proxy["platform"], proxy["url"], success=ok)
    if ok:
        redis_cache.delete(key)
        return 0
    streak = redis_cache.incr(key)
    redis_cache.expire(key, int(float(get_setting("health_check_streak_ttl_hours")) * 3600))
    return streak


def run() -> None:
    redis_cache = RedisCache()

    # Khử trùng theo id dòng - list_proxies(platform) trả về dòng 'all' dùng chung cho mọi nền
    # tảng có thể dùng nó, nên ping theo từng nền tảng sẽ ping cùng một proxy vật lý 3 lần mỗi
    # lượt chạy.
    by_id: dict[int, proxies.ProxyRow] = {}
    for platform in PLATFORMS:
        for proxy in proxies.list_proxies(platform):
            by_id[proxy["id"]] = proxy

    if not by_id:
        logger.info("proxy_health_check_no_proxies_configured")
        return

    for proxy in by_id.values():
        ok = ping_proxy(proxy)
        streak = _record_outcome(redis_cache, proxy, ok=ok)
        if ok:
            logger.info("proxy_health_check_ok", proxy_url=proxy["url"], platform=proxy["platform"])
            continue
        logger.error(
            "proxy_health_check_failed",
            telegram=streak >= int(get_setting("health_check_alert_after_failures")),
            proxy_url=proxy["url"],
            platform=proxy["platform"],
            consecutive_failures=streak,
            hint="connectivity probe got no response through this proxy - the proxy server itself "
            "may be down, not just rate-limited by a target platform",
        )


if __name__ == "__main__":
    run()
    sys.exit(0)
