"""Client gọn nhẹ cho API "get new proxy" của proxiestrust.com - cho services/pool.py xin
một IP mới trên một slot xoay vòng đã mua, khi circuit breaker của pool (xem
pool.REPIN_AFTER_CONSECUTIVE_FAILURES) kết luận một dòng platform_proxies lấy từ
proxiestrust đã chết, thay vì chỉ bỏ dòng đó và ghim lại tài khoản sang chỗ khác mãi mãi
(cách đó chỉ làm pool dùng được ngày càng nhỏ - không có gì khác trong project này hồi
sinh một dòng proxy đã chết).

Trả về None (không phải exception) khi chưa cấu hình token tương ứng, cùng quy ước với
clients/kira.py - một proxy proxiestrust không được làm mới phải hành xử y như trước khi
có module này (bị bỏ, tài khoản được ghim lại sang chỗ khác), không làm crash chỗ nào gọi
vào đây.

Project này có nhiều hơn một gói/token proxiestrust.com (khác quốc gia đầu ra, mua cho
các nền tảng khác nhau) - provider_key của get_new_proxy() chọn dòng proxy_providers
nào (xem db/proxy_settings.py) để dùng; đừng bao giờ dồn nhiều chỗ gọi vào cùng một
provider chỉ vì chúng cùng gọi module này. URL API của nhà cung cấp, token, chế độ
ip_allowlist và mọi tham số thời gian bên dưới đều lấy từ trang Settings của dashboard
(proxy_settings/proxy_providers), không nằm trong code."""

from __future__ import annotations

import re
import time
from typing import TypedDict

import requests

from social_crawler import env  # noqa: F401 - import để có tác dụng phụ load_dotenv()
from social_crawler.db.proxy_settings import get_provider, get_setting
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Cooldown riêng của nhà cung cấp giữa hai lần gọi get_new trên cùng token (quan sát
# 2026-09-17: khoảng 90s; người vận hành xác nhận khoảng cách tối thiểu khoảng 60s) từ
# chối lời gọi sớm với statusCode 405 và thông báo tiếng Việt "còn NN giây" thay vì trả lại
# lease trước đó vẫn còn sống. Chỗ gọi thử lại ngay sẽ quay về một nguồn proxy tệ hơn thay
# vì lấy được IP mới. Vì vậy ta:
#   1. Giãn các lời gọi của mình cách nhau ít nhất provider_min_get_new_interval_seconds
#      (theo từng provider) để thường không bao giờ dính 405.
#   2. Nếu vẫn bị từ chối vì cooldown, chờ một lần (tối đa
#      provider_max_cooldown_wait_seconds) rồi thử lại.
_COOLDOWN_MESSAGE_PATTERN = re.compile(r"(\d+)")

# Mốc thời gian monotonic của lần thử get_new HTTP gần nhất theo từng provider key — áp
# khoảng cách tối thiểu cho cả lượt tạo danh tính synthetic của hashtag + comments dùng
# chung một provider.
_last_get_new_at: dict[str, float] = {}

# Link reset trả về IP mới ngay, nhưng cổng còn đi bằng IP cũ thêm vài giây (đo 2026-10-07: IP mới có hiệu lực sau
# chưa tới 4s) - chờ chừng này để request đầu tiên của lượt crawl không đi bằng IP vừa bỏ.
_RESET_SETTLE_SECONDS = 5.0


def _redact(text: str, token: str) -> str:
    """Text exception của requests có nhúng nguyên URL request - kể cả tham số query token - nên
    tuyệt đối không được log nguyên văn (đã từng bị log như vậy trong
    proxy_health_check.log/daily_run.log, cho tới 2026-09-28)."""
    return text.replace(token, "***") if token else text


class NewProxy(TypedDict):
    host: str
    port: int
    # None cho lease ip_allow_on - proxiestrust cấp quyền cho loại này theo IP nguồn của chỗ
    # gọi (xem tham số ip_allowlist của get_new_proxy), không theo thông tin đăng nhập, nên
    # không có gì để đặt vào URL http://user:pass@.
    username: str | None
    password: str | None


def _parse_proxy_string(raw: str) -> NewProxy | None:
    """Chuỗi "host:port:username:password" (định dạng riêng của proxiestrust, khớp với thông
    tin "PORT XOAY" mà project này đã được đưa bằng tay) -> các phần mà các cột của
    platform_proxies cần. None nếu dạng không khớp - chỗ gọi log, không raise, vì một
    response sai định dạng từ API trả phí của bên thứ ba đúng là loại chuyện phải hạ xuống
    thành "lần này không làm mới được", không làm crash circuit breaker đã gọi vào đây."""
    parts = raw.split(":")
    if len(parts) != 4:
        return None
    host, port, username, password = parts
    if not (host and port.isdigit() and username and password):
        return None
    return {"host": host, "port": int(port), "username": username, "password": password}


def _parse_ip_allow_string(raw: str) -> NewProxy | None:
    """Chuỗi "host:port" (dạng proxy_ip_allow riêng của proxiestrust - không nhúng thông tin
    đăng nhập, xem docstring ip_allowlist của get_new_proxy). None nếu dạng không khớp, cùng
    lý do hạ-xuống-không-raise như _parse_proxy_string."""
    parts = raw.split(":")
    if len(parts) != 2:
        return None
    host, port = parts
    if not (host and port.isdigit()):
        return None
    return {"host": host, "port": int(port), "username": None, "password": None}


def _wait_min_interval(provider_key: str) -> None:
    """Ngủ cho tới khi đủ ít nhất provider_min_get_new_interval_seconds kể từ lần thử get_new
    gần nhất của provider này — giãn cách chủ động để ít dính cooldown 405 của nhà cung cấp
    hơn khi hashtag/comments tạo danh tính song song."""
    last = _last_get_new_at.get(provider_key)
    if last is None:
        return
    min_interval = float(get_setting("provider_min_get_new_interval_seconds"))
    remaining = min_interval - (time.monotonic() - last)
    if remaining <= 0:
        return
    logger.info(
        "proxiestrust_min_interval_wait",
        provider=provider_key,
        seconds=round(remaining, 1),
        min_interval_seconds=min_interval,
    )
    time.sleep(remaining)


def get_new_proxy(*, provider_key: str = "proxiestrust_default") -> NewProxy | None:
    """Xin một IP mới trên một gói proxiestrust.com. provider_key chọn dòng proxy_providers
    *nào* (gói/token/URL API/chế độ ip_allowlist) để dùng - project này có nhiều hơn một tài
    khoản proxiestrust (ví dụ proxiestrust_tiktok_us cho danh tính khách synthetic của
    TikTok, xem spiders/tiktok/client.py), mỗi cái có quốc gia đầu ra/slot xoay vòng riêng,
    nên tuyệt đối không được gộp thành một provider.

    ip_allowlist=True của provider truyền tham số ip_allow_on=on riêng của nhà cung cấp và
    đọc lại proxy_ip_allow thay vì proxy - đã xác nhận thực tế (2026-09-17) loại này lấy từ
    một pool *khác*, có vẻ ít bị lạm dụng hơn nhiều so với trường `proxy` mặc định xác thực
    bằng username:password, vốn cứ xoay vòng qua cùng khoảng 4 IP đã bị TikTok chặn.
    proxiestrust tự đưa vào allowlist IP nào thực hiện lời gọi này ("hệ thống tự động lấy ip
    máy chạy tool của bạn" của họ), nên chỉ máy thực sự gọi get_new mới có quyền - ở đây ổn
    vì cùng một tiến trình gọi get_new rồi tự thực hiện request qua proxy, nhưng lease này
    không thể đưa cho một máy/IP khác với máy đã tạo ra nó.

    Chặn thread đang gọi (không chỉ lời gọi này) tối đa provider_max_cooldown_wait_seconds
    nếu ngay lần thử đầu đã dính cooldown xoay vòng của nhà cung cấp - xem comment module ở
    trên để biết vì sao chờ một lần là đáng. Cũng áp provider_min_get_new_interval_seconds
    giữa các lần thử cho cùng provider. Chỗ gọi đang chạy trên event loop nên chạy hàm này
    trong một thread (ví dụ asyncio.to_thread), như mọi lời gọi mạng chặn khác ở đây.

    None khi provider chưa cấu hình token/URL API, request (hoặc một lần thử lại sau
    cooldown) thất bại, hoặc response không parse được - mọi trường hợp đều đã được log ở
    đây nên chỗ gọi chỉ cần coi None là "không làm mới được, quay về cách lẽ ra vẫn làm"."""
    provider = get_provider(provider_key)
    if provider is None or not provider["token"]:
        return None
    if not provider["api_url"]:
        logger.warning("proxy_provider_missing_api_url", provider=provider_key)
        return None
    token = provider["token"]
    # Gói "cổng xoay cố định" (2026-10-07, vd. sp07v6): token là chính chuỗi PORT XOAY host:port:user:pass và
    # api_url là LINK RESET - gọi link chỉ đổi IP phía sau cổng đó, cổng/user/pass giữ nguyên.
    static_port = _parse_proxy_string(token)
    if static_port is not None:
        return _reset_static_port(provider_key, provider["api_url"], static_port)
    ip_allowlist = provider["ip_allowlist"]
    request_timeout = float(get_setting("provider_request_timeout_seconds"))

    params = {"token": token}
    if ip_allowlist:
        params["ip_allow_on"] = "on"
    response_field = "proxy_ip_allow" if ip_allowlist else "proxy"
    parse = _parse_ip_allow_string if ip_allowlist else _parse_proxy_string

    for is_retry in (False, True):
        _wait_min_interval(provider_key)
        _last_get_new_at[provider_key] = time.monotonic()
        try:
            resp = requests.get(provider["api_url"], params=params, timeout=request_timeout)
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("proxiestrust_get_new_failed", provider=provider_key, error=_redact(str(exc), token))
            return None

        if body.get("status") == "SUCCESS":
            proxy = parse(str(body.get(response_field) or ""))
            if proxy is None:
                logger.warning("proxiestrust_get_new_unparseable", provider=provider_key, response=body)
                return None
            logger.info(
                "proxiestrust_get_new_ok",
                provider=provider_key,
                host=proxy["host"],
                ip_allowlist=ip_allowlist,
                time_seconds_to_die=body.get("time_seconds_to_die"),
                waited_out_cooldown=is_retry,
            )
            return proxy

        logger.warning("proxiestrust_get_new_rejected", provider=provider_key, response=body)
        if is_retry:
            return None  # đã chờ hết một lần cooldown - bị từ chối lần thứ hai là lỗi thật

        wait_seconds = _cooldown_wait_seconds(str(body.get("error") or ""))
        if wait_seconds is None:
            return None  # bị từ chối vì lý do khác - chờ cũng không giúp gì
        logger.info("proxiestrust_waiting_out_cooldown", seconds=wait_seconds)
        time.sleep(wait_seconds)

    return None  # không bao giờ tới đây - chỉ để thoả type checker


def _reset_static_port(provider_key: str, reset_url: str, proxy: NewProxy) -> NewProxy | None:
    """Gọi LINK RESET của một gói cổng xoay cố định rồi trả lại chính cổng đó (IP phía sau đã đổi). Link trả
    {"result": "success", "ipreal": "<IP mới>"}; bị từ chối vì cooldown thì chờ một lần rồi thử lại, giống
    get_new_proxy. Link reset tự nó là bí mật (không cần token) - không bao giờ log nguyên văn."""
    request_timeout = float(get_setting("provider_request_timeout_seconds"))
    for is_retry in (False, True):
        _wait_min_interval(provider_key)
        _last_get_new_at[provider_key] = time.monotonic()
        try:
            resp = requests.get(reset_url, timeout=request_timeout)
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("proxiestrust_reset_failed", provider=provider_key, error=_redact(str(exc), reset_url))
            return None
        if str(body.get("result") or body.get("status") or "").lower() == "success":
            logger.info(
                "proxiestrust_reset_ok",
                provider=provider_key,
                host=proxy["host"],
                ip=body.get("ipreal"),
                waited_out_cooldown=is_retry,
            )
            time.sleep(_RESET_SETTLE_SECONDS)
            return proxy
        message = str(body.get("content") or body.get("error") or "")
        logger.warning(
            "proxiestrust_reset_rejected", provider=provider_key, status_code=body.get("statusCode"), message=message
        )
        if is_retry:
            return None
        wait_seconds = _cooldown_wait_seconds(message)
        if wait_seconds is None:
            return None
        logger.info("proxiestrust_waiting_out_cooldown", seconds=wait_seconds)
        time.sleep(wait_seconds)
    return None


def _cooldown_wait_seconds(error_message: str) -> int | None:
    """Số giây phải chờ trước lần thử lại duy nhất của get_new_proxy, parse từ thông báo từ
    chối "còn NN giây" của nhà cung cấp - None nếu trông không giống từ chối vì cooldown
    (lỗi khác), để chỗ gọi không ngủ vô ích."""
    match = _COOLDOWN_MESSAGE_PATTERN.search(error_message)
    if match is None:
        return None
    return min(int(match.group(1)) + 2, int(get_setting("provider_max_cooldown_wait_seconds")))
