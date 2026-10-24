"""Client gọn nhẹ cho API "get new proxy" của proxiestrust.com - cho services/pool.py xin
một IP mới trên một slot xoay vòng đã mua, khi circuit breaker của pool (xem
pool.REPIN_AFTER_CONSECUTIVE_FAILURES) kết luận một dòng platform_proxies lấy từ
proxiestrust đã chết, thay vì chỉ bỏ dòng đó và ghim lại tài khoản sang chỗ khác mãi mãi
(cách đó chỉ làm pool dùng được ngày càng nhỏ - không có gì khác trong project này hồi
sinh một dòng proxy đã chết).

Trả về None (không phải exception) khi chưa cấu hình token tương ứng, cùng quy ước với
clients/ai.py - một proxy proxiestrust không được làm mới phải hành xử y như trước khi
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
from social_crawler.db.proxy_settings import get_provider, get_setting, list_provider_keys, record_provider_use
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
            record_provider_use(provider_key, f"error: {type(exc).__name__}")
            return None

        if body.get("status") == "SUCCESS":
            proxy = parse(str(body.get(response_field) or ""))
            if proxy is None:
                logger.warning("proxiestrust_get_new_unparseable", provider=provider_key, response=body)
                record_provider_use(provider_key, "error: response không parse được")
                return None
            logger.info(
                "proxiestrust_get_new_ok",
                provider=provider_key,
                host=proxy["host"],
                ip_allowlist=ip_allowlist,
                time_seconds_to_die=body.get("time_seconds_to_die"),
                waited_out_cooldown=is_retry,
            )
            record_provider_use(provider_key, "ok")
            return proxy

        logger.warning("proxiestrust_get_new_rejected", provider=provider_key, response=body)
        record_provider_use(provider_key, f"error: {body.get('error') or body.get('statusCode') or 'bị từ chối'}")
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
    get_new_proxy. Link reset tự nó là bí mật (không cần token) - không bao giờ log nguyên văn.

    Reset thất bại vẫn trả lại chính cổng đó (IP cũ, chưa xoay) thay vì None: cổng vẫn ra đúng quốc gia của
    gói, còn None khiến chỗ gọi rơi về pool platform_proxies - với TikTok đó là một proxy Singapore
    đã chết với TikTok (2026-10-13: khoảng 3/4 lần thử rơi vào nó vì link reset trả 502)."""
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
            record_provider_use(provider_key, f"error: reset lỗi {type(exc).__name__} - dùng IP cũ")
            return _unrotated(provider_key, proxy)
        if str(body.get("result") or body.get("status") or "").lower() == "success":
            logger.info(
                "proxiestrust_reset_ok",
                provider=provider_key,
                host=proxy["host"],
                ip=body.get("ipreal"),
                waited_out_cooldown=is_retry,
            )
            record_provider_use(provider_key, "ok")
            time.sleep(_RESET_SETTLE_SECONDS)
            return proxy
        message = str(body.get("content") or body.get("error") or "")
        logger.warning(
            "proxiestrust_reset_rejected", provider=provider_key, status_code=body.get("statusCode"), message=message
        )
        record_provider_use(provider_key, f"error: reset bị từ chối ({message[:120] or body.get('statusCode')})")
        if is_retry:
            return _unrotated(provider_key, proxy)
        wait_seconds = _cooldown_wait_seconds(message)
        if wait_seconds is None:
            return _unrotated(provider_key, proxy)
        logger.info("proxiestrust_waiting_out_cooldown", seconds=wait_seconds)
        time.sleep(wait_seconds)
    return _unrotated(provider_key, proxy)


def static_port_proxy(provider_key: str) -> NewProxy | None:
    """Cổng cố định của gói "cổng xoay cố định" `provider_key`, KHÔNG gọi link reset - cho phiên đã đăng nhập
    (cookie thật) cần giữ nguyên IP suốt phiên: xoay IP giữa chừng một phiên đăng nhập là tín hiệu checkpoint.
    None khi gói chưa cấu hình hoặc không phải dạng cổng cố định (token không phải host:port:user:pass)."""
    provider = get_provider(provider_key)
    if provider is None or not provider["token"]:
        return None
    proxy = _parse_proxy_string(provider["token"])
    if proxy is not None:
        record_provider_use(provider_key, "ok (giữ IP, không xoay)")
    return proxy


# --- webtui.vn (24/10/2026): proxy dân dụng VN theo cổng (PPPoE), xoay bằng API ---
# Một dòng proxy_providers với api_url = https://proxy.webtui.vn/api.php và token = API token của tài khoản webtui sở
# hữu MỌI cổng của tài khoản đó: my_proxies cho biết cổng nào (ip_proxy:proxy_port) là proxy_id nào; rotate_ip gọi lại
# PPPoE (IP mới có sau ~60s, webtui chặn xoay lại trong 60s). Cổng vẫn giữ nguyên nên dòng platform_proxies và ghim tài
# khoản không đổi - chỉ IP phía sau đổi.
_WEBTUI_LIST_TTL_SECONDS = 600.0
_WEBTUI_MIN_ROTATE_GAP_SECONDS = 70.0
_WEBTUI_SETTLE_MAX_SECONDS = 120.0
_WEBTUI_POLL_SECONDS = 10.0
_webtui_lists: dict[str, tuple[float, list[dict]]] = {}
_webtui_last_rotate: dict[int, float] = {}


def _is_webtui(provider: dict) -> bool:
    return "webtui" in (provider.get("api_url") or "")


def _webtui_get(api_url: str, token: str, action: str, **params: object) -> dict | None:
    timeout = float(get_setting("provider_request_timeout_seconds"))
    try:
        resp = requests.get(api_url, params={"action": action, "token": token, **params}, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("webtui_request_failed", action=action, error=_redact(str(exc), token))
        return None
    return body if isinstance(body, dict) and body.get("success") else None


def _webtui_find(provider_key: str, api_url: str, token: str, proxy_url: str) -> dict | None:
    """Mục my_proxies có ip_proxy:proxy_port == proxy_url (cache _WEBTUI_LIST_TTL_SECONDS)."""
    now = time.monotonic()
    cached = _webtui_lists.get(provider_key)
    if cached is None or now - cached[0] > _WEBTUI_LIST_TTL_SECONDS:
        body = _webtui_get(api_url, token, "my_proxies")
        if body is None:
            return None
        cached = (now, list(body.get("proxies") or []))
        _webtui_lists[provider_key] = cached
    return next((p for p in cached[1] if f"{p.get('ip_proxy')}:{p.get('proxy_port')}" == proxy_url), None)


def _rotate_webtui(provider_key: str, provider: dict, proxy_url: str) -> bool | None:
    """None: cổng không thuộc tài khoản webtui này. True/False: đã thử xoay (thành công / không). Chờ tới khi IP công
    khai đổi (tối đa _WEBTUI_SETTLE_MAX_SECONDS) để request kế tiếp đi bằng IP mới."""
    api_url, token = provider["api_url"], provider["token"] or ""
    entry = _webtui_find(provider_key, api_url, token, proxy_url)
    if entry is None:
        return None
    proxy_id = int(entry["id"])
    now = time.monotonic()
    if now - _webtui_last_rotate.get(proxy_id, -1e9) < _WEBTUI_MIN_ROTATE_GAP_SECONDS:
        logger.info("webtui_rotate_skipped_recent", provider=provider_key, proxy_url=proxy_url)
        return False
    _webtui_last_rotate[proxy_id] = now
    timeout = float(get_setting("provider_request_timeout_seconds"))
    try:
        # GET với tham số trên URL (xác nhận với người vận hành 24/10/2026 - trang tài liệu ghi POST nhưng sai).
        resp = requests.get(
            api_url, params={"action": "rotate_ip", "token": token, "proxy_id": proxy_id}, timeout=timeout
        )
        body = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning(
            "webtui_rotate_failed", provider=provider_key, proxy_url=proxy_url, error=_redact(str(exc), token)
        )
        record_provider_use(provider_key, f"error: xoay {proxy_url} lỗi {type(exc).__name__}")
        return False
    if not body.get("success"):
        message = str(body.get("message") or body.get("error") or "")[:150]
        logger.warning("webtui_rotate_rejected", provider=provider_key, proxy_url=proxy_url, message=message)
        record_provider_use(provider_key, f"error: xoay {proxy_url} bị từ chối ({message})")
        return False
    old_ip = body.get("old_ip") or entry.get("ip_public")
    new_ip = None
    deadline = time.monotonic() + _WEBTUI_SETTLE_MAX_SECONDS
    while time.monotonic() < deadline:
        time.sleep(_WEBTUI_POLL_SECONDS)
        info = _webtui_get(api_url, token, "check_ip", proxy_id=proxy_id)
        ip = ((info or {}).get("proxy") or {}).get("ip_public")
        if ip and ip != old_ip:
            new_ip = ip
            break
    _webtui_lists.pop(provider_key, None)  # ip_public trong danh sách đã cũ
    logger.info("webtui_rotated", provider=provider_key, proxy_url=proxy_url, old_ip=old_ip, new_ip=new_ip)
    record_provider_use(provider_key, f"ok: {proxy_url} {old_ip} -> {new_ip or 'chưa thấy IP mới'}")
    return True


def reset_provider_for_proxy_url(proxy_url: str) -> str | None:
    """Gói cổng cố định nào có cổng đúng bằng `proxy_url` (host:port của một dòng platform_proxies) thì gọi link
    reset của gói đó để đổi IP phía sau - cho proxy_health_check gọi khi dòng proxy đó ping lỗi, thay vì để nó chết
    luôn. Tài khoản webtui sở hữu cổng đó thì gọi rotate_ip. Trả về key của gói đã reset, hoặc None nếu không có gói
    nào sở hữu cổng này."""
    for key in list_provider_keys():
        provider = get_provider(key)
        if provider is not None and provider["token"] and _is_webtui(provider):
            if _rotate_webtui(key, provider, proxy_url) is not None:
                return key
            continue
        port = _parse_proxy_string(provider["token"] or "") if provider else None
        if provider is None or port is None or not provider["api_url"]:
            continue
        if f"{port['host']}:{port['port']}" == proxy_url:
            _reset_static_port(key, provider["api_url"], port)
            return key
    return None


def _unrotated(provider_key: str, proxy: NewProxy) -> NewProxy:
    logger.info("proxiestrust_reset_skipped_using_current_ip", provider=provider_key, host=proxy["host"])
    return proxy


def _cooldown_wait_seconds(error_message: str) -> int | None:
    """Số giây phải chờ trước lần thử lại duy nhất của get_new_proxy, parse từ thông báo từ
    chối "còn NN giây" của nhà cung cấp - None nếu trông không giống từ chối vì cooldown
    (lỗi khác), để chỗ gọi không ngủ vô ích."""
    match = _COOLDOWN_MESSAGE_PATTERN.search(error_message)
    if match is None:
        return None
    return min(int(match.group(1)) + 2, int(get_setting("provider_max_cooldown_wait_seconds")))
