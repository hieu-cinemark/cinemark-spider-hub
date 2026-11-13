"""Nguồn proxy: mỗi nguồn là MỘT link reset + MỘT key.

Trước đây module này có một registry adapter (static / webtui / zingproxy / rotate_url)
và các nhà cung cấp được nhận ra bằng cách ĐOÁN theo URL/token. Nay mọi nguồn proxy
dùng chung một cơ chế duy nhất:

  - Nguồn có `api_url` = link reset (bí mật, không bao giờ log nguyên văn) và `token` = key
    của nhà cung cấp.
  - Link reset có thể chứa placeholder, thay bằng giá trị thật lúc gọi:
        {token}     - key của nhà cung cấp (token trong DB)
        {proxy}     - địa chỉ cổng proxy (host:port) của dòng platform_proxies
        {username}  - user của dòng platform_proxies
        {password}  - password của dòng platform_proxies
        {ref}       - mã riêng của cổng trong hệ thống nhà cung cấp (cột
                      platform_proxies.provider_ref) - vd webtui cần proxy_id, zingproxy
                      cần key của đúng cổng đó
    Ví dụ: webtui dùng `...?action=rotate_ip&token={token}&proxy_id={ref}`, zingproxy dùng
    `.../open/change-ip/{ref}`, gói cổng xoay cố định dùng link reset không placeholder.
  - Sau khi gọi link reset, code ĐO IP đầu ra qua chính cổng proxy để chắc chắn IP đã
    đổi (không tin vào phản hồi JSON - mỗi nhà cung cấp trả về một kiểu khác nhau).

Một dòng `platform_proxies` biết nó thuộc nguồn nào qua cột `provider_key` (NULL =
proxy nhập tay, không xoay được). Nhờ đó không còn đoán nguồn bằng cách so `proxy_url`
với token của từng gói.

Các hàm public:
  reset_proxy(provider_key, provider, proxy_row) -> (ok, egress_ip | None)
      Đổi IP của một dòng proxy qua link reset của nguồn, xác nhận bằng IP đầu ra đo
      được. Dùng cho: health check (dòng proxy ping lỗi), lỗi mạng lặp lại (comet),
      và xoay IP khi đổi tài khoản Meta sang cổng khác.
  lease_proxy(provider_key) -> {host, port, username, password} | None
      Cấp một cổng kèm IP mới, dùng một lần - cho danh tính khách synthetic của TikTok.
  static_port(provider_key) -> {host, port, username, password} | None
      Cổng cố định của nguồn mà KHÔNG xoay - cho phiên đã đăng nhập cần giữ nguyên IP
      suốt phiên (xoay IP giữa chừa một phiên đăng nhập là tín hiệu checkpoint).

Không có hàm nào ném exception ra ngoài: lỗi được trả về như kết quả, đúng như mọi lời
gọi network khác trong project này.
"""

from __future__ import annotations

import os
import re
import time
from urllib.parse import quote

import requests

from social_crawler.core import env  # noqa: F401 - import để có tác dụng phụ load_dotenv()
from social_crawler.core.logger import get_logger
from social_crawler.db.proxy_settings import (
    ProviderConfig,
    get_provider,
    get_setting,
    record_provider_use,
)

logger = get_logger(__name__)

# Mốc thời gian monotonic của lần thử gọi nhà cung cấp gần nhất theo từng nguồn - giãn cách
# chủ động giữa hai lần gọi cùng một nguồn (nhà cung cấp từ chối lời gọi quá dồn dập).
_last_call_at: dict[str, float] = {}

# Chuỗi "host:port:username:password" - định dạng cổng của nguồn cổng-xoay-cố-định (token
# lưu luôn thông tin đăng nhập của cổng).
_PROXY_STRING = re.compile(r"^(?P<host>[^:]+):(?P<port>\d+):(?P<username>[^:]+):(?P<password>.+)$")

# Sau khi gọi link reset, cổng proxy còn đi bằng IP cũ thêm vài giây (đo 2026-10-07: IP mới
# có hiệu lực sau chưa tới 4s) - chờ chừng này để request đầu tiên của lượt crawl không đi
# bằng IP vừa bỏ.
_RESET_SETTLE_SECONDS = 5.0

# Đo IP đầu ra qua chính cổng proxy - chỉ dùng để XÁC NHẬN IP đã đổi, không dùng để tìm
# IP (mỗi nhà cung cấp trả phản hồi khác nhau nên không có mẫu chung để parse).
IP_ECHO_URL = "https://api.ipify.org"
# Tổng thời gian chờ IP mới xuất hiện sau khi gọi link reset.
_EGRESS_WAIT_SECONDS = 120.0
_EGRESS_POLL_SECONDS = 5.0


def _redact(text: str, secret: str) -> str:
    """Text exception của requests có nhúng nguyên URL request - kể cả tham số query token -
    nên tuyệt đối không được log nguyên văn (đã từng bị log như vậy trong
    proxy_health_check.log/daily_run.log, cho tới 2026-09-28)."""
    return text.replace(secret, "***") if secret else text


def _timeout() -> float:
    return float(get_setting("provider_request_timeout_seconds"))


def _max_cooldown_wait() -> float:
    return float(get_setting("provider_max_cooldown_wait_seconds"))


def _min_interval() -> float:
    """Giãn cách tối thiểu giữa hai lần gọi cùng một nguồn (chống bị nhà cung cấp từ chối vì
    gọi quá dồn dập)."""
    return float(get_setting("provider_min_call_interval_seconds"))


def _wait_min_interval(provider_key: str) -> None:
    last = _last_call_at.get(provider_key)
    if last is None:
        return
    remaining = _min_interval() - (time.monotonic() - last)
    if remaining <= 0:
        return
    logger.info(
        "proxy_provider_min_interval_wait",
        provider=provider_key,
        seconds=round(remaining, 1),
        min_interval_seconds=_min_interval(),
    )
    time.sleep(remaining)


def _mark_call(provider_key: str) -> None:
    _last_call_at[provider_key] = time.monotonic()


def _cooldown_wait_seconds(message: str) -> int | None:
    """Số giây phải chờ trước lần thử lại duy nhất, parse từ thông báo từ chối "còn NN
    giây" của nhà cung cấp - None nếu trông không giống từ chối vì cooldown (lỗi khác),
    để chỗ gọi không ngủ vô ích."""
    match = re.search(r"(\d+)", message)
    if match is None:
        return None
    return min(int(match.group(1)) + 2, int(_max_cooldown_wait()))


def _parse_proxy_string(raw: str) -> dict[str, str | int | None] | None:
    """Chuỗi "host:port:username:password" -> các phần mà cột platform_proxies cần. None
    nếu dạng không khớp - chỗ gọi log, không raise."""
    match = _PROXY_STRING.match(raw.strip())
    if match is None:
        return None
    return {
        "host": match.group("host"),
        "port": int(match.group("port")),
        "username": match.group("username"),
        "password": match.group("password"),
    }


def _build_reset_url(provider: ProviderConfig, proxy: dict | None) -> str:
    """Link reset của nguồn, đã thay placeholder bằng giá trị thật.

    Placeholder: {token}, {proxy}, {username}, {password}, {ref}. Placeholder nào không có giá
    trị (vd proxy không có username) thì bị thay bằng chuỗi rỗng - link vẫn gọi được,
    nhà cung cấp tự quyết định dùng gì. {token} được URL-encode (key có thể chứa ký tự đặc
    biệt), các placeholder còn lại giữ nguyên."""
    url = provider.get("api_url") or ""
    if not url:
        return ""
    token = provider.get("token") or ""
    proxy_url = str((proxy or {}).get("url") or "")
    username = str((proxy or {}).get("username") or "")
    password = str((proxy or {}).get("password") or "")
    ref = str((proxy or {}).get("provider_ref") or "")
    for name, value in (
        ("{token}", quote(token, safe="")),
        ("{proxy}", proxy_url),
        ("{username}", username),
        ("{password}", password),
        ("{ref}", ref),
    ):
        url = url.replace(name, value)
    return url


def _egress_ip(proxy: dict, timeout: float) -> str | None:
    """IP đầu ra thật đo qua chính cổng proxy - None nếu không đo được."""
    host = str(proxy.get("url") or "")
    if not host:
        return None
    username = str(proxy.get("username") or "")
    password = str(proxy.get("password") or "")
    auth = f"{quote(username, safe='')}:{quote(password, safe='')}@" if username and password else ""
    endpoint = f"http://{auth}{host}"
    try:
        response = requests.get(
            IP_ECHO_URL,
            proxies={"http": endpoint, "https": endpoint},
            timeout=timeout,
            allow_redirects=False,
        )
    except requests.RequestException:
        # Exception request có thể chứa username/password - không log exception.
        return None
    if response.status_code != 200:
        return None
    candidate = response.text.strip()
    return candidate if re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", candidate) else None


def _wait_new_ip(proxy: dict, old_ip: str | None, deadline_seconds: float) -> str | None:
    """Chờ IP đầu ra qua cổng proxy khác IP cũ - None nếu hết thời gian."""
    deadline = time.monotonic() + deadline_seconds
    timeout = _timeout()
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        ip = _egress_ip(proxy, min(timeout, max(remaining, 1.0)))
        if ip is not None and ip != old_ip:
            return ip
        time.sleep(min(_EGRESS_POLL_SECONDS, max(remaining, 0.1)))
    return None


def reset_proxy(provider_key: str, provider: ProviderConfig, proxy: dict | None) -> tuple[bool, str | None]:
    """Đổi IP của một dòng proxy qua link reset của nguồn, xác nhận bằng IP đầu ra đo được.

    (True, ip): IP đã đổi, ip là IP đầu ra mới đo được.
    (False, None): link reset lỗi, bị từ chối, hoặc IP không đổi trong thời gian chờ."""
    reset_url = _build_reset_url(provider, proxy)
    if not reset_url:
        return False, None

    timeout = _timeout()
    old_ip = _egress_ip(proxy, timeout) if proxy else None
    for attempt in (1, 2):
        _wait_min_interval(provider_key)
        _mark_call(provider_key)
        try:
            resp = requests.get(reset_url, timeout=timeout, allow_redirects=False)
        except requests.RequestException as exc:
            logger.warning(
                "proxy_provider_reset_failed",
                provider=provider_key,
                error=_redact(str(exc), reset_url),
            )
            record_provider_use(provider_key, f"error: reset lỗi {type(exc).__name__}")
            return False, None

        body: dict = {}
        try:
            parsed = resp.json()
            body = parsed if isinstance(parsed, dict) else {}
        except ValueError:
            body = {}

        # Nhà cung cấp trả nhiều kiểu khác nhau: có link trả HTTP 200 với {"status":"success"},
        # có link trả 200 với body rỗng, có link trả 302. Quy ước: chỉ coi là thất bại khi
        # HTTP lỗi, hoặc body nói rõ là lỗi.
        rejected = not 200 <= resp.status_code < 400
        if body:
            status = str(body.get("status") or body.get("result") or "").lower()
            if status in {"error", "failed", "fail", "rejected"} or body.get("success") is False:
                rejected = True
        if not rejected:
            break

        message = str(body.get("content") or body.get("error") or body.get("message") or "")
        logger.warning(
            "proxy_provider_reset_rejected",
            provider=provider_key,
            status_code=resp.status_code,
            message=message[:150],
        )
        record_provider_use(provider_key, f"error: reset bị từ chối ({message[:120] or resp.status_code})")
        if attempt == 2:
            return False, None
        wait_seconds = _cooldown_wait_seconds(message)
        if wait_seconds is None:
            return False, None
        logger.info("proxy_provider_waiting_out_cooldown", provider=provider_key, seconds=wait_seconds)
        time.sleep(wait_seconds)

    time.sleep(_RESET_SETTLE_SECONDS)
    new_ip = _wait_new_ip(proxy, old_ip, _EGRESS_WAIT_SECONDS) if proxy else None
    if new_ip is None:
        record_provider_use(provider_key, "error: reset xong nhưng chưa thấy IP mới")
        logger.warning("proxy_provider_no_new_ip", provider=provider_key)
        return False, None
    logger.info("proxy_provider_reset_ok", provider=provider_key, old_ip=old_ip, ip=new_ip)
    record_provider_use(provider_key, "ok")
    return True, new_ip


def _provider_rows(provider_key: str) -> list[dict]:
    """Các dòng platform_proxies thuộc nguồn `provider_key`."""
    from social_crawler.db.proxies import list_proxies_by_provider

    return list_proxies_by_provider(provider_key)


def _port_pinned(proxy_url: str) -> bool:
    """Cổng này đang được ghim cho một tài khoản (mọi nền tảng, mọi trạng thái) - không bao giờ
    tự đổi IP của nó. DB lỗi -> True (accounts_on_proxy_url trả số lớn): không biết thì không đổi."""
    from social_crawler.db.proxies import accounts_on_proxy_url

    return bool(accounts_on_proxy_url(proxy_url))


def _port_of(row: dict) -> dict[str, str | int | None]:
    url = str(row["url"])
    return {
        "host": url.split(":", 1)[0],
        "port": int(url.split(":", 1)[1]) if ":" in url else None,
        "username": row.get("username"),
        "password": row.get("password"),
    }


# With several TikTok lanes (runtime/lanes.TIKTOK_LANES) each lane keeps the port it leased for this long after its
# last lease, so another lane never resets the IP under a running guest session. One lane: no claim at all.
LANE_PORT_TTL_SECONDS = int(os.getenv("TIKTOK_LANE_PORT_TTL_SECONDS", "1800"))
_LANE_PORT_KEY = "tiktok:lane_port:{url}"


def _lane_owner() -> str | None:
    """Owner string of the current lane, or None when lanes are off (nothing to claim)."""
    from social_crawler.runtime import lanes

    if lanes.TIKTOK_LANES <= 1:
        return None
    return f"{lanes.INSTANCE_ID}:{lanes.worker_lane() or 0}"


def _claim_lane_port(url: str, owner: str) -> bool:
    """Reserve a port for this lane (or refresh our own reservation). A port held by another lane is skipped. Redis
    errors count as "held": two lanes resetting one port breaks both sessions, a skipped lease is only retried later."""
    from social_crawler.clients.redis import RedisCache

    cache = RedisCache()
    key = _LANE_PORT_KEY.format(url=url)
    if cache.set_nx(key, owner, ttl_seconds=LANE_PORT_TTL_SECONDS):
        return True
    if cache.get(key) == owner:
        cache.set(key, owner, ttl_seconds=LANE_PORT_TTL_SECONDS)
        return True
    return False


def _lane_candidates(rows: list[dict]) -> list[dict]:
    """Lanes off: every row, unchanged order. Lanes on: the ONE port this lane holds (its current port first, else the
    first free one), or [] when every port belongs to another lane."""
    owner = _lane_owner()
    if owner is None:
        return rows
    from social_crawler.clients.redis import RedisCache

    cache = RedisCache()
    mine = [r for r in rows if cache.get(_LANE_PORT_KEY.format(url=r["url"])) == owner]
    for row in mine + [r for r in rows if r not in mine]:
        if _claim_lane_port(str(row["url"]), owner):
            return [row]
    logger.info("proxy_provider_lease_no_free_lane_port", ports=len(rows), owner=owner)
    return []


def lease_proxy(provider_key: str) -> dict[str, str | int | None] | None:
    """Cấp một cổng kèm IP mới, dùng một lần - cho danh tính khách synthetic của TikTok.

    IP và danh tính phải được tạo cùng nhau: một danh tính synthetic đi kèm IP đã bị TikTok
    đánh dấu thì vô ích.

    - Chỉ dùng cổng KHÔNG có tài khoản ghim: đổi IP của cổng đang ghim là phá phiên của tài khoản đó.
    - Nguồn chưa có dòng platform_proxies nào thì dùng cổng nằm trong token dạng host:port:user:pass
      (gói "cổng xoay cố định" cũ, vd tiktok_us) - như static_port.
    - Reset lỗi vẫn trả lại cổng (IP cũ) thay vì None: None làm TikTokClient bỏ cả lượt
      (2026-10-13: link reset trả 502 khoảng 3/4 số lần).
    - Nhiều làn TikTok (TIKTOK_LANES > 1): mỗi làn chỉ dùng cổng của riêng nó (_lane_candidates).
    None chỉ khi nguồn không có cổng nào dùng được - chỗ gọi tự quyết định có rơi về pool không."""
    provider = get_provider(provider_key)
    if provider is None or not provider["api_url"]:
        return None
    candidates = [row for row in _provider_rows(provider_key) if not _port_pinned(str(row["url"]))]
    if not candidates:
        port = _parse_proxy_string(provider.get("token") or "")
        if port is None or _port_pinned(f"{port['host']}:{port['port']}"):
            return None
        candidates = [
            {"url": f"{port['host']}:{port['port']}", "username": port["username"], "password": port["password"]}
        ]
    # Several TikTok lanes: keep to this lane's own port so no lane resets the IP of another lane's session.
    candidates = _lane_candidates(candidates)
    if not candidates:
        return None
    for row in candidates:
        ok, _ = reset_proxy(provider_key, provider, row)
        if ok:
            return _port_of(row)
    logger.info("proxy_provider_lease_unrotated", provider=provider_key)
    return _port_of(candidates[0])


def static_port(provider_key: str) -> dict[str, str | int | None] | None:
    """Cổng cố định của nguồn `provider_key` mà KHÔNG xoay - cho phiên đã đăng nhập (cookie
    thật) cần giữ nguyên IP suốt phiên: xoay IP giữa chừa một phiên đăng nhập là tín hiệu
    checkpoint. None khi nguồn chưa cấu hình hoặc không có dòng proxy nào."""
    provider = get_provider(provider_key)
    if provider is None:
        return None
    rows = _provider_rows(provider_key)
    if not rows:
        # Nguồn cổng-xoay-cố-định cũ: cổng nằm trong token dạng host:port:user:pass.
        port = _parse_proxy_string(provider.get("token") or "")
        if port is not None:
            record_provider_use(provider_key, "ok (giữ IP, không xoay)")
            return port
        return None
    row = rows[0]
    record_provider_use(provider_key, "ok (giữ IP, không xoay)")
    return {
        "host": str(row["url"]).split(":", 1)[0],
        "port": int(str(row["url"]).split(":", 1)[1]) if ":" in str(row["url"]) else None,
        "username": row.get("username"),
        "password": row.get("password"),
    }


def reset_provider_for_proxy_url(proxy_url: str) -> str | None:
    """Nguồn nào sở hữu cổng đúng bằng `proxy_url` thì đổi IP của nó - cho proxy_health_check
    gọi khi dòng proxy đó ping lỗi, thay vì để nó chết luôn.

    Cổng đang có tài khoản ghim thì KHÔNG đổi IP: đổi IP dưới một phiên đã đăng nhập là
    tín hiệu checkpoint mạnh. Mỗi proxy thuộc riêng một tài khoản (12/11/2026) nên đây là
    mọi tài khoản, không riêng Meta: một phiên TikTok cũng đổi IP theo cách này."""
    from social_crawler.db.proxies import provider_key_for_proxy_url

    if _port_pinned(proxy_url):
        logger.info("proxy_rotate_skipped_pinned_sessions", proxy_url=proxy_url)
        return None
    provider_key = provider_key_for_proxy_url(proxy_url)
    if provider_key is None:
        return None
    provider = get_provider(provider_key)
    if provider is None:
        return None
    proxy = _proxy_row_for_url(proxy_url)
    ok, _ = reset_proxy(provider_key, provider, proxy)
    return provider_key if ok else None


def _proxy_row_for_url(proxy_url: str) -> dict | None:
    from social_crawler.db.proxies import proxy_row_for_url

    return proxy_row_for_url(proxy_url)


def rotate_port_for_account_switch(proxy_url: str) -> tuple[bool | None, str | None]:
    """Đổi IP của cổng đã được runtime/ip_switch giữ trước khi chuyển tài khoản Meta.

    (None, None): nguồn của cổng này không xoay được, hoặc cổng đang có tài khoản ghim;
    (True, IP mới): đã đổi; (False, None): xoay thất bại.

    Cổng đang có tài khoản ghim thì KHÔNG đổi IP (12/11/2026): ip_switch.claim gọi hàm này ngay trước request đầu của
    phiên khi Redis không nhớ tài khoản dùng cổng gần nhất (ghim tay mới, key hết hạn 30 ngày, Redis mất dữ liệu) - đổi
    IP lúc đó là đúng kiểu sự cố của tài khoản 126."""
    from social_crawler.db.proxies import provider_key_for_proxy_url

    if _port_pinned(proxy_url):
        logger.info("proxy_rotate_skipped_pinned_sessions", proxy_url=proxy_url, reason="account_switch")
        return None, None
    provider_key = provider_key_for_proxy_url(proxy_url)
    if provider_key is None:
        return None, None
    provider = get_provider(provider_key)
    if provider is None or not provider["api_url"]:
        return None, None
    ok, new_ip = reset_proxy(provider_key, provider, _proxy_row_for_url(proxy_url))
    return (True, new_ip) if ok else (False, None)
