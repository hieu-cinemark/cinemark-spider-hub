"""Registry các adapter của nhà cung cấp proxy - mỗi adapter biết cách xoay IP của
một cổng proxy và trả về IP đầu ra đã đo được.

Trước đây module này chỉ phục vụ một nhà cung cấp xoay IP cụ thể và các nhà cung cấp
khác được nhận ra bằng cách ĐOÁN theo URL (`"webtui" in api_url`, hostname
`api.zingproxy.com`, token dạng `host:port:user:pass`). Đoán như vậy có hai hậu quả: một
provider mới có thể bị adapter sai chỉ vì URL của nó trùng điều kiện, và không có cách
nào ghi đè. Nay mỗi gói có một cột `kind` tường minh trong proxy_providers, và code tra
adapter theo cột đó - thêm một loại nhà cung cấp mới chỉ cần viết một adapter và đăng
ký vào ADAPTERS.

Các loại (kind):
  static      - danh sách IP cố định, không xoay. Cổng do người vận hành nhập tay.
  webtui      - proxy dân dụng VN theo cổng (PPPoE), xoay bằng GET rotate_ip; cổng giữ
                nguyên, IP phía sau đổi.
  zingproxy   - link đổi IP của gói, phải đo được IP đầu ra mới qua chính cổng proxy
                mới coi là thành công.
  rotate_url  - một URL đổi IP chung: cổng cố định, gọi URL là đổi IP phía sau cổng
                (token lưu host:port:user:pass của cổng đó).

Mỗi adapter có 4 phương thức, và mỗi phương thức được một đường gọi khác dùng:

  rotate(provider_key, provider, proxy_url) -> (ok, egress_ip | None)
      Dùng khi runtime/ip_switch chuyển tài khoản Meta sang một cổng khác: phải đổi IP
      trước khi giao cổng. ok = None nghĩa là adapter không sở hữu cổng này (không phải
      lỗi); False là đã thử nhưng thất bại; True là đã đổi, egress_ip là IP công khai
      đo được sau khi xoay.
  reset_ip(provider_key, provider, proxy_url) -> (ok, egress_ip | None)
      Dùng khi một dòng platform_proxies ping lỗi (cron health check) hoặc lỗi mạng lặp
      lại (comet): đổi IP phía sau cổng rồi thử lại, thay vì để cổng chết luôn.
  lease(provider_key, provider) -> {host, port, username, password} | None
      Cấp một cổng kèm IP mới, dùng một lần - cho danh tính khách synthetic của TikTok,
      nơi IP và danh tính phải được tạo cùng nhau, một lần. Gói không có cách cấp IP mới
      (danh sách IP tĩnh) trả None.
  static_port(provider_key, provider) -> {host, port, username, password} | None
      Cổng cố định của gói mà KHÔNG cần xoay - cho phiên đã đăng nhập cần giữ nguyên IP
      suốt phiên (xoay IP giữa chừa một phiên đăng nhập là tín hiệu checkpoint).

Không có adapter nào ném exception ra ngoài: lỗi được trả về như kết quả, đúng như mọi
lời gọi network khác trong project này. Token/URL reset là bí mật - không bao giờ log
nguyên văn, chỉ log đã che.
"""

from __future__ import annotations

import re
import time
from typing import Protocol
from urllib.parse import urlsplit

import requests

from social_crawler.core import env  # noqa: F401 - import để có tác dụng phụ load_dotenv()
from social_crawler.core.logger import get_logger
from social_crawler.db.proxy_settings import (
    ProviderConfig,
    get_provider,
    get_setting,
    list_provider_keys,
    record_provider_use,
)

logger = get_logger(__name__)

# Mốc thời gian monotonic của lần thử gọi nhà cung cấp gần nhất theo từng provider key -
# giãn cách chủ động giữa hai lần gọi cùng một gói.
_last_call_at: dict[str, float] = {}

# Chuỗi "host:port:username:password" - định dạng cổng của gói rotate_url (token lưu luôn
# thông tin đăng nhập của cổng).
_PROXY_STRING = re.compile(r"^(?P<host>[^:]+):(?P<port>\d+):(?P<username>[^:]+):(?P<password>.+)$")

_IPV4 = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}")


class ProxyAdapter(Protocol):
    """Giao diện chung của mọi adapter - xem module docstring."""

    kind: str

    def rotate(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]: ...

    def reset_ip(
        self, provider_key: str, provider: ProviderConfig, proxy_url: str
    ) -> tuple[bool | None, str | None]: ...

    def static_port(self, provider_key: str, provider: ProviderConfig) -> dict[str, str | int | None] | None: ...

    def lease(self, provider_key: str, provider: ProviderConfig) -> dict[str, str | int | None] | None: ...


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
    """Giãn cách tối thiểu giữa hai lần gọi cùng một gói (chống bị nhà cung cấp từ chối vì
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


# --- rotate_url: một URL đổi IP chung, cổng cố định ------------------------------
# Gói "cổng xoay cố định": token lưu chính chuỗi host:port:user:pass của cổng, api_url
# là link reset - gọi link chỉ đổi IP phía sau cổng đó, cổng/user/pass giữ nguyên. Link
# reset tự nó là bí mật (không cần token) nên không bao giờ log nguyên văn.
#
# Link reset trả về IP mới ngay, nhưng cổng còn đi bằng IP cũ thêm vài giây (đo 2026-10-07:
# IP mới có hiệu lực sau chưa tới 4s) - chờ chừng này để request đầu tiên của lượt crawl
# không đi bằng IP vừa bỏ.
_RESET_SETTLE_SECONDS = 5.0


class RotateUrlAdapter:
    """Cổng cố định + một URL đổi IP phía sau cổng."""

    kind = "rotate_url"

    def _port_of(self, provider: ProviderConfig) -> dict[str, str | int | None] | None:
        return _parse_proxy_string(provider["token"] or "")

    def _owns(self, provider: ProviderConfig, proxy_url: str) -> bool:
        port = self._port_of(provider)
        return port is not None and f"{port['host']}:{port['port']}" == proxy_url

    def static_port(self, provider_key: str, provider: ProviderConfig) -> dict[str, str | int | None] | None:
        """Cổng của gói, KHÔNG gọi link reset - cho phiên đã đăng nhập cần giữ nguyên IP
        suốt phiên. None khi gói chưa cấu hình hoặc không phải dạng cổng cố định."""
        port = self._port_of(provider)
        if port is not None:
            record_provider_use(provider_key, "ok (giữ IP, không xoay)")
        return port

    def lease(self, provider_key: str, provider: ProviderConfig) -> dict[str, str | int | None] | None:
        """Cấp cổng kèm IP mới: xoay IP phía sau cổng rồi trả cổng đó.

        Đây là nghĩa của "lease" ở đây: một cổng cố định mà IP phía sau vừa được đổi sang
        một IP mới - đủ để một danh tính khách (TikTok) đi kèm IP mới mà không cần nhà
        cung cấp cấp một cổng mới. Không có IP mới đo được thì trả None (chỗ gọi rơi về
        pool) - vì IP cũ đã bị TikTok đánh dấu thì dùng lại cũng vô ích."""
        port = self._port_of(provider)
        if port is None or not provider["api_url"]:
            return None
        status, _ = self.reset_ip(provider_key, provider, f"{port['host']}:{port['port']}")
        if status is None:
            return None
        return port

    def rotate(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
        """(None, None): gói cổng-xoay-cố-định không dùng cho luồng chuyển tài khoản Meta.

        Lý do: IP sau cổng đổi được nhưng nhà cung cấp quyết định IP mới là ai, nên không
        bảo đảm IP mới chưa từng thuộc tài khoản khác - luồng đó cần IP đo được và kiểm tra
        được (xem runtime/ip_switch). Luồng reset (health check / lỗi mạng) thì dùng
        được."""
        return None, None

    def reset_ip(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
        """(None, None): cổng không thuộc gói này. (True, IP mới) / (False, None): đã thử
        xoay."""
        if not self._owns(provider, proxy_url) or not provider["api_url"]:
            return None, None

        reset_url = provider["api_url"]
        timeout = _timeout()
        for is_retry in (False, True):
            _wait_min_interval(provider_key)
            _mark_call(provider_key)
            try:
                resp = requests.get(reset_url, timeout=timeout)
                resp.raise_for_status()
                body = resp.json()
            except (requests.RequestException, ValueError) as exc:
                logger.warning("proxy_provider_reset_failed", provider=provider_key, error=_redact(str(exc), reset_url))
                record_provider_use(provider_key, f"error: reset lỗi {type(exc).__name__}")
                return False, None
            if str(body.get("result") or body.get("status") or "").lower() == "success":
                new_ip = body.get("ipreal")
                logger.info(
                    "proxy_provider_reset_ok",
                    provider=provider_key,
                    host=proxy_url.split(":", 1)[0],
                    ip=new_ip,
                    waited_out_cooldown=is_retry,
                )
                record_provider_use(provider_key, "ok")
                time.sleep(_RESET_SETTLE_SECONDS)
                return True, (str(new_ip) if new_ip else None)
            message = str(body.get("content") or body.get("error") or "")
            logger.warning(
                "proxy_provider_reset_rejected",
                provider=provider_key,
                status_code=body.get("statusCode"),
                message=message,
            )
            record_provider_use(provider_key, f"error: reset bị từ chối ({message[:120] or body.get('statusCode')})")
            if is_retry:
                return False, None
            wait_seconds = _cooldown_wait_seconds(message)
            if wait_seconds is None:
                return False, None
            logger.info("proxy_provider_waiting_out_cooldown", seconds=wait_seconds)
            time.sleep(wait_seconds)
        return False, None


# --- webtui.vn: proxy dân dụng VN theo cổng (PPPoE), xoay bằng API -----------------
# Một dòng proxy_providers với api_url trỏ tới API của tài khoản webtui sở hữu MỌI cổng
# của tài khoản đó: my_proxies cho biết cổng nào (ip_proxy:proxy_port) là proxy_id nào;
# rotate_ip gọi lại PPPoE (IP mới có sau ~60s, webtui chặn xoay lại trong 60s). Cổng vẫn
# giữ nguyên nên dòng platform_proxies và ghim tài khoản không đổi - chỉ IP phía sau đổi.
_WEBTUI_LIST_TTL_SECONDS = 600.0
_WEBTUI_MIN_ROTATE_GAP_SECONDS = 70.0
WEBTUI_MIN_ROTATE_GAP_SECONDS = _WEBTUI_MIN_ROTATE_GAP_SECONDS  # runtime/ip_switch chờ chừng này giữa hai lần xoay
_WEBTUI_SETTLE_MAX_SECONDS = 120.0
_WEBTUI_POLL_SECONDS = 10.0
_webtui_lists: dict[str, tuple[float, list[dict]]] = {}
# Mốc xoay gần nhất của mỗi cổng webtui - trong Redis (27/10/2026), dùng chung cho consumer,
# tiến trình con bootstrap/nuôi và cron health check: trước đó chỉ nhớ trong bộ nhớ từng
# tiến trình, tiến trình này xoay ngay sau tiến trình kia và webtui từ chối ("Chờ 34s") - job
# bị bỏ. Bị giới hạn thì CHỜ cho hết (tối đa _WEBTUI_MAX_GAP_WAIT_SECONDS) thay vì bỏ.
_WEBTUI_LAST_ROTATE_KEY = "webtui:last_rotate:{proxy_id}"
_WEBTUI_MAX_GAP_WAIT_SECONDS = 90.0
_WEBTUI_WAIT_MESSAGE = re.compile(r"(\d+)\s*s", re.IGNORECASE)
_webtui_last_rotate: dict[int, float] = {}  # dự phòng khi Redis trục trặc


class WebtuiAdapter:
    """Proxy dân dụng VN theo cổng, xoay IP bằng API của nhà cung cấp."""

    kind = "webtui"

    def static_port(self, provider_key: str, provider: ProviderConfig) -> None:
        """webtui không có "cổng cố định" trong token - cổng thuộc về tài khoản, xem my_proxies."""

    def lease(self, provider_key: str, provider: ProviderConfig) -> None:
        """webtui không cấp cổng mới - cổng của nó đã nằm trong platform_proxies và tự xoay
        IP qua rotate_ip."""

    def _get(self, api_url: str, token: str, action: str, **params: object) -> dict | None:
        timeout = _timeout()
        try:
            resp = requests.get(api_url, params={"action": action, "token": token, **params}, timeout=timeout)
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("webtui_request_failed", action=action, error=_redact(str(exc), token))
            return None
        return body if isinstance(body, dict) and body.get("success") else None

    def _find(self, provider_key: str, api_url: str, token: str, proxy_url: str) -> dict | None:
        """Mục my_proxies có ip_proxy:proxy_port == proxy_url (cache _WEBTUI_LIST_TTL_SECONDS)."""
        now = time.monotonic()
        cached = _webtui_lists.get(provider_key)
        if cached is None or now - cached[0] > _WEBTUI_LIST_TTL_SECONDS:
            body = self._get(api_url, token, "my_proxies")
            if body is None:
                return None
            cached = (now, list(body.get("proxies") or []))
            _webtui_lists[provider_key] = cached
        return next((p for p in cached[1] if f"{p.get('ip_proxy')}:{p.get('proxy_port')}" == proxy_url), None)

    def rotate(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
        """(None, None): cổng không thuộc tài khoản webtui này. (True, IP mới) / (False,
        None): đã thử xoay - thành công / không. Chờ tới khi IP công khai đổi (tối đa
        _WEBTUI_SETTLE_MAX_SECONDS) để request kế tiếp đi bằng IP mới."""
        api_url, token = provider["api_url"], provider["token"] or ""
        entry = self._find(provider_key, api_url, token, proxy_url)
        if entry is None:
            return None, None
        proxy_id = int(entry["id"])
        remaining = _WEBTUI_MIN_ROTATE_GAP_SECONDS - (time.time() - self._last_rotate_at(proxy_id))
        if remaining > _WEBTUI_MAX_GAP_WAIT_SECONDS:
            logger.info("webtui_rotate_skipped_recent", provider=provider_key, proxy_url=proxy_url)
            return False, None
        if remaining > 0:
            logger.info(
                "webtui_rotate_waiting_gap", provider=provider_key, proxy_url=proxy_url, seconds=round(remaining)
            )
            time.sleep(remaining)
        timeout = _timeout()
        for attempt in (1, 2):
            self._note_rotate(proxy_id)
            try:
                # GET với tham số trên URL (xác nhận với người vận hành 24/10/2026 - trang tài
                # liệu ghi POST nhưng sai).
                resp = requests.get(
                    api_url, params={"action": "rotate_ip", "token": token, "proxy_id": proxy_id}, timeout=timeout
                )
                body = resp.json()
            except (requests.RequestException, ValueError) as exc:
                logger.warning(
                    "webtui_rotate_failed", provider=provider_key, proxy_url=proxy_url, error=_redact(str(exc), token)
                )
                record_provider_use(provider_key, f"error: xoay {proxy_url} lỗi {type(exc).__name__}")
                return False, None
            if body.get("success"):
                break
            message = str(body.get("message") or body.get("error") or "")[:150]
            # "Chờ 34s": giới hạn 60 giây giữa hai lần xoay (một tiến trình/công cụ khác vừa
            # xoay) - chờ hết rồi thử lại một lần thay vì bỏ job.
            match = _WEBTUI_WAIT_MESSAGE.search(message)
            wait = float(match.group(1)) + 2 if match else None
            if attempt == 1 and wait is not None and wait <= _WEBTUI_MAX_GAP_WAIT_SECONDS:
                logger.info("webtui_rotate_waiting_provider", provider=provider_key, proxy_url=proxy_url, seconds=wait)
                time.sleep(wait)
                continue
            logger.warning("webtui_rotate_rejected", provider=provider_key, proxy_url=proxy_url, message=message)
            record_provider_use(provider_key, f"error: xoay {proxy_url} bị từ chối ({message})")
            return False, None
        old_ip = body.get("old_ip") or entry.get("ip_public")
        new_ip = None
        deadline = time.monotonic() + _WEBTUI_SETTLE_MAX_SECONDS
        while time.monotonic() < deadline:
            time.sleep(_WEBTUI_POLL_SECONDS)
            info = self._get(api_url, token, "check_ip", proxy_id=proxy_id)
            ip = str(((info or {}).get("proxy") or {}).get("ip_public") or "")
            # Đang gọi lại PPPoE thì check_ip trả "rotating" (đo 24/10/2026) - chỉ nhận khi đã
            # là một IP thật.
            if _IPV4.fullmatch(ip) and ip != old_ip:
                new_ip = ip
                break
        _webtui_lists.pop(provider_key, None)  # ip_public trong danh sách đã cũ
        logger.info("webtui_rotated", provider=provider_key, proxy_url=proxy_url, old_ip=old_ip, new_ip=new_ip)
        record_provider_use(provider_key, f"ok: {proxy_url} {old_ip} -> {new_ip or 'chưa thấy IP mới'}")
        # Chưa thấy IP mới sau thời gian chờ = chưa chắc đã đổi (runtime/ip_switch coi là xoay
        # hỏng).
        return (True, new_ip) if new_ip else (False, None)

    def reset_ip(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
        """webtui xoay IP qua cùng một API rotate_ip - dùng chung với rotate()."""
        return self.rotate(provider_key, provider, proxy_url)

    def _last_rotate_at(self, proxy_id: int) -> float:
        """Epoch lần xoay gần nhất của cổng (mọi tiến trình); 0 nếu chưa xoay."""
        try:
            from social_crawler.clients.redis import RedisCache

            value = RedisCache().get(_WEBTUI_LAST_ROTATE_KEY.format(proxy_id=proxy_id))
            if value is not None:
                return float(value)
        except Exception:  # noqa: BLE001 - Redis trục trặc: dùng mốc trong bộ nhớ
            pass
        return _webtui_last_rotate.get(proxy_id, 0.0)

    def _note_rotate(self, proxy_id: int) -> None:
        now = time.time()
        _webtui_last_rotate[proxy_id] = now
        try:
            from social_crawler.clients.redis import RedisCache

            RedisCache().set(_WEBTUI_LAST_ROTATE_KEY.format(proxy_id=proxy_id), now, ttl_seconds=3600)
        except Exception:  # noqa: BLE001 - Redis trục trặc: đã có mốc trong bộ nhớ
            pass


# --- static: danh sách IP cố định, không xoay -------------------------------------


class StaticAdapter:
    """Proxy nhập tay: không có API xoay nào. Dùng cho cổng do người vận hành nhập."""

    kind = "static"

    def static_port(self, provider_key: str, provider: ProviderConfig) -> None:
        """Danh sách IP tĩnh không có cổng cố định trong token."""

    def lease(self, provider_key: str, provider: ProviderConfig) -> None:
        """Danh sách IP tĩnh không cấp IP mới được - None."""

    def rotate(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
        """Cổng tĩnh không xoay được - (None, None) để chỗ gọi biết là không có API xoay
        (giữ nguyên IP, y như proxy tự xoay phía nhà cung cấp)."""
        return None, None

    def reset_ip(self, provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
        return None, None


# --- registry ---------------------------------------------------------------------

ADAPTERS: dict[str, ProxyAdapter] = {
    RotateUrlAdapter.kind: RotateUrlAdapter(),
    WebtuiAdapter.kind: WebtuiAdapter(),
    StaticAdapter.kind: StaticAdapter(),
}


def register_adapter(adapter: ProxyAdapter) -> None:
    """Đăng ký một adapter vào registry - dùng cho loại nằm ở module riêng (zingproxy) và
    để test cài adapter giả."""
    ADAPTERS[adapter.kind] = adapter


def _register_zingproxy() -> None:
    """Adapter zingproxy nằm ở module riêng (clients/zingproxy.py) vì nó dùng Redis khoá
    xoay và đo IP qua chính cổng - import muộn để tránh vòng import."""
    from social_crawler.clients import zingproxy

    register_adapter(zingproxy.ZingproxyAdapter())


_register_zingproxy()


def legacy_kind(provider: ProviderConfig) -> str:
    """Luật đoán cũ, chỉ dùng cho các gói tạo trước khi có cột `kind` (DB chưa chạy
    migration). Xem scripts/migrate_proxy_providers.py - cùng luật, để migration điền
    được đúng giá trị mà code đoán ra."""
    api_url = provider.get("api_url") or ""
    try:
        host = (urlsplit(api_url).hostname or "").lower()
    except ValueError:
        host = ""
    if "webtui" in api_url.lower():
        return "webtui"
    if host == "api.zingproxy.com":
        return "zingproxy"
    if _parse_proxy_string(provider.get("token") or "") is not None:
        return "rotate_url"
    return "static"


def adapter_for(provider: ProviderConfig) -> ProxyAdapter | None:
    """Adapter theo cột `kind` của gói. None khi gói chưa có kind (DB chưa migrate) hoặc
    loại không còn được hỗ trợ."""
    kind = (provider.get("kind") or "").strip()
    if not kind:
        kind = legacy_kind(provider)
    return ADAPTERS.get(kind)


def rotate_proxy(provider_key: str, provider: ProviderConfig, proxy_url: str) -> tuple[bool | None, str | None]:
    """Xoay IP của một cổng qua adapter của gói sở hữu nó. (None, None) khi không có
    adapter nào nhận cổng này."""
    adapter = adapter_for(provider)
    if adapter is None:
        logger.warning("proxy_provider_unknown_kind", provider=provider_key, kind=provider.get("kind"))
        return None, None
    return adapter.rotate(provider_key, provider, proxy_url)


def static_port_proxy(provider_key: str) -> dict[str, str | int | None] | None:
    """Cổng cố định của gói `provider_key` mà KHÔNG xoay - cho phiên đã đăng nhập (cookie
    thật) cần giữ nguyên IP suốt phiên: xoay IP giữa chừa một phiên đăng nhập là tín hiệu
    checkpoint. None khi gói chưa cấu hình hoặc không có cổng cố định."""
    provider = get_provider(provider_key)
    if provider is None or not provider["token"]:
        return None
    adapter = adapter_for(provider)
    if adapter is None:
        return None
    return adapter.static_port(provider_key, provider)


def lease_proxy(provider_key: str) -> dict[str, str | int | None] | None:
    """Cấp một cổng kèm IP mới, dùng một lần - cho danh tính khách synthetic của TikTok.

    IP và danh tính phải được tạo cùng nhau: một danh tính synthetic đi kèm IP đã bị TikTok
    đánh dấu thì vô dụng. Gói không xoay được (danh sách IP tĩnh) trả None - chỗ gọi rơi về
    pool proxy thường."""
    provider = get_provider(provider_key)
    if provider is None or not provider["token"]:
        return None
    adapter = adapter_for(provider)
    if adapter is None:
        return None
    return adapter.lease(provider_key, provider)


def reset_provider_for_proxy_url(proxy_url: str) -> str | None:
    """Gói nào sở hữu cổng đúng bằng `proxy_url` (host:port của một dòng platform_proxies)
    thì xoay IP của nó - cho proxy_health_check gọi khi dòng proxy đó ping lỗi, thay vì để
    nó chết luôn.

    Cổng đang có tài khoản Facebook / Threads ghim thì KHÔNG đổi IP (24/10/2026): đổi IP
    dưới một phiên đã đăng nhập là tín hiệu checkpoint mạnh (sáng 24/10 webtui hồi phục,
    health check xoay cả hai cổng, 4 tài khoản đổi IP cùng lúc). Cổng lỗi thì để circuit
    breaker cho proxy / tài khoản nghỉ như bình thường."""
    from social_crawler.db.proxies import meta_accounts_on_proxy_url

    pinned = meta_accounts_on_proxy_url(proxy_url)
    if pinned:
        logger.info("proxy_rotate_skipped_meta_sessions", proxy_url=proxy_url, meta_accounts=pinned)
        return None
    for key in list_provider_keys():
        provider = get_provider(key)
        if provider is None or not provider["token"]:
            continue
        adapter = adapter_for(provider)
        if adapter is None:
            continue
        status, _ = adapter.reset_ip(key, provider, proxy_url)
        if status is not None:
            return key
    return None


def rotate_port_for_account_switch(proxy_url: str) -> tuple[bool | None, str | None]:
    """Xoay cổng đã được runtime/ip_switch giữ trước khi chuyển tài khoản Meta.

    (None, None): không có API hỗ trợ; (True, IP mới): đã đổi; (False, None): xoay thất bại.
    """
    for key in list_provider_keys():
        provider = get_provider(key)
        if provider is None or not provider["token"]:
            continue
        status, new_ip = rotate_proxy(key, provider, proxy_url)
        if status is not None:
            return status, new_ip
    return None, None
