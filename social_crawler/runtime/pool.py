"""Pool tài khoản và proxy: chọn có xét circuit-breaker và ghi kết quả, nằm trên các bảng
platform_accounts/platform_proxies (xem db/accounts.py, db/proxies.py). Về thiết kế thì
không phụ thuộc nền tảng, nhưng được nối với Facebook trước (facebook/auth/accounts.py,
spiders/comet_graphql_client.py) - Threads/TikTok có thể dùng cùng các lời gọi
acquire_*/release_* theo cùng cách sau khi đã thử nghiệm ở đó.

Pool thay thế hai thứ trước đây tách rời và yếu hơn:
  - vòng xoay round-robin đơn giản của next_account() (facebook/auth/accounts.py, trước
    khi có pool) - không biết tài khoản hiện có thật sự khoẻ không, nên một tài khoản
    bị checkpoint/giới hạn rate vẫn bị thử lại theo lịch.
  - một dòng cố định duy nhất của get_proxy() hoàn toàn không theo dõi lỗi - proxy tồi
    thì vô hình; không có gì lùi lại khỏi nó hay thậm chí log rằng nó có thể là nguyên
    nhân.

Cả hai pool có cùng dạng: acquire (chọn có xét sức khoẻ + ghi last_used_at) / release
(ghi thành công hoặc lỗi nhẹ/nặng, cập nhật cooldown_until và consecutive_failures - xem
db.record_account_outcome / db.record_proxy_outcome cho lịch backoff chính xác).

acquire_proxy_for_account() (13/11/2026): mọi proxy là proxy xoay, chia theo danh sách của từng nền tảng và luân phiên giữa
các tài khoản - không còn ghim cố định tài khoản<->proxy. Trong lúc job chạy proxy chỉ phục vụ tài khoản của job; danh sách của
nền tảng hết chỗ thì mượn proxy rảnh của nền tảng khác - xem runtime/proxy_lease.py. Chỗ gọi truyền required=True để không bao
giờ âm thầm chạy không proxy (ProxyPoolExhaustedError -> job xếp lại).
"""

from __future__ import annotations

import time

import redis
from curl_cffi import requests as curl_requests

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.accounts import Account, claim_account, has_enabled_accounts, record_account_outcome
from social_crawler.db.proxies import (
    ProxyRow,
    claim_proxy,
    platform_has_any_proxy,
    record_proxy_outcome,
)
from social_crawler.runtime import proxy_lease

logger = get_logger(__name__)

# Spider scrapy thoát với mã này khi acquire_proxy_for_account(required=True) thất bại.
# crawl_request_consumer ánh xạ ngược nó thành ProxyPoolExhaustedError để message Kafka
# được xếp hàng lại với backoff thay vì bị commit như một lần thành công lặng lẽ (exit 0).
PROXY_EXHAUSTED_EXIT_CODE = 75


class ProxyPoolExhaustedError(RuntimeError):
    """Được acquire_proxy_for_account(required=True) raise khi nền tảng này đã cấu hình proxy
    nhưng hiện không cái nào dùng được cho đúng tài khoản này (proxy đã ghim của nó sập và
    cũng không có proxy thay thế khoẻ nào). Chỗ gọi cần proxy cho lưu lượng thường ngày -
    các client phát lại GraphQL/request có ký đang chạy, không phải trình duyệt đăng nhập
    một lần - nên để lỗi này lan ra và huỷ lượt chạy thay vì bắt rồi chạy tiếp không proxy:
    làm vậy vừa lộ IP thật của server, vừa âm thầm đổi danh tính IP đã gắn của một tài khoản
    đã ghim - đúng thứ mà ghim cố định sinh ra để ngăn. Xem
    comet_graphql_client.py/tiktok/client.py, nơi bắt lỗi này và raise lại thành
    NetworkError/TikTokNetworkError riêng để nó đi qua phần xử lý thử lại/cảnh báo mà mọi
    spider vốn đã có cho proxy chết."""


def build_proxy_url(proxy: ProxyRow | dict) -> str:
    """URL http:// cho một dòng proxy (hoặc bất kỳ dict nào có url/username/password). Chỉ kèm
    thông tin đăng nhập khi có đủ cả hai - dòng proxy dùng whitelist IP có username/password
    NULL, trước đây bị hiển thị thành "http://None:None@"."""
    if proxy.get("username") and proxy.get("password"):
        return f"http://{proxy['username']}:{proxy['password']}@{proxy['url']}"
    return f"http://{proxy['url']}"


def abort_spider_for_network_error(exc: BaseException) -> None:
    """Không bao giờ trả về. Cạn pool proxy → PROXY_EXHAUSTED_EXIT_CODE để consumer xếp hàng
    lại; mọi lỗi mạng khác → exit 1 (job thất bại, không phải một lượt crawl rỗng thành
    công)."""
    import sys

    exhausted = isinstance(exc, ProxyPoolExhaustedError) or isinstance(
        getattr(exc, "__cause__", None), ProxyPoolExhaustedError
    )
    if exhausted:
        logger.error(
            "spider_aborted_proxy_exhausted",
            error=str(exc),
            exit_code=PROXY_EXHAUSTED_EXIT_CODE,
        )
        sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
    logger.error("spider_aborted_network_error", error=str(exc), exit_code=1)
    sys.exit(1)


def account_pinned_proxy_usable(platform: str, account_key: str) -> bool:
    """Không còn ghim proxy (13/11/2026): tài khoản luôn chạy được nếu nền tảng có proxy - giữ hàm cho chỗ gọi cũ."""
    return True


def acquire_account(platform: str) -> Account | None:
    """Tài khoản khoẻ nhất hiện có của nền tảng - dùng lâu nhất chưa dùng lại trong các dòng
    đang bật, không cooldown, không bị checkpoint (xem db.claim_account). None nghĩa là hiện
    không có tài khoản nào dùng được.

    Cảnh báo (logger.error, không chỉ trả None) khi nền tảng này thực sự đã cấu hình tài
    khoản nhưng tất cả hiện đều không dùng được - đó là sự cố thật (việc thu thập của cả nền
    tảng đang đứng), khác với một cấu hình mới chưa từng có tài khoản nào."""
    from social_crawler.accounts.warmup import unwarmed_account_ids

    account = claim_account(platform, exclude=unwarmed_account_ids(platform))
    if account is None:
        if has_enabled_accounts(platform):
            logger.error(
                "account_pool_exhausted",
                platform=platform,
                note="every enabled account is checkpointed or cooling down - collection is stalled for this platform",
            )
        return None
    logger.debug("account_acquired", platform=platform, account_id=account["id"])
    return account


def release_account(
    platform: str, account_id: str, *, success: bool, hard_failure: bool = False, reason: str | None = None
) -> None:
    """Ghi lại chuyện gì đã xảy ra với tài khoản mà acquire_account() giao ra. Chỉ gọi sau khi
    đã thực sự thử đăng nhập/phát lại với Facebook/Threads/v.v. - không gọi cho một lượt chạy
    chỉ dùng lại session đã cache mà không kiểm tra mới, vì như vậy sẽ reset một chuỗi lỗi
    thật trên một lượt chạy chưa hề kiểm chứng gì. reason (text lỗi kỹ thuật thô) chỉ được
    dùng khi hard_failure=True - xem db.record_account_outcome, nơi gửi nó cho AI (Bee) để có
    một chẩn đoán ngắn dễ đọc lưu trên tài khoản."""
    record_account_outcome(platform, account_id, success=success, hard_failure=hard_failure, reason=reason)


def acquire_proxy(platform: str) -> ProxyRow | None:
    """Proxy tốt nhất hiện có, không cooldown, của nền tảng - xem db.claim_proxy. None nghĩa là
    hiện không có proxy nào dùng được (chưa cấu hình, hoặc (các) proxy duy nhất có sẵn đang
    giữa cooldown) - mọi chỗ gọi vốn đã coi "không có proxy" là kết quả hợp lệ, chỉ dùng
    khi bật."""
    proxy = claim_proxy(platform)
    if proxy is not None:
        logger.debug("proxy_acquired", platform=platform, proxy_url=proxy["url"])
    return proxy


def release_proxy(proxy: ProxyRow, *, success: bool) -> None:
    """Ghi lại chuyện gì đã xảy ra khi dùng proxy mà acquire_proxy() giao ra. Nhận cả ProxyRow
    (không chỉ url) vì cột platform riêng của một dòng dùng chung ('all') có thể khác với
    nền tảng đã tìm theo - xem docstring của ProxyRow trong db/proxies.py."""
    record_proxy_outcome(proxy["platform"], proxy["url"], success=success)
    proxy_lease.release(proxy)


# Facebook/Threads: không bao giờ chạy tài khoản không qua proxy (lộ IP máy chủ), kể cả khi nền tảng chưa có proxy nào.
META_PLATFORMS = {"facebook", "threads"}


def acquire_proxy_for_account(platform: str, account_key: str | None, *, required: bool = False) -> ProxyRow | None:
    """Proxy cho một job của tài khoản: luân phiên trong danh sách proxy của nền tảng, giữ riêng cho tài khoản trong lúc job
    chạy, hết chỗ thì mượn proxy rảnh của nền tảng khác (runtime/proxy_lease.py). Không có tài khoản (slot đăng nhập tay) thì
    lấy proxy dùng lâu nhất chưa dùng lại của nền tảng, không giữ chỗ.

    required=True raise ProxyPoolExhaustedError thay vì trả None khi không còn proxy nào - job được xếp lại chờ proxy trống,
    không chạy bằng IP máy chủ."""
    proxy = proxy_lease.claim(platform, account_key) if account_key else acquire_proxy(platform)
    if proxy is None:
        return _unavailable(platform, required)
    return proxy


_EGRESS_KEY_TMPL = "proxy_ip:account_egress:{account}"
_EGRESS_TTL_SECONDS = 30 * 86400
_EGRESS_URL = "https://api.ipify.org"


def check_login_egress(
    platform: str, account_key: str, proxy: dict[str, str | None], cache: RedisCache | None = None
) -> str:
    """IP đầu ra thật của proxy (dạng cấu hình Playwright) trước khi mở phiên đăng nhập / Restore. Không đo được thì raise - không
    mở tài khoản trên một đường mạng chưa xác nhận. Đổi IP giữa các phiên chỉ ghi log, không chặn: IP nhà mạng đổi là bình
    thường; log này để đối chiếu thời điểm khi có checkpoint. Không gửi gì tới nền tảng."""
    server = str(proxy["server"]).removeprefix("http://")
    auth = f"{proxy['username']}:{proxy['password']}@" if proxy.get("username") and proxy.get("password") else ""
    url = f"http://{auth}{server}"
    try:
        resp = curl_requests.get(_EGRESS_URL, proxies={"http": url, "https": url}, timeout=15)
        ip = resp.text.strip() if resp.status_code == 200 else ""
    except curl_requests.RequestsError as exc:
        raise ProxyPoolExhaustedError(f"{platform}: cannot verify egress IP of {server}") from exc
    if not ip:
        raise ProxyPoolExhaustedError(f"{platform}: cannot verify egress IP of {server}")
    account = account_key.strip().lower()
    try:
        cache = cache or RedisCache()
        key = _EGRESS_KEY_TMPL.format(account=account)
        previous = cache.get(key)
        cache.set(key, {"ip": ip, "proxy": server, "at": int(time.time())}, ttl_seconds=_EGRESS_TTL_SECONDS)
    except redis.RedisError as exc:
        logger.warning("account_egress_record_failed", account=account, error=str(exc)[:200])
        previous = None
    logger.info(
        "account_egress",
        platform=platform,
        account=account,
        ip=ip,
        proxy=server,
        previous_ip=(previous or {}).get("ip"),
        previous_proxy=(previous or {}).get("proxy"),
        changed=bool(previous) and previous.get("ip") != ip,
    )
    return ip


def _unavailable(platform: str, required: bool) -> ProxyRow | None:
    """Lối thoát dùng chung của acquire_proxy_for_account mỗi khi không lấy được proxy dùng
    được cho một tài khoản đã có (hoặc đang thiết lập) danh tính proxy. Chỉ raise khi
    required=True *và* nền tảng này thực sự đã cấu hình proxy (platform_has_any_proxy) - một
    nền tảng chưa bao giờ dùng proxy vẫn trả None dù thế nào, như trước khi có ghim cố định."""
    if required and (platform in META_PLATFORMS or platform_has_any_proxy(platform)):
        raise ProxyPoolExhaustedError(
            f"{platform}: no usable proxy available for this account, and running it unproxied would expose "
            "the real server IP / break its pinned-IP identity - refusing rather than doing that silently."
        )
    return None
