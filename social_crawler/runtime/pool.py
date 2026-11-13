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

acquire_proxy_for_account() thêm việc ghim cố định tài khoản↔proxy bên trên
acquire_proxy() - xem docstring của nó bên dưới. Mọi chỗ gọi thật
(comet_graphql_client.py, tiktok/client.py, bootstrap.py của từng nền tảng) nên đi qua
nó thay vì gọi thẳng acquire_proxy(), nếu không bảo đảm ghim không thực sự
giữ được. Nó cũng tự chữa một proxy đã ghim bị chết (ghim lại sau
REPIN_AFTER_CONSECUTIVE_FAILURES) và, với chỗ gọi truyền required=True, từ chối âm thầm
quay về chạy không proxy bằng cách raise ProxyPoolExhaustedError.
"""

from __future__ import annotations

import ipaddress
import os
import time

import redis
from curl_cffi import requests as curl_requests

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events
from social_crawler.db.accounts import Account, claim_account, has_enabled_accounts, record_account_outcome
from social_crawler.db.proxies import (
    AccountLookupError,
    ProxyRow,
    claim_proxy,
    get_account_proxy_assignment,
    get_proxy_by_id,
    get_proxy_raw_status,
    mark_proxy_rotating,
    mark_proxy_used,
    meta_proxy_violation,
    pin_account_to_free_proxy,
    platform_has_any_proxy,
    record_proxy_outcome,
    shared_proxy_violation,
)
from social_crawler.db.proxy_settings import get_setting
from social_crawler.runtime import ip_switch

logger = get_logger(__name__)

# Một proxy đã ghim chịu được bao nhiêu lần lỗi liên tiếp trước khi bị coi là chết thay vì
# chỉ đang cooldown tạm thời - xem acquire_proxy_for_account. Giờ là
# repin_after_consecutive_failures trong proxy_settings của dashboard (mặc định 5). Cố ý
# không phải 1: một lần trục trặc (timeout, nhà cung cấp xoay IP giữa request) không nên
# đốt cả danh tính IP của tài khoản; chỉ proxy cứ lỗi qua nhiều chu kỳ cooldown riêng biệt
# mới bị thay.

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
    """Tài khoản này có chạy được ngay bây giờ mà không dính một ghim đang cooldown không. True
    khi chưa ghim (lần dùng đầu sẽ ghim một proxy khoẻ) hoặc khi proxy đã ghim hiện nhận
    được qua get_proxy_by_id. False khi đã ghim vào một proxy đang giữa cooldown - các chỗ
    gọi như next_account của TikTok bỏ qua những tài khoản đó để vòng xoay không phí lần thử
    vào tài khoản sẽ raise ProxyPoolExhaustedError ngay."""
    try:
        assignment = get_account_proxy_assignment(platform, account_key)
    except AccountLookupError:
        return False
    if assignment is None:
        return True
    _, assigned_proxy_id = assignment
    if assigned_proxy_id is None:
        return True
    return get_proxy_by_id(assigned_proxy_id) is not None


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


def acquire_proxy_for_account(platform: str, account_key: str | None, *, required: bool = False) -> ProxyRow | None:
    """Proxy đã ghim cố định cho tài khoản này - mọi chỗ gọi crawl/bootstrap thật nên dùng hàm
    này thay vì gọi thẳng acquire_proxy(), để cùng một tài khoản luôn hiện ra cùng một IP
    thay vì rải ngẫu nhiên trên cả pool proxy (xem docstring của
    platform_accounts.assigned_proxy_id trong scripts/dev_db_schema.sql để biết vì sao điều
    đó quan trọng - đây là một trong những tín hiệu nhiều tài khoản mạnh nhất mà hệ thống
    phát hiện gian lận của FB/Threads/TikTok tìm kiếm). account_key là bất cứ thứ gì định
    danh tài khoản với chỗ gọi - account_id, email, hoặc chính key Redis "tài khoản đang
    active" đã chuẩn hoá mà comet_graphql_client.py vốn theo dõi - xem
    db.get_account_proxy_assignment để biết cách nó được khớp với một dòng.

    - account_key là None (không có ngữ cảnh tài khoản thật - slot đăng nhập tay
      DEFAULT_ACCOUNT_KEY) -> quay về hành vi không ghim cũ của acquire_proxy().
    - tài khoản chưa có assigned_proxy_id (lần đầu tiên lấy proxy) -> tự ghim vào một proxy
      CHƯA CÓ tài khoản nào khác (nguyên tắc "một proxy, một tài khoản" - xem
      db.pin_account_to_free_proxy). Không còn proxy trống thì trả None: tài khoản phải
      được ghim tay trên dashboard, không được chen vào một proxy đang có người dùng.
    - tài khoản đã được ghim nhưng đúng proxy đó hiện không dùng được -> giữ ghim và trả None
      (bỏ qua lượt này) nếu proxy chỉ đang cooldown tạm thời vì một lần trục trặc; còn nếu nó
      đã bị tắt hẳn hoặc đã lỗi liên tiếp repin_after_consecutive_failures (proxy_settings)
      lần, nó bị coi là chết và tài khoản được ghim lại sang một proxy trống mới (tự
      chữa, log rõ ràng vì đây là thay đổi danh tính IP đáng để người nhận ra).

    required=True raise ProxyPoolExhaustedError thay vì trả None khi nền tảng này đã cấu hình
    proxy nhưng hiện không cái nào dùng được cho tài khoản này - xem docstring của exception
    đó để biết lý do. Truyền giá trị này từ các client crawl thường ngày
    (comet_graphql_client.py, tiktok/client.py); để False (mặc định) cho proxy trình duyệt
    đăng nhập một lần, chỉ dùng khi bật, trong bootstrap.py của từng nền tảng, vốn được thiết
    kế để chấp nhận "không có proxy" là kết quả bình thường.

    Facebook/Threads (02/11/2026): có account_key thì không bao giờ tự chọn đường mạng khác để cứu lượt chạy. DB lỗi hoặc không
    có dòng tài khoản -> raise, kể cả required=False; required=True không bao giờ trả None; proxy đã ghim chết không tự ghim lại
    (xem META_AUTO_REPIN). Trước đó các trường hợp này giao ra một proxy bất kỳ hoặc None - và comet_graphql_client với None thì
    phát lại cookie từ IP của máy chủ.
    """
    if not account_key:
        return acquire_proxy(platform)

    fail_closed = platform in ip_switch.PLATFORMS
    try:
        assignment = get_account_proxy_assignment(platform, account_key)
    except AccountLookupError as exc:
        if fail_closed or required:
            raise ProxyPoolExhaustedError(str(exc)) from exc
        return None
    if assignment is None:
        if fail_closed:
            raise ProxyPoolExhaustedError(
                f"{platform}: no account row for {account_key!r} - refusing to pick a proxy for it"
            )
        return acquire_proxy(platform)
    account_row_id, assigned_proxy_id = assignment

    if assigned_proxy_id is not None:
        proxy = get_proxy_by_id(assigned_proxy_id)
        if proxy is not None:
            if not _can_hand_out(platform, proxy, account_key):
                return _unavailable(platform, required)
            mark_proxy_used(proxy["platform"], proxy["url"])
            return proxy

        raw = get_proxy_raw_status(assigned_proxy_id)
        repin_after = int(get_setting("repin_after_consecutive_failures"))
        proxy_is_dead = raw is None or not raw["enabled"] or raw["consecutive_failures"] >= repin_after
        if not proxy_is_dead:
            logger.warning(
                "assigned_proxy_cooling_down",
                platform=platform,
                account_key=account_key,
                assigned_proxy_id=assigned_proxy_id,
                consecutive_failures=raw["consecutive_failures"] if raw else None,
            )
            return _unavailable(platform, required)

        # Gán proxy là cố định (12/11/2026): KHÔNG nền tảng nào tự đổi proxy của tài khoản khi proxy đã ghim chết - trước
        # đó TikTok vẫn tự ghim lại, kể cả khi DB lỗi thoáng qua (get_proxy_raw_status trả None = "chết").
        if not _meta_auto_repin():
            # Đổi proxy là đổi môi trường xác thực của phiên: dừng và báo, người vận hành ghim lại trên dashboard.
            logger.error(
                "account_proxy_repin_required",
                telegram=True,
                platform=platform,
                account_key=account_key,
                previous_proxy_id=assigned_proxy_id,
                reason="disabled" if (raw is None or not raw["enabled"]) else "too_many_failures",
            )
            return _unavailable(platform, required)

        replacement = pin_account_to_free_proxy(platform, account_row_id)
        if replacement is None:
            logger.error(
                "account_proxy_repin_failed_no_free_proxy",
                platform=platform,
                account_key=account_key,
                previous_proxy_id=assigned_proxy_id,
                note="pinned proxy is dead and no free proxy exists - every proxy already belongs to "
                "another account; pin one manually on the dashboard",
            )
            return _unavailable(platform, required)

        if not _can_hand_out(platform, replacement, account_key):
            return _unavailable(platform, required)
        logger.error(
            "account_proxy_repinned",
            platform=platform,
            account_key=account_key,
            previous_proxy_id=assigned_proxy_id,
            new_proxy_url=replacement["url"],
            reason="disabled" if (raw is None or not raw["enabled"]) else "too_many_failures",
        )
        return replacement

    proxy = pin_account_to_free_proxy(platform, account_row_id)
    if proxy is None:
        return _unavailable(platform, required)
    logger.info("account_pinned_to_proxy", platform=platform, account_key=account_key, proxy_url=proxy["url"])
    if not _can_hand_out(platform, proxy, account_key):
        return _unavailable(platform, required)
    return proxy


def _can_hand_out(platform: str, proxy: ProxyRow, account_key: str) -> bool:
    """Mọi đường giao proxy cho tài khoản, kể cả vừa tự ghim: quy tắc "một proxy, một
    tài khoản" (mọi nền tảng), rồi quy tắc nhãn IP của Meta, rồi quyền dùng cổng, rồi IP thoát."""
    if violation := shared_proxy_violation(proxy["url"], account_key):
        _refuse_by_ip_policy(platform, account_key, proxy, violation)
        return False
    if platform in ip_switch.PLATFORMS and (violation := meta_proxy_violation(platform, proxy, account_key)):
        _refuse_by_ip_policy(platform, account_key, proxy, violation)
        return False
    # Mỗi IP một tài khoản Meta: cổng bận với tài khoản khác / đổi tài khoản mà không xoay được IP -> không giao.
    if not ip_switch.claim(platform, proxy, account_key):
        return False
    return platform != "facebook" or watch_exit_ip(proxy, account_key)


def _meta_auto_repin() -> bool:
    """META_AUTO_REPIN=true khôi phục việc tự ghim tài khoản (mọi nền tảng) sang proxy khác khi proxy cũ chết."""
    return (os.environ.get("META_AUTO_REPIN") or "").strip().lower() in {"1", "true", "yes", "on"}


def _refuse_by_ip_policy(platform: str, account_key: str, proxy: ProxyRow, violation: str) -> None:
    logger.error(
        "account_proxy_policy_refused",
        telegram=True,
        platform=platform,
        account=account_key,
        proxy_id=proxy["id"],
        violation=violation,
    )
    account_events.record(platform, account_key, "proxy_policy_refused", proxy_id=proxy["id"], violation=violation)


# IP thoát đo qua chính proxy trước khi giao cho tài khoản (04/11/2026). Nhãn static do người gắn nên có thể sai: thấy nhiều IP
# trong một giờ thì proxy đang tự xoay, đổi nhãn và không giao nữa. Đổi một lần giữa các phiên là bình thường, chỉ ghi lại.
_SEEN_IPS_KEY_TMPL = "proxy_ip:seen:{proxy_id}"
_WATCH_THROTTLE_KEY_TMPL = "proxy_ip:watch:{proxy_id}"
_WATCH_THROTTLE_SECONDS = 300
_WATCH_VERIFIED_KEY_TMPL = "proxy_ip:verified:{proxy_id}"
_ROTATION_WINDOW_SECONDS = 3600
ROTATION_DISTINCT_IPS = int(os.getenv("META_ROTATION_DISTINCT_IPS", "3"))


def watch_exit_ip(proxy: ProxyRow, account_key: str, cache: RedisCache | None = None) -> bool:
    """Only a recent successful measurement allows authenticated traffic. Unknown means wait."""
    try:
        cache = cache or RedisCache()
        proxy_id = proxy["id"]
        verified = cache.get(_WATCH_VERIFIED_KEY_TMPL.format(proxy_id=proxy_id))
        if (
            isinstance(verified, dict)
            and verified.get("url") == proxy["url"]
            and verified.get("ip")
            and 0 <= time.time() - float(verified.get("at", 0)) < _WATCH_THROTTLE_SECONDS
        ):
            return True
        # The probe lock is not evidence of a verified IP. A failed probe only delays retry briefly.
        if not cache.set_nx(_WATCH_THROTTLE_KEY_TMPL.format(proxy_id=proxy_id), 1, ttl_seconds=30):
            return False
        url = build_proxy_url(proxy)
        resp = curl_requests.get(_EGRESS_URL, proxies={"http": url, "https": url}, timeout=15)
        ip = resp.text.strip() if resp.status_code == 200 else ""
        if not ip:
            logger.warning("exit_ip_unverified", proxy_id=proxy_id, status=resp.status_code)
            return False
        ipaddress.ip_address(ip)
        now = int(time.time())
        key = _SEEN_IPS_KEY_TMPL.format(proxy_id=proxy_id)
        seen = [item for item in (cache.get(key) or []) if now - item[1] <= _ROTATION_WINDOW_SECONDS]
        previous = seen[-1][0] if seen else None
        seen.append([ip, now])
        cache.set(key, seen, ttl_seconds=_ROTATION_WINDOW_SECONDS)
    except (curl_requests.RequestsError, redis.RedisError, TypeError, ValueError) as exc:
        logger.warning("exit_ip_watch_failed", proxy_id=proxy["id"], error_type=type(exc).__name__)
        return False
    account_events.record("facebook", account_key, "exit_ip", proxy_id=proxy_id, ip=ip, previous_ip=previous)
    distinct = len({item[0] for item in seen})
    if distinct < ROTATION_DISTINCT_IPS:
        try:
            cache.set(
                _WATCH_VERIFIED_KEY_TMPL.format(proxy_id=proxy_id),
                {"ip": ip, "at": now, "url": proxy["url"]},
                ttl_seconds=_WATCH_THROTTLE_SECONDS,
            )
        except redis.RedisError:
            return False
        return True
    mark_proxy_rotating(proxy_id)
    logger.error("proxy_detected_rotating", telegram=True, proxy_id=proxy_id, distinct_ips=distinct)
    account_events.record("facebook", account_key, "proxy_detected_rotating", proxy_id=proxy_id, distinct_ips=distinct)
    return False


_EGRESS_KEY_TMPL = "proxy_ip:account_egress:{account}"
_EGRESS_TTL_SECONDS = 30 * 86400
_EGRESS_URL = "https://api.ipify.org"


def check_login_egress(
    platform: str, account_key: str, proxy: dict[str, str | None], cache: RedisCache | None = None
) -> str:
    """IP đầu ra thật của proxy (dạng cấu hình Playwright) trước khi mở phiên đăng nhập / Restore. Không đo được thì raise - không
    mở tài khoản trên một đường mạng chưa xác nhận. Đổi IP giữa các phiên chỉ ghi log, không chặn: IP nhà mạng đổi là bình
    thường (xem runtime/ip_switch.py); log này để đối chiếu thời điểm khi có checkpoint. Không gửi gì tới nền tảng."""
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
    if required and (platform in ip_switch.PLATFORMS or platform_has_any_proxy(platform)):
        raise ProxyPoolExhaustedError(
            f"{platform}: no usable proxy available for this account, and running it unproxied would expose "
            "the real server IP / break its pinned-IP identity - refusing rather than doing that silently."
        )
    return None
