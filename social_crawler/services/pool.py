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
nó thay vì gọi thẳng acquire_proxy()/get_proxy(), nếu không bảo đảm ghim không thực sự
giữ được. Nó cũng tự chữa một proxy đã ghim bị chết (ghim lại sau
REPIN_AFTER_CONSECUTIVE_FAILURES) và, với chỗ gọi truyền required=True, từ chối âm thầm
quay về chạy không proxy bằng cách raise ProxyPoolExhaustedError.
"""

from __future__ import annotations

import os

from social_crawler.db.accounts import Account, claim_account, has_enabled_accounts, record_account_outcome
from social_crawler.db.proxies import (
    ProxyRow,
    claim_proxy,
    get_account_proxy_assignment,
    get_proxy_by_id,
    get_proxy_raw_status,
    mark_proxy_used,
    pin_account_to_least_loaded_proxy,
    platform_has_any_proxy,
    record_proxy_outcome,
)
from social_crawler.db.proxy_settings import get_setting
from social_crawler.logger import get_logger

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
    assignment = get_account_proxy_assignment(platform, account_key)
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
    account = claim_account(platform)
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
    dùng khi hard_failure=True - xem db.record_account_outcome, nơi gửi nó cho Kira để có
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


def acquire_proxy_for_account(
    platform: str, account_key: str | None, *, required: bool = False, pinned_only: bool = False
) -> ProxyRow | None:
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
    - tài khoản chưa có assigned_proxy_id (lần đầu tiên lấy proxy) -> tự ghim vào proxy dùng
      được nào hiện đang có ít tài khoản còn sống được ghim nhất (đang bật, không bị
      checkpoint), để pool tài khoản lớn dần được rải đều thay vì dồn vào một proxy - và
      các ghim bị tắt/checkpoint không làm một proxy toàn tài khoản chết trông như "đầy".
    - tài khoản đã được ghim nhưng đúng proxy đó hiện không dùng được -> giữ ghim và trả None
      (bỏ qua lượt này) nếu proxy chỉ đang cooldown tạm thời vì một lần trục trặc; còn nếu nó
      đã bị tắt hẳn hoặc đã lỗi liên tiếp repin_after_consecutive_failures (proxy_settings)
      lần, nó bị coi là chết và tài khoản được ghim lại sang một proxy ít tải nhất mới (tự
      chữa, log rõ ràng vì đây là thay đổi danh tính IP đáng để người nhận ra).

    required=True raise ProxyPoolExhaustedError thay vì trả None khi nền tảng này đã cấu hình
    proxy nhưng hiện không cái nào dùng được cho tài khoản này - xem docstring của exception
    đó để biết lý do. Truyền giá trị này từ các client crawl thường ngày
    (comet_graphql_client.py, tiktok/client.py); để False (mặc định) cho proxy trình duyệt
    đăng nhập một lần, chỉ dùng khi bật, trong bootstrap.py của từng nền tảng, vốn được thiết
    kế để chấp nhận "không có proxy" là kết quả bình thường.

    pinned_only=True không bao giờ giao ra một proxy không phải (hoặc không vừa trở thành)
    ghim của chính tài khoản này: khi không tra được dòng của tài khoản - không có dòng khớp,
    hoặc lỗi DB trong get_account_proxy_assignment - nó báo "không có sẵn" thay vì quay về
    một acquire_proxy() không ghim tuỳ ý (xem pinned_login_proxy, chỗ gọi duy nhất cần điều
    này).
    """
    if not account_key:
        return _unavailable(platform, required) if pinned_only else acquire_proxy(platform)

    assignment = get_account_proxy_assignment(platform, account_key)
    if assignment is None:
        return _unavailable(platform, required) if pinned_only else acquire_proxy(platform)
    account_row_id, assigned_proxy_id = assignment

    if assigned_proxy_id is not None:
        proxy = get_proxy_by_id(assigned_proxy_id)
        if proxy is not None:
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

        replacement = pin_account_to_least_loaded_proxy(platform, account_row_id)
        if replacement is None:
            logger.error(
                "account_proxy_repin_failed_pool_exhausted",
                platform=platform,
                account_key=account_key,
                previous_proxy_id=assigned_proxy_id,
                note="pinned proxy is dead and no healthy replacement exists - whole platform proxy pool may be down",
            )
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

    proxy = pin_account_to_least_loaded_proxy(platform, account_row_id)
    if proxy is None:
        return _unavailable(platform, required)
    logger.info("account_pinned_to_proxy", platform=platform, account_key=account_key, proxy_url=proxy["url"])
    return proxy


class AutoLoginDisabledError(RuntimeError):
    """AUTO_LOGIN_KILL_SWITCH đang bật - xem pinned_login_proxy."""


def _auto_login_kill_switch_on() -> bool:
    return (os.environ.get("AUTO_LOGIN_KILL_SWITCH") or "").strip().lower() in {"1", "true", "yes", "on"}


def pinned_login_proxy(platform: str, account_key: str) -> dict[str, str | None]:
    """Cấu hình proxy Playwright cho một lần đăng nhập tự động bằng thông tin đăng nhập: luôn là
    proxy đã ghim cố định của chính tài khoản (ghim vào proxy ít tải nhất ở lần đăng nhập
    đầu tiên), bất kể cờ login_use_proxy của proxy. Không bao giờ quay về không proxy hay một
    proxy không ghim tuỳ ý - một lần đăng nhập tự động mới từ IP thật của server, hoặc từ một
    IP mà các lần phát lại sau không dùng, đúng là tín hiệu nhiều tài khoản mà việc ghim sinh
    ra để tránh - nên nó raise ProxyPoolExhaustedError và lần đăng nhập được thử lại ở lượt
    chạy sau. Ghim chỉ đang cooldown cũng được chờ theo cách đó; ghim đã chết (bị tắt, hoặc
    vượt repin_after_consecutive_failures) thì được ghim lại trước, đúng cách tự chữa mà phát
    lại thường ngày vốn làm, để lần đăng nhập và mọi lần phát lại sau đó dùng chung IP mới
    thay vì tài khoản kẹt sau một proxy không bao giờ sống lại.

    Mọi lần đăng nhập tự động (cả hai bootstrap, scripts/relogin_facebook_accounts.py,
    consumer/bộ lập lịch auto-login) đều đi qua đây, nên AUTO_LOGIN_KILL_SWITCH=true chặn tất
    cả cùng lúc bằng cách raise AutoLoginDisabledError."""
    if _auto_login_kill_switch_on():
        raise AutoLoginDisabledError(
            f"{platform}: AUTO_LOGIN_KILL_SWITCH is on - refusing to auto-login account {account_key!r}."
        )
    proxy = acquire_proxy_for_account(platform, account_key, required=True, pinned_only=True)
    if proxy is None:
        raise ProxyPoolExhaustedError(
            f"{platform}: account {account_key!r} has no usable pinned proxy - refusing to auto-login unproxied."
        )
    return {"server": f"http://{proxy['url']}", "username": proxy["username"], "password": proxy["password"]}


def _unavailable(platform: str, required: bool) -> ProxyRow | None:
    """Lối thoát dùng chung của acquire_proxy_for_account mỗi khi không lấy được proxy dùng
    được cho một tài khoản đã có (hoặc đang thiết lập) danh tính proxy. Chỉ raise khi
    required=True *và* nền tảng này thực sự đã cấu hình proxy (platform_has_any_proxy) - một
    nền tảng chưa bao giờ dùng proxy vẫn trả None dù thế nào, như trước khi có ghim cố định."""
    if required and platform_has_any_proxy(platform):
        raise ProxyPoolExhaustedError(
            f"{platform}: no usable proxy available for this account, and running it unproxied would expose "
            "the real server IP / break its pinned-IP identity - refusing rather than doing that silently."
        )
    return None
