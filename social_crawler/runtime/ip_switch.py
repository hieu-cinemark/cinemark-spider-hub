"""Mỗi IP chỉ một tài khoản Meta: đổi tài khoản trên một cổng proxy thì xoay IP trước (26/10/2026).

Người vận hành xác nhận: nhiều tài khoản Facebook/Threads đi qua cùng một IP là bị khoá ngay, còn IP nhà mạng đổi giữa
các phiên là bình thường. Trước đây ghim tài khoản<->cổng giữ nguyên IP của cổng, nên mọi tài khoản ghim chung một cổng
(2 tài khoản FB mỗi cổng webtui) - cùng job nuôi / kiểm tra cookie chạy lần lượt từng tài khoản - đều hiện ra cùng một
IP.

Từ 12/11/2026 nguyên tắc chung đã mạnh hơn: MỘT proxy thuộc riêng MỘT tài khoản, không kể nền tảng (xem
db.proxies.pin_account_to_free_proxy và db.proxies.shared_proxy_violation). Module này vẫn giữ vai trò khoá cổng
trong Redis để không bao giờ có hai tài khoản cùng lúc trên một IP, và xoay IP khi cổng được tái gán cho tài khoản
khác (IP cũ đã gắn với tài khoản cũ).

services/pool.acquire_proxy_for_account gọi claim() cho Facebook/Threads trước khi giao cổng:
  - cổng đang được tài khoản KHÁC dùng (còn request trong IDLE_SECONDS) -> không giao (job bị hoãn), không bao giờ hai
    tài khoản cùng lúc trên một IP;
  - tài khoản dùng cổng gần nhất khác tài khoản này -> xoay IP (link reset của nguồn, chờ thấy IP mới) rồi mới giao;
    xoay hỏng -> không giao;
  - cổng không có API xoay (proxy tự xoay phía nhà cung cấp, vd. #14/#15) -> giao như cũ;
  - cổng nhãn static -> không bao giờ xoay (04/11/2026: lần nhận đầu của tài khoản 126 đổi IP #12 ngay trước request đầu của
    phiên, vài phút sau dính checkpoint). Chung IP giữa các tài khoản do shared_proxy_violation chặn theo ghim proxy.
IP sau khi xoay lấy từ kho IP của nhà mạng (kho nhỏ, vd 113.189.102.x) nên có thể nhận lại đúng IP một tài khoản
KHÁC vừa dùng: mỗi IP ghi lại tài khoản đã dùng nó (IP_OWNER_DAYS ngày); IP mới thuộc tài khoản khác thì xoay thêm (tối đa
MAX_ROTATIONS lần, chờ giới hạn xoay của nhà cung cấp giữa các lần), vẫn trùng thì không giao.
Client crawl gọi touch() mỗi request để giữ quyền dùng cổng suốt job. Việc chạy lần lượt nhiều tài khoản (nuôi tài khoản,
kiểm tra cookie) gọi release() ngay khi xong mỗi tài khoản: trước đó cổng bị giữ thêm IDLE_SECONDS sau lượt cuối, chặn tài
khoản của làn crawl ghim cùng cổng ("no usable proxy" liên tục, 26/10/2026).
"""

from __future__ import annotations

import os
import time

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

PLATFORMS = {"facebook", "threads"}
# Không request nào trong chừng này giây thì coi như tài khoản đã xong việc với cổng (nhả cho tài khoản khác).
IDLE_SECONDS = int(os.getenv("META_PORT_IDLE_SECONDS", "900"))
ENABLED = os.getenv("META_ROTATE_ON_ACCOUNT_SWITCH", "1") not in ("0", "false", "False", "")

ACTIVE_KEY_TMPL = "proxy_ip:active:{proxy_id}"  # tài khoản đang dùng cổng (TTL IDLE_SECONDS)
LAST_KEY_TMPL = "proxy_ip:last_account:{proxy_id}"  # tài khoản dùng IP hiện tại của cổng gần nhất
_LAST_TTL_SECONDS = 30 * 86400
OWNER_KEY_TMPL = "proxy_ip:owner:{ip}"  # tài khoản đã dùng IP công khai này
IP_OWNER_DAYS = float(os.getenv("META_IP_OWNER_DAYS", "7"))
MAX_ROTATIONS = int(os.getenv("META_MAX_ROTATIONS", "3"))
_sleep = time.sleep  # test thay bằng hàm không chờ


def _norm(account_key: str) -> str:
    return account_key.strip().lower()


def claim(platform: str, proxy: dict, account_key: str | None, cache: RedisCache | None = None) -> bool:
    """True: tài khoản dùng được cổng này ngay (đã xoay IP nếu vừa đổi tài khoản). False: cổng đang bận với tài khoản
    khác, hoặc cần xoay mà xoay không được."""
    if not ENABLED or platform not in PLATFORMS or not account_key or not proxy.get("id"):
        return True
    cache = cache or RedisCache()
    account = _norm(account_key)
    active_key = ACTIVE_KEY_TMPL.format(proxy_id=proxy["id"])
    active = cache.get(active_key)
    if active and active != account:
        logger.info(
            "proxy_port_busy_other_account", platform=platform, proxy_id=proxy["id"], account=account, busy_with=active
        )
        return False
    # set_nx hỏng = một tiến trình khác vừa nhận cổng.
    if not active and not cache.set_nx(active_key, account, IDLE_SECONDS) and cache.get(active_key) != account:
        logger.info("proxy_port_busy_other_account", platform=platform, proxy_id=proxy["id"], account=account)
        return False
    last_key = LAST_KEY_TMPL.format(proxy_id=proxy["id"])
    last = cache.get(last_key)
    if last != account and proxy.get("ip_type") == "static":
        logger.info(
            "proxy_static_ip_kept", platform=platform, proxy_id=proxy["id"], account=account, previous_account=last
        )
        cache.set(last_key, account, ttl_seconds=_LAST_TTL_SECONDS)
    elif last != account:
        started = time.monotonic()
        rotated, new_ip = _rotate_avoiding_reuse(platform, proxy, account, cache)
        if rotated is False:
            cache.delete(active_key)
            logger.warning(
                "proxy_switch_rotate_failed",
                platform=platform,
                proxy_id=proxy["id"],
                account=account,
                previous_account=last,
                hint="không xoay được IP khi đổi tài khoản - không cho tài khoản dùng chung IP",
            )
            return False
        logger.info(
            "proxy_rotated_for_account_switch" if rotated else "proxy_switch_no_rotate_api",
            platform=platform,
            proxy_id=proxy["id"],
            account=account,
            previous_account=last,
            seconds=round(time.monotonic() - started, 1),
            ip=new_ip,
        )
        if new_ip:
            cache.set(OWNER_KEY_TMPL.format(ip=new_ip), account, ttl_seconds=int(IP_OWNER_DAYS * 86400))
        cache.set(last_key, account, ttl_seconds=_LAST_TTL_SECONDS)
    cache.set(active_key, account, ttl_seconds=IDLE_SECONDS)
    return True


def _rotate_avoiding_reuse(
    platform: str, proxy: dict, account: str, cache: RedisCache
) -> tuple[bool | None, str | None]:
    """Xoay IP cổng; IP mới từng thuộc tài khoản KHÁC (trong IP_OWNER_DAYS) thì xoay lại. (None, None): cổng không có API
    xoay; (True, IP): xong; (False, None): xoay hỏng hoặc vẫn trùng sau MAX_ROTATIONS lần."""
    from social_crawler.clients import proxy_provider

    for attempt in range(1, MAX_ROTATIONS + 1):
        status, new_ip = proxy_provider.rotate_port_for_account_switch(proxy["url"])
        if status is not True or not new_ip:
            return status, new_ip
        owner = cache.get(OWNER_KEY_TMPL.format(ip=new_ip))
        if not owner or owner == account:
            return True, new_ip
        logger.info(
            "proxy_ip_used_by_other_account",
            platform=platform,
            proxy_id=proxy["id"],
            ip=new_ip,
            owner=owner,
            account=account,
            attempt=attempt,
        )
        if attempt < MAX_ROTATIONS:
            # Giãn cách giữa hai lần xoay cùng một nguồn (cài trong proxy_settings).
            from social_crawler.db.proxy_settings import get_setting

            _sleep(float(get_setting("provider_min_call_interval_seconds")) + 5)
    logger.warning(
        "proxy_ip_reuse_unavoidable",
        platform=platform,
        proxy_id=proxy["id"],
        account=account,
        attempts=MAX_ROTATIONS,
        hint="kho IP của cổng này quá nhỏ - thêm cổng/nhà cung cấp, hoặc tăng META_MAX_ROTATIONS",
    )
    return False, None


def touch(platform: str, proxy: dict | None, account_key: str | None, cache: RedisCache | None = None) -> None:
    """Gia hạn quyền dùng cổng của tài khoản đang chạy (gọi mỗi request)."""
    if not ENABLED or platform not in PLATFORMS or not proxy or not account_key or not proxy.get("id"):
        return
    (cache or RedisCache()).set(
        ACTIVE_KEY_TMPL.format(proxy_id=proxy["id"]), _norm(account_key), ttl_seconds=IDLE_SECONDS
    )


def release(platform: str, proxy: dict | None, account_key: str | None, cache: RedisCache | None = None) -> None:
    """Tài khoản xong việc với cổng: nhả ngay cho tài khoản khác (chỉ khi chính nó đang giữ). Lần sau tài khoản khác nhận
    cổng thì vẫn xoay IP như thường (LAST_KEY giữ nguyên)."""
    if not ENABLED or platform not in PLATFORMS or not proxy or not account_key or not proxy.get("id"):
        return
    cache = cache or RedisCache()
    active_key = ACTIVE_KEY_TMPL.format(proxy_id=proxy["id"])
    if cache.get(active_key) == _norm(account_key):
        cache.delete(active_key)
        logger.info("proxy_port_released", platform=platform, proxy_id=proxy["id"], account=_norm(account_key))
