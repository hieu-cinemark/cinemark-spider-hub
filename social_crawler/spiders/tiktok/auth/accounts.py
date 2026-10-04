"""
Chọn dòng platform_accounts (platform='tiktok') mà một lần chạy client đóng vai - query
mới từ Supabase ở mỗi lời gọi (xem social_crawler/db/accounts.py), cùng kiểu với
auth/accounts.py của facebook/threads.

Khác Facebook/Instagram, TikTok không bao giờ tự động đăng nhập bằng mật khẩu - xem
docstring module của constants/tiktok.py để biết vì sao không bao giờ đụng tới trình
duyệt sau khi danh tính của tài khoản đã được bắt một lần từ một phiên trình duyệt thật,
đã được tin cậy. Một session đã đăng nhập được import giống Facebook/Threads: người dùng
dán header Cookie (phải có sessionid + ttwid), rồi bootstrap bắt lại device_id/odin_id.
Trường `cookie` giữ header đó; password/totp_secret/email không được dùng.
platform_accounts không có cột device_id/odin_id riêng, nên phần này dùng lại hai cột
chung có sẵn thay vì đổi schema:

  - account_id -> device_id
  - token      -> odin_id
  - cookie     -> chuỗi header `Cookie:` thô (ttwid/msToken/s_v_web_id, cộng
                  sessionid/sid_tt/v.v. với một tài khoản đã đăng nhập thật)
"""

from __future__ import annotations

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.tiktok import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.db.accounts import get_accounts, list_enabled_accounts
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.tiktok.auth.cookies import cookie_map

logger = get_logger(__name__)


def cookie_names(cookie: str | None) -> list[str]:
    return sorted(cookie_map(cookie or ""))


def is_logged_in_cookie(cookie: str | None) -> bool:
    """Các dòng khách chỉ có ttwid có thể tra được hashtag rồi nhận 200 rỗng ở item_list. Một
    lần đăng nhập web thật mang sessionid (và thường cả sid_tt)."""
    names = {name.lower() for name in cookie_map(cookie or "")}
    return bool(names & {"sessionid", "sid_tt"})


def next_account(
    redis_cache: RedisCache,
    *,
    exclude_ids: set[str] | None = None,
    require_login: bool = True,
    require_usable_proxy: bool = False,
) -> dict[str, str] | None:
    """None nếu không có dòng tiktok đang bật nào khớp. Ưu tiên cookie đã đăng nhập để một lượt
    crawl không xoay sang dòng khách sau khi restore một tài khoản khác. `exclude_ids` bỏ qua
    các device_id đã nhận 200 rỗng trong lượt chạy này.

    `require_usable_proxy`: khi True (spider comment / trình duyệt của TikTok), bỏ qua tài
    khoản có proxy đã ghim cố định đang giữa cooldown - nếu không, vòng xoay cứ giao ra các
    tài khoản "đã ghim" mà acquire_proxy_for_account lập tức thất bại. Nếu mọi tài khoản còn
    lại đều ghim vào proxy đang cooldown, raise ProxyPoolExhaustedError để spider thoát với
    PROXY_EXHAUSTED_EXIT_CODE và consumer xếp hàng lại.

    Cũng raise ProxyPoolExhaustedError khi get_accounts() rỗng nhưng vẫn còn dòng đang bật
    đang giữa cooldown tài khoản - nếu không, chỉ một cooldown do lỗi nhẹ cũng khiến spider log
    tiktok_account_unusable và consumer đánh dấu comments_crawl_finished (thành công lặng lẽ)."""
    accounts = get_accounts("tiktok")
    if not accounts:
        cooling = list_enabled_accounts("tiktok")
        if cooling:
            ids = [row["id"] for row in cooling]
            logger.warning("tiktok_all_accounts_cooling", device_ids=ids)
            raise pool.ProxyPoolExhaustedError(f"tiktok: every enabled account is mid-cooldown (device_ids={ids})")
        return None

    skip = exclude_ids or set()
    remaining = [row for row in accounts if row["id"] not in skip]
    logged_in = [row for row in remaining if is_logged_in_cookie(row.get("cookie"))]
    candidates = logged_in if require_login else (logged_in or remaining)
    if not candidates:
        if require_login and remaining:
            logger.warning(
                "tiktok_no_logged_in_account",
                guest_count=len(remaining),
                guest_device_ids=[row["id"] for row in remaining],
                guest_cookie_names=[cookie_names(row.get("cookie")) for row in remaining],
            )
            return None
        # Mọi tài khoản hiện dùng được đều đã thử trong lượt chạy này, nhưng các tài khoản khác có
        # thể vẫn đang cooldown - xếp hàng lại thay vì báo "không có tài khoản".
        cooling = [row for row in list_enabled_accounts("tiktok") if row["id"] not in skip]
        if cooling:
            ids = [row["id"] for row in cooling]
            logger.warning(
                "tiktok_remaining_accounts_cooling",
                tried=sorted(skip),
                cooling=ids,
            )
            raise pool.ProxyPoolExhaustedError(
                f"tiktok: tried all currently usable accounts; others mid-cooldown (device_ids={ids})"
            )
        return None

    if require_usable_proxy:
        usable = [row for row in candidates if pool.account_pinned_proxy_usable("tiktok", row["id"])]
        if not usable:
            cooling_ids = [row["id"] for row in candidates]
            logger.warning(
                "tiktok_all_pinned_proxies_cooling",
                device_ids=cooling_ids,
                note="every remaining account is sticky-pinned to a cooling proxy",
            )
            raise pool.ProxyPoolExhaustedError(
                f"tiktok: every remaining account is pinned to a proxy that is cooling down (device_ids={cooling_ids})"
            )
        if len(usable) < len(candidates):
            logger.info(
                "tiktok_skipped_cooling_pinned_accounts",
                skipped=len(candidates) - len(usable),
                usable=len(usable),
            )
        candidates = usable

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(candidates)
    return candidates[index]
