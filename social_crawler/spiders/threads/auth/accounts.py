"""Chọn dòng platform_accounts (platform='threads') mà một lần chạy bootstrap đóng vai -
threads.com đăng nhập qua hệ thống tài khoản của Instagram, nên phần này giống
social_crawler.spiders.facebook.auth.accounts từng trường một (xem module đó để biết "vì
sao" query mới ở mỗi lời gọi thay vì cache lúc import)."""

from __future__ import annotations

from logging import getLogger

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.threads import ACCOUNT_ROTATION_REDIS_KEY
from social_crawler.db.accounts import get_accounts

logger = getLogger(__name__)


def account_key(user: str) -> str:
    """Hậu tố key Redis định danh một tài khoản - id đăng nhập đã chuẩn hoá, để cùng một tài
    khoản luôn ánh xạ tới cùng cache storage_state/token bất kể chữ hoa/khoảng trắng lúc lưu."""
    return user.strip().lower()


def next_account(redis_cache: RedisCache) -> dict[str, str] | None:
    """None nếu không có dòng threads đang bật nào trong platform_accounts - chỗ gọi coi đó là
    "quay về đăng nhập tay / một slot mặc định duy nhất", giống nghĩa của INSTAGRAM_ACCOUNTS
    rỗng trước đây."""
    accounts = get_accounts("threads")
    if not accounts:
        return None

    index = (redis_cache.incr(ACCOUNT_ROTATION_REDIS_KEY) - 1) % len(accounts)
    account = dict(accounts[index])
    # Một số định dạng xuất tài khoản nối thêm dữ liệu sau secret TOTP bằng hậu tố phân cách
    # "|" (ví dụ mã khôi phục) - nếu không, pyotp.TOTP() từ chối thẳng với "Non-base32 digit
    # found" vì "|" không phải ký tự base32 hợp lệ. Bỏ đi một cách phòng thủ ở đây (không chỉ
    # lúc migrate) phòng khi sau này có dòng được dán vào vẫn còn dính cái tật đó.
    account["2fa"] = account["2fa"].split("|", 1)[0]

    return account
