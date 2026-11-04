"""Giai đoạn nuôi tài khoản mới trước khi crawler được dùng.

claim_account/get_accounts xếp tài khoản chưa dùng bao giờ (last_used_at NULL) lên đầu, nên trước 19/10/2026 một tài
khoản vừa thêm bị đem đi crawl ngay lần đầu crawler cần tài khoản - chưa lướt feed ngày nào, đúng kiểu tài khoản mới
"chỉ gọi API tìm kiếm" mà Meta khoá (18/10: 12/14 tài khoản FB chết trong một buổi chiều).

Tài khoản thêm từ ACCOUNT_WARMUP_SINCE trở đi chỉ được giao cho crawler khi đã có ACCOUNT_WARMUP_DAYS ngày nuôi thành
công (nurture_accounts.py ghi lại mỗi ngày làm ấm OK, theo lịch Việt Nam). Tài khoản thêm trước mốc đó đã chạy thật từ
lâu nên được miễn. Không dựa vào last_used_at: đường xoay Threads không ghi cột này (mọi tài khoản Threads đều NULL), còn
FB 63/64 thêm 19/10 đã bị claim vài giờ sau khi thêm. Một ngày nuôi = ít nhất một phiên nuôi thành công trong ngày đó
(giờ VN); tài khoản đang nuôi được nuôi 2 phiên/ngày. Vòng đời đầy đủ của tài khoản: docs/facebook-accounts.md.
"""

from __future__ import annotations

import os

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.accounts import accounts_created_since, manual_nurture_days, nurture_only_accounts

logger = get_logger(__name__)

# Số ngày nuôi thành công cần có trước khi crawler được dùng tài khoản thêm từ WARMUP_SINCE; tài khoản thêm trước mốc đó được miễn.
# Hiện tại (31/10/2026, người vận hành chọn): 1 ngày cho mọi tài khoản mới - chỉ còn 2-3 tài khoản FB, không có kinh phí
# thay tài khoản, cần tài khoản vào crawl sớm; tải của tài khoản mới vẫn thấp trong 3 ngày crawl đầu nhờ hạn mức ngày x
# RAMP (runtime/search_budget.py). Lịch sử: 3 -> 7 (24/10) -> 2 (25/10) -> 5 (27/10, tài khoản mua dính checkpoint ngay
# lượt tìm kiếm đầu) -> 2 (30/10) -> 1 (31/10). ACCOUNT_WARMUP_DAYS=0 TẮT HẲN giai đoạn nuôi - đừng đặt trong .env (31/10:
# biến này = 0 trên VPS làm tài khoản vừa nhập cookie bị đem đi tìm kiếm ngay và bị đăng xuất).
WARMUP_DAYS = int(os.getenv("ACCOUNT_WARMUP_DAYS", "1"))
WARMUP_SINCE = os.getenv("ACCOUNT_WARMUP_SINCE", "2026-10-19T00:00:00+00:00")
# Tài khoản thêm từ mốc này còn được tăng tải dần lúc mới crawl (runtime/search_budget.py).
WARMUP_STRICT_SINCE = os.getenv("ACCOUNT_WARMUP_STRICT_SINCE", "2026-10-27T00:00:00+07:00")
_TTL_SECONDS = 120 * 24 * 3600
_last_logged: dict[str, set[str]] = {}


def _days_key(platform: str, account_id: str) -> str:
    return f"nurture_days:{platform}:{account_id.strip().lower()}"


def _count_key(platform: str, account_id: str) -> str:
    return f"nurture_days_count:{platform}:{account_id.strip().lower()}"


def record_nurture_day(platform: str, account_id: str, day: str, cache: RedisCache | None = None) -> int:
    """Ghi một ngày nuôi thành công (mỗi ngày tính một lần); trả số ngày đã nuôi."""
    cache = cache or RedisCache()
    if cache.sadd(_days_key(platform, account_id), day):
        cache.expire(_days_key(platform, account_id), _TTL_SECONDS)
        count = cache.incr(_count_key(platform, account_id))
        cache.expire(_count_key(platform, account_id), _TTL_SECONDS)
        return count
    return nurture_days(platform, account_id, cache)


def nurture_days(platform: str, account_id: str, cache: RedisCache | None = None) -> int:
    try:
        return int((cache or RedisCache()).get(_count_key(platform, account_id)) or 0)
    except TypeError, ValueError:
        return 0


def unwarmed_account_ids(platform: str, cache: RedisCache | None = None) -> set[str]:
    """account_id của các tài khoản thêm từ WARMUP_SINCE mà chưa nuôi đủ WARMUP_DAYS ngày."""
    # Tài khoản đánh dấu "chỉ nuôi" (platform_accounts.nurture_only) luôn tính là đang nuôi, kể cả khi đã đủ ngày.
    nurture_only = set(nurture_only_accounts(platform))
    if WARMUP_DAYS <= 0:
        return nurture_only
    cache = cache or RedisCache()
    required = _required_days(platform)
    manual = manual_nurture_days(platform)
    pending = {
        aid for aid, days in required.items() if nurture_days(platform, aid, cache) + manual.get(aid, 0) < days
    } | nurture_only
    # Chỉ log khi danh sách đổi: hàm này chạy mỗi lần làn Facebook hỏi tài khoản (mỗi job + mỗi phút lúc chờ).
    if pending and _last_logged.get(platform) != pending:
        logger.info(
            "accounts_in_warmup",
            platform=platform,
            accounts=sorted(pending),
            required_days=WARMUP_DAYS,
        )
    _last_logged[platform] = pending
    return pending


def is_warming_key(platform: str, account_key: str, cache: RedisCache | None = None) -> bool:
    """`account_key` (account_id hoặc email - con trỏ "tài khoản active" trong Redis dùng email cho Facebook) thuộc một
    tài khoản đang trong giai đoạn nuôi."""
    if not account_key:
        return False
    key = account_key.strip().lower()
    for aid, email in nurture_only_accounts(platform).items():
        if key in {aid.strip().lower(), (email or "").strip().lower()}:
            return True
    if WARMUP_DAYS <= 0:
        return False
    cache = cache or RedisCache()
    required = _required_days(platform)
    manual = manual_nurture_days(platform)
    for aid, email in accounts_created_since(platform, WARMUP_SINCE).items():
        if (
            key in {aid.strip().lower(), (email or "").strip().lower()}
            and nurture_days(platform, aid, cache) + manual.get(aid, 0) < required[aid]
        ):
            return True
    return False


def _required_days(platform: str) -> dict[str, int]:
    """account_id -> số ngày nuôi cần có, cho mọi tài khoản đang bật thêm từ WARMUP_SINCE."""
    return dict.fromkeys(accounts_created_since(platform, WARMUP_SINCE), WARMUP_DAYS)
