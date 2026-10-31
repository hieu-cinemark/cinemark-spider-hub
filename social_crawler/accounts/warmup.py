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
from social_crawler.db.accounts import accounts_created_since, nurture_only_accounts

logger = get_logger(__name__)

# Số ngày nuôi thành công cần có trước khi crawler được dùng tài khoản, theo NGÀY THÊM tài khoản (created_at):
#   - từ WARMUP_STRICT_SINCE (27/10/2026): max(WARMUP_DAYS, WARMUP_DAYS_STRICT)
#   - từ WARMUP_LONG_SINCE (24/10/2026):   WARMUP_DAYS
#   - từ WARMUP_SINCE (19/10/2026):        min(WARMUP_DAYS, WARMUP_DAYS_LEGACY)
#   - trước WARMUP_SINCE: miễn (tài khoản chạy thật từ trước khi có giai đoạn nuôi)
# Hiện tại (31/10/2026, người vận hành chọn): 1 ngày cho mọi tài khoản mới - chỉ còn 2-3 tài khoản FB, không có kinh phí
# thay tài khoản, cần tài khoản vào crawl sớm; tải của tài khoản mới vẫn thấp trong 3 ngày crawl đầu nhờ hạn mức ngày x
# RAMP (runtime/search_budget.py). Lịch sử: 3 -> 7 (24/10) -> 2 (25/10) -> 5 (27/10, tài khoản mua dính checkpoint ngay
# lượt tìm kiếm đầu) -> 2 (30/10) -> 1 (31/10). ACCOUNT_WARMUP_DAYS=0 TẮT HẲN giai đoạn nuôi - đừng đặt trong .env (31/10:
# biến này = 0 trên VPS làm tài khoản vừa nhập cookie bị đem đi tìm kiếm ngay và bị đăng xuất).
WARMUP_DAYS = int(os.getenv("ACCOUNT_WARMUP_DAYS", "1"))
WARMUP_DAYS_LEGACY = int(os.getenv("ACCOUNT_WARMUP_DAYS_LEGACY", "2"))
WARMUP_LONG_SINCE = os.getenv("ACCOUNT_WARMUP_LONG_SINCE", "2026-10-24T00:00:00+07:00")
WARMUP_SINCE = os.getenv("ACCOUNT_WARMUP_SINCE", "2026-10-19T00:00:00+00:00")
WARMUP_DAYS_STRICT = int(os.getenv("ACCOUNT_WARMUP_DAYS_STRICT", "1"))
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


def nurture_days_before(platform: str, account_id: str, day: str, cache: RedisCache | None = None) -> int:
    """Số NGÀY nuôi thành công trước `day` (không tính chính ngày đó) - để "từ ngày nuôi thứ N" đếm theo ngày, không theo
    phiên: tài khoản đang làm ấm có 2 phiên/ngày, phiên chiều của ngày đầu đã thấy nurture_days = 1 (30/10/2026)."""
    cache = cache or RedisCache()
    days = nurture_days(platform, account_id, cache)
    try:
        if cache.sismember(_days_key(platform, account_id), day):
            days -= 1
    except Exception:  # noqa: BLE001 - không đọc được thì đếm như cũ
        logger.warning("nurture_days_before_failed", platform=platform, account=account_id)
    return max(0, days)


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
    pending = {aid for aid, days in required.items() if nurture_days(platform, aid, cache) < days} | nurture_only
    # Chỉ log khi danh sách đổi: hàm này chạy mỗi lần làn Facebook hỏi tài khoản (mỗi job + mỗi phút lúc chờ).
    if pending and _last_logged.get(platform) != pending:
        logger.info(
            "accounts_in_warmup",
            platform=platform,
            accounts=sorted(pending),
            # Số ngày cần của TỪNG tài khoản (thêm từ WARMUP_STRICT_SINCE là WARMUP_DAYS_STRICT) - trước 28/10/2026 log
            # in WARMUP_DAYS chung, nhìn như tài khoản chỉ cần 2 ngày trong khi thực tế cần 5.
            required_days={aid: required.get(aid) for aid in sorted(pending)},
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
    for aid, email in accounts_created_since(platform, WARMUP_SINCE).items():
        if (
            key in {aid.strip().lower(), (email or "").strip().lower()}
            and nurture_days(platform, aid, cache) < required[aid]
        ):
            return True
    return False


def _required_days(platform: str) -> dict[str, int]:
    """account_id -> số ngày nuôi cần có, cho mọi tài khoản đang bật thêm từ WARMUP_SINCE."""
    long_ids = set(accounts_created_since(platform, WARMUP_LONG_SINCE))
    strict_ids = set(accounts_created_since(platform, WARMUP_STRICT_SINCE))
    return {
        aid: max(WARMUP_DAYS, WARMUP_DAYS_STRICT)
        if aid in strict_ids
        else WARMUP_DAYS
        if aid in long_ids
        else min(WARMUP_DAYS, WARMUP_DAYS_LEGACY)
        for aid in accounts_created_since(platform, WARMUP_SINCE)
    }
