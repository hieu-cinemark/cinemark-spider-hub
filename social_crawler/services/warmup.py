"""Giai đoạn nuôi tài khoản mới trước khi crawler được dùng.

claim_account/get_accounts xếp tài khoản chưa dùng bao giờ (last_used_at NULL) lên đầu, nên trước 19/10/2026 một tài
khoản vừa thêm bị đem đi crawl ngay lần đầu crawler cần tài khoản - chưa lướt feed ngày nào, đúng kiểu tài khoản mới
"chỉ gọi API tìm kiếm" mà Meta khoá (18/10: 12/14 tài khoản FB chết trong một buổi chiều).

Tài khoản thêm từ ACCOUNT_WARMUP_SINCE trở đi chỉ được giao cho crawler khi đã có ACCOUNT_WARMUP_DAYS ngày nuôi thành
công (nurture_accounts.py ghi lại mỗi ngày làm ấm OK, theo lịch Việt Nam). Tài khoản thêm trước mốc đó đã chạy thật từ
lâu nên được miễn. Không dựa vào last_used_at: đường xoay Threads không ghi cột này (mọi tài khoản Threads đều NULL), còn
FB 63/64 thêm 19/10 đã bị claim vài giờ sau khi thêm. ACCOUNT_WARMUP_DAYS=0 tắt hẳn.
"""

from __future__ import annotations

import os

from social_crawler.clients.redis import RedisCache
from social_crawler.db.accounts import accounts_created_since
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# 3 -> 7 ngày (24/10/2026): tài khoản clone mới dính checkpoint ngay khi vào crawl. Chỉ áp cho tài khoản thêm từ
# WARMUP_LONG_SINCE; tài khoản thêm trước đó (từ WARMUP_SINCE) giữ WARMUP_DAYS_LEGACY - không để tài khoản đang crawl
# bị loại cả tuần.
WARMUP_DAYS = int(os.getenv("ACCOUNT_WARMUP_DAYS", "7"))
WARMUP_DAYS_LEGACY = int(os.getenv("ACCOUNT_WARMUP_DAYS_LEGACY", "2"))
WARMUP_LONG_SINCE = os.getenv("ACCOUNT_WARMUP_LONG_SINCE", "2026-10-24T00:00:00+07:00")
WARMUP_SINCE = os.getenv("ACCOUNT_WARMUP_SINCE", "2026-10-19T00:00:00+00:00")
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
    if WARMUP_DAYS <= 0:
        return set()
    cache = cache or RedisCache()
    required = _required_days(platform)
    pending = {aid for aid, days in required.items() if nurture_days(platform, aid, cache) < days}
    # Chỉ log khi danh sách đổi: hàm này chạy mỗi lần làn Facebook hỏi tài khoản (mỗi job + mỗi phút lúc chờ).
    if pending and _last_logged.get(platform) != pending:
        logger.info("accounts_in_warmup", platform=platform, accounts=sorted(pending), required_days=WARMUP_DAYS)
    _last_logged[platform] = pending
    return pending


def is_warming_key(platform: str, account_key: str, cache: RedisCache | None = None) -> bool:
    """`account_key` (account_id hoặc email - con trỏ "tài khoản active" trong Redis dùng email cho Facebook) thuộc một
    tài khoản đang trong giai đoạn nuôi."""
    if WARMUP_DAYS <= 0 or not account_key:
        return False
    cache = cache or RedisCache()
    key = account_key.strip().lower()
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
    return {
        aid: WARMUP_DAYS if aid in long_ids else min(WARMUP_DAYS, WARMUP_DAYS_LEGACY)
        for aid in accounts_created_since(platform, WARMUP_SINCE)
    }
