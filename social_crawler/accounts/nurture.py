"""Các phiên lướt feed nhẹ cho tài khoản trong pool của Facebook, Threads, và (khi bật)
TikTok.

Facebook/threads (nurture_one): dùng lại storage_state Playwright đã bắt được của mỗi
tài khoản (hoặc trường cookie trên dòng) và dành một lúc ngắn trên feed trang chủ: cuộn và mở vài bài rồi quay lại. Không like, theo dõi,
tìm kiếm hay bình luận: chưa có bằng chứng các thao tác đó giúp tài khoản ổn định (03/11/2026).

TikTok (nurture_one_tiktok, --platform tiktok): một dạng khác cho một mục tiêu khác - vào
một mẫu nhỏ, xoay vòng các trang /tag/<x> chung chung (đúng bề mặt mà việc crawl của
hashtag_search phụ thuộc) và cuộn từng trang, để tăng độ tin cậy "lịch sử sử dụng thật"
mà docstring của constants/tiktok.py nói /api/challenge/item_list/ cần, ngoài một lần
bắt danh tính (xem docstring của tiktok/auth/identity.py cho lỗi cụ thể mà phần này sinh
ra để chống lại: một cặp device_id/odin_id vừa suy ra lại ký challenge/detail ổn với tư
cách khách, rồi nhận item_list rỗng khi user_is_login=true). Ghi cookie kết quả thẳng
lại dòng của tài khoản - TikTok không có cache session Redis riêng.

Không phần nào ở đây là bot đăng nhập/2FA và không phần nào gõ mật khẩu - nếu không có
session hợp lệ, tài khoản bị bỏ qua kèm gợi ý lệnh bootstrap/import cookie bằng tay.

Lượt chạy từ dashboard/pool giới hạn hai tài khoản mỗi nền tảng, bỏ qua tài khoản đã làm
ấm thành công hôm nay (theo lịch Việt Nam), chờ một khoảng ngẫu nhiên trước lượt lướt
đầu, và nghỉ vài phút giữa các tài khoản.

    python -m social_crawler.accounts.nurture
    python -m social_crawler.accounts.nurture --platform facebook --show-browser
    python -m social_crawler.accounts.nurture --platform tiktok --show-browser --hashtags 3
    python -m social_crawler.accounts.nurture --account bat.9337632
    python -m social_crawler.accounts.nurture --visits 0
    python -m social_crawler.accounts.nurture --force --limit 0
"""

from __future__ import annotations

import argparse
import random
import time

from patchright.sync_api import Playwright, sync_playwright

from social_crawler.accounts.warmup import (
    WARMUP_DAYS,
    record_nurture_day,
    unwarmed_account_ids,
)
from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import bind_run_id, get_logger
from social_crawler.db.accounts import (
    get_accounts_by_check_status,
    list_enabled_accounts,
    record_cookie_check,
)
from social_crawler.runtime import account_lease, pool, proxy_lease
from social_crawler.spiders.facebook.auth.browser_interaction import (
    human_wait,
    launch_chromium,
    natural_scroll,
    new_context,
)
from social_crawler.spiders.facebook.auth.cookies import extract_user_agent, parse_cookie_header
from social_crawler.spiders.facebook.auth.identity import BrowserIdentityError

logger = get_logger(__name__)

from social_crawler.accounts.nurture_page import (
    PLATFORMS,
    checkpointed,
    looks_logged_out,
    session_alive,
    visit_posts,
)
from social_crawler.accounts.nurture_quota import already_nurtured_today, mark_nurtured_today, vn_today
from social_crawler.accounts.nurture_support import (
    POOL_LIMIT_DEFAULT,
    NurtureProxyUnavailable,
    _account_key,
    alert_ui_changed,
    handle_checkpoint,
    load_state,
    proxy_for,
    was_warned,
)
from social_crawler.accounts.nurture_tiktok import nurture_one_tiktok


def nurture_one(
    pw: Playwright,
    *,
    platform: str,
    account: dict[str, str],
    redis_cache: RedisCache,
    headless: bool,
    min_scrolls: int,
    max_scrolls: int,
    visits: int,
) -> str:
    """Nuôi MỘT tài khoản Threads hoặc TikTok: mở Chrome qua proxy của tài khoản, nạp phiên đã lưu, lướt feed (cuộn tự
    nhiên, mở vài bài) rồi lưu lại phiên và ghi ngày nuôi. Trả `"ok"` khi xong; `"skipped"` khi không nuôi được vì
    chưa có phiên, không có proxy, lệch danh tính trình duyệt hoặc là Facebook (Facebook không nuôi qua hàm này);
    `"failed"` khi gặp checkpoint, cảnh báo của Meta, phiên đã chết hoặc lỗi bất ngờ. Phiên chết không tính là lỗi
    của proxy (proxy vẫn được nhả với success=True)."""
    cfg = PLATFORMS[platform]
    account_key = _account_key(platform, account)
    if platform == "facebook":
        logger.info("nurture_skipped_platform", platform=platform)
        return "skipped"
    stored = load_state(platform, account, redis_cache, account_key)
    if stored is None:
        logger.error(
            "nurture_skipped_no_session",
            platform=platform,
            account=account_key,
            hint=cfg["bootstrap_hint"],
        )
        return "skipped"

    cookies = parse_cookie_header(account["cookie"]) if account.get("cookie") else {}
    user_agent = extract_user_agent(cookies, upgrade=True)
    context_kwargs = {"user_agent": user_agent} if user_agent else {}
    try:
        playwright_proxy, proxy_row = proxy_for(platform, account_key)
    except NurtureProxyUnavailable as exc:
        logger.warning("nurture_skipped_no_proxy", platform=platform, account=account_key, error=exc)
        return "skipped"

    browser = launch_chromium(pw, headless=headless, proxy=playwright_proxy)
    outcome = "failed"
    try:
        context = new_context(browser, account_key=account_key, storage_state=stored, **context_kwargs)
        page = context.new_page()
        page.goto(cfg["home"], wait_until="domcontentloaded", timeout=60_000)
        human_wait(page, 2500, 3500)

        if checkpointed(page):
            return handle_checkpoint(page, platform, account, account_key, "load", proxy_row)
        if was_warned(page, platform, account_key, "load", proxy_row):
            return "failed"
        if looks_logged_out(page.url, platform) or not session_alive(context, platform):
            logger.error(
                "nurture_session_dead",
                platform=platform,
                account=account_key,
                url=page.url,
                hint=cfg["bootstrap_hint"],
            )
            # Hiện tài khoản trên dashboard để người vận hành nạp cookie mới.
            record_cookie_check(platform, account["id"], status="dead", note="nurture: logged out on load")
            # Phiên chết là lỗi của tài khoản, không của proxy (đã tới được Facebook) - trước 31/10/2026 ghi success=False
            # đưa proxy vào cooldown oan (#17 khi tài khoản 109 bị đăng xuất).
            if proxy_row is not None:
                pool.release_proxy(proxy_row, success=True)
            return "failed"

        natural_scroll(
            page,
            min_scrolls=max(2, min_scrolls // 2),
            max_scrolls=max(3, max_scrolls // 2),
            min_px=400,
            max_px=1100,
            pause_base_ms=1800,
            pause_jitter_ms=2200,
            backscroll_chance=0.2,
        )

        opened = visit_posts(page, platform, visits)
        if checkpointed(page):
            return handle_checkpoint(page, platform, account, account_key, "visit", proxy_row)
        if visits > 0 and opened == 0:
            alert_ui_changed(
                redis_cache,
                page,
                platform=platform,
                account=account_key,
                reason="post_links",
            )
        natural_scroll(
            page,
            min_scrolls=min_scrolls,
            max_scrolls=max_scrolls,
            min_px=400,
            max_px=1100,
            pause_base_ms=1800,
            pause_jitter_ms=2200,
            backscroll_chance=0.2,
        )
        human_wait(page, 4000, 6000)

        if not session_alive(context, platform):
            logger.error("nurture_lost_session_mid_run", platform=platform, account=account_key, url=page.url)
            record_cookie_check(platform, account["id"], status="dead", note="nurture: logged out mid-run")
            # Phiên chết là lỗi của tài khoản, không của proxy (đã tới được Facebook) - trước 31/10/2026 ghi success=False
            # đưa proxy vào cooldown oan (#17 khi tài khoản 109 bị đăng xuất).
            if proxy_row is not None:
                pool.release_proxy(proxy_row, success=True)
            return "failed"

        if was_warned(page, platform, account_key, "end", proxy_row):
            return "failed"
        redis_cache.set(cfg["state_key"].format(account=account_key), context.storage_state())
        record_cookie_check(platform, account["id"], status="alive", note=None)
        if proxy_row is not None:
            pool.release_proxy(proxy_row, success=True)
        mark_nurtured_today(redis_cache, platform, account_key)
        days = record_nurture_day(platform, account["id"], vn_today(), redis_cache)
        logger.info(
            "nurture_ok",
            nurture_days=days,
            warmup_days=WARMUP_DAYS,
            platform=platform,
            account=account_key,
            url=page.url,
            opened=opened,
        )
        outcome = "ok"
        return outcome
    except BrowserIdentityError as exc:
        logger.warning("nurture_skipped_identity_mismatch", platform=platform, account=account_key, error=exc)
        return "skipped"
    except Exception:
        logger.exception("nurture_error", platform=platform, account=account_key)
        if proxy_row is not None:
            pool.release_proxy(proxy_row, success=False)
        return "failed"
    finally:
        browser.close()
        # Nhả proxy ngay cho tài khoản kế tiếp (runtime/proxy_lease).
        proxy_lease.release(proxy_row, account_key)


def _iter_jobs(
    platforms: list[str],
    account_filter: str | None,
    redis_cache: RedisCache,
    *,
    limit: int | None,
    respect_quota: bool,
) -> list[tuple[str, dict[str, str]]]:
    jobs: list[tuple[str, dict[str, str]]] = []
    skipped_quota = 0
    needle = account_filter.strip().lower() if account_filter else None
    for platform in platforms:
        candidates: list[tuple[str, dict[str, str]]] = []
        # Lần kiểm tra cookie gần nhất báo "dead" thì lướt cũng chỉ gặp trang đăng nhập (2026-10-07: cả hai lượt nurture
        # đều chọn trúng 4 tài khoản như vậy, ok=0) - cần nạp cookie mới. Chỉ định --account thì vẫn chạy.
        dead = {account["id"] for account in get_accounts_by_check_status(platform, "dead")}
        # Tài khoản mới đang trong giai đoạn nuôi lên trước: crawler chưa được dùng chúng cho tới khi đủ WARMUP_DAYS ngày.
        # Chúng được nuôi 2 phiên/ngày (hạn mức theo nửa ngày), tài khoản đã ấm 1 phiên/ngày.
        warming = unwarmed_account_ids(platform, redis_cache)
        for account in list_enabled_accounts(platform):
            key = _account_key(platform, account)
            if needle and needle not in key and needle not in account["id"].lower():
                continue
            if not needle and account["id"] in dead:
                logger.info("nurture_skipped_dead_session", platform=platform, account=key)
                continue
            # Tài khoản đang thuộc một làn Facebook (services/lanes.py) có thể đang crawl ngay lúc này - mở thêm trình
            # duyệt lướt feed cùng lúc là hai phiên song song của một tài khoản (25/10/2026).
            if platform == "facebook" and redis_cache.get(f"facebook:lane_lock:{key}") is not None:
                logger.info("nurture_skipped_in_lane", platform=platform, account=key)
                continue
            if respect_quota and already_nurtured_today(redis_cache, platform, key, warming=account["id"] in warming):
                skipped_quota += 1
                logger.info("nurture_skipped_quota", platform=platform, account=key, day=vn_today())
                continue
            candidates.append((platform, account))
        random.shuffle(candidates)
        candidates.sort(key=lambda job: job[1]["id"] not in warming)
        if limit is not None:
            candidates = candidates[:limit]
        jobs.extend(candidates)
    if skipped_quota:
        logger.info("nurture_quota_skipped_total", skipped=skipped_quota, day=vn_today())
    return jobs


def main() -> int:
    """Điểm vào dòng lệnh `python -m social_crawler.accounts.nurture`: chọn tài khoản Threads/TikTok (lọc bằng `--platform`,
    `--account`), chờ một độ trễ khởi động ngẫu nhiên (`--start-delay-*`) rồi nuôi lần lượt từng tài khoản với khoảng nghỉ ngẫu
    nhiên giữa các lượt (`--gap-min`, `--gap-max`); tài khoản đang bận (lease của job khác) bị bỏ qua. Trả 0 khi không có lượt
    nào thất bại, 2 khi có."""
    parser = argparse.ArgumentParser(description="Browse Threads/TikTok to keep pool sessions/identities warm.")
    parser.add_argument("--platform", choices=("threads", "tiktok", "all"), default="all")
    parser.add_argument("--account", default=None, help="Substring match on account id/email")
    parser.add_argument("--show-browser", action="store_true")
    parser.add_argument(
        "--gap-min",
        type=int,
        default=180,
        help="Minimum seconds to wait between accounts (dashboard/pool runs use a few minutes)",
    )
    parser.add_argument(
        "--gap-max",
        type=int,
        default=540,
        help="Maximum seconds to wait between accounts",
    )
    parser.add_argument(
        "--start-delay-min",
        type=int,
        default=0,
        help="Minimum seconds to wait before the first account (jitter so a 07:00 schedule is not exact)",
    )
    parser.add_argument("--start-delay-max", type=int, default=0)
    parser.add_argument(
        "--limit",
        type=int,
        default=POOL_LIMIT_DEFAULT,
        help="Max accounts per platform this run. 0 = no cap. Ignored when --account is set.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Warm accounts even if they already had a successful warm-up today",
    )
    parser.add_argument("--min-scrolls", type=int, default=4)
    parser.add_argument("--max-scrolls", type=int, default=9)
    parser.add_argument("--visits", type=int, default=3, help="Facebook/threads: how many posts to open then go back")
    parser.add_argument("--hashtags", type=int, default=2, help="TikTok: how many /tag/<x> pages to visit and scroll")
    parser.add_argument(
        "--run-id",
        help="Set by crawl_request_consumer for a dashboard-triggered warm-up - binds this value onto "
        "every log line this process emits (see logger.bind_run_id). Not needed for manual/local use.",
    )
    args = parser.parse_args()
    if args.run_id:
        bind_run_id(args.run_id)

    # "all" cố ý chỉ gồm facebook+threads - làm ấm tiktok còn mới/phải bật có chủ đích (rủi ro
    # khác: vào các trang /tag/ thật thay vì chỉ làm mới một session có sẵn) và scheduler của
    # cinemark-api vốn đã kích hoạt làm ấm facebook/threads theo tên (NURTURE_PLATFORMS trong
    # app/services/scheduler.py) - âm thầm gộp tiktok vào "all" sẽ thay đổi việc mà lần kích
    # hoạt có sẵn đó làm. Truyền --platform tiktok một cách rõ ràng.
    platforms = ["threads"] if args.platform == "all" else [args.platform]
    redis_cache = RedisCache()
    per_platform_limit = None if args.account else (None if args.limit <= 0 else args.limit)
    jobs = _iter_jobs(
        platforms,
        args.account,
        redis_cache,
        limit=per_platform_limit,
        respect_quota=not args.force,
    )
    if not jobs:
        logger.info("nurture_no_accounts", platform=args.platform, account=args.account, day=vn_today())
        return 0

    start_lo = max(0, min(args.start_delay_min, args.start_delay_max))
    start_hi = max(0, max(args.start_delay_min, args.start_delay_max))
    if start_hi > 0:
        delay = random.randint(start_lo, start_hi)
        logger.info("nurture_start_delay", seconds=delay, accounts=len(jobs))
        time.sleep(delay)

    counts = {"ok": 0, "skipped": 0, "failed": 0}

    def gap_sleep(remaining: int) -> None:
        gap = random.randint(max(0, min(args.gap_min, args.gap_max)), max(args.gap_min, args.gap_max))
        logger.info("nurture_gap", seconds=gap, remaining=remaining)
        time.sleep(gap)

    with sync_playwright() as pw:

        def run(platform: str, account: dict[str, str]) -> str:
            if platform == "tiktok":
                return nurture_one_tiktok(
                    pw,
                    account=account,
                    redis_cache=redis_cache,
                    headless=not args.show_browser,
                    min_scrolls=args.min_scrolls,
                    max_scrolls=args.max_scrolls,
                    hashtags=max(1, args.hashtags),
                )
            return nurture_one(
                pw,
                platform=platform,
                account=account,
                redis_cache=redis_cache,
                headless=not args.show_browser,
                min_scrolls=args.min_scrolls,
                max_scrolls=args.max_scrolls,
                visits=max(0, args.visits),
            )

        def leased_run(platform: str, account: dict[str, str]) -> str:
            try:
                with account_lease.hold(platform, _account_key(platform, account), "nurture", redis_cache):
                    return run(platform, account)
            except account_lease.AccountBusyError as exc:
                logger.info("nurture_skipped_account_busy", platform=platform, error=exc)
                return "skipped"

        skipped: list[tuple[str, dict[str, str]]] = []
        for i, (platform, account) in enumerate(jobs):
            result = leased_run(platform, account)
            if result == "skipped":
                skipped.append((platform, account))
            else:
                counts[result] += 1
            if i < len(jobs) - 1:
                gap_sleep(len(jobs) - i - 1)
        # Tài khoản bị bỏ (cổng proxy đang bận với tài khoản khác - vd. bình luận Threads trên cổng chung - hoặc proxy
        # đang nghỉ) được thử lại MỘT lần ở cuối lượt thay vì mất cả phiên nuôi (30/10/2026).
        for j, (platform, account) in enumerate(skipped):
            gap_sleep(len(skipped) - j)
            logger.info("nurture_retry_skipped", platform=platform, account=_account_key(platform, account))
            counts[leased_run(platform, account)] += 1

    logger.info("nurture_done", telegram=True, **counts, total=len(jobs))
    return 0 if counts["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
