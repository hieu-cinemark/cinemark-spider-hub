"""Nuôi tài khoản TikTok: lướt feed và hashtag mồi như người dùng thật."""

from __future__ import annotations

import random

from patchright.sync_api import Playwright

from social_crawler.accounts.nurture_quota import _mark_nurtured_today
from social_crawler.accounts.nurture_support import NurtureProxyUnavailable, _account_key, _proxy_for
from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger
from social_crawler.db.accounts import (
    get_account_pk,
    update_account_cookie,
)
from social_crawler.runtime import pool
from social_crawler.spiders.facebook.auth.browser_interaction import (
    human_wait,
    launch_chromium,
    natural_scroll,
    new_context,
)
from social_crawler.spiders.tiktok.auth.cookies import (
    REQUIRED_LOGIN_COOKIES as TIKTOK_REQUIRED_COOKIES,
)
from social_crawler.spiders.tiktok.auth.cookies import (
    build_storage_state_from_cookies as tiktok_state_from_cookies,
)
from social_crawler.spiders.tiktok.auth.cookies import cookie_map as tiktok_cookie_map
from social_crawler.spiders.tiktok.auth.cookies import to_cookie_header as tiktok_to_cookie_header

logger = get_logger(__name__)

# Làm ấm TikTok lướt các trang hashtag thật (không phải feed For You chung) - việc crawl
# của hashtag_search gọi /api/challenge/item_list/, đúng endpoint mà docstring của
# identity.py nói một cặp device_id/odin_id vừa suy ra "ký challenge/detail ổn với tư cách
# khách rồi nhận item_list rỗng khi user_is_login=true". Một người thật xây dựng độ tin cậy
# trên đúng bề mặt đó (vào các trang /tag/<x>, cuộn, xem) là đòn bẩy duy nhất project này
# có để tăng độ tin cậy vượt quá một lần bắt danh tính - xem thử nghiệm được ghi lại trong
# constants/tiktok.py. Giữ chung chung/an toàn (không theo từ khoá phim cụ thể) để việc
# làm ấm không bao giờ trộn tín hiệu sở thích thật không liên quan vào một tài khoản dành
# cho hashtag phim.
TIKTOK_NURTURE_HASHTAGS = ("fyp", "foryou", "xuhuong", "trending", "viral")


def nurture_one_tiktok(
    pw: Playwright,
    *,
    account: dict[str, str],
    redis_cache: RedisCache,
    headless: bool,
    min_scrolls: int,
    max_scrolls: int,
    hashtags: int,
) -> str:
    """Dạng riêng của TikTok, tách khỏi nurture_one ở trên: ở đây không có cache storage_state
    trong Redis để dùng lại/làm mới (xem docstring của tiktok/auth/cookies.py - cột
    platform_accounts.cookie CHÍNH LÀ session bền vững), và feed là một lần cuộn dọc liên
    tục, không phải các bài riêng lẻ để mở/quay lại - vào vài trang /tag/ thật và cuộn từng
    trang là cách tương đương gần nhất với "cuộn + vào vài bài" của nurture_one cho
    một giao diện khác hẳn như vậy. Ghi mọi cookie mà trình duyệt có được cuối cùng thẳng
    lại dòng của tài khoản - msToken/ttwid mới từ một lượt lướt thật đúng là thứ item_list
    muốn đi cặp với device_id/odin_id đáng tin của nó (xem cookies_for_identity trong
    tiktok/auth/identity.py)."""
    account_key = _account_key("tiktok", account)
    raw_cookie = account.get("cookie") or ""
    if not raw_cookie:
        logger.error(
            "nurture_skipped_no_session",
            platform="tiktok",
            account=account_key,
            hint="paste a logged-in Cookie header via the dashboard, then Try saved session",
        )
        return "skipped"

    cookies = tiktok_cookie_map(raw_cookie)
    missing = [name for name in TIKTOK_REQUIRED_COOKIES if name not in cookies]
    if missing:
        logger.error("nurture_skipped_missing_cookies", platform="tiktok", account=account_key, missing=missing)
        return "skipped"

    stored = tiktok_state_from_cookies(cookies)
    try:
        playwright_proxy, proxy_row = _proxy_for("tiktok", account_key)
    except NurtureProxyUnavailable as exc:
        logger.warning("nurture_skipped_no_proxy", platform="tiktok", account=account_key, error=exc)
        return "skipped"

    browser = launch_chromium(pw, headless=headless, proxy=playwright_proxy)
    try:
        context = new_context(browser, account_key=account_key, storage_state=stored)
        page = context.new_page()
        sample = random.sample(TIKTOK_NURTURE_HASHTAGS, k=min(max(1, hashtags), len(TIKTOK_NURTURE_HASHTAGS)))
        visited = 0
        for tag in sample:
            try:
                page.goto(f"https://www.tiktok.com/tag/{tag}", wait_until="domcontentloaded", timeout=45_000)
            except Exception:  # noqa: BLE001 - thao tác trình duyệt/Redis phụ khi nuôi tài khoản: lỗi nào cũng chỉ bỏ qua bước đó, không dừng lượt nuôi
                logger.warning("nurture_tiktok_nav_failed", account=account_key, hashtag=tag)
                continue
            human_wait(page, 2000, 2500)

            if not any(c.get("name") == "sessionid" for c in context.cookies()):
                logger.error(
                    "nurture_session_dead",
                    platform="tiktok",
                    account=account_key,
                    url=page.url,
                    hint="paste a fresh logged-in Cookie header via the dashboard",
                )
                if proxy_row is not None:
                    pool.release_proxy(proxy_row, success=False)
                return "failed"

            visited += 1
            natural_scroll(
                page,
                min_scrolls=min_scrolls,
                max_scrolls=max_scrolls,
                min_px=600,
                max_px=1400,
                pause_base_ms=2500,
                pause_jitter_ms=3500,
                backscroll_chance=0.15,
            )
            human_wait(page, 1500, 2000)

        if visited == 0:
            # Từng lần điều hướng lỗi ở trên đều đã được log (nurture_tiktok_nav_failed) - đây là bản
            # tổng kết còn thiếu: khác nurture_session_dead và except bên ngoài bên dưới, lối thoát
            # này chưa bao giờ báo cả lượt làm ấm *nói chung* thất bại vì mọi hashtag trong mẫu đều
            # trống.
            logger.error(
                "nurture_failed_no_hashtags_visited",
                platform="tiktok",
                account=account_key,
                hashtags_attempted=len(sample),
            )
            if proxy_row is not None:
                pool.release_proxy(proxy_row, success=False)
            return "failed"

        fresh_cookie_header = tiktok_to_cookie_header(context.cookies())
        row_id = get_account_pk("tiktok", account_key)
        row_id_updated = row_id is not None and update_account_cookie("tiktok", row_id, fresh_cookie_header)
        if proxy_row is not None:
            pool.release_proxy(proxy_row, success=True)
        _mark_nurtured_today(redis_cache, "tiktok", account_key)
        logger.info(
            "nurture_ok",
            platform="tiktok",
            account=account_key,
            hashtags_visited=visited,
            cookie_saved=row_id_updated,
        )
        return "ok"
    except Exception:
        logger.exception("nurture_error", platform="tiktok", account=account_key)
        if proxy_row is not None:
            pool.release_proxy(proxy_row, success=False)
        return "failed"
    finally:
        browser.close()
