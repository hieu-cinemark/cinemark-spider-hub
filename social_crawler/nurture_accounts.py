"""Các phiên lướt feed nhẹ cho tài khoản trong pool của Facebook, Threads, và (khi bật)
TikTok.

Facebook/threads (nurture_one): dùng lại storage_state Playwright đã bắt được của mỗi
tài khoản (hoặc trường cookie trên dòng) và dành một lúc ngắn trên feed trang chủ: cuộn,
vài lượt like tuỳ cơ hội, tối đa một comment ngắn, và mở vài bài rồi quay lại.

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

    python -m social_crawler.nurture_accounts
    python -m social_crawler.nurture_accounts --platform facebook --show-browser
    python -m social_crawler.nurture_accounts --platform tiktok --show-browser --hashtags 3
    python -m social_crawler.nurture_accounts --account bat.9337632
    python -m social_crawler.nurture_accounts --no-like --no-comment --visits 0
    python -m social_crawler.nurture_accounts --force --limit 0
"""

from __future__ import annotations

import argparse
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from patchright.sync_api import Playwright, sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import STATE_REDIS_KEY_TMPL as FB_STATE_KEY
from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL as THREADS_STATE_KEY
from social_crawler.db.accounts import get_account_pk, list_enabled_accounts, record_cookie_check, update_account_cookie
from social_crawler.logger import bind_run_id, get_logger
from social_crawler.services import pool
from social_crawler.spiders.facebook.auth.accounts import account_key as fb_account_key
from social_crawler.spiders.facebook.auth.browser_interaction import (
    click_first_by_role,
    human_wait,
    move_mouse_naturally,
    natural_scroll,
    new_context,
    type_like_human,
)
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES as FB_REQUIRED_COOKIES,
)
from social_crawler.spiders.facebook.auth.cookies import (
    build_storage_state_from_cookies as fb_state_from_cookies,
)
from social_crawler.spiders.facebook.auth.cookies import extract_user_agent, parse_cookie_header
from social_crawler.spiders.facebook.auth.triggers import dismiss_cookie_banner
from social_crawler.spiders.threads.auth.accounts import account_key as threads_account_key
from social_crawler.spiders.threads.auth.cookies import REQUIRED_LOGIN_COOKIES as THREADS_REQUIRED_COOKIES
from social_crawler.spiders.threads.auth.cookies import build_storage_state_from_cookies as threads_state_from_cookies
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
TIKTOK_LIKE_SELECTORS = (
    '[data-e2e="like-icon"]',
    '[data-e2e="browse-like-icon"]',
    'button[aria-label*="Like"]',
    'button[aria-label*="Thích"]',
)

PLATFORMS = {
    "facebook": {
        "home": "https://www.facebook.com/",
        "state_key": FB_STATE_KEY,
        "required_cookies": FB_REQUIRED_COOKIES,
        "logged_out_hints": ("/login", "checkpoint"),
        "bootstrap_hint": "python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual",
        "post_href": re.compile(r"/(posts|reel|videos|permalink|photo)/|story\.php", re.I),
        "like_labels": (
            '[aria-label="Like"]',
            '[aria-label="Thích"]',
            '[aria-label^="Like:"]',
            '[aria-label^="Thích:"]',
        ),
        "comment_labels": (
            '[aria-label*="Write a comment"]',
            '[aria-label*="Viết bình luận"]',
            '[aria-label*="Comment"]',
            '[aria-label*="Bình luận"]',
            'div[role="textbox"][contenteditable="true"]',
        ),
    },
    "threads": {
        "home": "https://www.threads.com/",
        "state_key": THREADS_STATE_KEY,
        "required_cookies": THREADS_REQUIRED_COOKIES,
        "logged_out_hints": ("/login", "checkpoint", "accounts/login"),
        "bootstrap_hint": "python -m social_crawler.spiders.threads.auth.bootstrap --show-browser --manual",
        "post_href": re.compile(r"/post/", re.I),
        "like_labels": ('[aria-label="Like"]', '[aria-label="Thích"]'),
        "comment_labels": (
            '[aria-label*="Reply"]',
            '[aria-label*="Trả lời"]',
            'div[role="textbox"][contenteditable="true"]',
        ),
    },
}

# Các phản hồi ngắn, chung chung - không spam từ khoá, không tên phim. Tối đa một câu như
# vậy mỗi tài khoản mỗi lượt, và chỉ khi bài đã mở thực sự có ô soạn thảo.
# Phản ứng chung chung - bài trên feed là bài bất kỳ, không phải bài phim, nên các câu kiểu "Mong phim hay" đặt dưới
# một bài nấu ăn tự lộ là bot. Chỉ dùng khi bật --comment.
COMMENT_PHRASES = (
    "Hay quá",
    "Đúng vậy",
    "Tuyệt vời",
    "Quá đỉnh",
    "❤️",
    "👍",
)

# \d: kiểm tra trực tiếp 2026-09-25 - giao diện hiện tại của Facebook có một nút bật/tắt
# Like thật với aria-label luôn chỉ là chữ trơn ("Thích"/"Like", không hậu tố), cộng thêm
# một link tóm tắt reaction RIÊNG ngay cạnh (cũng role="button") có aria-label luôn kèm
# số ("Thích: 14K người", "Yêu thích: 3,3K người") và mở hộp thoại "ai đã bày tỏ cảm xúc"
# thay vì like gì cả - các mục wildcard ^="Thích:"/^="Like:" trong
# PLATFORMS["facebook"]["like_labels"] có để bắt một biến thể giao diện cũ hơn, nơi số đếm
# được cho là nằm ngay trên nút like, nhưng giờ chỉ còn khớp link tóm tắt reaction này,
# không phải nút like thật. Có bất kỳ chữ số nào trong label là đủ nhận ra nó mà không cần
# theo dõi mọi cách diễn đạt Facebook dùng (khớp đúng sự cố nurture_ui_changed mà comment
# này ghi lại: run_id a512674d, button_samples cho thấy hộp thoại "ai đã bày tỏ cảm xúc"
# được mở thay vì like được ghi nhận).
_SKIP_LIKE = re.compile(r"Unlike|Remove Like|Bỏ thích|Loved|Yêu thích|\d", re.I)
_SPONSORED = re.compile(r"Sponsored|Được tài trợ", re.I)
_DEBUG_DIR = Path(__file__).resolve().parent
# Mỗi nền tảng+lý do chỉ một lần báo Telegram trong khoảng này - nếu không, một thay đổi
# giao diện sẽ báo một lần cho mỗi tài khoản trong cùng lượt chạy.
_UI_ALERT_TTL_SECONDS = 6 * 3600
# Ngày theo lịch Việt Nam - múi giờ của người vận hành. Mỗi tài khoản một lần làm ấm thành
# công mỗi ngày; bấm lần hai trên dashboard trong cùng ngày thì bỏ qua thay vì lướt lại cả
# pool.
_VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
_QUOTA_TTL_SECONDS = 40 * 3600
_POOL_LIMIT_DEFAULT = 2


def _vn_today() -> str:
    return datetime.now(_VN_TZ).date().isoformat()


def _quota_key(platform: str, account_key: str) -> str:
    return f"nurture_day:{platform}:{account_key}:{_vn_today()}"


def _already_nurtured_today(redis_cache: RedisCache, platform: str, account_key: str) -> bool:
    try:
        return redis_cache.exists(_quota_key(platform, account_key))
    except Exception as exc:
        # Âm thầm coi "không biết" là "chưa làm ấm" thay đổi hành vi thật (tài khoản này có thể bị
        # làm ấm hơn một lần mỗi ngày, phá đúng mục đích của hạn mức) - đáng biết khi nó xảy ra
        # thay vì trông giống hệt một ngày mới thật sự.
        logger.warning("nurture_quota_check_failed", platform=platform, account=account_key, error=str(exc))
        return False


def _mark_nurtured_today(redis_cache: RedisCache, platform: str, account_key: str) -> None:
    try:
        redis_cache.set(_quota_key(platform, account_key), {"ok": True}, ttl_seconds=_QUOTA_TTL_SECONDS)
    except Exception:
        logger.warning("nurture_quota_mark_failed", platform=platform, account=account_key)


def _account_key(platform: str, account: dict[str, str]) -> str:
    if platform == "facebook":
        return fb_account_key(account.get("email") or account["id"])
    if platform == "tiktok":
        # Cùng quy ước với client.py/pool.py - account["id"] (dùng lại từ account_id, xem
        # tiktok/auth/accounts.py) CHÍNH LÀ device_id, vốn là key mà mọi lời gọi pool/proxy của
        # tiktok dùng.
        return account["id"]
    return threads_account_key(account["id"])


def _valid_state(state: Any, required: tuple[str, ...]) -> bool:
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        return False
    names = {c.get("name") for c in state["cookies"] if isinstance(c, dict)}
    return all(name in names for name in required)


def _load_state(platform: str, account: dict[str, str], redis_cache: RedisCache, account_key: str) -> dict | None:
    cfg = PLATFORMS[platform]
    state_key = cfg["state_key"].format(account=account_key)
    stored = redis_cache.get(state_key)
    if stored is not None and not _valid_state(stored, cfg["required_cookies"]):
        logger.warning("nurture_discarding_invalid_state", platform=platform, account=account_key)
        stored = None
    if stored is not None:
        return stored

    raw = account.get("cookie") or ""
    if not raw:
        return None
    cookies = parse_cookie_header(raw)
    builder = fb_state_from_cookies if platform == "facebook" else threads_state_from_cookies
    stored = builder(cookies)
    if not _valid_state(stored, cfg["required_cookies"]):
        return None
    redis_cache.set(state_key, stored)
    logger.info("nurture_imported_cookie", platform=platform, account=account_key)
    return stored


def _proxy_for(platform: str, account_key: str) -> tuple[dict | None, dict | None]:
    proxy_cfg = pool.acquire_proxy_for_account(platform, account_key)
    if not proxy_cfg or not proxy_cfg["login_use_proxy"]:
        return None, proxy_cfg
    return {
        "server": f"http://{proxy_cfg['url']}",
        "username": proxy_cfg["username"],
        "password": proxy_cfg["password"],
    }, proxy_cfg


def _session_alive(context, platform: str) -> bool:
    """Mọi cookie mà một session đã đăng nhập cần, không chỉ cookie user-id: Threads giữ
    ds_user_id sau khi thu hồi session và chỉ bỏ sessionid, nên kiểm tra chỉ ds_user_id đã
    tính một feed đã đăng xuất (không nút like, không link bài) là làm ấm thành công và lưu
    trạng thái chết đè lên bản đã cache."""
    names = {c.get("name") for c in context.cookies()}
    return all(name in names for name in PLATFORMS[platform]["required_cookies"])


def _looks_logged_out(url: str, platform: str) -> bool:
    lowered = url.lower()
    return any(hint in lowered for hint in PLATFORMS[platform]["logged_out_hints"])


def _sample_button_labels(page, limit: int = 8) -> list[str]:
    labels: list[str] = []
    try:
        locators = page.get_by_role("button").all()[:40]
    except Exception:
        return labels
    for locator in locators:
        try:
            label = (locator.get_attribute("aria-label") or locator.inner_text() or "").strip()
        except Exception:
            continue
        if not label:
            continue
        labels.append(label[:80])
        if len(labels) >= limit:
            break
    return labels


def _alert_ui_changed(
    redis_cache: RedisCache,
    page,
    *,
    platform: str,
    account: str,
    reason: str,
    **extra: Any,
) -> None:
    """Facebook/Threads đổi markup like/comment/permalink đủ thường xuyên để một lượt chạy 0
    like âm thầm trông như làm ấm khoẻ mạnh. Khử trùng trong Redis để một lượt 6 tài khoản
    không gửi 6 tin Telegram giống hệt nhau."""
    key = f"nurture_ui_alert:{platform}:{reason}"
    try:
        if redis_cache.exists(key):
            logger.info("nurture_ui_changed_repeat", platform=platform, account=account, reason=reason, **extra)
            return
        redis_cache.set(key, {"account": account, "reason": reason}, ttl_seconds=_UI_ALERT_TTL_SECONDS)
    except Exception:
        logger.warning("nurture_ui_alert_dedupe_failed", platform=platform, reason=reason)

    debug_path = _DEBUG_DIR / f"debug_nurture_ui_{platform}_{reason}.png"
    try:
        page.screenshot(path=str(debug_path))
    except Exception:
        debug_path = None

    logger.warning(
        "nurture_ui_changed",
        telegram=True,
        platform=platform,
        account=account,
        reason=reason,
        url=getattr(page, "url", ""),
        hint="Update selectors in social_crawler/nurture_accounts.py (like_labels / comment_labels / post_href)",
        debug_screenshot=str(debug_path) if debug_path else None,
        button_samples=_sample_button_labels(page),
        **extra,
    )


def _safe_click(page, locator) -> bool:
    try:
        if not locator.is_visible():
            return False
        move_mouse_naturally(page, locator)
        locator.click(timeout=2500)
        return True
    except Exception:
        return False


def _like_candidate_count(page, platform: str) -> int:
    total = 0
    for selector in PLATFORMS[platform]["like_labels"]:
        try:
            total += page.locator(selector).count()
        except Exception:
            continue
    return total


def _random_likes(page, platform: str, max_likes: int) -> int:
    if max_likes <= 0:
        return 0
    clicked = 0
    seen: set[str] = set()
    for selector in PLATFORMS[platform]["like_labels"]:
        for locator in page.locator(selector).all()[:24]:
            if clicked >= max_likes:
                return clicked
            try:
                label = (locator.get_attribute("aria-label") or "").strip()
            except Exception:
                continue
            if not label or label in seen or _SKIP_LIKE.search(label):
                continue
            seen.add(label)
            if not _safe_click(page, locator):
                continue
            clicked += 1
            logger.info("nurture_liked", platform=platform, label=label[:80])
            human_wait(page, 800, 1800)
    return clicked


def _post_links(page, platform: str) -> list:
    pattern = PLATFORMS[platform]["post_href"]
    home = PLATFORMS[platform]["home"]
    found = []
    seen: set[str] = set()
    for locator in page.locator("a[href]").all()[:80]:
        try:
            href = locator.get_attribute("href") or ""
            if not href or href in seen:
                continue
            absolute = urljoin(home, href)
            if not pattern.search(absolute):
                continue
            nearby = locator.inner_text(timeout=500) or ""
            if _SPONSORED.search(nearby):
                continue
        except Exception:
            continue
        seen.add(href)
        found.append(locator)
        if len(found) >= 12:
            break
    return found


def _go_home(page, platform: str) -> None:
    try:
        page.goto(PLATFORMS[platform]["home"], wait_until="domcontentloaded", timeout=45_000)
        human_wait(page, 1500, 2200)
    except Exception:
        logger.warning("nurture_home_nav_failed", platform=platform, url=page.url)


def _maybe_comment(page, platform: str) -> str:
    """Trả về ok / no_composer / submit_failed để phân biệt thiếu ô soạn thảo (đổi tên giao
    diện) với tìm thấy ô nhưng không gửi được."""
    phrase = random.choice(COMMENT_PHRASES)
    found_box = False
    for selector in PLATFORMS[platform]["comment_labels"]:
        box = page.locator(selector).first
        try:
            box.wait_for(state="visible", timeout=2500)
        except Exception:
            continue
        found_box = True
        try:
            move_mouse_naturally(page, box)
            box.click(timeout=2000)
            human_wait(page, 400, 700)
            type_like_human(box, phrase)
            human_wait(page, 500, 900)
            if not click_first_by_role(page, ("Post", "Đăng", "Comment", "Bình luận", "Reply", "Trả lời")):
                box.press("Enter")
            human_wait(page, 1200, 2000)
            logger.info("nurture_commented", platform=platform)
            return "ok"
        except Exception:
            logger.info("nurture_comment_skipped", platform=platform)
            return "submit_failed"
    return "no_composer" if not found_box else "submit_failed"


def _visit_posts(page, platform: str, visits: int, *, comment: bool) -> tuple[int, str | None]:
    if visits <= 0:
        return 0, None
    links = _post_links(page, platform)
    if not links:
        return 0, None
    sample = random.sample(links, k=min(visits, len(links)))
    opened = 0
    comment_result: str | None = None
    comment_slot = random.randrange(len(sample)) if comment and sample else -1
    home_url = page.url
    for i, locator in enumerate(sample):
        if not _safe_click(page, locator):
            continue
        human_wait(page, 1800, 2800)
        if _looks_logged_out(page.url, platform):
            _go_home(page, platform)
            break
        opened += 1
        if i == comment_slot and comment_result is None:
            comment_result = _maybe_comment(page, platform)
        try:
            page.go_back(wait_until="domcontentloaded", timeout=20_000)
            human_wait(page, 1200, 2000)
        except Exception:
            _go_home(page, platform)
        if _looks_logged_out(page.url, platform) or page.url.startswith("about:"):
            try:
                page.goto(home_url, wait_until="domcontentloaded", timeout=45_000)
            except Exception:
                _go_home(page, platform)
            human_wait(page, 1200, 1800)
        logger.info("nurture_visited_post", platform=platform, url=page.url)
    return opened, comment_result


def nurture_one(
    pw: Playwright,
    *,
    platform: str,
    account: dict[str, str],
    redis_cache: RedisCache,
    headless: bool,
    min_scrolls: int,
    max_scrolls: int,
    like: bool,
    comment: bool,
    visits: int,
) -> str:
    cfg = PLATFORMS[platform]
    account_key = _account_key(platform, account)
    stored = _load_state(platform, account, redis_cache, account_key)
    if stored is None:
        logger.error(
            "nurture_skipped_no_session",
            platform=platform,
            account=account_key,
            hint=cfg["bootstrap_hint"],
        )
        return "skipped"

    cookies = parse_cookie_header(account["cookie"]) if account.get("cookie") else {}
    user_agent = extract_user_agent(cookies)
    context_kwargs = {"user_agent": user_agent} if user_agent else {}
    playwright_proxy, proxy_row = _proxy_for(platform, account_key)

    browser = pw.chromium.launch(headless=headless, proxy=playwright_proxy)
    outcome = "failed"
    try:
        context = new_context(browser, account_key=account_key, storage_state=stored, **context_kwargs)
        page = context.new_page()
        page.goto(cfg["home"], wait_until="domcontentloaded", timeout=60_000)
        human_wait(page, 2500, 3500)
        if platform == "facebook":
            dismiss_cookie_banner(page)

        if _looks_logged_out(page.url, platform) or not _session_alive(context, platform):
            logger.error(
                "nurture_session_dead",
                platform=platform,
                account=account_key,
                url=page.url,
                hint=cfg["bootstrap_hint"],
            )
            # Cùng tín hiệu mà check_facebook_cookies.py ghi - hiện tài khoản trên dashboard và đưa nó
            # vào hàng đợi đăng nhập lại.
            record_cookie_check(platform, account["id"], status="dead", note="nurture: logged out on load")
            if proxy_row is not None:
                pool.release_proxy(proxy_row, success=False)
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

        liked = _random_likes(page, platform, random.randint(1, 3) if like else 0)
        if like and liked == 0 and _like_candidate_count(page, platform) == 0:
            _alert_ui_changed(
                redis_cache,
                page,
                platform=platform,
                account=account_key,
                reason="like_selectors",
            )

        opened, comment_result = _visit_posts(page, platform, visits, comment=comment)
        if visits > 0 and opened == 0:
            _alert_ui_changed(
                redis_cache,
                page,
                platform=platform,
                account=account_key,
                reason="post_links",
            )
        if comment and opened > 0 and comment_result in ("no_composer", "submit_failed"):
            _alert_ui_changed(
                redis_cache,
                page,
                platform=platform,
                account=account_key,
                reason="comment_composer" if comment_result == "no_composer" else "comment_submit",
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

        if not _session_alive(context, platform):
            logger.error("nurture_lost_session_mid_run", platform=platform, account=account_key, url=page.url)
            record_cookie_check(platform, account["id"], status="dead", note="nurture: logged out mid-run")
            if proxy_row is not None:
                pool.release_proxy(proxy_row, success=False)
            return "failed"

        redis_cache.set(cfg["state_key"].format(account=account_key), context.storage_state())
        record_cookie_check(platform, account["id"], status="alive", note=None)
        if proxy_row is not None:
            pool.release_proxy(proxy_row, success=True)
        _mark_nurtured_today(redis_cache, platform, account_key)
        logger.info(
            "nurture_ok",
            platform=platform,
            account=account_key,
            url=page.url,
            liked=liked,
            opened=opened,
            commented=comment_result == "ok",
            comment_result=comment_result,
        )
        outcome = "ok"
        return outcome
    except Exception:
        logger.exception("nurture_error", platform=platform, account=account_key)
        if proxy_row is not None:
            pool.release_proxy(proxy_row, success=False)
        return "failed"
    finally:
        browser.close()


def _tiktok_liked(page) -> bool:
    for selector in TIKTOK_LIKE_SELECTORS:
        try:
            locator = page.locator(selector).first
            if not locator.is_visible():
                continue
            label = (locator.get_attribute("aria-label") or "").strip()
            if _SKIP_LIKE.search(label):
                continue
            move_mouse_naturally(page, locator)
            locator.click(timeout=2500)
            return True
        except Exception:
            continue
    return False


def nurture_one_tiktok(
    pw: Playwright,
    *,
    account: dict[str, str],
    redis_cache: RedisCache,
    headless: bool,
    min_scrolls: int,
    max_scrolls: int,
    like: bool,
    hashtags: int,
) -> str:
    """Dạng riêng của TikTok, tách khỏi nurture_one ở trên: ở đây không có cache storage_state
    trong Redis để dùng lại/làm mới (xem docstring của tiktok/auth/cookies.py - cột
    platform_accounts.cookie CHÍNH LÀ session bền vững), và feed là một lần cuộn dọc liên
    tục, không phải các bài riêng lẻ để mở/quay lại - vào vài trang /tag/ thật và cuộn từng
    trang là cách tương đương gần nhất với "cuộn + like + vào vài bài" của nurture_one cho
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
    playwright_proxy, proxy_row = _proxy_for("tiktok", account_key)

    browser = pw.chromium.launch(headless=headless, proxy=playwright_proxy)
    try:
        context = new_context(browser, account_key=account_key, storage_state=stored)
        page = context.new_page()
        sample = random.sample(TIKTOK_NURTURE_HASHTAGS, k=min(max(1, hashtags), len(TIKTOK_NURTURE_HASHTAGS)))
        liked_total = 0
        visited = 0
        for tag in sample:
            try:
                page.goto(f"https://www.tiktok.com/tag/{tag}", wait_until="domcontentloaded", timeout=45_000)
            except Exception:
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
            if like and random.random() < 0.5 and _tiktok_liked(page):
                liked_total += 1
                logger.info("nurture_liked", platform="tiktok", hashtag=tag)
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
            liked=liked_total,
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
        for account in list_enabled_accounts(platform):
            key = _account_key(platform, account)
            if needle and needle not in key and needle not in account["id"].lower():
                continue
            if respect_quota and _already_nurtured_today(redis_cache, platform, key):
                skipped_quota += 1
                logger.info("nurture_skipped_quota", platform=platform, account=key, day=_vn_today())
                continue
            candidates.append((platform, account))
        random.shuffle(candidates)
        if limit is not None:
            candidates = candidates[:limit]
        jobs.extend(candidates)
    if skipped_quota:
        logger.info("nurture_quota_skipped_total", skipped=skipped_quota, day=_vn_today())
    return jobs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Browse Facebook/Threads/TikTok to keep pool sessions/identities warm."
    )
    parser.add_argument("--platform", choices=("facebook", "threads", "tiktok", "all"), default="all")
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
        default=_POOL_LIMIT_DEFAULT,
        help="Max accounts per platform this run. 0 = no cap. Ignored when --account is set.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Warm accounts even if they already had a successful warm-up today",
    )
    parser.add_argument("--min-scrolls", type=int, default=4)
    parser.add_argument("--max-scrolls", type=int, default=9)
    parser.add_argument("--like", action=argparse.BooleanOptionalAction, default=True)
    # Tắt mặc định (2026-10-07): comment một câu mẫu lên bài lạ trên feed, lặp lại giữa nhiều tài khoản, là dấu
    # hiệu "mạng lưới tài khoản giả" rõ nhất - chỉ bật khi chủ động truyền --comment.
    parser.add_argument("--comment", action=argparse.BooleanOptionalAction, default=False)
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
    platforms = ["facebook", "threads"] if args.platform == "all" else [args.platform]
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
        logger.info("nurture_no_accounts", platform=args.platform, account=args.account, day=_vn_today())
        return 0

    start_lo = max(0, min(args.start_delay_min, args.start_delay_max))
    start_hi = max(0, max(args.start_delay_min, args.start_delay_max))
    if start_hi > 0:
        delay = random.randint(start_lo, start_hi)
        logger.info("nurture_start_delay", seconds=delay, accounts=len(jobs))
        time.sleep(delay)

    counts = {"ok": 0, "skipped": 0, "failed": 0}
    with sync_playwright() as pw:
        for i, (platform, account) in enumerate(jobs):
            if platform == "tiktok":
                result = nurture_one_tiktok(
                    pw,
                    account=account,
                    redis_cache=redis_cache,
                    headless=not args.show_browser,
                    min_scrolls=args.min_scrolls,
                    max_scrolls=args.max_scrolls,
                    like=args.like,
                    hashtags=max(1, args.hashtags),
                )
            else:
                result = nurture_one(
                    pw,
                    platform=platform,
                    account=account,
                    redis_cache=redis_cache,
                    headless=not args.show_browser,
                    min_scrolls=args.min_scrolls,
                    max_scrolls=args.max_scrolls,
                    like=args.like,
                    comment=args.comment,
                    visits=max(0, args.visits),
                )
            counts[result] += 1
            if i < len(jobs) - 1:
                gap_lo = max(0, min(args.gap_min, args.gap_max))
                gap_hi = max(args.gap_min, args.gap_max)
                gap = random.randint(gap_lo, gap_hi)
                logger.info("nurture_gap", seconds=gap, remaining=len(jobs) - i - 1)
                time.sleep(gap)

    logger.info("nurture_done", telegram=True, **counts, total=len(jobs))
    return 0 if counts["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
