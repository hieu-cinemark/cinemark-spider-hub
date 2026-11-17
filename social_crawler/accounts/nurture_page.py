"""Thao tác trên trang khi nuôi tài khoản: kiểm tra phiên/checkpoint, bấm an toàn, lấy link bài, đi vào trang chủ, xem bài.

Tách từ nurture.py (17/11/2026); nurture.py vẫn re-export mọi tên.
"""

from __future__ import annotations

import random
import re
from urllib.parse import urljoin

from social_crawler.constants.facebook import STATE_REDIS_KEY_TMPL as FB_STATE_KEY
from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL as THREADS_STATE_KEY
from social_crawler.core.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    human_wait,
    move_mouse_naturally,
)
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES as FB_REQUIRED_COOKIES,
)
from social_crawler.spiders.threads.auth.cookies import REQUIRED_LOGIN_COOKIES as THREADS_REQUIRED_COOKIES

logger = get_logger(__name__)


PLATFORMS = {
    "facebook": {
        "home": "https://www.facebook.com/",
        "state_key": FB_STATE_KEY,
        "required_cookies": FB_REQUIRED_COOKIES,
        "logged_out_hints": ("/login", "checkpoint"),
        "bootstrap_hint": "python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual",
        "post_href": re.compile(r"/(posts|reel|videos|permalink|photo)/|story\.php", re.IGNORECASE),
    },
    "threads": {
        "home": "https://www.threads.com/",
        "state_key": THREADS_STATE_KEY,
        "required_cookies": THREADS_REQUIRED_COOKIES,
        "logged_out_hints": ("/login", "checkpoint", "accounts/login"),
        "bootstrap_hint": "python -m social_crawler.spiders.threads.auth.bootstrap --show-browser --manual",
        "post_href": re.compile(r"/post/", re.IGNORECASE),
    },
}
_SPONSORED = re.compile(r"Sponsored|Được tài trợ", re.IGNORECASE)


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


def _checkpointed(page) -> bool:
    """Facebook / Instagram đẩy sang /checkpoint/ (xác minh danh tính, khoá tạm) - có khi ngay lúc mở trang, có khi giữa
    lượt (2026-10-22: isavonucomu81 vào được bảng tin rồi bị đẩy sang checkpoint lúc like). Cookie vẫn còn nên
    _session_alive() không bắt được; không có nút like / link bài ở đó nên trước đây bị báo nhầm là nurture_ui_changed."""
    return "/checkpoint" in (page.url or "").lower()


def _sample_button_labels(page, limit: int = 8) -> list[str]:
    labels: list[str] = []
    try:
        locators = page.get_by_role("button").all()[:40]
    except Exception:  # noqa: BLE001 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
        return labels
    for locator in locators:
        try:
            label = (locator.get_attribute("aria-label") or locator.inner_text() or "").strip()
        except Exception:  # noqa: BLE001, S112 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
            continue
        if not label:
            continue
        labels.append(label[:80])
        if len(labels) >= limit:
            break
    return labels


def _safe_click(page, locator) -> bool:
    try:
        if not locator.is_visible():
            return False
        move_mouse_naturally(page, locator)
        locator.click(timeout=2500)
        return True
    except Exception:  # noqa: BLE001 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
        return False


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
        except Exception:  # noqa: BLE001, S112 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
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
    except Exception:  # noqa: BLE001 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
        logger.warning("nurture_home_nav_failed", platform=platform, url=page.url)


def _visit_posts(page, platform: str, visits: int) -> int:
    if visits <= 0:
        return 0
    links = _post_links(page, platform)
    if not links:
        return 0
    sample = random.sample(links, k=min(visits, len(links)))
    opened = 0
    home_url = page.url
    for locator in sample:
        if not _safe_click(page, locator):
            continue
        human_wait(page, 1800, 2800)
        if _looks_logged_out(page.url, platform):
            _go_home(page, platform)
            break
        opened += 1
        try:
            page.go_back(wait_until="domcontentloaded", timeout=20_000)
            human_wait(page, 1200, 2000)
        except Exception:  # noqa: BLE001 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
            _go_home(page, platform)
        if _looks_logged_out(page.url, platform) or page.url.startswith("about:"):
            try:
                page.goto(home_url, wait_until="domcontentloaded", timeout=45_000)
            except Exception:  # noqa: BLE001 - Playwright ném nhiều loại lỗi khi trang đổi/đóng; bước thăm dò này bỏ qua và đi tiếp
                _go_home(page, platform)
            human_wait(page, 1200, 1800)
        logger.info("nurture_visited_post", platform=platform, url=page.url)
    return opened
