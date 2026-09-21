"""Light feed-browse sessions for Facebook, Threads, and (opt-in) TikTok pool
accounts.

Facebook/threads (nurture_one): reuses each account's already-captured
Playwright storage_state (or the cookie field on the row) and spends a
short while on the home feed: scroll, a few opportunistic likes, at most
one short comment, and opening a couple of posts then going back.

TikTok (nurture_one_tiktok, --platform tiktok): a different shape for a
different goal - visits a small, rotating sample of generic /tag/<x> pages
(the exact surface hashtag_search's own crawl depends on) and scrolls each
one, to grow the "real usage history" trust constants/tiktok.py's own
docstring says /api/challenge/item_list/ needs beyond a one-shot identity
capture (see tiktok/auth/identity.py's own docstring for the specific
failure this exists to work against: a freshly re-derived device_id/odin_id
pair that signs challenge/detail fine as a guest, then gets an empty
item_list once user_is_login=true). Writes the resulting cookies straight
back onto the account's row - TikTok has no separate Redis session cache.

None of this is a login/2FA bot and none of it types passwords - if there
is no valid session, the account is skipped with a pointer at the manual
bootstrap/cookie-import command instead.

Dashboard/pool runs cap at two accounts per platform, skip anyone already
warmed successfully today (Vietnam calendar), wait a random delay before
the first browse, and pause minutes between accounts.

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

from social_crawler.constants.facebook import STATE_REDIS_KEY_TMPL as FB_STATE_KEY
from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL as THREADS_STATE_KEY
from social_crawler.logger import bind_run_id, get_logger
from social_crawler.services import pool
from social_crawler.services.db import get_account_pk, list_enabled_accounts, update_account_cookie
from social_crawler.services.redis import RedisCache
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

# TikTok nurture browses actual hashtag pages (not the generic For You feed)
# - hashtag_search's own crawl calls /api/challenge/item_list/, the specific
# endpoint identity.py's own docstring says a freshly-derived device_id/
# odin_id pair "signs challenge/detail fine as a guest and then gets an
# empty item_list when user_is_login=true". A real person building up trust
# on that exact surface (visiting /tag/<x> pages, scrolling, watching)
# is the only lever this project has to grow that trust beyond a one-shot
# identity capture - see constants/tiktok.py's own documented experiment.
# Kept generic/safe (not movie-keyword-specific) so nurturing never mixes
# unrelated real interest signal into an account meant for movie hashtags.
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
        "session_cookie": "c_user",
        "logged_out_hints": ("/login", "checkpoint"),
        "bootstrap_hint": "python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual",
        "post_href": re.compile(r"/(posts|reel|videos|permalink|photo)/|story\.php", re.I),
        "like_labels": ('[aria-label="Like"]', '[aria-label="Thích"]', '[aria-label^="Like:"]', '[aria-label^="Thích:"]'),
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
        "session_cookie": "ds_user_id",
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

# Short, generic reactions - not keyword spam, not movie titles. One of
# these at most per account per run, and only if a composer is actually
# sitting on the opened post.
COMMENT_PHRASES = (
    "Hay quá",
    "Ủng hộ nha",
    "Đúng vậy",
    "Xem rồi hay lắm",
    "Đáng xem",
    "Mong phim hay",
)

_SKIP_LIKE = re.compile(r"Unlike|Remove Like|Bỏ thích|Loved|Yêu thích", re.I)
_SPONSORED = re.compile(r"Sponsored|Được tài trợ", re.I)
_DEBUG_DIR = Path(__file__).resolve().parent
# One Telegram ping per platform+reason in this window - a UI change would
# otherwise fire once per account in the same run.
_UI_ALERT_TTL_SECONDS = 6 * 3600
# Calendar day in Vietnam - the operator timezone. One successful warm-up
# per account per day; a second dashboard click the same day skips instead
# of browsing the whole pool again.
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
        # Silently treating "can't tell" as "not nurtured yet" changes real
        # behavior (this account could get nurtured more than once a day,
        # defeating the whole point of the quota) - worth knowing when it
        # happens rather than looking identical to a genuinely fresh day.
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
        # Same convention as client.py/pool.py - account["id"] (repurposed
        # from account_id, see tiktok/auth/accounts.py) IS the device_id,
        # already the key every tiktok pool/proxy call uses.
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
    name = PLATFORMS[platform]["session_cookie"]
    return any(c.get("name") == name for c in context.cookies())


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
    """Facebook/Threads rename like/comment/permalink markup often enough
    that a silent 0-likes run looks like a healthy warm-up. Deduped in
    Redis so a 6-account run doesn't send 6 identical Telegram pings."""
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
    """Returns ok / no_composer / submit_failed so a missing composer
    (UI rename) is distinguishable from a found box that wouldn't send."""
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
            if proxy_row is not None:
                pool.release_proxy(proxy_row, success=False)
            return "failed"

        redis_cache.set(cfg["state_key"].format(account=account_key), context.storage_state())
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
    """TikTok's own shape, separate from nurture_one above: there's no
    Redis storage_state cache to reuse/refresh here (see tiktok/auth/
    cookies.py's own docstring - the platform_accounts.cookie column IS the
    durable session), and the feed is one continuous vertical scroll, not
    discrete posts to open/go-back-from - visiting a handful of real /tag/
    pages and scrolling each is the closest equivalent to nurture_one's
    "scroll + like + visit a few posts" for a UI shaped this differently.
    Writes whatever cookies the browser ends up with straight back onto the
    account's row - a fresh msToken/ttwid from an actual browse is exactly
    what item_list wants paired with its trusted device_id/odin_id (see
    tiktok/auth/identity.py's cookies_for_identity)."""
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
            # Every individual nav failure above is already logged
            # (nurture_tiktok_nav_failed) - this is the missing summary:
            # unlike nurture_session_dead and the outer except below, this
            # exit path never said the nurture as a *whole* failed because
            # every single hashtag in the sample came up empty.
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
    parser = argparse.ArgumentParser(description="Browse Facebook/Threads/TikTok to keep pool sessions/identities warm.")
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
    parser.add_argument("--comment", action=argparse.BooleanOptionalAction, default=True)
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

    # "all" deliberately stays facebook+threads only - tiktok nurture is
    # new/opt-in (different risk shape: visits real /tag/ pages rather than
    # just refreshing an existing session) and cinemark-api's scheduler
    # already triggers facebook/threads nurture by name (NURTURE_PLATFORMS
    # in app/services/scheduler.py) - silently folding tiktok into "all"
    # would change what that existing trigger does. Pass --platform tiktok
    # explicitly.
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
