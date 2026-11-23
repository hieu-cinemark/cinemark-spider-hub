"""Phần dùng chung của luồng nuôi tài khoản: nạp phiên đã lưu, chọn và kiểm tra proxy, xử lý checkpoint, cảnh báo khi giao diện đổi."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from social_crawler.accounts.account_warning import detect_warning, rest_warned_account
from social_crawler.accounts.nurture_page import (
    PLATFORMS,
    _sample_button_labels,
)
from social_crawler.accounts.session_quality import assess
from social_crawler.clients.redis import RedisCache
from social_crawler.core.fingerprint import CURL_CFFI_IMPERSONATE_TARGET
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events
from social_crawler.db.accounts import (
    record_account_outcome,
)
from social_crawler.runtime import pool
from social_crawler.spiders.facebook.auth.accounts import account_key as fb_account_key
from social_crawler.spiders.facebook.auth.cookies import (
    build_storage_state_from_cookies as fb_state_from_cookies,
)
from social_crawler.spiders.facebook.auth.cookies import extract_user_agent, parse_cookie_header
from social_crawler.spiders.threads.auth.accounts import account_key as threads_account_key
from social_crawler.spiders.threads.auth.cookies import build_storage_state_from_cookies as threads_state_from_cookies

logger = get_logger(__name__)

_DEBUG_DIR = Path(__file__).resolve().parents[1]  # ảnh debug vẫn rơi vào social_crawler/ như trước

# Mỗi nền tảng+lý do chỉ một lần báo Telegram trong khoảng này - nếu không, một thay đổi
# giao diện sẽ báo một lần cho mỗi tài khoản trong cùng lượt chạy.
_UI_ALERT_TTL_SECONDS = 6 * 3600

_POOL_LIMIT_DEFAULT = 2


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
    if platform == "facebook":
        quality = assess(cookies, extract_user_agent(cookies, upgrade=False), account.get("id"))
        if not quality.usable:
            logger.warning("nurture_cookie_refused", account=account_key, problems=quality.problems)
            account_events.record("facebook", account_key, "cookie_refused", problems=quality.problems, **quality.facts)
            return None
    builder = fb_state_from_cookies if platform == "facebook" else threads_state_from_cookies
    stored = builder(cookies)
    if not _valid_state(stored, cfg["required_cookies"]):
        return None
    redis_cache.set(state_key, stored)
    logger.info("nurture_imported_cookie", platform=platform, account=account_key)
    return stored


class NurtureProxyUnavailable(RuntimeError):
    """Proxy đã ghim của tài khoản đang nghỉ/không với tới nền tảng - bỏ lượt làm ấm này."""


def _proxy_for(platform: str, account_key: str) -> tuple[dict | None, dict | None]:
    """Proxy ĐÃ GHIM của tài khoản (cùng IP crawler dùng với cookie này). Trước 19/10/2026 lượt làm ấm chạy không proxy -
    tức IP datacenter của VPS - khi proxy đã ghim đang cooldown hoặc login_use_proxy tắt: cùng một session hiện ra từ hai
    IP khác hẳn, đúng tín hiệu Meta khoá. Giờ không có proxy dùng được thì bỏ lượt (NurtureProxyUnavailable)."""
    try:
        proxy_cfg = pool.acquire_proxy_for_account(platform, account_key, required=True)
    except pool.ProxyPoolExhaustedError as exc:
        raise NurtureProxyUnavailable(str(exc)) from exc
    if not proxy_cfg:
        if pool.platform_has_any_proxy(platform):
            raise NurtureProxyUnavailable("pinned proxy unavailable")
        return None, None
    if not _proxy_reaches(platform, proxy_cfg):
        pool.release_proxy(proxy_cfg, success=False)
        raise NurtureProxyUnavailable(f"proxy {proxy_cfg['url']} cannot reach {platform}")
    return {
        "server": f"http://{proxy_cfg['url']}",
        "username": proxy_cfg["username"],
        "password": proxy_cfg["password"],
    }, proxy_cfg


def _proxy_reaches(platform: str, proxy_cfg: dict) -> bool:
    """Một GET nhanh tới trang chủ nền tảng qua proxy trước khi mở trình duyệt. Đường mạng nhà dân phía sau proxy hay rớt
    (Viettel trả trang "adslfail" với chứng chỉ sai tên - log 19/10/2026): mở trình duyệt lúc đó chỉ ra trang lỗi."""
    from curl_cffi import requests as curl_requests

    url = pool.build_proxy_url(proxy_cfg)
    try:
        resp = curl_requests.get(
            PLATFORMS.get(platform, {}).get("home", "https://www.tiktok.com/"),
            proxies={"http": url, "https": url},
            timeout=20,
            impersonate=CURL_CFFI_IMPERSONATE_TARGET,
            allow_redirects=True,
        )
    except Exception as exc:  # noqa: BLE001 - lỗi mạng/TLS nào cũng là "proxy không dùng được lúc này"
        logger.warning("nurture_proxy_preflight_failed", platform=platform, proxy_url=proxy_cfg["url"], error=exc)
        return False
    return resp.status_code < 500


def _handle_checkpoint(page, platform: str, account: dict[str, str], account_key: str, step: str, proxy_row) -> str:
    """Tài khoản bị checkpoint: tắt + gắn 'checkpoint' (đăng nhập lại tự động không qua được bước xác minh, cần người
    xử lý) thay vì đánh dấu cookie chết. Trả về outcome của nurture_one."""
    logger.error("nurture_checkpoint", platform=platform, account=account_key, step=step, url=page.url)
    record_account_outcome(
        platform, account["id"], success=False, hard_failure=True, reason=f"nurture ({step}): redirected to {page.url}"
    )
    # Lỗi của tài khoản, không của proxy (đã tới được nền tảng): ghi lỗi proxy sẽ đưa cổng vào cooldown, ảnh hưởng tài khoản khác.
    if proxy_row is not None:
        pool.release_proxy(proxy_row, success=True)
    return "failed"


def _warned(page, platform: str, account_key: str, step: str, proxy_row) -> bool:
    """Meta hiện cảnh báo "hành vi tự động" / "tạm thời bị chặn" (account_warning.py): cho tài khoản nghỉ, không tính
    ngày nuôi. Phiên vẫn sống nên không đánh dấu cookie chết; IP không có lỗi nên nhả proxy bình thường."""
    if platform != "facebook":
        return False
    marker = detect_warning(page)
    if not marker:
        return False
    rest_warned_account(platform, account_key, marker, where=f"nurture:{step}", url=page.url)
    if proxy_row is not None:
        pool.release_proxy(proxy_row, success=True)
    return True


def _alert_ui_changed(
    redis_cache: RedisCache,
    page,
    *,
    platform: str,
    account: str,
    reason: str,
    **extra: Any,
) -> None:
    """Facebook/Threads đổi markup permalink đủ thường xuyên để một lượt chạy không mở được bài nào âm thầm trông như làm ấm khoẻ mạnh. Khử trùng trong Redis để một lượt 6 tài khoản
    không gửi 6 tin Telegram giống hệt nhau."""
    key = f"nurture_ui_alert:{platform}:{reason}"
    try:
        if redis_cache.exists(key):
            logger.info("nurture_ui_changed_repeat", platform=platform, account=account, reason=reason, **extra)
            return
        redis_cache.set(key, {"account": account, "reason": reason}, ttl_seconds=_UI_ALERT_TTL_SECONDS)
    except Exception:  # noqa: BLE001 - thao tác trình duyệt/Redis phụ khi nuôi tài khoản: lỗi nào cũng chỉ bỏ qua bước đó, không dừng lượt nuôi
        logger.warning("nurture_ui_alert_dedupe_failed", platform=platform, reason=reason)

    debug_path = _DEBUG_DIR / f"debug_nurture_ui_{platform}_{reason}.png"
    try:
        page.screenshot(path=str(debug_path))
    except Exception:  # noqa: BLE001 - thao tác trình duyệt/Redis phụ khi nuôi tài khoản: lỗi nào cũng chỉ bỏ qua bước đó, không dừng lượt nuôi
        debug_path = None

    logger.warning(
        "nurture_ui_changed",
        telegram=True,
        platform=platform,
        account=account,
        reason=reason,
        url=getattr(page, "url", ""),
        hint="Update selectors in social_crawler/accounts/nurture.py (post_href)",
        debug_screenshot=str(debug_path) if debug_path else None,
        button_samples=_sample_button_labels(page),
        **extra,
    )
