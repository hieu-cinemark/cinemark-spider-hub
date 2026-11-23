"""Dựng ngữ cảnh trình duyệt đã đăng nhập cho một lần bootstrap Facebook: nạp phiên đã lưu, kiểm tra phiên còn dùng được, đánh dấu phiên chết."""

from __future__ import annotations

import os
from typing import Any

from patchright.sync_api import Playwright

from social_crawler.accounts.session_quality import assess
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    DEFAULT_ACCOUNT_KEY,
    STATE_REDIS_KEY_TMPL,
)
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events
from social_crawler.db.accounts import get_account_by_key, record_cookie_check
from social_crawler.runtime import account_lease, pool
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.facebook.auth.accounts import next_account
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, launch_chromium
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    parse_cookie_header,
)
from social_crawler.spiders.facebook.auth.cookies import (
    extract_facebook_user_agent as extract_user_agent,
)
from social_crawler.spiders.facebook.auth.identity import new_facebook_context as new_context
from social_crawler.spiders.facebook.auth.identity import save_native_state

logger = get_logger(__name__)


def _mark_session_dead(redis_cache: RedisCache, account_key: str, *, page_url: str) -> None:
    """Session dùng lại (storage_state hoặc cột cookie) không bắt được request GraphQL nào - gần
    như luôn là Facebook đã đăng xuất nó phía server. Trước 2026-10-05 lượt chạy chỉ raise, nên tài
    khoản vẫn là ACTIVE_ACCOUNT_REDIS_KEY và mọi job comment sau đó (vốn ghim vào tài khoản active)
    lại bootstrap đúng tài khoản chết này, cứ khoảng 45 giây một lần. Giờ:
      - xoá storage_state đã lưu và con trỏ active nếu đang trỏ vào nó,
      - ghi last_check_status='dead' (để luồng đăng nhập lại nhặt nó),
      - trả tài khoản về pool với lỗi nhẹ (cooldown) để next_account() chọn tài khoản khác."""
    redis_cache.delete(STATE_REDIS_KEY_TMPL.format(account=account_key))
    if redis_cache.get(ACTIVE_ACCOUNT_REDIS_KEY) == account_key:
        redis_cache.delete(ACTIVE_ACCOUNT_REDIS_KEY)
    row = get_account_by_key("facebook", account_key)
    if row is None:
        return
    if "/checkpoint" in (page_url or "").lower():
        # Trang xác minh danh tính (vd. /checkpoint/828281030927956/, 24/10/2026 aurorassas40): tắt + gắn 'checkpoint'
        # (lỗi nặng) thay vì 'dead', vì nạp lại cookie cũng không qua được.
        note = f"token refresh redirected to {page_url} (identity confirmation)"
        record_cookie_check("facebook", row["id"], status="dead", note=note)
        pool.release_account("facebook", row["id"], success=False, hard_failure=True, reason=note)
        logger.error("facebook_session_checkpointed", account=account_key, page_url=page_url, telegram=True)
        return
    note = f"reused session captured no GraphQL request (page {page_url}) - logged out server-side?"
    record_cookie_check("facebook", row["id"], status="dead", note=note)
    pool.release_account("facebook", row["id"], success=False, reason=note)
    logger.warning("facebook_session_marked_dead", account=account_key, telegram=True)


def _is_valid_storage_state(state: Any) -> bool:
    """Kiểm tra nhanh một dict storage_state Playwright nạp từ Redis trước khi đưa cho
    new_context() - một cache hỏng/thiếu (đổi schema qua một lần deploy, sửa tay, ghi bị gián
    đoạn) nên kích hoạt đăng nhập mới thay vì một lần crash không chẩn đoán được sâu bên trong
    Playwright."""
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        return False
    cookie_names = {c.get("name") for c in state["cookies"] if isinstance(c, dict)}
    return all(name in cookie_names for name in REQUIRED_LOGIN_COOKIES)


def _get_authenticated_context(
    pw: Playwright,
    redis_cache: RedisCache,
    headless: bool | None,
    force_manual: bool = False,
    prefer_account: str | None = None,
):
    """Logic đăng nhập/dùng lại session dùng chung cho mọi luồng bootstrap (search, comments,
    ...). Chọn tài khoản mà lượt chạy này đóng vai - xoay vòng qua các dòng đang bật trong bảng
    platform_accounts (platform='facebook', xem db/accounts.py) nếu có, nếu không thì một slot
    "default" cố định duy nhất cho đăng nhập tay / cookie import - rồi dùng lại storage_state
    đã cache của chính tài khoản đó nếu có, import thẳng trường "cookie" của nó nếu có đặt (bỏ
    qua hẳn việc đăng nhập bằng trình duyệt), hoặc không thì mở trình duyệt có giao diện để
    đăng nhập một lần. Trả về cả account_key, để chỗ gọi lưu cache token dưới đúng tài khoản
    đó thay vì một cache toàn cục dùng chung.

    Truyền force_manual=True để tự đăng nhập bằng tay kể cả khi tài khoản được xoay tới đã
    cấu hình thông tin đăng nhập - cần ở lần đầu một tài khoản gặp màn hình
    checkpoint/xác minh; storage_state vẫn được lưu dưới key
    của chính tài khoản đó, nên mọi lần chạy sau tiếp tục headless như thường."""
    if prefer_account:
        account = get_account_by_key("facebook", prefer_account)
        if account is None:
            raise RuntimeError(f"No facebook account matching {prefer_account!r}")
        account_key = normalize_account_key(account.get("email") or account["id"])
    else:
        account = next_account()
        if account is not None:
            account_key = normalize_account_key(account.get("email") or account["id"])
        else:
            account_key = DEFAULT_ACCOUNT_KEY

    # Giữ tới hết tiến trình; job khác / đăng nhập tay cùng tài khoản phải chờ (runtime/account_lease.py).
    if account is not None:
        account_lease.acquire("facebook", account_key, "bootstrap", redis_cache)

    state_key = STATE_REDIS_KEY_TMPL.format(account=account_key)
    stored_state = redis_cache.get(state_key)
    if stored_state is not None and not _is_valid_storage_state(stored_state):
        # Một cache hỏng/thiếu (đổi schema qua một lần deploy, sửa tay, ghi bị gián đoạn) nếu không
        # sẽ hiện ra thành một exception thô, không chẩn đoán được sâu bên trong lời gọi tạo context
        # của Playwright - bỏ nó đi và chuyển sang đăng nhập mới, y như khi chưa cache gì.
        logger.warning("discarding_invalid_stored_state", account=account_key, key=state_key)
        stored_state = None

    # Trường "cookie" của tài khoản có thể mang một mục synthetic useragent=... (xem
    # cookies.extract_user_agent) ghi lại trình duyệt mà Facebook thực sự thấy lúc đăng nhập -
    # khớp nó ở đây (cho cả đường import cookie lẫn đường đăng nhập tự động/tay bên dưới) giúp
    # session này trông nhất quán qua các lần chạy thay vì nhảy sang UA mặc định của
    # Playwright.
    account_cookies = parse_cookie_header(account["cookie"]) if account and account.get("cookie") else None
    account_user_agent = extract_user_agent(account_cookies) if account_cookies else None
    context_kwargs = {"user_agent": account_user_agent} if account_user_agent else {}

    if stored_state is None and account_cookies and not force_manual:
        quality = assess(account_cookies, extract_user_agent(account_cookies, upgrade=False), (account or {}).get("id"))
        if not quality.usable:
            account_events.record("facebook", account_key, "cookie_refused", problems=quality.problems, **quality.facts)
            raise RuntimeError(f"Account {account_key!r} cookie refused: {', '.join(quality.problems)}")
        stored_state = build_storage_state_from_cookies(account_cookies)
        cookie_names = {c["name"] for c in stored_state["cookies"]}
        missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in cookie_names]
        if missing:
            failure_reason = (
                f"Account {account_key!r} has a 'cookie' value but it's missing required cookie(s) "
                f"{missing} - a valid logged-in session needs at least {REQUIRED_LOGIN_COOKIES}."
            )
            if account is not None:
                # Cột cookie sai định dạng không bao giờ tự lành - tắt nó giống đường phát hiện checkpoint
                # bên dưới, thay vì để nó ở trạng thái đã nhận mà chưa trả: không có cái này, dòng đó cứ bị
                # next_account() giao ra ở mỗi vòng xoay (last_used_at đã được ghi lúc nhận) rồi lại raise ở
                # đây, âm thầm chiếm một slot LRU thay vì được gắn cờ để người sửa.
                pool.release_account("facebook", account["id"], success=False, hard_failure=True, reason=failure_reason)
                logger.error(
                    "account_disabled_bad_cookie",
                    telegram=True,
                    platform="facebook",
                    account=account_key,
                )
            raise RuntimeError(failure_reason)
        redis_cache.set(state_key, stored_state)
        logger.info(
            "imported_cookie_from_account",
            account=account_key,
            cookie_count=len(cookie_names),
            matched_user_agent=account_user_agent is not None,
        )

    need_login = stored_state is None
    # Phiên mới chỉ đến từ cookie của trình duyệt thật (02/11/2026): đăng nhập mật khẩu trong trình duyệt tự động đã bị Facebook
    # từ chối từ tháng 9, kể cả khi gõ tay. --manual vẫn cho người vận hành tự đăng nhập, có người giám sát.
    if need_login and account is not None and not force_manual:
        logger.error("session_missing_import_cookie", telegram=True, platform="facebook", account=account_key)
        raise RuntimeError(f"Account {account_key!r} has no session; import cookies from a real browser.")
    # Có tài khoản thật thì luôn bắt buộc proxy đã ghim (02/11/2026, cả đăng nhập --manual): một session lập trên một IP
    # rồi phát lại từ IP khác chính là chỗ lệch mà việc ghim ngăn chặn. Chỉ slot đăng nhập tay không gắn dòng tài khoản
    # nào mới được chạy không proxy khi hiện không có proxy.
    # FACEBOOK_BROWSER_DIRECT=1: thử nghiệm trên máy người vận hành, trình duyệt đi thẳng còn request crawl vẫn qua proxy
    # đã ghim. Mặc định tắt, đừng đặt trên server: đây chính là kiểu lệch IP mà việc ghim ngăn chặn.
    direct = os.getenv("FACEBOOK_BROWSER_DIRECT", "").strip().lower() in ("1", "true")
    if direct:
        logger.warning("facebook_browser_direct_no_proxy", account=account_key)
    proxy_cfg = (
        None
        if direct
        else pool.acquire_proxy_for_account(
            "facebook", account_key if account else None, required=account is not None or not need_login
        )
    )
    proxy = None
    # 24/10/2026: luôn dùng proxy đã ghim khi có (bỏ điều kiện login_use_proxy) - trình duyệt mở từ IP VPS rồi crawl
    # từ proxy là đúng kiểu lệch IP mà việc ghim ngăn chặn.
    if proxy_cfg:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

    if account is not None and proxy:
        pool.check_login_egress("facebook", account_key, proxy, redis_cache)

    browser_headless = headless if headless is not None else not need_login
    logger.info(
        "launching_browser",
        account=account_key,
        headless=browser_headless,
        need_login=need_login,
        proxy=proxy["server"] if proxy else None,
    )
    browser = launch_chromium(pw, headless=browser_headless, proxy=proxy)

    try:
        if need_login:
            context = new_context(browser, account_key=account_key, redis_cache=redis_cache, **context_kwargs)
            page = context.new_page()
            page.goto("https://www.facebook.com/login")
            logger.info(
                "manual_login_required",
                account=account_key,
                hint="press Enter here once you're done logging in",
            )
            input()

            # Lỗi ở đây, rõ ràng, nếu đăng nhập không thực sự thành công - nếu không, bước tiếp theo
            # (vào trang chủ để tìm kiếm) chỉ rơi lại trang đã đăng xuất và lỗi với thông báo khó hiểu
            # "không tìm thấy ô tìm kiếm" thay vì vấn đề thật.
            if not any(c["name"] == "c_user" for c in context.cookies()):
                debug_path = BASE_DIR / "debug_login_failed.png"
                page.screenshot(path=str(debug_path))
                # Tắt tài khoản thay vì để mọi vòng xoay sau đâm vào cùng bức tường. account có thể là None ở đây (hoàn
                # toàn không có dòng platform_accounts) - khi đó không có gì để tắt.
                failure_reason = (
                    f"Login for account {account_key!r} did not succeed - no c_user cookie present "
                    f"afterwards (wrong password, or Facebook may have shown a checkpoint/2FA prompt "
                    f"instead of logging straight in)."
                )
                if account is not None:
                    pool.release_account(
                        "facebook", account["id"], success=False, hard_failure=True, reason=failure_reason
                    )
                    logger.error(
                        "account_disabled_checkpoint_suspected",
                        telegram=True,
                        platform="facebook",
                        account=account_key,
                        debug_screenshot=str(debug_path),
                    )
                raise RuntimeError(
                    f"{failure_reason} Saved a screenshot to {debug_path} for inspection. "
                    f"Re-run with --show-browser to watch it live."
                )

            if account is not None:
                pool.release_account("facebook", account["id"], success=True)
            save_native_state(redis_cache, account_key, context)
        else:
            context = new_context(
                browser, account_key=account_key, redis_cache=redis_cache, storage_state=stored_state, **context_kwargs
            )
            page = context.new_page()

        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        return browser, context, page, account_key
    except Exception:
        # Không có gì từ đây trở xuống trả trình duyệt về cho chỗ gọi, nên sẽ không ai khác gọi
        # browser.close() cho nó - đóng ở đây trước khi raise lại thay vì rò rỉ tiến trình Chromium
        # ở mỗi lần đăng nhập/checkpoint thất bại.
        browser.close()
        raise
