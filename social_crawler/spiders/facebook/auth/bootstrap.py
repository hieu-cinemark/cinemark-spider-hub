"""
Bootstrap một phiên đăng nhập Facebook và bắt một request GraphQL thật để lấy
doc_id / fb_dtsg / lsd / __rev... rồi dùng chúng để phát lại request qua HTTP thường
(curl_cffi).

Chạy một lần (hoặc định kỳ khi cache hết hạn):

    python -m social_crawler.spiders.facebook.auth.bootstrap --query "test"

Lần chạy đầu chưa có storage_state: nếu bảng platform_accounts (xem accounts.py,
db/accounts.py) có một dòng facebook đang bật, nó import thẳng trường "cookie" của tài
khoản được xoay tới (hoàn toàn không đăng nhập bằng trình duyệt). Nếu không, nó tự đăng
nhập bằng id/password của tài khoản (+ TOTP từ "2fa"), luôn qua đúng một proxy đã ghim cố
định của tài khoản đó (pool.pinned_login_proxy) - không bao giờ không proxy, không bao giờ
dùng proxy chưa ghim. Truyền --manual để tự đăng nhập bằng tay; AUTO_LOGIN_KILL_SWITCH=true
tắt auto-login. Các lần chạy sau dùng lại storage_state đã lưu và chạy headless.

Cả phiên đăng nhập (cookie) lẫn cache token bắt được đều lưu trong Redis, không trên đĩa -
Playwright nhận thẳng storage_state dạng dict, nên hoàn toàn không cần file local.

Phần tương tác thật với form đăng nhập, chọn request GraphQL, xoay tài khoản và logic
import cookie nằm ở các module anh em (triggers.py / request_capture.py / accounts.py /
cookies.py / browser_interaction.py) - file này chỉ nối chúng lại và mở ra CLI.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable, NamedTuple
from urllib.parse import parse_qsl

# patchright, không phải playwright: một bản fork Playwright đã vá, sửa các chỗ rò rỉ CDP
# (Chrome DevTools Protocol) mà các hệ thống phát hiện bot như reCAPTCHA Enterprise dựa vào
# (tác dụng phụ của Runtime.enable, addScriptToEvaluateOnNewDocument, v.v.) - cùng API, thay
# thế trực tiếp. Riêng việc ghi đè navigator.webdriver của Playwright thường không giấu được
# các dấu vết sâu hơn này, đó là lý do cứ bị captcha ở đây dù đã gõ phím/di chuột giống
# người và dùng proxy đúng vùng địa lý.
from patchright.sync_api import Playwright, sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_MAX_AGE_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    REPLIES_REDIS_KEY_TMPL,
    STATE_REDIS_KEY_TMPL,
    STATIC_BODY_FIELDS,
    STATIC_HEADER_FIELDS,
)
from social_crawler.db.accounts import get_account_by_key, reactivate_account, record_cookie_check
from social_crawler.logger import bind_run_id, get_logger
from social_crawler.services import pool
from social_crawler.spiders.comet_graphql_client import is_search_recipe
from social_crawler.spiders.facebook.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.facebook.auth.accounts import next_account
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, has_display, new_context
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    extract_user_agent,
    parse_cookie_header,
)
from social_crawler.spiders.facebook.auth.request_capture import (
    capture_graphql_requests,
    name_requests,
    pick_comments_request,
    pick_initial_request,
    pick_paginated_comments_request,
    pick_paginated_request,
    scrape_comments_pagination_doc_id,
    synthesize_comments_pagination,
)
from social_crawler.spiders.facebook.auth.triggers import (
    MissingTotpSecretError,
    TwoFactorPromptNotHandledError,
    auto_login,
    comments_trigger,
    replies_trigger,
    search_trigger,
)

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
    checkpoint/xác minh mà auto-login không bấm qua được; storage_state vẫn được lưu dưới key
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
    # Tài khoản có thông tin đăng nhập đã lưu mà không có session dùng được thì tự đăng nhập
    # (auto_login) - nhưng chỉ bao giờ qua đúng một proxy đã ghim cố định của chính nó (xem
    # pool.pinned_login_proxy): một lần đăng nhập mới bằng thông tin đăng nhập từ một IP
    # mới/IP thật của server là một trong những tín hiệu mạnh nhất mà hệ thống phát hiện gian
    # lận của Facebook theo dõi (đó là thứ đã khiến honghieu3403b@gmail.com bị gắn cờ ngày
    # 2026-09-11, khi đăng nhập không ghim proxy). --manual vẫn ép đăng nhập có người giám sát
    # thay thế, và AUTO_LOGIN_KILL_SWITCH=true tắt hẳn auto-login (pinned_login_proxy raise).
    auto = need_login and account is not None and not force_manual
    proxy = None
    if auto:
        try:
            if not account.get("password"):
                raise RuntimeError(f"Account {account_key!r} has no password stored - cannot auto-login.")
            proxy = pool.pinned_login_proxy("facebook", account_key)
        except RuntimeError as exc:
            # Lỗi nhẹ - bản thân tài khoản không có vấn đề gì. Vẫn trả nó lại để LRU của next_account()
            # chuyển sang tài khoản khác thay vì giao lại đúng tài khoản này ở vòng xoay sau.
            logger.error("auto_login_skipped", telegram=True, platform="facebook", account=account_key, error=str(exc))
            pool.release_account("facebook", account["id"], success=False, reason=str(exc))
            raise
    else:
        # required=not need_login: một lần đăng nhập tay mới là sự kiện một lần, có người giám sát,
        # có thể hợp lý khi chạy không proxy nếu hiện không có proxy nào - nhưng dùng lại một
        # session đã cache là lưu lượng thường ngày y như client phát lại của
        # comet_graphql_client.py, vốn bắt buộc proxy đã ghim (một session lập trên một IP rồi phát
        # lại từ IP khác chính là chỗ lệch mà việc ghim ngăn chặn).
        proxy_cfg = pool.acquire_proxy_for_account(
            "facebook", account_key if account else None, required=not need_login
        )
        if proxy_cfg and proxy_cfg["login_use_proxy"]:
            proxy = {
                "server": f"http://{proxy_cfg['url']}",
                "username": proxy_cfg["username"],
                "password": proxy_cfg["password"],
            }

    if headless is not None:
        browser_headless = headless
    elif auto:
        # Không ai theo dõi một lần đăng nhập tự động: có giao diện ở máy có màn hình (gần với trình
        # duyệt của người dùng thật hơn), headless ở máy không có màn hình như server crawl
        # systemd, nơi khởi chạy có giao diện chỉ crash.
        browser_headless = not has_display()
    else:
        browser_headless = not need_login
    logger.info(
        "launching_browser",
        account=account_key,
        headless=browser_headless,
        need_login=need_login,
        proxy=proxy["server"] if proxy else None,
    )
    try:
        browser = pw.chromium.launch(headless=browser_headless, proxy=proxy)
    except Exception as exc:
        if auto:
            logger.error(
                "auto_login_browser_launch_failed",
                telegram=True,
                platform="facebook",
                account=account_key,
                error=str(exc),
            )
            pool.release_account("facebook", account["id"], success=False, reason=str(exc))
        raise

    try:
        if need_login:
            context = new_context(browser, account_key=account_key, **context_kwargs)
            page = context.new_page()
            if auto:
                logger.info("auto_login_attempt", account=account_key, proxy=proxy["server"])
                try:
                    auto_login(page, account)
                except (MissingTotpSecretError, TwoFactorPromptNotHandledError) as exc:
                    # Một lỗ hổng cấu hình/tự động hoá (chưa có totp_secret, hoặc màn hình 2FA mà selector của
                    # ta không điền được), không phải bằng chứng tài khoản bị checkpoint - KHÔNG được tắt cứng
                    # như trường hợp chung "không có c_user" bên dưới (cả hai đã xảy ra thật với những tài
                    # khoản hoàn toàn tốt).
                    debug_path = BASE_DIR / f"debug_auto_login_{account_key}.png"
                    page.screenshot(path=str(debug_path))
                    logger.error(
                        "account_missing_totp_secret"
                        if isinstance(exc, MissingTotpSecretError)
                        else "account_2fa_prompt_not_handled",
                        telegram=True,
                        platform="facebook",
                        account=account_key,
                        debug_screenshot=str(debug_path),
                    )
                    pool.release_account("facebook", account["id"], success=False, reason=str(exc))
                    raise RuntimeError(f"Auto-login for account {account_key!r} could not finish 2FA: {exc}") from exc
                except Exception as exc:
                    # Form chưa bao giờ được gửi (proxy timeout, một ô không bao giờ render) - không có gì cho
                    # thấy bản thân tài khoản tồi, nên đánh lỗi nhẹ như auto_login_skipped thay vì để nó ở
                    # trạng thái đã nhận mà không ghi kết quả nào.
                    logger.error(
                        "auto_login_crashed", telegram=True, platform="facebook", account=account_key, error=str(exc)
                    )
                    pool.release_account("facebook", account["id"], success=False, reason=str(exc))
                    raise
            else:
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
                # Một lần đăng nhập thật bằng thông tin đăng nhập đã lưu của chính tài khoản này, không phải
                # người gõ ở màn hình nhắc nhập tay - tín hiệu rõ ràng nhất cho thấy chính tài khoản này
                # (không chỉ lượt chạy này) bị checkpoint, nên tắt nó thay vì để mọi vòng xoay sau đâm vào
                # cùng bức tường. account có thể là None ở đây (hoàn toàn không có dòng platform_accounts,
                # chỉ đăng nhập tay) - khi đó không có gì để tắt.
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
            redis_cache.set(state_key, context.storage_state())
        else:
            context = new_context(browser, account_key=account_key, storage_state=stored_state, **context_kwargs)
            page = context.new_page()

        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        return browser, context, page, account_key
    except Exception:
        # Không có gì từ đây trở xuống trả trình duyệt về cho chỗ gọi, nên sẽ không ai khác gọi
        # browser.close() cho nó - đóng ở đây trước khi raise lại thay vì rò rỉ tiến trình Chromium
        # ở mỗi lần đăng nhập/checkpoint thất bại.
        browser.close()
        raise


class _BootstrapType(NamedTuple):
    """Mọi thứ khác nhau giữa một lần bootstrap "search" và "comments", gom về một chỗ - trước
    đây việc phân nhánh type=="search"/"comments" bị lặp ba lần riêng rẽ trong bootstrap(),
    mỗi lần một if/elif không có `else`, nên một loại mới thêm vào một chỗ mà quên chỗ khác sẽ
    âm thầm không làm gì thay vì lỗi rõ ràng."""

    trigger: Callable[[str], Callable]
    pick_initial: Callable[[list], Any]
    pick_paginated: Callable[[list], Any | None]
    cache_key_tmpl: str
    saved_log_event: str


_BOOTSTRAP_TYPES = {
    "search": _BootstrapType(
        trigger=search_trigger,
        pick_initial=pick_initial_request,
        pick_paginated=pick_paginated_request,
        cache_key_tmpl=CACHE_REDIS_KEY_TMPL,
        saved_log_event="saved_token_cache",
    ),
    "comments": _BootstrapType(
        trigger=comments_trigger,
        pick_initial=pick_comments_request,
        pick_paginated=pick_paginated_comments_request,
        cache_key_tmpl=COMMENTS_REDIS_KEY_TMPL,
        saved_log_event="saved_comments_query_cache",
    ),
    # Dùng lại nguyên pick_comments_request/pick_paginated_comments_request: cả hai chỉ tìm chữ
    # "comment" trong friendly_name của request và tránh request gói parallelfetch, vốn khớp
    # một request GraphQL danh sách reply cũng tốt như một request comment cấp một - Facebook
    # không đặt cho query reply một friendly_name có dạng khác.
    "replies": _BootstrapType(
        trigger=replies_trigger,
        pick_initial=pick_comments_request,
        pick_paginated=pick_paginated_comments_request,
        cache_key_tmpl=REPLIES_REDIS_KEY_TMPL,
        saved_log_event="saved_replies_query_cache",
    ),
}


def bootstrap(
    query: str,
    headless: bool | None = None,
    type: str = "search",
    force_manual: bool = False,
    prefer_account: str | None = None,
) -> None:
    bootstrap_type = _BOOTSTRAP_TYPES.get(type)
    if bootstrap_type is None:
        raise ValueError(f"Unknown bootstrap type: {type}")

    redis_cache = RedisCache()

    with sync_playwright() as pw:
        browser, context, page, account_key = _get_authenticated_context(
            pw, redis_cache, headless, force_manual, prefer_account=prefer_account
        )

        try:
            pagination_doc_ids: dict[str, str] = {}

            def _on_js_response(response) -> None:
                scrape_comments_pagination_doc_id(response, pagination_doc_ids)

            requests_seen = capture_graphql_requests(
                page,
                bootstrap_type.trigger(query),
                on_response=_on_js_response if type in ("comments", "replies") else None,
            )

            named = name_requests(requests_seen)
            logger.info("captured_graphql_requests", names=[name for _, name in named], count=len(named))

            if not named:
                # Một session import từ cookie/đã cache có thể trông hợp lệ (có đủ mọi tên trong
                # REQUIRED_LOGIN_COOKIES - xem _is_valid_storage_state/import_cookies) trong khi thực ra đã
                # chết: Facebook đăng xuất session phía server mà không bao giờ xoá các tên cookie đó phía
                # client, nên phép kiểm tra c_user của _get_authenticated_context (chỉ chạy với lần đăng
                # nhập *mới*, không phải session import/cache) không bao giờ bắt được trường hợp này. Lưu
                # cùng loại bằng chứng debug mà phép kiểm tra đó lưu khi đăng nhập thật thất bại, để chuyện
                # này không chỉ hiện ra thành "không bắt được request nào" trơn trọi mà không có cách phân
                # biệt "session thực sự đã chết" với "giao diện Facebook đã đổi" hay "selector của trigger
                # bị hỏng" nếu không chạy tương tác lại.
                debug_path = BASE_DIR / f"debug_no_graphql_captured_{account_key}.png"
                page.screenshot(path=str(debug_path))
                cookies_now = {c["name"]: c["value"] for c in context.cookies()}
                logger.error(
                    "no_graphql_captured_diagnostics",
                    telegram=True,
                    account=account_key,
                    page_url=page.url,
                    has_c_user=bool(cookies_now.get("c_user")),
                    debug_screenshot=str(debug_path),
                )
                _mark_session_dead(redis_cache, account_key, page_url=page.url)

            initial_request = bootstrap_type.pick_initial(named)
            paginated_request = bootstrap_type.pick_paginated(named)

            headers = {k.lower(): v for k, v in initial_request.headers.items()}
            cookies = {c["name"]: c["value"] for c in context.cookies()}

            # xác nhận ta thực sự đã đăng nhập (c_user phải là user id thật)
            if not cookies.get("c_user"):
                raise RuntimeError("Cookie c_user is missing - the session does not appear to be logged in.")

            # cache nguyên variables của request thật (kể cả mọi cờ __relay_internal__pv__... mà schema
            # hiện tại yêu cầu) thay vì tự dựng - chỉ ghi đè text/count/cursor khi phát lại
            initial_body = dict(parse_qsl(initial_request.post_data or "", keep_blank_values=True))
            cache = {
                "captured_at": int(time.time()),
                "cookies": cookies,
                "headers": {k: headers[k] for k in STATIC_HEADER_FIELDS if k in headers},
                "body_static": {k: initial_body[k] for k in STATIC_BODY_FIELDS if k in initial_body},
                "doc_id": initial_body.get("doc_id"),
                "fb_api_req_friendly_name": initial_body.get("fb_api_req_friendly_name"),
                "variables_template": json.loads(initial_body.get("variables", "{}")),
            }

            if paginated_request is not None:
                paginated_body = dict(parse_qsl(paginated_request.post_data or "", keep_blank_values=True))
                cache["pagination"] = {
                    "doc_id": paginated_body.get("doc_id"),
                    "fb_api_req_friendly_name": paginated_body.get("fb_api_req_friendly_name"),
                    "variables_template": json.loads(paginated_body.get("variables", "{}")),
                }
            elif type in ("comments", "replies") and pagination_doc_ids:
                # Cuộn headless thường không bao giờ bắn CommentsListComponentsPaginationQuery (đã xác nhận
                # 2026-09-16) kể cả trên bài có hàng nghìn comment - chỉ query gốc xuất hiện trong GraphQL
                # bắt được. Chunk JS của Relay vẫn lộ doc_id đã lưu của query đó, nên tự dựng khối
                # pagination từ nó + variables gốc thay vì để mọi lượt crawl comment kẹt ở trang 1.
                relay_name, doc_id = next(iter(pagination_doc_ids.items()))
                for name, did in pagination_doc_ids.items():
                    if name.startswith("CommentsListComponentsPaginationQuery"):
                        relay_name, doc_id = name, did
                        break
                cache["pagination"] = synthesize_comments_pagination(
                    cache["variables_template"],
                    doc_id=doc_id,
                )
                logger.info(
                    "synthesized_comments_pagination_from_js",
                    account=account_key,
                    relay_operation=relay_name,
                    doc_id=doc_id,
                )
            elif paginated_request is None:
                if type in ("comments", "replies"):
                    # Cache comments/replies không có pagination bị
                    # crawl_request_consumer._facebook_comments_cache_usable / _facebook_replies_cache_usable
                    # coi là thiếu - lưu nó chỉ bắt mọi job sau phải bootstrap lại. Thà cho lượt chạy này thất
                    # bại (và để Redis trống) còn hơn ghi một cache độc.
                    raise RuntimeError(
                        f"Captured a {type} query but no pagination (scroll did not "
                        "fire CommentsListComponentsPaginationQuery and no "
                        "pagination doc_id was found in JS). Re-run bootstrap on a "
                        "normal /posts/ URL with many comments until pagination is captured."
                    )
                logger.warning(
                    "no_paginated_request_captured",
                    note="pagination will be unavailable until a future bootstrap run captures one",
                )

            cache_key = bootstrap_type.cache_key_tmpl.format(account=account_key)
            redis_cache.set(cache_key, cache, ttl_seconds=CACHE_MAX_AGE_SECONDS)
            base_key = CACHE_REDIS_KEY_TMPL.format(account=account_key)
            if bootstrap_type.cache_key_tmpl != CACHE_REDIS_KEY_TMPL and not is_search_recipe(
                redis_cache.get(base_key)
            ):
                # Chỉ ghi khi key search chưa có công thức search hợp lệ: ghi đè vô điều kiện (như
                # trước 2026-10-05) thay công thức search bằng query comment, và mọi lượt search sau đó
                # âm thầm bỏ qua từ khoá. Khi phải ghi, consumer sẽ thấy đây không phải công thức search
                # (_facebook_session_is_cached) và bootstrap search lại trước lượt crawl search kế tiếp.
                #
                # __init__ của comet_graphql_client.py (lớp cơ sở dùng chung mà mọi FacebookGraphQLClient
                # dùng, kể cả comments/replies) luôn cần cả mục CACHE_REDIS_KEY_TMPL cho tài khoản này -
                # đó là thứ cung cấp cookie/header cho request bất kể loại query cụ thể nào, chỉ
                # doc_id/variables_template mới thực sự khác theo loại bootstrap (xem dạng của `cache` ở
                # trên, dựng giống hệt nhau dù thế nào). Không có cái này, một tài khoản chỉ từng chạy
                # bootstrap "comments"/"replies" sẽ không bao giờ dựng được client - đã xảy ra thật
                # (2026-09-16): _ensure_comments_cache của crawl_request_consumer.py chỉ kiểm tra/làm mới
                # cache query comment của tài khoản NÀY, nên một lần bootstrap chỉ cho comment cứ "thành
                # công" trong khi mọi lượt crawl comment thật lập tức SessionExpiredError vì thiếu cache
                # session cơ sở, thoát 0 (bắt, log, không raise lại) mà không lấy được comment nào - một kiểu
                # lỗi âm thầm, xảy ra 100% số lần, không phải thỉnh thoảng.
                redis_cache.set(base_key, cache, ttl_seconds=CACHE_MAX_AGE_SECONDS)
            logger.info(
                bootstrap_type.saved_log_event,
                telegram=True,
                key=cache_key,
                account=account_key,
                ttl_seconds=CACHE_MAX_AGE_SECONDS,
            )
            if prefer_account:
                row = get_account_by_key("facebook", prefer_account)
                if row is not None:
                    reactivate_account("facebook", row["id"])
                    logger.info("account_reactivated_after_restore", platform="facebook", account=account_key)
        finally:
            # storage_state có thể đã thay đổi (FB xoay cookie) - lưu lại kể cả khi các bước bắt/chọn
            # ở trên thất bại (ví dụ không bắt được request GraphQL nào), để một lần đăng nhập đã thành
            # công không bị bỏ phí, và luôn đóng trình duyệt để lỗi ở đây không rò rỉ tiến trình
            # Chromium.
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Search keyword used to trigger a GraphQL search request")
    parser.add_argument("--post-url", help="Post/reel URL used to trigger a GraphQL comments-list request")
    parser.add_argument(
        "--type",
        choices=["comments", "replies"],
        default="comments",
        help="Only used with --post-url: 'comments' captures the top-level comments-list request (default), "
        "'replies' opens one comment's replies thread and captures that request instead - run this once "
        "against a post/URL where a top-level comment clearly has replies.",
    )
    parser.add_argument(
        "--cookies-file",
        help="Path to a JSON file with cookies from an already-logged-in browser session "
        '(either {"c_user": "...", "xs": "...", ...} or a full Playwright cookie list). '
        "Skips manual login entirely - run this once, then run --query/--post-url normally.",
    )
    parser.add_argument(
        "--account",
        help="platform_accounts email (or id if email is blank). With --cookies-file, saves the "
        "imported session under that account. With --query/--post-url, reuses that account's "
        "cached storage_state or cookie field instead of rotating the pool - used by dashboard "
        "restore / cookie-import refresh so a checkpointed row can still be tried.",
    )
    parser.add_argument(
        "--show-browser", action="store_true", help="Show the browser window even if a session already exists"
    )
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Log in by hand even if platform_accounts has credentials for the rotated account - use this "
        "once when that account hits a checkpoint/verification screen auto-login can't click through. "
        "The session still gets saved under that same account, so later runs go back to headless auto-login.",
    )
    parser.add_argument(
        "--run-id",
        help="Set by crawl_request_consumer.py for a dashboard-triggered refresh - binds this value onto "
        "every log line this process emits (see logger.bind_run_id) so cinemark-api's refresh_tracker can "
        "isolate this exact run's own lines out of the shared consumer.log. Not needed for manual/local use.",
    )
    args = parser.parse_args()

    if args.run_id:
        bind_run_id(args.run_id)

    if args.cookies_file:
        from social_crawler.spiders.facebook.auth.cookies import import_cookies, load_exported_cookies

        import_cookies(
            load_exported_cookies(Path(args.cookies_file).read_text(encoding="utf-8")),
            account=args.account,
        )
    elif args.post_url:
        bootstrap(
            args.post_url,
            headless=False if args.show_browser else None,
            type=args.type,
            force_manual=args.manual,
            prefer_account=args.account,
        )
    else:
        bootstrap(
            args.query or "test",
            headless=False if args.show_browser else None,
            force_manual=args.manual,
            prefer_account=args.account,
        )
