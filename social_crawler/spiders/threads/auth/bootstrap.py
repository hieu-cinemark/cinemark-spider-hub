"""
Bootstrap một phiên đăng nhập threads.com và bắt một request GraphQL thật để lấy
doc_id / fb_dtsg / lsd / __rev... rồi dùng chúng để phát lại request qua HTTP thường
(curl_cffi). Giống social_crawler.spiders.facebook.auth.bootstrap - xem docstring của
module đó để biết đầy đủ lý do (dùng lại storage_state, xoay tài khoản/import cookie, vì
sao bắt cache token thay vì tự dựng, và cùng cách phân nhánh _BootstrapType cho search và
comments).

Chạy một lần (hoặc định kỳ khi cache hết hạn):

    python -m social_crawler.spiders.threads.auth.bootstrap --query "test"

Lần chạy đầu chưa có storage_state: nếu bảng platform_accounts (xem accounts.py,
db/accounts.py) có một dòng threads đang bật, nó import thẳng trường "cookie" của tài
khoản được xoay tới (hoàn toàn không đăng nhập bằng trình duyệt). Nếu không, nó tự đăng
nhập bằng id/password của tài khoản (+ TOTP từ "2fa"), luôn qua đúng một proxy đã ghim cố
định của tài khoản đó (pool.pinned_login_proxy). Truyền --manual để tự đăng nhập bằng
tay; AUTO_LOGIN_KILL_SWITCH=true tắt auto-login. Các lần chạy sau dùng lại storage_state
đã lưu và chạy headless.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable, NamedTuple
from urllib.parse import parse_qsl

from patchright.sync_api import Playwright, sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_MAX_AGE_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    STATE_REDIS_KEY_TMPL,
    STATIC_BODY_FIELDS,
    STATIC_HEADER_FIELDS,
)
from social_crawler.db.accounts import get_account_by_key, reactivate_account
from social_crawler.logger import bind_run_id, get_logger
from social_crawler.services import pool
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR, has_display, new_context
from social_crawler.spiders.facebook.auth.request_capture import name_requests
from social_crawler.spiders.facebook.auth.triggers import MissingTotpSecretError, TwoFactorPromptNotHandledError
from social_crawler.spiders.threads.auth.accounts import account_key as normalize_account_key
from social_crawler.spiders.threads.auth.accounts import next_account
from social_crawler.spiders.threads.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    extract_user_agent,
    import_cookies,
    parse_cookie_header,
)
from social_crawler.spiders.threads.auth.request_capture import (
    capture_graphql_requests,
    pick_comments_request,
    pick_initial_request,
    pick_paginated_comments_request,
    pick_paginated_request,
)
from social_crawler.spiders.threads.auth.triggers import auto_login, comments_trigger, search_trigger

logger = get_logger(__name__)


class _BootstrapType(NamedTuple):
    """Mọi thứ khác nhau giữa một lần bootstrap "search" và "comments" - giống _BootstrapType
    của facebook.auth.bootstrap, xem docstring của module đó để biết vì sao có cái này (một
    cặp if/elif type=="search"/"comments" duy nhất lặp ba lần từng âm thầm không làm gì khi
    thêm loại mới vào một chỗ mà quên chỗ khác)."""

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
        # COMMENTS_REDIS_KEY_TMPL, KHÔNG phải CACHE_REDIS_KEY_TMPL: key đó giữ công thức search mà
        # threads_search phát lại. Ngày 2026-10-05 một lần bootstrap --post-url ghi đè nó bằng một
        # query feed đăng xuất, và mọi lượt search sau đó âm thầm trả về feed chung thay vì kết quả
        # theo từ khoá. Spider comment không cần công thức này (nó chỉ dùng cookie của session search
        # để GET text_feed), nên consumer giờ bootstrap Threads bằng --query - xem
        # crawl_request_consumer._ensure_threads_session.
        cache_key_tmpl=COMMENTS_REDIS_KEY_TMPL,
        saved_log_event="saved_comments_query_cache",
    ),
}


def _is_valid_storage_state(state: Any) -> bool:
    """Cùng lý do như facebook.auth.bootstrap._is_valid_storage_state - một cache hỏng/thiếu nên
    kích hoạt đăng nhập mới thay vì một lần crash không chẩn đoán được sâu bên trong
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
    """Logic đăng nhập/dùng lại session dùng chung - giống
    facebook.auth.bootstrap._get_authenticated_context từng trường một, chỉ là lấy từ bảng
    platform_accounts (platform='threads') / các hằng của threads.com."""
    if prefer_account:
        account = get_account_by_key("threads", prefer_account)
        if account is None:
            raise RuntimeError(f"No threads account matching {prefer_account!r}")
        account_key = normalize_account_key(account["id"])
    else:
        account = next_account(redis_cache)
        if account is not None:
            account_key = normalize_account_key(account["id"])
        else:
            account_key = DEFAULT_ACCOUNT_KEY

    state_key = STATE_REDIS_KEY_TMPL.format(account=account_key)
    stored_state = redis_cache.get(state_key)
    if stored_state is not None and not _is_valid_storage_state(stored_state):
        logger.warning("discarding_invalid_stored_state", account=account_key, key=state_key)
        stored_state = None

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
                # Cột cookie sai định dạng không bao giờ tự lành - tắt nó thay vì để nó ở trạng thái đã
                # nhận mà chưa trả: không có cái này, dòng đó cứ bị next_account() giao ra ở mỗi vòng xoay
                # (last_used_at đã được ghi lúc nhận) rồi lại raise ở đây, âm thầm chiếm một slot LRU thay
                # vì được gắn cờ để người sửa.
                pool.release_account("threads", account["id"], success=False, hard_failure=True, reason=failure_reason)
                logger.error(
                    "account_disabled_bad_cookie",
                    telegram=True,
                    platform="threads",
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
    # Cùng lý do như facebook.auth.bootstrap: tự đăng nhập bằng thông tin đăng nhập đã lưu,
    # nhưng chỉ bao giờ qua proxy đã ghim cố định của chính tài khoản (pool.pinned_login_proxy)
    # - không bao giờ không proxy, không bao giờ dùng proxy chưa ghim.
    auto = need_login and account is not None and not force_manual
    proxy = None
    if auto:
        try:
            if not account.get("password"):
                raise RuntimeError(f"Account {account_key!r} has no password stored - cannot auto-login.")
            proxy = pool.pinned_login_proxy("threads", account_key)
        except RuntimeError as exc:
            logger.error("auto_login_skipped", telegram=True, platform="threads", account=account_key, error=str(exc))
            pool.release_account("threads", account["id"], success=False, reason=str(exc))
            raise
    else:
        # required=not need_login - xem facebook.auth.bootstrap.
        proxy_cfg = pool.acquire_proxy_for_account("threads", account_key if account else None, required=not need_login)
        if proxy_cfg and proxy_cfg["login_use_proxy"]:
            proxy = {
                "server": f"http://{proxy_cfg['url']}",
                "username": proxy_cfg["username"],
                "password": proxy_cfg["password"],
            }

    if headless is not None:
        browser_headless = headless
    elif auto:
        # Chỉ mở có giao diện khi máy có màn hình - xem facebook.auth.bootstrap.
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
                platform="threads",
                account=account_key,
                error=str(exc),
            )
            pool.release_account("threads", account["id"], success=False, reason=str(exc))
        raise

    try:
        if need_login:
            context = new_context(browser, **context_kwargs)
            page = context.new_page()
            if auto:
                logger.info("auto_login_attempt", account=account_key, proxy=proxy["server"])
                try:
                    auto_login(page, account)
                except (MissingTotpSecretError, TwoFactorPromptNotHandledError) as exc:
                    # Lỗ hổng cấu hình/tự động hoá, không phải checkpoint - giống facebook.auth.bootstrap,
                    # KHÔNG được tắt cứng tài khoản như trường hợp chung "không có ds_user_id" bên dưới.
                    debug_path = BASE_DIR / f"debug_auto_login_{account_key}.png"
                    page.screenshot(path=str(debug_path))
                    logger.error(
                        "account_missing_totp_secret"
                        if isinstance(exc, MissingTotpSecretError)
                        else "account_2fa_prompt_not_handled",
                        telegram=True,
                        platform="threads",
                        account=account_key,
                        debug_screenshot=str(debug_path),
                    )
                    pool.release_account("threads", account["id"], success=False, reason=str(exc))
                    raise RuntimeError(f"Auto-login for account {account_key!r} could not finish 2FA: {exc}") from exc
                except Exception as exc:
                    # Form chưa bao giờ được gửi (proxy timeout, một ô không bao giờ render) - đánh lỗi nhẹ, xem
                    # facebook.auth.bootstrap.
                    logger.error(
                        "auto_login_crashed", telegram=True, platform="threads", account=account_key, error=str(exc)
                    )
                    pool.release_account("threads", account["id"], success=False, reason=str(exc))
                    raise
            else:
                page.goto("https://www.threads.com/login/")
                logger.info(
                    "manual_login_required",
                    account=account_key,
                    hint="press Enter here once you're done logging in",
                )
                input()

            if not any(c["name"] == "ds_user_id" for c in context.cookies()):
                debug_path = BASE_DIR / "debug_login_failed.png"
                page.screenshot(path=str(debug_path))
                # Cùng lý do như phép kiểm tra của facebook.auth.bootstrap: một lần đăng nhập thật bằng
                # thông tin đăng nhập đã lưu của chính tài khoản này mà không ra cookie đã đăng nhập là tín
                # hiệu rõ ràng nhất cho thấy đúng tài khoản này bị checkpoint, nên tắt nó thay vì để mọi
                # vòng xoay sau đâm vào cùng bức tường. account có thể là None (chỉ đăng nhập tay, không có
                # dòng platform_accounts) - khi đó không có gì để tắt.
                failure_reason = "no ds_user_id cookie after login attempt"
                if account is not None:
                    pool.release_account(
                        "threads", account["id"], success=False, hard_failure=True, reason=failure_reason
                    )
                    logger.error(
                        "account_disabled_checkpoint_suspected",
                        telegram=True,
                        platform="threads",
                        account=account_key,
                        debug_screenshot=str(debug_path),
                    )
                raise RuntimeError(
                    f"Login for account {account_key!r} did not succeed - no ds_user_id cookie present "
                    f"afterwards (wrong password, or Instagram may have shown a checkpoint/2FA prompt "
                    f"instead of logging straight in). Saved a screenshot to {debug_path} for inspection. "
                    f"Re-run with --show-browser to watch it live."
                )

            if account is not None:
                pool.release_account("threads", account["id"], success=True)
            redis_cache.set(state_key, context.storage_state())
        else:
            context = new_context(browser, storage_state=stored_state, **context_kwargs)
            page = context.new_page()

        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        return browser, context, page, account_key
    except Exception:
        browser.close()
        raise


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
            requests_seen = capture_graphql_requests(page, bootstrap_type.trigger(query))

            named = name_requests(requests_seen)
            logger.info("captured_graphql_requests", names=[name for _, name in named], count=len(named))

            initial_request = bootstrap_type.pick_initial(named)
            paginated_request = bootstrap_type.pick_paginated(named)

            if paginated_request is None:
                logger.warning(
                    "no_paginated_request_captured",
                    note="pagination will be unavailable until a future bootstrap run captures one",
                )

            headers = {k.lower(): v for k, v in initial_request.headers.items()}
            cookies = {c["name"]: c["value"] for c in context.cookies()}

            if not cookies.get("ds_user_id"):
                raise RuntimeError("Cookie ds_user_id is missing - the session does not appear to be logged in.")

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

            cache_key = bootstrap_type.cache_key_tmpl.format(account=account_key)
            redis_cache.set(cache_key, cache, ttl_seconds=CACHE_MAX_AGE_SECONDS)
            logger.info(
                bootstrap_type.saved_log_event,
                telegram=True,
                key=cache_key,
                account=account_key,
                ttl_seconds=CACHE_MAX_AGE_SECONDS,
            )
            if prefer_account:
                row = get_account_by_key("threads", prefer_account)
                if row is not None:
                    reactivate_account("threads", row["id"])
                    logger.info("account_reactivated_after_restore", platform="threads", account=account_key)
        finally:
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Search keyword used to trigger a GraphQL search request")
    parser.add_argument("--post-url", help="Post URL used to trigger a GraphQL replies-list request")
    parser.add_argument(
        "--cookies-file",
        help="Path to a JSON file with cookies from an already-logged-in browser session "
        '(either {"ds_user_id": "...", "sessionid": "...", ...} or a full Playwright cookie list). '
        "Skips manual login entirely - run this once, then run --query normally.",
    )
    parser.add_argument(
        "--account",
        help="platform_accounts id (or email). With --cookies-file, saves the imported session "
        "under that account. With --query/--post-url, reuses that account's cached storage_state "
        "or cookie field instead of rotating the pool.",
    )
    parser.add_argument(
        "--show-browser", action="store_true", help="Show the browser window even if a session already exists"
    )
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Log in by hand even if platform_accounts has credentials for the rotated account - use this "
        "once when that account hits a checkpoint/verification screen auto-login can't click through.",
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
        from social_crawler.spiders.facebook.auth.cookies import load_exported_cookies

        import_cookies(
            load_exported_cookies(Path(args.cookies_file).read_text(encoding="utf-8")),
            account=args.account,
        )
    elif args.post_url:
        bootstrap(
            args.post_url,
            headless=False if args.show_browser else None,
            type="comments",
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
