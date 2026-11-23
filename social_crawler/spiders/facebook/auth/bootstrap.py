"""
Bootstrap một phiên đăng nhập Facebook và bắt một request GraphQL thật để lấy
doc_id / fb_dtsg / lsd / __rev... rồi dùng chúng để phát lại request qua HTTP thường
(curl_cffi).

Chạy một lần (hoặc định kỳ khi cache hết hạn):

    python -m social_crawler.spiders.facebook.auth.bootstrap --query "test"

Lần chạy đầu chưa có storage_state: nếu bảng platform_accounts (xem accounts.py,
db/accounts.py) có một dòng facebook đang bật, nó import thẳng trường "cookie" của tài
khoản được xoay tới (hoàn toàn không đăng nhập bằng trình duyệt). Không có cookie thì dừng và báo cần nạp cookie từ trình
duyệt thật; --manual để người vận hành tự đăng nhập. Các lần chạy sau dùng lại storage_state đã lưu và chạy headless.

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
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import parse_qsl

# patchright, không phải playwright: một bản fork Playwright đã vá, sửa các chỗ rò rỉ CDP
# (Chrome DevTools Protocol) mà các hệ thống phát hiện bot như reCAPTCHA Enterprise dựa vào
# (tác dụng phụ của Runtime.enable, addScriptToEvaluateOnNewDocument, v.v.) - cùng API, thay
# thế trực tiếp. Riêng việc ghi đè navigator.webdriver của Playwright thường không giấu được
# các dấu vết sâu hơn này, đó là lý do cứ bị captcha ở đây dù đã gõ phím/di chuột giống
# người và dùng proxy đúng vùng địa lý.
from patchright.sync_api import sync_playwright

from social_crawler.accounts.account_warning import detect_warning, rest_warned_account
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    REPLIES_REDIS_KEY_TMPL,
    STATIC_BODY_FIELDS,
    STATIC_HEADER_FIELDS,
)
from social_crawler.core.logger import bind_run_id, get_logger
from social_crawler.db.accounts import get_account_by_key, reactivate_account, record_cookie_check
from social_crawler.runtime import pool
from social_crawler.runtime.lanes import release_account as release_lane_account
from social_crawler.spiders.common.comet_graphql_client import is_search_recipe
from social_crawler.spiders.facebook.auth.bootstrap_context import _get_authenticated_context, _mark_session_dead
from social_crawler.spiders.facebook.auth.browser_interaction import BASE_DIR
from social_crawler.spiders.facebook.auth.identity import save_native_state
from social_crawler.spiders.facebook.auth.request_capture import (
    capture_graphql_requests,
    name_requests,
    pick_comments_request,
    pick_last_initial_request,
    pick_paginated_comments_request,
    pick_paginated_request,
    pick_replies_request,
    scrape_comments_pagination_doc_id,
    synthesize_comments_pagination,
)
from social_crawler.spiders.facebook.auth.triggers import (
    comments_trigger,
    replies_trigger,
    search_trigger,
)

logger = get_logger(__name__)


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
        pick_initial=pick_last_initial_request,
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
    "replies": _BootstrapType(
        trigger=replies_trigger,
        pick_initial=pick_replies_request,
        pick_paginated=pick_replies_request,
        cache_key_tmpl=REPLIES_REDIS_KEY_TMPL,
        saved_log_event="saved_replies_query_cache",
    ),
}


def _light_session_check(page, context, redis_cache: RedisCache, account_key: str, prefer_account: str | None) -> None:
    """Thử phiên đã lưu, nhẹ như người mở Facebook xem bảng tin: vào trang chủ, dừng, lướt vài lần rồi thôi. Không gõ từ
    khoá, không mở trang tìm kiếm, không cố bắt GraphQL (token/recipe Facebook dựng qua HTTP khi nhận job crawl). Nút
    "Thử phiên đã lưu" từng chạy luôn chuỗi tìm kiếm cố định "tin tức hôm nay" - đúng mẫu lặp lại của bot, và là việc đầu
    tiên một phiên vừa nạp cookie làm (27/10/2026 từng gây checkpoint).

    Chỉ kết luận "chết" khi có bằng chứng rõ (checkpoint, về trang đăng nhập, mất c_user, header logout - header đã được
    stop_on_logout xử lý). Kết quả mơ hồ (trang chậm, không thấy feed) KHÔNG xoá phiên, chỉ báo lỗi để thử lại sau."""
    from social_crawler.spiders.facebook.auth.browser_interaction import human_wait, natural_scroll
    from social_crawler.spiders.facebook.auth.triggers import pass_consent_page

    page.goto("https://www.facebook.com/", wait_until="domcontentloaded")
    pass_consent_page(page, "https://www.facebook.com/")
    human_wait(page, 3000, 3000)
    natural_scroll(page, min_scrolls=2, max_scrolls=3, min_px=300, max_px=800, pause_base_ms=1800, pause_jitter_ms=2500)
    human_wait(page, 1500, 2500)

    url = (page.url or "").lower()
    if "/checkpoint" in url:
        _mark_session_dead(redis_cache, account_key, page_url=page.url)
        raise RuntimeError(f"Session check hit a checkpoint page: {page.url}")
    cookies_now = {c["name"] for c in context.cookies()}
    on_login = "/login" in url or page.locator('input[name="pass"]').count() > 0
    if on_login or "c_user" not in cookies_now:
        _mark_session_dead(redis_cache, account_key, page_url=page.url)
        raise RuntimeError(f"Session check was sent to the login page: {page.url}")
    marker = detect_warning(page)
    if marker:
        rest_warned_account("facebook", account_key, marker, where="session_check", url=page.url)
        release_lane_account(account_key, redis_cache)
        raise RuntimeError(f"Meta warning on account {account_key!r}: {marker!r} - account rested.")
    if "/privacy/consent" in url:
        raise RuntimeError(f"Session check stuck on consent page, session not judged: {page.url}")

    logger.info("facebook_session_check_ok", account=account_key, page_url=page.url)
    if prefer_account:
        row = get_account_by_key("facebook", prefer_account)
        if row is not None:
            record_cookie_check("facebook", row["id"], status="alive", note="light session check passed")
            reactivate_account("facebook", row["id"])


def bootstrap(
    query: str,
    headless: bool | None = None,
    type: str = "search",
    force_manual: bool = False,
    prefer_account: str | None = None,
    check_only: bool = False,
) -> None:
    """check_only=True: chỉ kiểm tra phiên còn sống (xem _light_session_check), không gõ tìm kiếm, không lưu token.

    Mở trình duyệt (Playwright) với tài khoản được chọn, chạy luồng kích hoạt của loại `type` (search, comments,
    replies) để Facebook bắn request GraphQL thật, bắt `doc_id` / `fb_dtsg` / `lsd` /
    `__rev`... rồi lưu vào Redis cho `FacebookGraphQLClient` phát lại bằng HTTP. `force_manual=True` ép tự đăng nhập bằng tay kể cả khi tài khoản đã có sẵn cookie/phiên; hết proxy thì dừng spider bằng mã thoát riêng."""
    bootstrap_type = _BOOTSTRAP_TYPES.get(type)
    if bootstrap_type is None:
        raise ValueError(f"Unknown bootstrap type: {type}")

    redis_cache = RedisCache()

    with sync_playwright() as pw:
        try:
            browser, context, page, account_key = _get_authenticated_context(
                pw, redis_cache, headless, force_manual, prefer_account=prefer_account
            )
        except pool.ProxyPoolExhaustedError as exc:
            # Cổng bận / tài khoản bận / chưa xác nhận được đường mạng: mã 75 để consumer xếp lại, không phạt tài khoản.
            pool.abort_spider_for_network_error(exc)

        def stop_on_logout(response):
            from social_crawler.spiders.facebook.auth.http_refresh import session_logged_out

            if session_logged_out(response.headers):
                _mark_session_dead(redis_cache, account_key, page_url=page.url)
                page.close()
                raise RuntimeError("Facebook sent an explicit logout header; bootstrap stopped")

        page.on("response", stop_on_logout)
        try:
            pagination_doc_ids: dict[str, str] = {}

            def _on_js_response(response) -> None:
                scrape_comments_pagination_doc_id(response, pagination_doc_ids)

            on_response = None
            if type in ("comments", "replies"):
                on_response = _on_js_response
            if check_only:
                _light_session_check(page, context, redis_cache, account_key, prefer_account)
                return
            requests_seen = capture_graphql_requests(page, bootstrap_type.trigger(query), on_response=on_response)

            # Meta hiện cảnh báo "hành vi tự động" / "tạm thời bị chặn" (nấc trước checkpoint): không lưu token để phát
            # lại thêm request trên tài khoản đã bị gắn cờ - cho nghỉ, nhả làn, lượt sau dùng tài khoản khác.
            marker = detect_warning(page)
            if marker:
                rest_warned_account("facebook", account_key, marker, where=f"bootstrap:{type}", url=page.url)
                redis_cache.delete(CACHE_REDIS_KEY_TMPL.format(account=account_key))
                if redis_cache.get(ACTIVE_ACCOUNT_REDIS_KEY) == account_key:
                    redis_cache.delete(ACTIVE_ACCOUNT_REDIS_KEY)
                release_lane_account(account_key, redis_cache)
                raise RuntimeError(f"Meta warning on account {account_key!r}: {marker!r} - account rested.")

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
                if "/privacy/consent" in (page.url or "") and cookies_now.get("c_user"):
                    # Kẹt ở trang đồng ý cookie, phiên vẫn còn - không đánh dấu chết (xem triggers.pass_consent_page).
                    logger.error(
                        "facebook_stuck_on_consent_page", telegram=True, account=account_key, page_url=page.url
                    )
                else:
                    logger.warning("session_state_unconfirmed_no_query", account=account_key)

            try:
                initial_request = bootstrap_type.pick_initial(named)
            except RuntimeError:
                # Chỉ thiếu query hoặc chỉ có CometLogoutHandlerQuery thì chưa đủ để kết luận đã đăng xuất.
                if "/privacy/consent" in (page.url or "") and any(c["name"] == "c_user" for c in context.cookies()):
                    logger.error(
                        "facebook_stuck_on_consent_page", telegram=True, account=account_key, page_url=page.url
                    )
                    raise
                raise
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
                "request_context_id": uuid.uuid4().hex,
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

            if type == "replies":
                cache["reply_operations"] = {}
                for request, name in named:
                    if name in ("Depth1CommentsListPaginationQuery", "Depth2CommentsListPaginationQuery"):
                        body = dict(parse_qsl(request.post_data or "", keep_blank_values=True))
                        cache["reply_operations"][name] = {
                            "doc_id": body.get("doc_id"),
                            "fb_api_req_friendly_name": name,
                            "variables_template": json.loads(body.get("variables") or "{}"),
                        }
            cache_key = bootstrap_type.cache_key_tmpl.format(account=account_key)
            redis_cache.set(cache_key, cache, ttl_seconds=7 * 86400)
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
                redis_cache.set(base_key, cache, ttl_seconds=7 * 86400)
            logger.info(
                bootstrap_type.saved_log_event,
                telegram=True,
                key=cache_key,
                account=account_key,
                ttl_seconds=7 * 86400,
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
            save_native_state(redis_cache, account_key, context)
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
        "--check-only",
        action="store_true",
        help="Only check that the saved session is alive (home feed, a few scrolls, no search, no token capture). "
        "Used by the dashboard 'restore saved session' button.",
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
            check_only=args.check_only,
        )
