"""Đảm bảo phiên đăng nhập và các query đã bắt (search, comment, reply, page feed, reels) trước khi chạy job."""

from __future__ import annotations

import asyncio
import os
import time

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    PAGE_FEED_REDIS_KEY_TMPL,
    PAGE_REELS_REDIS_KEY_TMPL,
    REPLIES_REDIS_KEY_TMPL,
)
from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY as THREADS_ACTIVE_ACCOUNT_REDIS_KEY,
)
from social_crawler.constants.threads import (
    CACHE_REDIS_KEY_TMPL as THREADS_CACHE_REDIS_KEY_TMPL,
)
from social_crawler.constants.threads import (
    DEFAULT_ACCOUNT_KEY as THREADS_DEFAULT_ACCOUNT_KEY,
)
from social_crawler.consumer.common import PYTHON_BIN, TOKEN_REFRESH_QUERY, logger
from social_crawler.consumer.process import _run_subprocess
from social_crawler.db.accounts import get_accounts, is_account_usable
from social_crawler.runtime import pool
from social_crawler.spiders.common.comet_graphql_client import is_search_recipe

# Xoay tài khoản đang active theo thời gian (17/10/2026: crawl liên tục, mọi tài khoản FB/Threads đã bật): sau chừng này
# phút dùng một tài khoản, con trỏ ACTIVE bị gỡ để bước bootstrap trước crawl chọn tài khoản khoẻ dùng lâu nhất chưa dùng
# lại (pool.acquire_account, LRU). Trước đây chỉ đổi tài khoản khi session hỏng - một tài khoản gánh cả ngày.
# Mặc định TẮT từ 19/10/2026: bật lúc 18/10 13:25, tới tối cùng ngày 12/14 tài khoản FB mất session/checkpoint và
# 5 tài khoản Threads bị đăng xuất - mỗi lần xoay là một tài khoản khác hiện ra trên cùng IP proxy, đúng mẫu Meta khoá.
# Bật lại bằng env ACCOUNT_ROTATE_MINUTES=<phút> chỉ khi mỗi tài khoản đã có proxy riêng.
ACCOUNT_ROTATE_MINUTES = float(os.getenv("ACCOUNT_ROTATE_MINUTES", "0"))


_ACTIVE_SINCE_KEY_TMPL = "{platform}:active_since:{account}"


def _rotation_due(cache: RedisCache, platform: str, account: str, active_key: str) -> bool:
    """True (và gỡ con trỏ ACTIVE) khi `account` đã active quá ACCOUNT_ROTATE_MINUTES và còn tài khoản khoẻ khác."""
    if ACCOUNT_ROTATE_MINUTES <= 0 or not account:
        return False
    since_key = _ACTIVE_SINCE_KEY_TMPL.format(platform=platform, account=account)
    now = time.time()
    since = cache.get(since_key)
    if since is None:
        cache.set(since_key, now, ttl_seconds=int(ACCOUNT_ROTATE_MINUTES * 60 * 4) + 60)
        return False
    if now - float(since) < ACCOUNT_ROTATE_MINUTES * 60:
        return False
    try:
        current = account.strip().lower()
        others = [
            row
            for row in get_accounts(platform)
            if current not in {str(row.get(k) or "").strip().lower() for k in ("account_id", "email")}
        ]
    except Exception as exc:  # noqa: BLE001 - không đọc được danh sách thì giữ tài khoản hiện tại
        logger.warning("account_rotation_check_failed", platform=platform, error=str(exc)[:200])
        return False
    cache.delete(since_key)
    if not others:
        return False
    cache.delete(active_key)
    logger.info(
        "account_rotated", platform=platform, previous=account, minutes=ACCOUNT_ROTATE_MINUTES, candidates=len(others)
    )
    return True


def _active_account_usable(cache: RedisCache, platform: str, account: str, active_key: str) -> bool:
    """Tài khoản mà con trỏ ACTIVE_ACCOUNT đang trỏ còn được bật (không bị tắt/checkpoint) không. Không thì xoá con
    trỏ và báo "cache không dùng được", để bước bootstrap trước crawl chọn tài khoản khoẻ khác qua next_account()
    thay vì cứ dùng lại token cache của một tài khoản đã bị tắt."""
    from social_crawler.accounts.warmup import is_warming_key

    if is_account_usable(platform, account) and not is_warming_key(platform, account, cache):
        return True
    logger.warning("active_account_disabled_rebootstrapping", platform=platform, account=account)
    if cache.get(active_key) == account:
        cache.delete(active_key)
    return False


def _facebook_session_is_cached(account: str | None = None) -> bool:
    """`account` (hoặc tài khoản Facebook đang active, hoặc mặc định nếu bootstrap chưa từng
    chạy) còn cache token sống trong Redis không. Chính việc Redis cho key hết hạn *là* tín
    hiệu "hết hạn" - graphql_client.py raise SessionExpiredError ngay khi key này mất, nên chỉ
    cần kiểm tra tồn tại, không cần tính TTL riêng.

    `account`, khi có, kiểm tra ĐÚNG tài khoản đó thay vì tài khoản nào đang active - xem
    comment trong _run_comments_spider để biết vì sao một job crawl comment ghim một tài
    khoản cho cả ba lời gọi ensure_* thay vì để mỗi lời gọi tự đọc/xoay
    ACTIVE_ACCOUNT_REDIS_KEY."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    if account is None and not _active_account_usable(cache, "facebook", target, ACTIVE_ACCOUNT_REDIS_KEY):
        return False
    if account is None and _rotation_due(cache, "facebook", target, ACTIVE_ACCOUNT_REDIS_KEY):
        return False
    session = cache.get(CACHE_REDIS_KEY_TMPL.format(account=target))
    if session and not is_search_recipe(session):
        # Key search đang giữ công thức comment (xem comet_graphql_client.is_search_recipe) - coi như
        # chưa có để bootstrap search lại.
        logger.warning(
            "facebook_session_cache_not_search",
            account=target,
            friendly_name=session.get("fb_api_req_friendly_name"),
        )
        return False
    if not session:
        return False
    from social_crawler.constants.facebook import CACHE_MAX_AGE_SECONDS
    from social_crawler.spiders.facebook.auth.http_refresh import RECIPE_TTL

    for key_template in (
        CACHE_REDIS_KEY_TMPL,
        COMMENTS_REDIS_KEY_TMPL,
        REPLIES_REDIS_KEY_TMPL,
        PAGE_FEED_REDIS_KEY_TMPL,
        PAGE_REELS_REDIS_KEY_TMPL,
    ):
        key = key_template.format(account=target)
        retained = session if key_template == CACHE_REDIS_KEY_TMPL else cache.get(key)
        if retained:
            cache.set(key, retained, ttl_seconds=RECIPE_TTL)
    return time.time() - float(session.get("http_refreshed_at", session.get("captured_at", 0))) < CACHE_MAX_AGE_SECONDS


async def _ensure_facebook_session(
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str | None = None,
    honor_platform_cancel: bool = True,
    allow_other_account: bool = True,
) -> bool:
    """Refresh a retained recipe over HTTP. Missing setup requires an explicit manual bootstrap.

    Never switches accounts or opens a browser when refresh fails.
    """
    # Chạy ngoài event loop: RedisCache bọc client `redis` đồng bộ, và loop này giờ dùng chung
    # với vòng lặp consumer của threads/tiktok (xem run()) - một lời gọi Redis chậm/treo ở đây
    # sẽ làm treo luôn việc poll Kafka của chúng, đúng kiểu dính chéo giữa các nền tảng mà việc
    # tách mỗi nền tảng một vòng lặp sinh ra để loại bỏ.
    if await asyncio.to_thread(_facebook_session_is_cached, account):
        return True

    from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient, NetworkError
    from social_crawler.spiders.facebook.auth.http_refresh import refresh

    def refresh_existing():
        cache = RedisCache()
        target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
        if not is_account_usable("facebook", target):
            return False
        if not cache.get(CACHE_REDIS_KEY_TMPL.format(account=target)):
            logger.warning("facebook_manual_session_setup_required", account=target)
            return False
        fail_key = f"facebook:http_refresh_failed:{target}"
        if cache.get(fail_key):
            return False
        client = FacebookGraphQLClient(redis_cache=cache, account=target)
        try:
            try:
                refreshed = refresh(client)
            except Exception:
                cache.set(fail_key, 1, ttl_seconds=1800)
                raise
            if not refreshed:
                cache.set(fail_key, 1, ttl_seconds=1800)
            return refreshed
        finally:
            client._session.close()
            from social_crawler.runtime import account_lease

            account_lease.release("facebook", target)

    try:
        return await asyncio.to_thread(refresh_existing)
    except pool.ProxyPoolExhaustedError:
        raise
    except NetworkError as exc:
        if isinstance(exc.__cause__, pool.ProxyPoolExhaustedError):
            raise exc.__cause__ from exc
        logger.warning("facebook_http_refresh_failed", error_type=type(exc).__name__)
        return False
    except Exception as exc:  # noqa: BLE001 - fail closed without opening a browser
        logger.warning("facebook_http_refresh_failed", error_type=type(exc).__name__)
        return False


def _requeue_if_blocked(returncode: int, context: str) -> None:
    """Bootstrap không mở tài khoản vì cổng / tài khoản đang bận hoặc chưa xác nhận được đường mạng: job xếp lại sau, không phải
    tài khoản hỏng. Trước đó làn cho tài khoản nghỉ 2 giờ vì lỗi này (31/10/2026: "cổng bận" làm 114 nghỉ 2 giờ)."""
    if returncode == pool.PROXY_EXHAUSTED_EXIT_CODE:
        raise pool.ProxyPoolExhaustedError(context)


def _facebook_comments_cache_usable(account: str | None = None) -> bool:
    """`account` (hoặc tài khoản Facebook đang active) có cache query comment trong Redis thực sự
    phân trang được không - xem FacebookGraphQLClient._get_comments_cache. Khác cache tìm
    kiếm (_facebook_session_is_cached), mẫu query này dùng được cho comment của *bất kỳ* bài
    nào sau khi đã bắt được (chỉ biến `id` thay đổi theo bài - xem get_comments trong
    graphql_client.py), nên chỉ cần bootstrap một lần mỗi TTL, không phải mỗi bài.

    Kiểm tra sub-key `pagination`, không chỉ xem cache có tồn tại không - bootstrap.py chỉ
    bắt được CommentsListComponentsPaginationQuery nếu bài nó cuộn trong lượt đó thực sự có
    đủ comment để Facebook trả trang thứ hai (xem comments_trigger); một cache bootstrap trên
    bài ít comment vẫn tồn tại nhưng không bao giờ lấy được quá khoảng 2 comment cho bất kỳ
    bài nào, một cách âm thầm, cho tới khi lần bootstrap sau tình cờ gặp bài nhiều comment
    (đã xảy ra thật: mọi lượt crawl comment chỉ được tối đa khoảng 2 suốt nhiều giờ chính vì
    chuyện này). Kiểm tra lại ở mỗi lời gọi thay vì chỉ tin vào việc tồn tại nghĩa là một
    cache tồi sẽ được thử lại thay vì kẹt tới khi hết TTL."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    comments_cache = cache.get(COMMENTS_REDIS_KEY_TMPL.format(account=target))
    return bool(comments_cache and comments_cache.get("pagination"))


def _facebook_replies_cache_usable(account: str | None = None) -> bool:
    """`account` (hoặc tài khoản Facebook đang active) có cache query reply trong Redis thực sự
    phân trang được không - cùng lý do và cùng yêu cầu sub-key `pagination` như
    _facebook_comments_cache_usable, chỉ là cho get_replies/get_replies_next_page của
    _fetch_replies thay vì vòng lặp comment cấp một. Là cache *riêng* với cache comment
    (REPLIES_REDIS_KEY_TMPL, không phải COMMENTS_REDIS_KEY_TMPL) vì bắt nó cần một cách kích
    hoạt trình duyệt khác ("bấm mở reply của một comment" của replies_trigger, không phải "chỉ
    mở+sắp xếp danh sách cấp một" của comments_trigger) - đã xảy ra thật (2026-09-16): một
    lần bootstrap chỉ cho comment khiến mọi comment có reply trong một lượt crawl thật đều
    lỗi session_expired ở đúng cache này, dù bản thân comment cấp một vẫn lấy bình thường."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    replies_cache = cache.get(REPLIES_REDIS_KEY_TMPL.format(account=target))
    from social_crawler.spiders.facebook.auth.http_refresh import PUBLIC_DOCS_KEY

    public = cache.get(PUBLIC_DOCS_KEY) or {}
    if public.get("operations", {}).get("Depth1CommentsListPaginationQuery"):
        return True
    recipe = (
        (replies_cache or {}).get("reply_operations", {}).get("Depth1CommentsListPaginationQuery")
        or (replies_cache or {}).get("pagination")
        or replies_cache
        or {}
    )
    return recipe.get("fb_api_req_friendly_name") == "Depth1CommentsListPaginationQuery" and bool(recipe.get("doc_id"))


def _threads_search_cache_usable() -> bool:
    """Tài khoản Threads đang active có cache session *tìm kiếm* dùng được trong Redis không.
    Dùng cho cả threads_search lẫn threads_comments: spider comment chỉ cần cookie của session
    (nó GET /api/v1/text_feed/<id>/replies/, xem graphql_client.get_text_feed_replies), mà
    bootstrap search tạo ra đủ cả cookie lẫn công thức search đúng.

    Không chỉ kiểm tra key có tồn tại: ngày 2026-10-05 một lần bootstrap --post-url đã ghi một
    query feed đăng xuất (BarcelonaLoggedOutFeedPaginationQuery) vào đúng key này. Query đó
    không có biến `query`, nên mọi lượt search sau đó âm thầm trả về feed chung thay vì kết quả
    theo từ khoá. Cache mà friendly_name không chứa "search" bị coi là hỏng để bootstrap lại."""
    cache = RedisCache()
    account = cache.get(THREADS_ACTIVE_ACCOUNT_REDIS_KEY) or THREADS_DEFAULT_ACCOUNT_KEY
    if not _active_account_usable(cache, "threads", account, THREADS_ACTIVE_ACCOUNT_REDIS_KEY):
        return False
    if _rotation_due(cache, "threads", account, THREADS_ACTIVE_ACCOUNT_REDIS_KEY):
        return False
    session = cache.get(THREADS_CACHE_REDIS_KEY_TMPL.format(account=account))
    if not session:
        return False
    if not is_search_recipe(session):
        logger.warning(
            "threads_session_cache_not_search", account=account, friendly_name=session.get("fb_api_req_friendly_name")
        )
        return False
    return True


async def _ensure_threads_session(
    *, run_id: str | None = None, platform: str | None = "threads", honor_platform_cancel: bool = True
) -> bool:
    """Bản Threads của _ensure_facebook_session: bootstrap search (--query, không bao giờ
    --post-url) khi cache thiếu, hết hạn, hoặc không phải công thức search - xem
    _threads_search_cache_usable."""
    if await asyncio.to_thread(_threads_search_cache_usable):
        return True

    logger.info("threads_session_missing_refreshing_first")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.threads.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    _requeue_if_blocked(returncode, "threads session refresh")
    if returncode != 0:
        logger.error("threads_session_refresh_before_crawl_failed", returncode=returncode)
        return False
    return True


_COMMENTS_CACHE_USABLE_CHECK = {
    "facebook": _facebook_comments_cache_usable,
}


async def _ensure_comments_cache(
    platform: str,
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    honor_platform_cancel: bool = True,
) -> bool:
    """Use existing Facebook query recipes; missing setup never opens a browser.

    Threads retains its existing search-session preflight.
    """
    if platform == "threads":
        # Không bootstrap --post-url cho Threads: mở permalink nguội luôn ra route đăng xuất, không
        # bắt được query reply nào, và trước đây còn ghi đè công thức search (xem
        # _threads_search_cache_usable). Spider comment chỉ cần cookie, nên bootstrap search là đủ.
        return await _ensure_threads_session(
            run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
        )
    check = _COMMENTS_CACHE_USABLE_CHECK[platform]
    usable = await asyncio.to_thread(check, account)
    if usable:
        return True

    logger.warning("comments_query_setup_required", platform=platform, account=account)
    return False


async def _ensure_replies_cache(
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str = "facebook",
    honor_platform_cancel: bool = True,
) -> bool:
    """Check for a known Depth1 reply recipe, without browser bootstrap."""
    if await asyncio.to_thread(_facebook_replies_cache_usable, account):
        return True

    logger.warning("replies_query_setup_required", account=account)
    return False


async def _ensure_page_reels_query(cache: RedisCache, account: str, page_url: str, run_id: str | None) -> None:
    if page_url and not cache.get(PAGE_REELS_REDIS_KEY_TMPL.format(account=account)):
        logger.warning("page_reels_query_setup_required", account=account)
