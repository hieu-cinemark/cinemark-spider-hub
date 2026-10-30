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
from social_crawler.consumer.common import COMMENTS_BOOTSTRAP_MODULE, PYTHON_BIN, TOKEN_REFRESH_QUERY, logger
from social_crawler.consumer.process import _run_subprocess
from social_crawler.db.accounts import get_accounts, is_account_usable
from social_crawler.spiders.common.comet_graphql_client import is_search_recipe

LANE_REFRESH_FAIL_REST_HOURS = 2

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
    return bool(session)


async def _ensure_facebook_session(
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str | None = None,
    honor_platform_cancel: bool = True,
    allow_other_account: bool = True,
) -> bool:
    """Làm mới cache token Facebook trước nếu nó thiếu/hết hạn, để một lượt crawl kích hoạt
    ngay sau khoảng nhàn rỗi >6 giờ (xem CACHE_MAX_AGE_SECONDS) thành công ngay lần đầu thay
    vì lỗi session_expired rồi phải chờ lượt kế tiếp của scripts/refresh_token.sh hoặc kích
    hoạt lại bằng tay. Trả về có session để crawl sau khi hàm này xong không - False nghĩa là
    bản thân lần làm mới thất bại, nên chỗ gọi nên bỏ cuộc thay vì chạy một lượt crawl chắc
    chắn dính ngay lỗi session_expired đó.

    `account`, khi có, được truyền cho bootstrap.py dưới dạng --account - làm mới/kiểm tra
    đúng tài khoản đó thay vì để next_account() của bootstrap.py tự do xoay vòng (xem
    _run_comments_spider)."""
    # Chạy ngoài event loop: RedisCache bọc client `redis` đồng bộ, và loop này giờ dùng chung
    # với vòng lặp consumer của threads/tiktok (xem run()) - một lời gọi Redis chậm/treo ở đây
    # sẽ làm treo luôn việc poll Kafka của chúng, đúng kiểu dính chéo giữa các nền tảng mà việc
    # tách mỗi nền tảng một vòng lặp sinh ra để loại bỏ.
    if await asyncio.to_thread(_facebook_session_is_cached, account):
        return True

    logger.info("facebook_session_expired_refreshing_first", account=account)
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
    if account:
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0 and account and returncode > 0 and not allow_other_account:
        # Làn Facebook (services/lanes.py): không bootstrap tài khoản khác (sẽ giẫm lên làn khác) - nhả tài khoản của
        # làn, lần hỏi sau làn tự nhận tài khoản khoẻ khác.
        from datetime import UTC, datetime, timedelta

        from social_crawler.db.accounts import rest_account_until
        from social_crawler.runtime.lanes import release_account

        # Cho nghỉ LANE_REFRESH_FAIL_REST_HOURS: chỉ nhả thì làn nhận lại ngay đúng tài khoản này (nếu nó dùng ít nhất) -
        # mở trình duyệt thất bại liên tục, mỗi lần một cảnh báo Telegram.
        rest_account_until("facebook", account, datetime.now(tz=UTC) + timedelta(hours=LANE_REFRESH_FAIL_REST_HOURS))
        release_account(account)
        logger.warning(
            "facebook_lane_session_refresh_failed",
            account=account,
            returncode=returncode,
            rest_hours=LANE_REFRESH_FAIL_REST_HOURS,
        )
        return False
    if returncode != 0 and account and returncode > 0:
        # Tài khoản được ghim (thường là con trỏ active) không làm mới được - bootstrap đã đánh dấu nó
        # chết và đưa vào cooldown. Thử một lần không ghim để next_account() chọn tài khoản khoẻ
        # khác, thay vì để mọi job sau cứ bootstrap lại đúng tài khoản chết này. returncode âm là bị
        # huỷ (bấm Dừng) - không thử lại.
        logger.warning("facebook_session_refresh_failed_trying_other_account", account=account, returncode=returncode)
        args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
        )
    if returncode != 0:
        logger.error("facebook_session_refresh_before_crawl_failed", returncode=returncode)
        return False
    return True


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
    return bool(replies_cache and replies_cache.get("pagination"))


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
    """Cùng ý tưởng với _ensure_facebook_session, cho cache query comment của một nền tảng thay
    vì cache tìm kiếm - bootstrap nó từ post_url (một bài thật mà request này đã nêu, để
    bootstrap có thứ để mở và bắt query comment) ở lần đầu cache thiếu/không dùng được (xem
    phép kiểm tra riêng của từng nền tảng trong _COMMENTS_CACHE_USABLE_CHECK).

    Threads không đi đường này mà bootstrap search (xem _ensure_threads_session).

    `account`: chỉ cho facebook, truyền cho bootstrap.py dưới dạng --account. Xem
    _run_comments_spider để biết vì sao điều này quan trọng."""
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

    logger.info("comments_cache_missing_bootstrapping", platform=platform, post_url=post_url, account=account)
    args = [PYTHON_BIN, "-m", COMMENTS_BOOTSTRAP_MODULE[platform], "--post-url", post_url]
    if account and platform == "facebook":
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("comments_bootstrap_failed", platform=platform, returncode=returncode)
        return False
    return True


async def _ensure_replies_cache(
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str = "facebook",
    honor_platform_cancel: bool = True,
) -> bool:
    """Chỉ cho Facebook (Threads/TikTok không có bản tương đương - xem docstring của
    _fetch_replies về việc vì sao Threads không cần và comment TikTok hoàn toàn không nằm
    trong COMMENTS_PLATFORMS_NEEDING_CACHE): cùng ý tưởng với _ensure_comments_cache, cho
    cache query reply riêng mà một comment có reply cần (_facebook_replies_cache_usable).
    Bootstrap với --type replies, vốn cần một post_url có comment cấp một chứa ít nhất một
    comment có reply để bấm mở - chính post_url mà lượt crawl comment được xếp hàng cho trên
    thực tế là đủ (phần lớn bài thật có tương tác đều có ít nhất một reply ở đâu đó), và một
    lần bootstrap không tìm được thì chỉ thất bại gọn gàng (comments_bootstrap_failed) thay
    vì âm thầm không cache gì."""
    if await asyncio.to_thread(_facebook_replies_cache_usable, account):
        return True

    logger.info("replies_cache_missing_bootstrapping", post_url=post_url, account=account)
    args = [
        PYTHON_BIN,
        "-m",
        "social_crawler.spiders.facebook.auth.bootstrap",
        "--post-url",
        post_url,
        "--type",
        "replies",
    ]
    if account:
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("replies_bootstrap_failed", returncode=returncode)
        return False
    return True


# Bắt query tab Reels (lượt xem reel) cần một trang CÓ reel: thử trên trang của job đang chạy, mỗi trang tối đa một lần /
# REELS_BOOTSTRAP_PAGE_TTL, và không quá REELS_BOOTSTRAP_MAX_TRIES lần / REELS_BOOTSTRAP_WINDOW (mỗi lần mở trình duyệt
# ~1-2 phút) - đa số trang nguồn có reel nên thường lần đầu là được.
REELS_BOOTSTRAP_PAGE_TTL = 7 * 86400


REELS_BOOTSTRAP_MAX_TRIES = 3


REELS_BOOTSTRAP_WINDOW = 6 * 3600


async def _ensure_page_reels_query(cache: RedisCache, account: str, page_url: str, run_id: str | None) -> None:
    if not page_url or cache.get(PAGE_REELS_REDIS_KEY_TMPL.format(account=account)):
        return
    if cache.get(f"page_reels_bootstrap_tried:{page_url}"):
        return
    tries_key = "page_reels_bootstrap_tries"
    tries = int(cache.get(tries_key) or 0)
    if tries >= REELS_BOOTSTRAP_MAX_TRIES:
        return
    cache.set(tries_key, tries + 1, ttl_seconds=REELS_BOOTSTRAP_WINDOW)
    cache.set(f"page_reels_bootstrap_tried:{page_url}", 1, ttl_seconds=REELS_BOOTSTRAP_PAGE_TTL)
    logger.info("page_reels_cache_missing_bootstrapping", account=account, page_url=page_url)
    returncode = await _run_subprocess(
        [PYTHON_BIN, "-m", COMMENTS_BOOTSTRAP_MODULE["facebook"], "--page-reels-url", page_url, "--account", account],
        run_id=run_id,
        platform="facebook",
    )
    if returncode != 0:
        logger.warning("page_reels_bootstrap_failed", page_url=page_url, returncode=returncode)
