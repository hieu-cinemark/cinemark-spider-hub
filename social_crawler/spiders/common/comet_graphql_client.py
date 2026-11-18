"""
Lớp cơ sở dùng chung cho client GraphQL của Facebook và Threads - cả hai chạy trên cùng
stack GraphQL Comet/Barcelona (đã xác nhận với một request
BarcelonaPostPageStrongIdTargetQuery thật bắt được trên threads.com), nên gần như mọi
thứ bên dưới (thiết lập session, bóp nhịp, thử lại/backoff, tạo biến theo mẫu, parse
response) từng bị lặp gần như nguyên văn giữa facebook/auth/graphql_client.py và
threads/auth/graphql_client.py - chỉ khác tên nền tảng và vài hằng số. Cùng cách chia lớp
cơ sở/lớp con mà spiders/tiktok/client.py vốn dùng vì cùng lý do.

Lớp con đặt các thuộc tính class bên dưới (tên nền tảng, mẫu key Redis theo nền tảng,
hằng giãn cách/thử lại request) và thêm các method theo từng loại request của nó (search,
comments, ...) gọi self._run(...); xem FacebookGraphQLClient/ThreadsGraphQLClient để thấy
dạng. Thứ gì thực sự khác giữa hai nền tảng - search/comments có lọc theo ngày của
Facebook, header x-csrftoken thêm của Threads - nằm ở lớp con; ở đây không giả định cái
nào.
"""

from __future__ import annotations

import atexit
import copy
import hashlib
import json
import random
import time
from typing import Any

import redis
from curl_cffi import CurlOpt
from curl_cffi import requests as curl_requests

from social_crawler.clients import proxy_provider
from social_crawler.clients.redis import RedisCache
from social_crawler.core.fingerprint import (
    resolve_profile,
)
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events
from social_crawler.db.accounts import disable_account, get_accounts
from social_crawler.runtime import account_lease, pool, proxy_lease

logger = get_logger(__name__)

# Đã tách ra module riêng (17/11/2026) - import + re-export để `from ...comet_graphql_client import X` cũ chạy như cũ.
from social_crawler.spiders.common.comet_comments import CommentsMixin
from social_crawler.spiders.common.comet_errors import (
    _LOGGED_OUT_ERROR_CODES,
    REJECTED_SESSION_REST_HOURS,
    TRANSPORT_ERRORS,
    AccountRestrictedError,
    CheckpointRequiredError,
    GraphQLDocumentError,
    GraphQLResponseError,
    GraphQLSchemaError,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    _is_checkpoint_response,
    _url_without_query,
)
from social_crawler.spiders.common.comet_parse import (
    _apply_session_counters,
    _apply_variable_overrides,
    _loggable,
    _parse_graphql_response,
    normalize_cached_fingerprint,
)
from social_crawler.spiders.common.comet_throttle import ThrottleMixin

# Lần reset IP gần nhất theo cổng proxy (monotonic) - xem CometGraphQLClient._rotate_proxy_after_network_error.
_last_rotate_at: dict[str, float] = {}
_ROTATE_MIN_GAP_SECONDS = 90.0
# "Rỗng mềm" (http_refresh.is_empty_node, 18/11/2026). facebook-gql-scraper gửi lại NGAY tối đa 10 lần - kiểu dễ bị khoá nhất.
# Ở đây: nghỉ dài rồi thử lại đúng một lần; vẫn rỗng thì bỏ request đó. Rỗng sau khi đã nghỉ EMPTY_NODE_REST_AFTER lần trong
# EMPTY_NODE_WINDOW_SECONDS giây (khác request) mới coi là bị bóp và cho tài khoản nghỉ - bài đã xoá chỉ ra một lần lẻ.
EMPTY_NODE_RETRY_WAIT_SECONDS = (60.0, 120.0)
EMPTY_NODE_WINDOW_SECONDS = 1800
EMPTY_NODE_REST_AFTER = 3


__all__ = [
    "AccountRestrictedError",
    "CheckpointRequiredError",
    "CometGraphQLClient",
    "NetworkError",
    "RateLimitedError",
    "SessionExpiredError",
]


def is_search_recipe(cache: Any) -> bool:
    """Cache token này có phải công thức query *tìm kiếm* không (friendly_name chứa "search").

    Cache của key search có thể bị ghi bằng công thức khác: ngày 2026-10-05 cả Facebook lẫn
    Threads đều có key search chứa query comment/feed do một lần bootstrap comment ghi vào. Phát
    lại công thức đó bằng search() âm thầm bỏ qua từ khoá (biến `query`/`text` không có trong
    mẫu nên phần ghi đè không áp được), nên phải kiểm tra trước khi tin cache."""
    if not isinstance(cache, dict):
        return False
    return "search" in str(cache.get("fb_api_req_friendly_name") or "").lower()


class CometGraphQLClient(ThrottleMixin, CommentsMixin):
    """Do lớp con đặt - xem FacebookGraphQLClient/ThreadsGraphQLClient."""

    # (kết nối, đọc) giây. 15 giây cũ hay timeout giữa chừng qua proxy dân cư chậm - response tìm kiếm Facebook nặng
    # 1,5-2,8 MB mỗi trang (đo 16/10/2026), log Threads: "7436 out of 33716 bytes received" sau 15 giây.
    REQUEST_TIMEOUT = (10.0, 45.0)
    # Request "treo" (kết nối xong nhưng không có byte nào về - log 17/10/2026: "55002 milliseconds with 0 bytes received")
    # bị cắt sau STALL_SECONDS rồi thử lại ngay, thay vì chờ hết REQUEST_TIMEOUT. Response lớn đang tải chậm (vài trăm
    # byte/giây trở lên) không bị cắt.
    STALL_SECONDS = 15
    STALL_MIN_BYTES_PER_SECOND = 100
    ASSET_GAP_SECONDS = (0.3, 1.0)
    # Lỗi mạng liên tiếp (treo/timeout/CONNECT lỗi - không có response) tới mức này thì bỏ, không thử đủ MAX_RETRIES:
    # lần 2 đã reset IP cổng proxy, lần 3 vẫn hỏng là đường proxy đang sập. Log 19/10/2026: mỗi từ khoá FB treo đủ 5 lần
    # (~2 phút) rồi hỏng, cả lượt đốt hết từ khoá này tới từ khoá khác. Báo lên như cạn proxy (xem _post_with_retry) để
    # consumer xếp lại request và nghỉ nền tảng với backoff tăng dần thay vì bỏ từ khoá.
    NETWORK_GIVE_UP_ATTEMPTS = 3
    # Phần hạn mức mà request của client này tính vào ("fresh"/"deep", services/search_budget.py) - spider đặt theo
    # lane của job; None = chỉ tính vào tổng trong ngày.
    budget_share: str | None = None
    # "Dừng đọc" thỉnh thoảng giữa hai request (xem LONG_PAUSE_* trong constants của từng nền tảng); 0 = không.
    LONG_PAUSE_CHANCE: float = 0.0
    LONG_PAUSE_MIN_SECONDS: float = 0.0
    LONG_PAUSE_MAX_SECONDS: float = 0.0
    # Threads: request luôn được đếm theo giờ, nhưng chỉ spider bắt RequestBudgetExceeded (tìm kiếm, bình luận) mới bật cờ
    # này để bị dừng giữa job. Facebook luôn chặn - mọi spider Facebook dùng tài khoản đều bắt lỗi đó.
    enforce_hourly_cap: bool = False

    PLATFORM: str
    REFERER_URL: str
    CACHE_REDIS_KEY_TMPL: str
    ACTIVE_ACCOUNT_REDIS_KEY: str
    DEFAULT_ACCOUNT_KEY: str
    GRAPHQL_URL: str
    MAX_RETRIES: int
    MIN_REQUEST_INTERVAL_SECONDS: float
    REQUEST_INTERVAL_JITTER_SECONDS: float
    RETRY_BACKOFF_BASE_SECONDS: float
    RETRY_BACKOFF_JITTER_SECONDS: float
    THROTTLE_REDIS_KEY_TMPL: str
    ADAPTIVE_INTERVAL_MAX_SECONDS: float

    # Do lớp con có tính năng comment đặt (xem FacebookGraphQLClient/ThreadsGraphQLClient) -
    # nền tảng không có tính năng comment (hiện không có) đơn giản là không đặt, và
    # get_comments/get_comments_next_page bên dưới không dùng được cho nó.
    # COMMENTS_REDIS_KEY_TMPL: str
    #
    # Do lớp con có lấy cả reply-của-comment đặt (hiện chỉ có FacebookGraphQLClient - xem
    # `--type replies` của bootstrap.py). Nền tảng không có cái này không bao giờ gọi
    # get_replies/get_replies_next_page bên dưới.
    # REPLIES_REDIS_KEY_TMPL: str
    #
    # Tên biến Relay cho cursor/count của query phân trang comment - đã xác nhận giống hệt
    # ("commentsAfterCursor"/"commentsAfterCount") trên query comment của Facebook; để lớp con
    # ghi đè được (không gán cứng ở đây) vì tên thật của Threads chỉ biết được khi bootstrap
    # của nó thực sự bắt được một request comment có phân trang - xem lớp con đó để biết nó có
    # cần ghi đè không.
    COMMENTS_CURSOR_KEY = "commentsAfterCursor"
    COMMENTS_COUNT_KEY = "commentsAfterCount"
    # Tên biến dùng để truyền id của bài đích - Facebook gọi là "id" (một feedback id base64,
    # xem _comment_target_id bên đó); Threads gọi là "postID" và truyền id bài dạng số thô,
    # không mã hoá (đã xác nhận với một request
    # BarcelonaPostPageStrongIdDirectRepliesRefetchQuery thật bắt được).
    COMMENTS_ID_KEY = "id"

    def __init__(self, redis_cache: RedisCache | None = None, account: str | None = None):
        self._redis = redis_cache or RedisCache()
        # Mặc định là tài khoản mà bootstrap.py vừa đăng nhập (lại) gần nhất - xem
        # ACTIVE_ACCOUNT_REDIS_KEY - để việc xoay vòng qua platform_accounts trong các lần chạy
        # bootstrap tự động chuyển sang `scrapy crawl ...` mà không cần truyền gì ở đây. Truyền
        # `account` rõ ràng để ghim một lượt chạy vào một tài khoản.
        pinned = account is not None
        self._account = account or self._redis.get(self.ACTIVE_ACCOUNT_REDIS_KEY) or self.DEFAULT_ACCOUNT_KEY
        cache_key = self.CACHE_REDIS_KEY_TMPL.format(account=self._account)
        cached = self._redis.get(cache_key)
        # Chỉ tự quay về phương án dự phòng khi chỗ gọi không ghim rõ tài khoản (một `account=` rõ
        # ràng nghĩa là chỗ gọi muốn *đúng tài khoản đó*, ví dụ thử lại tay với một tài khoản có
        # tên - xem docstring của `_find_fallback_session` để biết vì sao điều này càng quan trọng
        # khi càng ít tài khoản để xoay, chứ không phải ngược lại).
        if cached is None and not pinned:
            cached, self._account = self._find_fallback_session()
        if cached is None and self.PLATFORM != "facebook":
            raise SessionExpiredError(
                f"No token cache found in Redis (key={cache_key!r}, account={self._account!r}), or Redis "
                "is unreachable, or the cache expired. Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --query "test"'
            )
        self._cache = cached
        if cached:
            age = time.time() - self._cache["captured_at"]
            logger.info("loaded_token_cache", account=self._account, age_hours=round(age / 3600, 1))
        # Danh tính được kiểm sau khi lấy lease và đọc lại snapshot mới nhất.

        proxy = None
        lease_acquired = False
        try:
            # Giữ tới hết tiến trình spider (runtime/account_lease.py); tài khoản bận thì đi đúng đường proxy bận bên dưới.
            account_lease.acquire(self.PLATFORM, self._account, "crawl", self._redis)
            lease_acquired = True
            # Đọc lại sau khi có lease: người giữ trước có thể vừa lưu cookie mới. Bản đọc lúc chưa có lease có thể đã cũ.
            leased = self._redis.get(self.CACHE_REDIS_KEY_TMPL.format(account=self._account))
            self._check_protocol_pause()
            if leased is None and self.PLATFORM == "facebook":
                # Recipe đã bị vô hiệu không được hồi sinh từ snapshot trước lúc lấy lease.
                if cached is not None:
                    raise SessionExpiredError("Facebook session was invalidated while waiting for its lease")
                from social_crawler.spiders.facebook.auth.sdk_fallback import seed

                leased = seed(self._redis, self._account)
                if leased is None:
                    raise SessionExpiredError("HTTP setup requires imported cookies and their original User-Agent")
            self._cache = self._prepare_cache(leased if leased is not None else self._cache)
            self._profile = self._transport_profile()
            self._req_counter = self._initial_req_counter()
            self._proxy_cfg = pool.acquire_proxy_for_account(self.PLATFORM, self._account, required=True)
        except pool.ProxyPoolExhaustedError as exc:
            if lease_acquired:
                account_lease.release(self.PLATFORM, self._account)
            # required=True: đây là lưu lượng crawl thường ngày, không phải trình duyệt đăng nhập một
            # lần - không bao giờ quay về chạy không proxy (xem docstring của
            # ProxyPoolExhaustedError). Raise lại thành NetworkError để nó đi qua đúng phần xử lý thử
            # lại/cảnh báo Telegram mà mọi spider vốn đã có cho "proxy sập" (xem ví dụ
            # `except NetworkError` trong facebook/features/search/search.py).
            raise NetworkError(str(exc)) from exc
        except BaseException:
            if lease_acquired:
                account_lease.release(self.PLATFORM, self._account)
            raise
        self._proxy_outcome_recorded = False
        if self._proxy_cfg:
            atexit.register(proxy_lease.release, self._proxy_cfg, self._account, self._redis)
            proxy_url = pool.build_proxy_url(self._proxy_cfg)
            proxy = {"http": proxy_url, "https": proxy_url}
        logger.info(
            "graphql_session_ready", account=self._account, proxy=self._proxy_cfg["url"] if self._proxy_cfg else None
        )

        try:
            from social_crawler.spiders.facebook.auth.sdk_fallback import is_http_recipe, session_headers

            http_recipe = self.PLATFORM == "facebook" and is_http_recipe(self._cache)
            # Recipe HTTP không có header bắt từ trình duyệt nên lấy bộ header và Client Hints của chính target (khớp UA đã kiểm
            # tra trong transport_profile). Recipe bắt từ trình duyệt giữ nguyên header đã bắt, không dùng mặc định.
            facebook_options = {"default_headers": http_recipe, "trust_env": False}
            self._session = curl_requests.Session(
                impersonate=self._profile["target"],
                proxies=proxy,
                **(facebook_options if self.PLATFORM == "facebook" else {}),
                curl_options={
                    CurlOpt.LOW_SPEED_LIMIT: self.STALL_MIN_BYTES_PER_SECOND,
                    CurlOpt.LOW_SPEED_TIME: self.STALL_SECONDS,
                },
            )
            if http_recipe:
                self._session.headers.update(session_headers(self._profile))
                self.REFERER_URL = "https://web.facebook.com/"
                self.GRAPHQL_URL = "https://web.facebook.com/api/graphql/"
            self._last_request_at = None
            if self.PLATFORM == "facebook":
                from social_crawler.runtime.facebook_pacing import PacedSession

                self._session = PacedSession(self._session, self._mark_request_finished)
            if self.PLATFORM == "facebook":
                self._record_session_opened()
            if self.PLATFORM == "facebook" and is_http_recipe(self._cache) and not self._cache.get("doc_id"):
                from social_crawler.spiders.facebook.auth.sdk_fallback import ensure_recipe

                try:
                    ready = ensure_recipe(self, "search")
                except GraphQLResponseError as exc:
                    raise SessionExpiredError("HTTP setup received an unusable discovery response") from exc
                if not ready:
                    raise SessionExpiredError("HTTP setup did not discover the search operation")
        except BaseException:
            if getattr(self, "_session", None) is not None:
                self._session.close()
            self.release_proxy_lease()
            account_lease.release(self.PLATFORM, self._account)
            raise
        # Bộ đếm __req của phiên này (xem _next_req_counter) - khởi đầu từ giá trị
        # bootstrap bắt được, rồi tăng dần mỗi request.

    def _record_session_opened(self) -> None:
        """Mỗi lần một tiến trình mở phiên: client nào, proxy nào, cookie ra sao. Không có giá trị cookie."""
        from social_crawler.accounts.session_quality import assess

        cookies = self._cache.get("cookies") or {}
        user_agent = (self._cache.get("headers") or {}).get("user-agent")
        account_events.record(
            self.PLATFORM,
            self._account,
            "session_opened",
            transport=type(self._session._wrapped).__module__,
            recipe_source=self._cache.get("recipe_source") or "browser",
            proxy_id=(self._proxy_cfg or {}).get("id"),
            **assess(cookies, user_agent).facts,
        )

    def _prepare_cache(self, cache):
        if self.PLATFORM == "facebook":
            from social_crawler.spiders.facebook.auth.sdk_fallback import is_http_recipe, profile

            if is_http_recipe(cache):
                profile(cache)
                return copy.deepcopy(cache)
            # Giữ nguyên những gì trình duyệt ghi nhận; không bao giờ sửa danh tính bằng cách đổi header.
            from social_crawler.spiders.facebook.auth.identity import BrowserIdentityError, transport_profile

            try:
                transport_profile(cache.get("headers") or {})
            except BrowserIdentityError as exc:
                raise SessionExpiredError(str(exc)) from exc
            return copy.deepcopy(cache)
        return normalize_cached_fingerprint(cache, self._account)

    def _transport_profile(self):
        if self.PLATFORM == "facebook":
            from social_crawler.spiders.facebook.auth.sdk_fallback import is_http_recipe, profile

            if is_http_recipe(self._cache):
                return profile(self._cache)
            from social_crawler.spiders.facebook.auth.identity import transport_profile

            return transport_profile(self._cache.get("headers") or {})
        return resolve_profile(self._account, self._cache.get("headers", {}).get("user-agent"))

    def _initial_req_counter(self) -> int:
        """Giá trị __req mà bootstrap bắt được, để bộ đếm bắt đầu từ đúng chỗ của phiên
        thay vì nhảy về 0 (một phiên mà __req tụt về 0 trông như tab mới - lệch với
        __user/__rev đã cache)."""
        raw = (self._cache.get("body_static") or {}).get("__req")
        try:
            return int(str(raw))
        except TypeError, ValueError:
            if getattr(self, "PLATFORM", None) == "facebook" and raw is not None:
                raise GraphQLSchemaError("Unsupported captured Facebook request counter; refusing to reset it")
            return 1

    def _next_req_counter(self) -> int:
        """Trả về giá trị __req cho request tiếp theo, rồi tăng bộ đếm.

        Trình duyệt thật tăng __req đúng +1 mỗi request (đo thật 27/10/2026 từ traffic
        Chromium 151: chuỗi 1,2,3,4 - bước tăng luôn là 1). Bắt chước đúng nhịp đó: con
        số phải tăng đơn điệu và khớp số request đã gửi, vì Facebook so __req với hoạt
        động thật của phiên. (Bản đầu tôi cho bước ngẫu nhiên 1-3 - sai với dữ liệu đo.)
        """
        if getattr(self, "PLATFORM", None) == "facebook":
            context = {
                "capture_id": self._cache.get("request_context_id"),
                "captured_at": self._cache.get("captured_at"),
                "session": (self._cache.get("body_static") or {}).get("__s"),
            }
            digest = hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()
            key = f"facebook:req_counter:{self._account}:{digest}"
            try:
                self._req_counter = self._redis.next_counter(key, self._req_counter, 7 * 86400)
            except redis.RedisError as exc:
                raise NetworkError("Facebook request sequence unavailable; no request sent") from exc
        else:
            self._req_counter += 1
        return self._req_counter

    def _find_fallback_session(self) -> tuple[dict[str, Any] | None, str]:
        """Chỉ được gọi khi cache token của chính tài khoản "đang active" thiếu/hết hạn (xem
        __init__) - quét mọi tài khoản đang bật *khác* (get_accounts() vốn đã loại mọi tài khoản
        đang cooldown hoặc bị checkpoint, nên ứng viên nào ở đây cũng khoẻ theo DB) để tìm một
        tài khoản có cache vẫn còn token chưa hết hạn, dùng tài khoản đầu tiên tìm được thay vì
        làm hỏng cả lượt chạy. Trả về (None, self._account) không đổi nếu cũng không có tài khoản
        nào khác có.

        Vì sao chuyện này quan trọng hơn vẻ ngoài: ACTIVE_ACCOUNT_REDIS_KEY là một con trỏ dùng
        chung, được đặt một lần mỗi lần chạy bootstrap.py, rồi được *mọi* crawl_request của nền
        tảng này dùng lại cho tới lần bootstrap sau - một lượt quét theo lịch "chạy mọi từ khoá
        đang bật" (xem scheduler.py/POST /<platform>/run của cinemark-api) bắn mỗi từ khoá một
        tiến trình con, mỗi cái tự dựng client mới chỉ đọc đúng con trỏ đó. Với pool tài khoản
        nhỏ và danh sách từ khoá dài (đã xác nhận thực tế 2026-09-16: 6 tài khoản Threads đang
        bật cho 44 từ khoá đang bật trong một lượt quét), *một* tài khoản mà con trỏ đó chỉ tới
        bị checkpoint/giới hạn rate giữa chừng - hoặc đơn giản là cũ đi giữa các lượt chạy theo
        lịch - từng làm hỏng mọi từ khoá còn xếp hàng sau nó, dù các tài khoản khác vẫn có
        session cache hoàn toàn tốt nằm không trong Redis suốt thời gian đó. Cập nhật
        ACTIVE_ACCOUNT_REDIS_KEY thành tài khoản tìm được, để tiến trình con của từ khoá *kế
        tiếp* trong cùng lượt quét cũng dùng luôn thay vì lặp lại đúng lần quét này và lại quay
        về phương án dự phòng từ đầu."""
        current_key = self.CACHE_REDIS_KEY_TMPL.format(account=self._account)
        for row in get_accounts(self.PLATFORM):
            candidate = (row.get("email") or row["id"]).strip().lower()
            cache_key = self.CACHE_REDIS_KEY_TMPL.format(account=candidate)
            if cache_key == current_key:
                continue  # đã biết tài khoản này chết - không kiểm tra lại
            cached = self._redis.get(cache_key)
            if cached is None:
                continue
            logger.warning(
                "active_account_session_dead_falling_back",
                telegram=True,
                platform=self.PLATFORM,
                dead_account=self._account,
                fallback_account=candidate,
            )
            self._redis.set(self.ACTIVE_ACCOUNT_REDIS_KEY, candidate)
            return cached, candidate
        return None, self._account

    def _record_proxy_outcome_once(self, *, success: bool) -> None:
        """Ghi kết quả proxy của client này (xem services/pool.py) tối đa một lần mỗi instance,
        không phải mỗi request một lần - một lượt quét có thể bắn hàng chục request qua cùng một
        client, và thiết kế mỗi-lần-gọi-một-connection-mới của db.py giả định chỗ gọi hiếm khi
        gọi tới nó (xem docstring module của nó), không phải mỗi request GraphQL một lần. Tín
        hiệu đầu tiên đã đủ đại diện: proxy lỗi một lần vẫn nhận cooldown tương ứng với lỗi đó
        kể cả khi một request sau trên cùng client tình cờ thành công, và proxy rõ ràng khoẻ thì
        không cần mọi request sau xác nhận lại điều đó."""
        if self._proxy_cfg is None or self._proxy_outcome_recorded:
            return
        self._proxy_outcome_recorded = True
        # Job vẫn chạy tiếp qua proxy này: chỗ giữ được nhả ở release_proxy_lease (đóng client / tiến trình thoát).
        pool.release_proxy(self._proxy_cfg, success=success, keep_lease=True)

    def release_proxy_lease(self) -> None:
        """Nhả chỗ giữ proxy của client cho tài khoản khác (chỉ khi tài khoản này còn giữ). Gọi khi đóng client trong tiến
        trình dài (consumer); tiến trình spider nhả qua atexit, bị kill thì chỗ giữ tự hết sau proxy_lease.LEASE_SECONDS."""
        proxy_lease.release(
            getattr(self, "_proxy_cfg", None), getattr(self, "_account", None), getattr(self, "_redis", None)
        )

    def _ensure_lease(self) -> None:
        if getattr(self, "_session_dead", False):
            raise SessionExpiredError("Session was already rejected; no further requests are permitted")
        account = getattr(self, "_account", None)
        if account_lease.lost(self.PLATFORM, account):
            raise NetworkError(f"lost the lease on account {account!r}") from pool.ProxyPoolExhaustedError(str(account))
        self._check_protocol_pause()

    def _check_protocol_pause(self) -> None:
        if self.PLATFORM != "facebook" or not getattr(self, "_redis", None):
            return
        try:
            paused = self._redis.get(f"facebook:protocol_pause:{self._account}")
        except redis.RedisError as exc:
            raise NetworkError(
                "Facebook response-validation state unavailable; no request sent"
            ) from pool.ProxyPoolExhaustedError(type(exc).__name__)
        if isinstance(paused, dict) and float(paused.get("until", 0)) > time.time():
            raise NetworkError(
                "Facebook response validation is temporarily paused; job must wait"
            ) from pool.ProxyPoolExhaustedError("protocol_pause")

    def _pause_protocol(self, category: str, response=None) -> None:
        self._session_dead = True
        details = {"category": category, "status": getattr(response, "status_code", None)}
        self._redis.set(
            f"facebook:protocol_pause:{self._account}", {**details, "until": time.time() + 300}, ttl_seconds=300
        )
        account_events.record("facebook", self._account, "protocol_pause", **details)
        logger.warning("facebook_protocol_pause", account=self._account, **details)

    def _persist_response_cookies(self) -> None:
        if self.PLATFORM != "facebook":
            return
        from social_crawler.spiders.facebook.auth.http_refresh import persist_response_cookies

        try:
            persist_response_cookies(self)
        except (redis.RedisError, RuntimeError) as exc:
            self._session_dead = True
            raise NetworkError("Facebook cookie snapshot could not be committed; requests stopped") from exc

    def _check_response_security_body(self, response) -> None:
        """Xem lỗi có cấu trúc trước khi thử lại, kể cả khi server trả HTTP 5xx."""
        if self.PLATFORM != "facebook":
            return
        from social_crawler.spiders.facebook.auth.http_refresh import check_security_errors

        check_security_errors(self, response)

        # Threads vẫn giữ phương án dự phòng cục bộ trong tiến trình.

    def _removed_cookies(self) -> set[str]:
        from social_crawler.runtime.facebook_pacing import PacedSession

        session = getattr(self, "_session", None)
        return session.removed_cookies() if isinstance(session, PacedSession) else set()

    def _headers(self, friendly_name: str, lsd: str) -> dict[str, str | None]:
        """Header chung cho cả hai nền tảng - Threads ghi đè để thêm các trường origin/x-csrftoken
        riêng qua super()._headers(...)."""
        headers = dict(self._cache["headers"])
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "referer": self.REFERER_URL,
                "x-fb-friendly-name": friendly_name,
                "x-fb-lsd": lsd,
                # Header của một fetch() cùng origin, đúng như trình duyệt gửi cho /api/graphql/
                # (đo 27/10/2026). Không đặt thì curl_cffi điền bộ mặc định của một lần ĐIỀU HƯỚNG
                # trang (accept text/html..., sec-fetch-mode navigate, sec-fetch-site none) - một
                # request GraphQL mang header của thao tác gõ URL là thứ trình duyệt không bao giờ gửi.
                "accept": "*/*",
                "origin": self.REFERER_URL.rstrip("/"),
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                # None = xoá hẳn header mặc định của curl_cffi (chỉ điều hướng mới có các header này).
                "sec-fetch-user": None,
                "upgrade-insecure-requests": None,
                "cache-control": None,
            }
        )
        return headers

    def _require_search_recipe(self) -> None:
        """Gọi đầu mỗi search(): từ chối phát lại một công thức không phải tìm kiếm (xem
        is_search_recipe) thay vì âm thầm trả về dữ liệu sai."""
        if not is_search_recipe(self._cache):
            raise SessionExpiredError(
                f"Cached {self.PLATFORM} query {self._cache.get('fb_api_req_friendly_name')!r} is not a "
                'search query. Re-run bootstrap.py --query "..." to capture the search recipe.'
            )

    def _run(
        self,
        *,
        doc_id: str | None,
        friendly_name: str | None,
        template: dict[str, Any] | None,
        template_source: str,
        overrides: dict[str, Any],
        extra_headers: dict[str, str] | None = None,
        _recovered: bool = False,
        _empty_retried: bool = False,
    ) -> dict[str, Any]:
        """extra_headers: header riêng của từng query, thêm sau header chung (ví dụ x-root-field-name của query hồ sơ
        Threads - xem ThreadsGraphQLClient.fetch_profile)."""
        if template is None:
            raise SessionExpiredError(
                f"Cache has no {template_source} (it was created by an older bootstrap.py). "
                "Re-run bootstrap.py to refresh the cache."
            )

        self._ensure_lease()
        if self.PLATFORM == "facebook" and not _recovered:
            from social_crawler.spiders.facebook.auth.http_refresh import PUBLIC_DOCS_KEY

            public = self._redis.get(PUBLIC_DOCS_KEY) or {}
            doc_id = public.get("operations", {}).get(friendly_name, doc_id)
            from social_crawler.spiders.facebook.auth.sdk_fallback import DOCS_KEY

            authenticated = self._redis.get(DOCS_KEY.format(account=self._account)) or {}
            doc_id = authenticated.get("operations", {}).get(friendly_name, doc_id)
        static = self._cache["body_static"]
        variables = _apply_variable_overrides(template, overrides)
        logger.info("sending_graphql_request", friendly_name=friendly_name, **_loggable(overrides))

        body = {
            **static,
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": friendly_name,
            "server_timestamps": "true",
            "doc_id": doc_id,
            "variables": json.dumps(variables, separators=(",", ":")),
        }
        # __req là bộ đếm TĂNG ĐƠN ĐIỆU theo phiên của trình duyệt (mỗi request gửi đi
        # là tăng), nên bản bootstrap bắt được rồi phát lại y hệt cho hàng trăm request là
        # dấu vân tay replay rõ nhất: cùng một con số lặp lại hàng trăm lần. Tăng dần mỗi
        # lần gửi để giống trình duyệt thật. jazoest là checksum của lsd, phải tính lại
        # theo lsd hiện hành - lsd trong cache có thể đã đổi (xem _apply_session_counters).
        if self.PLATFORM != "facebook":
            body = _apply_session_counters(body, self._next_req_counter())

        headers = self._headers(friendly_name, static.get("lsd", ""))
        if extra_headers:
            headers.update(extra_headers)
        resp = self._post_with_retry(
            headers=headers,
            cookies=self._cache["cookies"],
            body=body,
        )

        self._check_session_headers(resp)
        if self.PLATFORM == "facebook" and (resp.status_code == 403 or 300 <= resp.status_code < 400):
            # Chưa xác nhận đăng xuất nên giữ phiên, nhưng job kế tiếp không được dùng lại tài khoản này.
            self._rest_restricted_account(f"http_{resp.status_code}")
            raise AccountRestrictedError(f"Facebook denied this request with HTTP {resp.status_code}")
        if resp.status_code in (401, 403):
            self._drop_rejected_session(f"HTTP {resp.status_code}")
            raise SessionExpiredError(
                f"{self.PLATFORM.capitalize()} rejected the request (status={resp.status_code}). Re-run bootstrap.py."
            )

        if self.PLATFORM == "facebook" and resp.status_code != 200:
            raise GraphQLResponseError(f"Facebook request failed with HTTP {resp.status_code}")
        logger.info("received_response", status_code=resp.status_code, bytes=len(resp.text))
        try:
            parsed = _parse_graphql_response(resp.text, strict=self.PLATFORM == "facebook")
        except GraphQLResponseError:
            self._pause_protocol("invalid_graphql", resp)
            raise NetworkError(
                "Facebook returned an unvalidated GraphQL response; job must wait"
            ) from pool.ProxyPoolExhaustedError("protocol_pause")
        if self.PLATFORM == "facebook" and not isinstance(parsed, dict):
            self._pause_protocol("invalid_graphql_shape", resp)
            raise NetworkError(
                "Facebook returned an unexpected GraphQL payload; job must wait"
            ) from pool.ProxyPoolExhaustedError("protocol_pause")

        if isinstance(parsed, dict) and str(parsed.get("error")) in {str(code) for code in _LOGGED_OUT_ERROR_CODES}:
            self._drop_rejected_session(f"error {parsed.get('error')} {str(parsed.get('errorSummary') or '')[:80]}")
            raise SessionExpiredError(
                f"{self.PLATFORM.capitalize()} says the session is logged out (error={parsed.get('error')}). "
                "Re-run bootstrap.py after logging the account in again."
            )

        if _is_checkpoint_response(parsed):
            self._raise_checkpoint(parsed)

        if self.PLATFORM == "facebook":
            from social_crawler.spiders.facebook.auth.http_refresh import (
                classify_error,
                discover_public,
                error_summary,
                extract_tokens,
                restriction_signal,
                save_tokens,
            )

            signal = restriction_signal(parsed)
            if signal:
                self._rest_restricted_account(signal)
                raise AccountRestrictedError(f"Facebook restricted account {self._account!r}: {signal}")
            kind = classify_error(parsed)
            if kind == "schema":
                raise GraphQLSchemaError(f"Variables changed for {friendly_name}; browser bootstrap is not a repair")
            if kind == "doc_id":
                if not _recovered:
                    try:
                        operations = discover_public(self._redis)
                    except Exception as exc:  # noqa: BLE001 - anonymous discovery cannot affect account state
                        logger.warning("public_doc_discovery_failed", error_type=type(exc).__name__)
                        operations = {}
                    replacement = operations.get(friendly_name)
                    if not replacement or replacement == doc_id:
                        from social_crawler.spiders.facebook.auth.sdk_fallback import discover_authenticated

                        replacement = discover_authenticated(self, {friendly_name}, force=True).get(friendly_name)
                    if replacement and replacement != doc_id:
                        result = self._run(
                            doc_id=replacement,
                            friendly_name=friendly_name,
                            template=template,
                            template_source=template_source,
                            overrides=overrides,
                            extra_headers=extra_headers,
                            _recovered=True,
                        )
                        from social_crawler.spiders.facebook.auth.sdk_fallback import DOCS_KEY

                        key = DOCS_KEY.format(account=self._account)
                        known = (self._redis.get(key) or {}).get("operations", {})
                        self._redis.set(
                            key,
                            {"captured_at": time.time(), "operations": {**known, friendly_name: replacement}},
                            ttl_seconds=86400,
                        )
                        return result
                raise GraphQLDocumentError(f"No usable discovered doc_id for {friendly_name}")
            if kind:
                tokens = extract_tokens(resp.text)
                if not _recovered and tokens.get("fb_dtsg") and tokens["fb_dtsg"] != static.get("fb_dtsg"):
                    save_tokens(self, tokens)
                    return self._run(
                        doc_id=doc_id,
                        friendly_name=friendly_name,
                        template=template,
                        template_source=template_source,
                        overrides=overrides,
                        extra_headers=extra_headers,
                        _recovered=True,
                    )
                logger.warning(
                    "graphql_error_unclassified",
                    account=self._account,
                    friendly_name=friendly_name,
                    **error_summary(parsed),
                )
                raise GraphQLResponseError(f"GraphQL returned an error for {friendly_name}")
            from social_crawler.spiders.facebook.auth.http_refresh import is_empty_node

            if is_empty_node(parsed):
                self._persist_response_cookies()
                if not _empty_retried:
                    self._wait_after_empty_node(friendly_name)
                    return self._run(
                        doc_id=doc_id,
                        friendly_name=friendly_name,
                        template=template,
                        template_source=template_source,
                        overrides=overrides,
                        extra_headers=extra_headers,
                        _recovered=_recovered,
                        _empty_retried=True,
                    )
                if self._note_empty_node(friendly_name):
                    self._rest_restricted_account("empty_node_repeated")
                    raise AccountRestrictedError(
                        f"Facebook keeps returning empty data for account {self._account!r} after resting"
                    )
                raise GraphQLResponseError(f"Facebook returned an empty node for {friendly_name} twice")
        self._persist_response_cookies()
        return parsed

    def _wait_after_empty_node(self, friendly_name: str | None) -> None:
        """Nghỉ dài trước lần thử lại duy nhất; vẫn kiểm quyền dùng tài khoản trong lúc chờ (lease có thể mất)."""
        wait = random.uniform(*EMPTY_NODE_RETRY_WAIT_SECONDS)
        account_events.record(self.PLATFORM, self._account, "empty_node", friendly_name=friendly_name)
        logger.warning(
            "facebook_empty_node_waiting", account=self._account, friendly_name=friendly_name, seconds=round(wait)
        )
        deadline = time.monotonic() + wait
        while (left := deadline - time.monotonic()) > 0:
            time.sleep(min(5.0, left))
            self._ensure_lease()

    def _note_empty_node(self, friendly_name: str | None) -> bool:
        """Ghi một lần "vẫn rỗng sau khi nghỉ"; True khi tài khoản đã gặp EMPTY_NODE_REST_AFTER lần như vậy trong cửa sổ."""
        key = f"facebook:empty_node:{self._account}"
        now = time.time()
        try:
            seen = [t for t in (self._redis.get(key) or []) if now - float(t) < EMPTY_NODE_WINDOW_SECONDS]
            seen.append(now)
            self._redis.set(key, seen, ttl_seconds=EMPTY_NODE_WINDOW_SECONDS)
        except redis.RedisError, TypeError, ValueError:
            return False
        logger.warning(
            "facebook_empty_node_persisted", account=self._account, friendly_name=friendly_name, recent=len(seen)
        )
        return len(seen) >= EMPTY_NODE_REST_AFTER

    def _check_session_headers(self, response) -> None:
        if self.PLATFORM != "facebook":
            return
        from social_crawler.spiders.facebook.auth.http_refresh import logout_signal, redirects_to_checkpoint

        headers = getattr(response, "headers", {})
        if signal := logout_signal(headers):
            account_events.record(
                self.PLATFORM,
                self._account,
                "logout_signal",
                signal=signal,
                status=getattr(response, "status_code", None),
                url=_url_without_query(getattr(response, "url", "")),
                location=_url_without_query(dict(headers).get("location", "")),
                transport=type(getattr(self._session, "_wrapped", self._session)).__module__,
            )
            self._drop_rejected_session(f"explicit logout response header ({signal})")
            raise SessionExpiredError("Facebook explicitly logged this session out; requests stopped")
        # Xoá xs hoặc c_user là đăng xuất dù không kèm tín hiệu nào ở trên.
        if self._removed_cookies() & {"c_user", "xs"}:
            account_events.record(self.PLATFORM, self._account, "logout_signal", signal="session_cookie_removed")
            self._drop_rejected_session("session cookie removed by a response")
            raise SessionExpiredError("Facebook removed this session's cookie; requests stopped")
        from social_crawler.runtime.facebook_pacing import PacedSession

        if isinstance(self._session, PacedSession):
            uid = self._session.cookies.get_dict().get("c_user")
            expected = self._cache.get("cookies", {}).get("c_user")
            if uid != expected:
                self._drop_rejected_session("Response changed the session identity")
                raise SessionExpiredError("Facebook session identity changed; requests stopped")
        if redirects_to_checkpoint(headers):
            self._raise_checkpoint({"redirect": "location header to /checkpoint/"})

    def _raise_checkpoint(self, parsed: Any) -> None:
        self._session_dead = True
        account_events.record(self.PLATFORM, self._account, "checkpoint_signal", response=str(parsed)[:200])
        disabled = disable_account(
            self.PLATFORM,
            self._account,
            reason=f"checkpoint response during replay traffic: {str(parsed)[:200]}",
        )
        # Tắt trong DB là chưa đủ: token cache + con trỏ ACTIVE_ACCOUNT vẫn còn thì tiến trình con của từ khoá kế
        # tiếp dùng lại đúng tài khoản này (2026-10-07: 14 lần checkpoint liên tiếp trên Threads). Xoá cả hai để
        # lượt sau bootstrap với tài khoản khoẻ khác (next_account chỉ chọn tài khoản đang bật).
        self._redis.delete(self.CACHE_REDIS_KEY_TMPL.format(account=self._account))
        if self._redis.get(self.ACTIVE_ACCOUNT_REDIS_KEY) == self._account:
            self._redis.delete(self.ACTIVE_ACCOUNT_REDIS_KEY)
        logger.error(
            "account_disabled_checkpoint_suspected" if disabled else "account_checkpoint_suspected",
            telegram=True,
            platform=self.PLATFORM,
            account=self._account,
            disabled=disabled,
            response=parsed,
        )
        raise CheckpointRequiredError(
            f"{self.PLATFORM.capitalize()} returned checkpoint_required for account {self._account!r} - "
            "log in as this account through a real browser to resolve the checkpoint, then re-run bootstrap.py."
        )

    def _rest_restricted_account(self, signal: str) -> None:
        """Facebook báo rõ đang hạn chế: dừng client này, cho tài khoản nghỉ (account_warning.WARNING_REST_HOURS) và nhả khỏi
        làn / con trỏ active để job kế tiếp lấy tài khoản khác. Phiên vẫn sống nên giữ token, cookie và không ghi "dead"."""
        self._session_dead = True
        account_events.record(self.PLATFORM, self._account, "restriction_signal", signal=signal)
        from social_crawler.accounts.account_warning import rest_warned_account
        from social_crawler.runtime.lanes import release_account

        rest_warned_account(self.PLATFORM, self._account, signal, where="http_response")
        if self._redis.get(self.ACTIVE_ACCOUNT_REDIS_KEY) == self._account:
            self._redis.delete(self.ACTIVE_ACCOUNT_REDIS_KEY)
        release_account(self._account, self._redis)

    def _drop_rejected_session(self, reason: str) -> None:
        """Nền tảng từ chối phiên (401/403, "chưa đăng nhập"): xoá token cache + con trỏ active và cho tài khoản nghỉ
        REJECTED_SESSION_REST_HOURS giờ, ghi phiên chết. Trước 24/10/2026 chỉ raise - job kế tiếp (20-60 giây sau) lại
        phát đúng phiên đã bị từ chối, lặp liên tục: tín hiệu bot rất rõ với Facebook."""
        self._session_dead = True
        from datetime import UTC, datetime, timedelta

        from social_crawler.db.accounts import get_account_by_key, record_cookie_check, rest_account_until

        self._redis.delete(self.CACHE_REDIS_KEY_TMPL.format(account=self._account))
        if self._redis.get(self.ACTIVE_ACCOUNT_REDIS_KEY) == self._account:
            self._redis.delete(self.ACTIVE_ACCOUNT_REDIS_KEY)
        if self.PLATFORM == "facebook":
            from social_crawler.runtime.lanes import release_account

            release_account(self._account, self._redis)  # làn đang giữ tài khoản này nhận tài khoản khác
        until = datetime.now(tz=UTC) + timedelta(hours=REJECTED_SESSION_REST_HOURS)
        rested = rest_account_until(self.PLATFORM, self._account, until)
        row = get_account_by_key(self.PLATFORM, self._account)
        if row is not None:
            record_cookie_check(
                self.PLATFORM, row["id"], status="dead", note=f"session rejected during replay: {reason}"
            )
        logger.error(
            "session_rejected_account_rested",
            telegram=True,
            platform=self.PLATFORM,
            account=self._account,
            reason=reason,
            rest_hours=REJECTED_SESSION_REST_HOURS if rested else None,
        )

    def _rotate_proxy_after_network_error(self, attempt: int) -> None:
        """Từ lần lỗi mạng thứ 2 liền nhau: nếu proxy của client là cổng xoay có link reset (proxy_providers) thì đổi IP
        phía sau cổng rồi thử lại - đường mạng nhà dân phía sau hay rớt cả phút. Mỗi cổng tối đa một lần reset mỗi
        _ROTATE_MIN_GAP_SECONDS giây (dùng chung giữa các client trong tiến trình)."""
        if self.PLATFORM == "facebook" or attempt < 2 or not self._proxy_cfg:
            return
        url = str(self._proxy_cfg.get("url") or "")
        now = time.monotonic()
        if not url or now - _last_rotate_at.get(url, -1e9) < _ROTATE_MIN_GAP_SECONDS:
            return
        _last_rotate_at[url] = now
        try:
            provider = proxy_provider.reset_provider_for_proxy_url(url)
        except Exception as exc:  # noqa: BLE001 - reset lỗi thì cứ thử lại như cũ
            logger.warning("proxy_rotate_failed", platform=self.PLATFORM, proxy_url=url, error=str(exc)[:200])
            return
        if provider:
            logger.info("proxy_rotated_after_network_error", platform=self.PLATFORM, proxy_url=url, provider=provider)

    def _post_with_retry(self, headers: dict[str, str | None], cookies: dict[str, str], body: dict[str, Any]) -> Any:
        """POST có thử lại với backoff tăng dần khi bị giới hạn rate (429), lỗi server (5xx) và lỗi
        ở cấp mạng - các lỗi này là tạm thời và thường tự hồi phục, khác với token chết
        (401/403), thứ chỗ gọi xử lý riêng và không bao giờ thử lại ở đây."""
        proxy_lease.touch(getattr(self, "_proxy_cfg", None), getattr(self, "_account", None))
        last_exc: Exception | None = None
        resp = None
        # True ngay khi bất kỳ lần thử nào trong lời gọi này gặp 429/5xx/lỗi mạng - cấp cho
        # _adjust_interval để một request chỉ thành công sau khi thử lại vẫn được tính là căng
        # thẳng, không phải response sạch.
        stressed = False

        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
        network_failures = 0

        for attempt in range(1, self.MAX_RETRIES + 1):
            # Mỗi lần gửi thật đều qua bóp nhịp + đếm hạn mức, kể cả lần thử lại (trước 02/11/2026 chỉ đếm một lần cho cả chuỗi:
            # 429 x4 rồi 200 là 5 POST nhưng 1 lần tính).
            self._throttle()
            if self.PLATFORM == "facebook":
                body = _apply_session_counters(body, self._next_req_counter()) if "__req" in body else body
            try:
                resp = self._session.post(
                    self.GRAPHQL_URL,
                    headers=headers,
                    cookies=cookies,
                    data=body,
                    timeout=self.REQUEST_TIMEOUT,
                    **({"allow_redirects": False} if self.PLATFORM == "facebook" else {}),
                )
            except TRANSPORT_ERRORS as exc:
                last_exc = exc
                stressed = True
                logger.warning(
                    "request_failed",
                    platform=self.PLATFORM,
                    attempt=attempt,
                    max_retries=self.MAX_RETRIES,
                    error_type=type(exc).__name__,
                )
                self._rotate_proxy_after_network_error(attempt)
                network_failures += 1
                if network_failures >= self.NETWORK_GIVE_UP_ATTEMPTS:
                    break
            else:
                self._check_session_headers(resp)
                if self.PLATFORM == "facebook":
                    self._check_response_security_body(resp)
                if self.PLATFORM == "facebook" and resp.status_code == 429:
                    self._rest_restricted_account("http_429")
                    raise RateLimitedError("Facebook rate-limited this request; requests stopped")
                network_failures = 0
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    stressed = True
                    logger.warning(
                        "graphql_returned_error_status",
                        platform=self.PLATFORM,
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                    )
                else:
                    self._adjust_interval(stressed=stressed)
                    # Có response = proxy chạy được, kể cả khi phải thử lại sau một lần timeout chập chờn. Trước đây ghi
                    # success=not stressed: một lần thử lại thành công vẫn bị tính là proxy lỗi (ghi một lần cho cả
                    # client) -> proxy vào cooldown -> "platform_proxy_exhausted" dừng cả lượt Threads (16-17/10/2026).
                    self._record_proxy_outcome_once(success=True)
                    return resp

            if attempt < self.MAX_RETRIES:
                # Jitter cộng thêm vào mức cơ sở tăng theo cấp số - một lần thử lại rơi đúng 2s/4s/8s mỗi
                # lần tự nó đã là kiểu mẫu đều đặn mà jitter giãn cách từng request ở chỗ khác vốn tránh.
                delay = self.RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(
                    0, self.RETRY_BACKOFF_JITTER_SECONDS
                )
                logger.info("retrying", delay_seconds=round(delay, 1))
                time.sleep(delay)

        self._adjust_interval(stressed=True)
        self._record_proxy_outcome_once(success=False)

        # 429 còn nguyên sau mọi lần thử lại nghĩa là nền tảng thật sự đang giới hạn rate tài
        # khoản/IP này, không phải token đã chết - giữ tách biệt với SessionExpiredError để chỗ gọi
        # không chẩn đoán nhầm thành "chạy lại bootstrap.py" (chỉ thêm lưu lượng đăng nhập đúng lúc
        # nền tảng đang bóp tài khoản này).
        if resp is not None and resp.status_code == 429:
            if self.PLATFORM == "facebook":
                self._rest_restricted_account("http_429")
            raise RateLimitedError(
                f"{self.PLATFORM.capitalize()} rate-limited this request (status=429) even after "
                f"{self.MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        # resp ở đây vẫn là None - mọi lần thử đều raise RequestsError (lỗi ở cấp kết nối), chưa
        # hề tới được server của nền tảng, nên đây là vấn đề mạng/proxy, không phải session chết.
        # __cause__ là ProxyPoolExhaustedError: spider thoát PROXY_EXHAUSTED_EXIT_CODE, consumer xếp lại request và
        # nghỉ nền tảng (backoff tăng dần) - proxy vừa bị ghi lỗi ở trên nên lượt sau gặp cooldown thay vì treo tiếp.
        proxy_url = self._proxy_cfg.get("url") if self._proxy_cfg else None
        raise NetworkError(
            f"Request failed after {network_failures} consecutive network errors ({type(last_exc).__name__})"
        ) from pool.ProxyPoolExhaustedError(f"{self.PLATFORM} proxy {proxy_url} unreachable")
