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

import copy
import hashlib
import json
import random
import time
import uuid
from collections.abc import Callable
from typing import Any

import redis
from curl_cffi import CurlOpt
from curl_cffi import requests as curl_requests

from social_crawler.clients import proxy_provider
from social_crawler.clients.redis import RedisCache
from social_crawler.core.fingerprint import (
    ACCEPT_LANGUAGE,
    resolve_profile,
)
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events
from social_crawler.db.accounts import disable_account, get_accounts
from social_crawler.runtime import account_lease, ip_switch, pool

logger = get_logger(__name__)

# Phiên bị từ chối (xem _drop_rejected_session): tài khoản nghỉ chừng này giờ. Mã lỗi Facebook "chưa đăng nhập" /
# "phiên hết hạn" trả trong body (HTTP 200).
REJECTED_SESSION_REST_HOURS = 6
_LOGGED_OUT_ERROR_CODES = {1357001, 1357004}
# Tài khoản bị khoá chờ xác minh: HTTP 200, body {"error": 1357053, "errorSummary": "...examiner votre compte",
# "redirectTo": "/checkpoint/<id>/"} (27/10/2026). Trước đó spider coi body này là trang 0 kết quả - tài khoản checkpoint
# vẫn được dùng tiếp, và job đào bài cũ đánh dấu nhầm từ khoá là "hết bài".
_CHECKPOINT_ERROR_CODES = {1357053}


def _is_checkpoint_response(parsed: Any) -> bool:
    if not isinstance(parsed, dict):
        return False
    return (
        parsed.get("message") == "checkpoint_required"
        or str(parsed.get("error")) in {str(code) for code in _CHECKPOINT_ERROR_CODES}
        or "/checkpoint/" in str(parsed.get("redirectTo") or "")
    )


# Lần reset IP gần nhất theo cổng proxy (monotonic) - xem CometGraphQLClient._rotate_proxy_after_network_error.
_last_rotate_at: dict[str, float] = {}
_ROTATE_MIN_GAP_SECONDS = 90.0

# Điều chỉnh kiểu AIMD cho khoảng bóp nhịp thích ứng theo tài khoản (xem
# CometGraphQLClient._adjust_interval): tăng nhanh khi có bất kỳ dấu hiệu căng thẳng nào
# (một lần thử lại là đủ để phản ứng), giảm chậm để một request sạch ngay sau một đợt khó
# khăn không xoá ngay sự thận trọng.
_ADAPTIVE_INTERVAL_GROWTH_FACTOR = 1.7
_ADAPTIVE_INTERVAL_DECAY_FACTOR = 0.85
# Một khoảng đã tăng sống được bao lâu mà không có tín hiệu căng thẳng mới trước khi
# _current_interval quay về đọc lại MIN_REQUEST_INTERVAL_SECONDS - một tài khoản đã khó
# khăn 10 phút từ một giờ trước thì giờ không nên còn bị bóp nhịp quá thận trọng.
_ADAPTIVE_INTERVAL_TTL_SECONDS = 1800

__all__ = [
    "AccountRestrictedError",
    "CheckpointRequiredError",
    "CometGraphQLClient",
    "NetworkError",
    "RateLimitedError",
    "RequestBudgetExceeded",
    "SessionExpiredError",
    "find_page_info",
]


TRANSPORT_ERRORS = (curl_requests.RequestsError,)


def _url_without_query(url: Any) -> str:
    """Scheme, host, path: query có thể mang token nên không đưa vào dòng thời gian."""
    return str(url or "").split("?", 1)[0][:200]


class RequestBudgetExceeded(RuntimeError):
    """Tài khoản đã dùng quá trần request Facebook trong ngày (services/search_budget.py) - spider dừng gọn (bài đã lấy
    vẫn giữ), job không tính là lỗi."""


class SessionExpiredError(RuntimeError):
    """Cache token thiếu/hết hạn hoặc nền tảng từ chối request (401/403) - chạy lại bootstrap.py."""


class GraphQLSchemaError(RuntimeError):
    """Query variables no longer match the server schema; requires a code update."""


class GraphQLDocumentError(RuntimeError):
    """Persisted query unavailable; public rediscovery did not resolve it."""


class GraphQLResponseError(RuntimeError):
    """GraphQL reported errors; never interpret them as an empty successful page."""


class NetworkError(RuntimeError):
    """Mọi lần thử lại đều không nhận được response HTTP nào (proxy sập, lỗi DNS, lỗi bắt tay
    TLS, timeout) - nền tảng thực ra chưa hề thấy request này, nên vấn đề không phải
    token/session. Chạy lại bootstrap.py không sửa được proxy chết; hãy kiểm tra dòng
    platform_proxies đã cấu hình."""


class RateLimitedError(RuntimeError):
    """Nền tảng đang giới hạn rate tài khoản/IP này (429) kể cả sau khi thử lại với backoff.
    Đây KHÔNG phải token chết - chạy lại bootstrap.py không giúp gì mà chỉ đốt thêm một vòng
    đăng nhập với một tài khoản đang bị bóp. Hãy lùi lại và thử lại sau."""


class CheckpointRequiredError(RuntimeError):
    """Nền tảng gắn cờ tài khoản này giữa phiên và đòi xác minh lại - thấy dưới dạng response
    400 với body {"message": "checkpoint_required", "status": "fail"} trên một request phát
    lại bình thường (không chỉ lúc đăng nhập của bootstrap.py, vốn đã có phép kiểm tra riêng
    cho việc này). Token đã cache vẫn trông còn mới và mọi lần thử lại chỉ nhận cùng
    response, nên _run() tắt tài khoản (xem disable_account) và cảnh báo ngay thay vì thử lại
    - phải có người thật sự đăng nhập qua trình duyệt thật và gỡ checkpoint thì tài khoản này
    mới dùng lại được."""


class AccountRestrictedError(CheckpointRequiredError):
    """Nền tảng báo rõ đang hạn chế tài khoản; tài khoản đã được cho nghỉ, phiên vẫn được giữ. Cùng cách dừng lượt crawl với
    checkpoint trong spider."""


def is_search_recipe(cache: Any) -> bool:
    """Cache token này có phải công thức query *tìm kiếm* không (friendly_name chứa "search").

    Cache của key search có thể bị ghi bằng công thức khác: ngày 2026-10-05 cả Facebook lẫn
    Threads đều có key search chứa query comment/feed do một lần bootstrap comment ghi vào. Phát
    lại công thức đó bằng search() âm thầm bỏ qua từ khoá (biến `query`/`text` không có trong
    mẫu nên phần ghi đè không áp được), nên phải kiểm tra trước khi tin cache."""
    if not isinstance(cache, dict):
        return False
    return "search" in str(cache.get("fb_api_req_friendly_name") or "").lower()


class CometGraphQLClient:
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
        # Identity is validated after taking the lease and re-reading the latest snapshot.

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
                # An invalidated recipe must not be resurrected from a pre-lease snapshot.
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
            # Preserve browser observations; never repair identity by changing headers.
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
        pool.release_proxy(self._proxy_cfg, success=success)

    def _throttle_key(self) -> str:
        return self.THROTTLE_REDIS_KEY_TMPL.format(account=self._account)

    def _current_interval(self) -> float:
        """Khoảng giãn cách cơ sở dùng ngay lúc này - bình thường là MIN_REQUEST_INTERVAL_SECONDS,
        hoặc một giá trị cao hơn lưu trong Redis nếu tài khoản này gần đây gặp lỗi
        429/5xx/mạng (xem _adjust_interval). Lưu bền (không chỉ trong bộ nhớ) vì các lần chạy
        bootstrap.py/scrapy crawl là tiến trình con sống ngắn - không có Redis thì một lượt chạy
        bị bóp ngay trước khi thoát sẽ không dạy được gì cho lượt sau."""
        stored = self._redis.get(self._throttle_key())
        if stored is None:
            return self.MIN_REQUEST_INTERVAL_SECONDS
        return max(self.MIN_REQUEST_INTERVAL_SECONDS, float(stored))

    def _adjust_interval(self, *, stressed: bool) -> None:
        """Được gọi sau khi mỗi request kết thúc: tăng khoảng đã lưu khi có bất kỳ tín hiệu
        429/5xx/mạng nào, giảm dần lại khi response sạch và lần gọi này chưa có tín hiệu nào. Xem
        các hằng _ADAPTIVE_INTERVAL_* cấp module cho hệ số tăng/giảm và TTL."""
        key = self._throttle_key()
        stored = self._redis.get(key)
        if stored is None:
            if not stressed:
                return  # đã ở mức cơ sở - không có gì để lưu
            current = self.MIN_REQUEST_INTERVAL_SECONDS
        else:
            current = max(self.MIN_REQUEST_INTERVAL_SECONDS, float(stored))

        if stressed:
            new_interval = min(self.ADAPTIVE_INTERVAL_MAX_SECONDS, current * _ADAPTIVE_INTERVAL_GROWTH_FACTOR)
        else:
            new_interval = max(self.MIN_REQUEST_INTERVAL_SECONDS, current * _ADAPTIVE_INTERVAL_DECAY_FACTOR)

        if new_interval <= self.MIN_REQUEST_INTERVAL_SECONDS:
            self._redis.delete(key)
            return

        logger.info(
            "adaptive_interval_adjusted",
            platform=self.PLATFORM,
            account=self._account,
            stressed=stressed,
            interval_seconds=round(new_interval, 2),
        )
        self._redis.set(key, new_interval, ttl_seconds=_ADAPTIVE_INTERVAL_TTL_SECONDS)

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
        """Inspect structured errors before retry, even when the server returned HTTP 5xx."""
        if self.PLATFORM != "facebook":
            return
        from social_crawler.spiders.facebook.auth.http_refresh import check_security_errors

        check_security_errors(self, response)

    def _throttle(self) -> None:
        """Giãn cách các request tới nền tảng - không có gì khác làm việc này, vì mọi spider ở đây
        gọi thẳng curl_cffi thay vì đi qua downloader của Scrapy. Mốc request trước lưu cả trong Redis theo tài khoản
        (24/10/2026): mỗi job là một tiến trình con mới, chỉ nhớ trong bộ nhớ thì request đầu của job nào cũng bắn ngay."""
        # Mọi lần gửi thật đều đi qua đây, nên đây là chỗ duy nhất cần kiểm tra quyền dùng tài khoản.
        self._ensure_lease()
        self._count_request()
        base_interval = self._current_interval()
        if self.PLATFORM == "facebook":
            self._throttle_facebook(base_interval)
            return
        last_key = f"{self.PLATFORM}:last_request_at:{self._account}"
        try:
            shared_last = float(self._redis.get(last_key) or 0) or None
        except (TypeError, ValueError, redis.RedisError) as exc:
            if self.PLATFORM == "facebook":
                raise NetworkError("Facebook throttle state unavailable; no request sent") from exc
            shared_last = None
        if shared_last and (self._last_request_at is None or shared_last > self._last_request_at):
            self._last_request_at = shared_last
        if self._last_request_at is not None:
            target_gap = base_interval + random.uniform(0, self.REQUEST_INTERVAL_JITTER_SECONDS)
            if self.LONG_PAUSE_CHANCE > 0 and random.random() < self.LONG_PAUSE_CHANCE:
                target_gap += random.uniform(self.LONG_PAUSE_MIN_SECONDS, self.LONG_PAUSE_MAX_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.debug(
                    "throttling", delay_seconds=round(remaining, 2), base_interval_seconds=round(base_interval, 2)
                )
                time.sleep(remaining)
                self._ensure_lease()  # lease có thể mất trong lúc chờ
        self._last_request_at = time.time()
        try:
            self._redis.set(last_key, self._last_request_at, ttl_seconds=3600)
        except redis.RedisError as exc:
            if self.PLATFORM == "facebook":
                raise NetworkError("Facebook throttle state could not be saved; no request sent") from exc
            # Threads retains its process-local fallback.

    def _removed_cookies(self) -> set[str]:
        from social_crawler.runtime.facebook_pacing import PacedSession

        session = getattr(self, "_session", None)
        return session.removed_cookies() if isinstance(session, PacedSession) else set()

    def _mark_request_finished(self) -> None:
        """Share response completion with the next job, including network failures."""
        self._last_response_at = time.time()
        new_removals = self._removed_cookies() - getattr(self, "_forgotten_cookies", set())
        if new_removals:
            from social_crawler.spiders.facebook.auth.http_refresh import forget_removed_cookies

            self._forgotten_cookies = getattr(self, "_forgotten_cookies", set()) | new_removals
            forget_removed_cookies(self, new_removals)
        try:
            self._redis.set(f"facebook:last_response_at:{self._account}", self._last_response_at, ttl_seconds=86400)
        except redis.RedisError as exc:
            raise NetworkError("Facebook response pacing state unavailable; requests stopped") from exc

    def _throttle_asset(self) -> None:
        """A cookieless CDN bundle still needs the lease and counts toward the budget, but not the account's rest:
        browsers fetch bundles in bursts, and 5-10 seconds each would use up the discovery deadline in about ten."""
        self._ensure_lease()
        self._count_request()
        time.sleep(random.uniform(*self.ASSET_GAP_SECONDS))

    def _throttle_facebook(self, base_interval: float) -> None:
        from social_crawler.runtime.facebook_pacing import rest_range

        cookies = (getattr(self, "_cache", None) or {}).get("cookies") or {}
        identity = str(cookies.get("c_user") or self._account)
        minimum, maximum = rest_range(identity, self.MIN_REQUEST_INTERVAL_SECONDS, self.REQUEST_INTERVAL_JITTER_SECONDS)
        scale = max(1, base_interval / self.MIN_REQUEST_INTERVAL_SECONDS)
        minimum, maximum = minimum * scale, maximum * scale
        if not getattr(self, "_rest_profile_logged", False):
            logger.info(
                "facebook_request_rest_profile",
                account=self._account,
                min_seconds=round(minimum, 2),
                max_seconds=round(maximum, 2),
            )
            self._rest_profile_logged = True
        try:
            # The start timestamp is only a migration/crash fallback when no response was recorded.
            last = float(
                self._redis.get(f"facebook:last_response_at:{self._account}")
                or self._redis.get(f"facebook:last_request_at:{self._account}")
                or getattr(self, "_last_response_at", None)
                or getattr(self, "_last_request_at", None)
                or 0
            )
            if last:
                target = random.uniform(minimum, maximum)
                if self.LONG_PAUSE_CHANCE > 0 and random.random() < self.LONG_PAUSE_CHANCE:
                    target += random.uniform(self.LONG_PAUSE_MIN_SECONDS, self.LONG_PAUSE_MAX_SECONDS)
                # Ignore backwards-clock elapsed time: at most one sampled pause after a clock correction.
                remaining = max(0, target - max(0, time.time() - last))
                if remaining > 0:
                    logger.info("facebook_request_wait", account=self._account, delay_seconds=round(remaining, 2))
                    time.sleep(remaining)
            self._ensure_lease()
            self._last_request_at = time.time()
            self._redis.set(f"facebook:last_request_at:{self._account}", self._last_request_at, ttl_seconds=86400)
        except (TypeError, ValueError, redis.RedisError) as exc:
            raise NetworkError("Facebook request pacing unavailable; no request sent") from exc

    def _count_request(self) -> None:
        """Hạn mức request mỗi tài khoản (services/search_budget.py): Facebook theo ngày + theo phần (budget_share) +
        theo giờ; Threads theo giờ."""
        from social_crawler.runtime import search_budget

        if self.PLATFORM == "threads":
            if search_budget.THREADS_HOURLY_CAP <= 0:
                return
            try:
                hour = search_budget.count_hour_request("threads", self._account, self._redis)
            except redis.RedisError:
                return
            if self.enforce_hourly_cap and hour > search_budget.hourly_hard_cap("threads"):
                raise RequestBudgetExceeded(f"account {self._account!r} used {hour} Threads requests this hour")
            return
        if self.PLATFORM != "facebook":
            return
        share = self.budget_share
        try:
            from social_crawler.runtime import facebook_run_budget

            facebook_run_budget.reserve(self._account, self._redis, self._ensure_lease, self._verify_proxy_after_rest)
            used = search_budget.count_request(self._account, self._redis, share)
            hour = search_budget.used_this_hour("facebook", self._account, self._redis)
            factor = search_budget.ramp_factor(self._account, self._redis)
            if search_budget.DAILY_CAP > 0:
                cap = search_budget.hard_cap() * factor
                if used > cap:
                    raise RequestBudgetExceeded(f"account {self._account!r} used {used} Facebook requests today")
                if (
                    share
                    and search_budget.used_share(self._account, share, self._redis)
                    > search_budget.share_hard_cap(share) * factor
                ):
                    raise RequestBudgetExceeded(f"account {self._account!r} exceeded its {share} budget")
            else:
                cap = search_budget.new_account_cap(self._account, self._redis)
                if cap and used > cap * search_budget.HARD_CAP_FACTOR:
                    raise RequestBudgetExceeded(f"new account {self._account!r} exceeded its daily budget")
            if search_budget.HOURLY_CAP > 0 and hour > search_budget.hourly_hard_cap("facebook") * factor:
                raise RequestBudgetExceeded(f"account {self._account!r} used {hour} Facebook requests this hour")
        except redis.RedisError as exc:
            raise NetworkError("Facebook budget unavailable; no request sent") from exc

    def _verify_proxy_after_rest(self) -> None:
        proxy = getattr(self, "_proxy_cfg", None)
        if proxy and not pool.watch_exit_ip(proxy, self._account, self._redis):
            raise NetworkError("Facebook proxy could not be verified after rest") from pool.ProxyPoolExhaustedError(
                self._account
            )

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
        self._persist_response_cookies()
        return parsed

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

    def _comment_target_id(self, post_id: str) -> str:
        """Cách query comment của nền tảng này định địa chỉ một bài - Facebook dùng
        base64("feedback:<post_id>") (xem FacebookGraphQLClient), lớp con có tính năng comment
        phải ghi đè bằng cách định địa chỉ riêng đã xác nhận với request thật."""
        raise NotImplementedError(f"{self.PLATFORM} has no comments feature (no _comment_target_id override)")

    def _reply_target_id(self, legacy_comment_id: str) -> str:
        """Cách query reply-của-comment của nền tảng này định địa chỉ comment cha. Mặc định dùng
        cùng cách với _comment_target_id (cách định địa chỉ base64("feedback:<id>") của
        Facebook, đã xác nhận cho bài - CHƯA được xác nhận độc lập cho id comment; lớp con nên
        ghi đè khi một request reply thật bắt được cho thấy khác)."""
        return self._comment_target_id(legacy_comment_id)

    def _get_comments_cache(self) -> dict[str, Any]:
        comments_key = self.COMMENTS_REDIS_KEY_TMPL.format(account=self._account)
        comments = self._redis.get(comments_key)
        if comments is None:
            raise SessionExpiredError(
                f"No comments query cached in Redis (key={comments_key!r}, account={self._account!r}). Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --post-url "<a post url with comments>"'
            )
        return comments

    def get_comments(self, post_id: str) -> dict[str, Any]:
        """Lấy trang comment đầu tiên của một bài - dùng chung cho mọi nền tảng có tính năng
        comment (xem _comment_target_id).

        Reset rõ cursor về null dù đây là query "ban đầu", không phải query "phân trang": trên
        nền tảng mà cùng một query refetch đảm nhận cả hai vai (đã xác nhận trên Threads - xem
        pick_paginated_comments_request trong request_capture.py), request bắt được giữa lúc
        cuộn trong bootstrap đã mang sẵn một giá trị cursor thật (giờ đã cũ) trong mẫu. Để
        nguyên thì trang 1 sẽ âm thầm hỏi "những gì sau cursor cũ đó" thay vì trang đầu thật, và
        nhận về direct_replies rỗng. Vô hại trên nền tảng có query comment gốc/phân trang thực sự
        tách riêng (Facebook): mẫu đó đơn giản là không có key cursor nào để lượt duyệt này đụng
        tới."""
        comments = self._get_comments_cache()
        return self._run(
            doc_id=comments.get("doc_id"),
            friendly_name=comments.get("fb_api_req_friendly_name"),
            template=comments.get("variables_template"),
            template_source="comments variables_template",
            overrides={self.COMMENTS_ID_KEY: self._comment_target_id(post_id), self.COMMENTS_CURSOR_KEY: None},
        )

    def get_comments_next_page(self, post_id: str, cursor: str, count: int = -1) -> dict[str, Any]:
        """Lấy trang comment kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Cần bootstrap.py đã bắt được một request comment có phân trang - nó
        tự làm việc này bằng cách cuộn danh sách comment sau khi đổi thứ tự sắp xếp (xem
        comments_trigger riêng của từng nền tảng).

        Mặc định count=-1 khớp với CommentsListComponentsPaginationQuery của chính Comet (đã xác
        nhận thực tế 2026-09-16): kích thước trang dương vẫn bị giới hạn khoảng 10; -1 là thứ
        trình duyệt gửi để lấy trang dày nhất sau cursor."""
        comments = self._get_comments_cache()
        pagination = comments.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no comments pagination info (no paginated comments query was captured). "
                "Re-run bootstrap.py --post-url against a post with more comments than fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="comments pagination.variables_template",
            overrides={
                self.COMMENTS_ID_KEY: self._comment_target_id(post_id),
                self.COMMENTS_CURSOR_KEY: cursor,
                self.COMMENTS_COUNT_KEY: count,
            },
        )

    def _get_replies_cache(self) -> dict[str, Any]:
        replies_key = self.REPLIES_REDIS_KEY_TMPL.format(account=self._account)
        replies = self._redis.get(replies_key)
        if replies is None:
            raise SessionExpiredError(
                f"No replies query cached in Redis (key={replies_key!r}, account={self._account!r}). Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --post-url "<a post url whose top-level '
                'comment has replies>" --type replies'
            )
        return replies

    def get_replies(self, legacy_comment_id: str) -> dict[str, Any]:
        """Lấy trang reply đầu tiên của một comment cấp một - giống get_comments ở trên, chỉ là định
        địa chỉ vào một comment thay vì một bài (xem _reply_target_id) và cache dưới
        REPLIES_REDIS_KEY_TMPL."""
        replies = self._get_replies_cache()
        return self._run(
            doc_id=replies.get("doc_id"),
            friendly_name=replies.get("fb_api_req_friendly_name"),
            template=replies.get("variables_template"),
            template_source="replies variables_template",
            overrides={self.COMMENTS_ID_KEY: self._reply_target_id(legacy_comment_id), self.COMMENTS_CURSOR_KEY: None},
        )

    def get_replies_next_page(self, legacy_comment_id: str, cursor: str, count: int = 10) -> dict[str, Any]:
        """Lấy trang reply kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Giống get_comments_next_page ở trên."""
        replies = self._get_replies_cache()
        pagination = replies.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no replies pagination info (no paginated replies query was captured). "
                "Re-run bootstrap.py --post-url ... --type replies against a comment with more replies than fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="replies pagination.variables_template",
            overrides={
                self.COMMENTS_ID_KEY: self._reply_target_id(legacy_comment_id),
                self.COMMENTS_CURSOR_KEY: cursor,
                self.COMMENTS_COUNT_KEY: count,
            },
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
        ip_switch.touch(self.PLATFORM, getattr(self, "_proxy_cfg", None), getattr(self, "_account", None))
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


def _loggable(overrides: dict[str, Any]) -> dict[str, Any]:
    """Một số giá trị ghi đè (cursor phân trang Relay) là các khối mã hoá không đọc được dài
    hàng nghìn ký tự - cắt ngắn mọi thứ dài trước khi đưa vào log thay vì làm mọi request
    chìm trong nhiễu."""
    return {
        key: (f"{value[:40]}...({len(value)} chars)" if isinstance(value, str) and len(value) > 60 else value)
        for key, value in overrides.items()
    }


def normalize_cached_fingerprint(cache: dict[str, Any], account_key: str | None = None) -> dict[str, Any]:
    """Chuẩn hoá lại dấu vân tay trong một cache token đã lưu, để nó khớp với dấu vân tay
    mà đường replay dùng (hồ sơ thiết bị của tài khoản đó).

    account_key: tài khoản mà cache này thuộc về - quyết định hồ sơ dùng để chuẩn hoá.
    Không truyền thì dùng hồ sơ mặc định (biến thể đầu tiên, đúng trình duyệt của
    patchright).

    Vì sao cần: cache token được bootstrap bắt lúc trình duyệt còn chạy, nên nó mang
    User-Agent và Client Hints của đúng trình duyệt lúc bắt. Sau khi nâng phiên bản
    (Chromium 151 thay cho 146), các cache cũ vẫn mang sec-ch-ua của 146 trong khi đường
    replay đã giả TLS của 151 - một phiên lại lệch fingerprint ngay trong lúc dùng.

    Sửa ở đây (lúc đọc cache) thay vì bắt người vận hành chạy lại bootstrap cho mọi
    tài khoản: cache vẫn dùng được ngay, chỉ là phần "dấu vân tay" của nó được đồng bộ
    lại. Cache trả về là BẢN SAO - không sửa cái đã lưu trong Redis.

    Cookie/session không đụng vào: chúng không phụ thuộc vào phiên bản trình duyệt, và
    Facebook gắn chúng với dấu vân tịp đã tạo ra chúng - đổi cookie là đổi cả phiên.
    """
    headers = dict(cache.get("headers") or {})
    ua = headers.get("user-agent")
    if not ua:
        return cache

    # Cùng quy tắc với đường trình duyệt (browser_interaction.new_context): UA bắt được là UA
    # riêng của tài khoản và khai trung thực được thì giữ, không thì hồ sơ của tài khoản. UA
    # cũ khai phiên bản khác Chromium thật (cache Chrome 146) rơi vào nhánh hồ sơ.
    profile = resolve_profile(account_key, ua)
    headers["user-agent"] = profile["user_agent"]
    # Client Hints đi kèm UA - phải đồng bộ luôn, không thể giữ của phiên bản cũ. Bộ
    # header dưới đây là đúng những gì trình duyệt thật gửi (đo 27/10/2026).
    headers["sec-ch-ua"] = profile["sec_ch_ua"]
    headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
    headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]
    headers["sec-ch-ua-platform-version"] = profile["sec_ch_ua_platform_version"]
    headers["sec-ch-ua-full-version-list"] = profile["sec_ch_ua_full_version_list"]

    # accept-language: browser chạy locale vi-VN; cache cũ không có trường này trong
    # STATIC_HEADER_FIELDS nên thiếu luôn -> thêm vào để không lệch locale với đường replay.
    headers.setdefault("accept-language", ACCEPT_LANGUAGE)

    normalized = dict(cache)
    normalized["headers"] = headers
    return normalized


def _apply_session_counters(body: dict[str, Any], req_value: int) -> dict[str, Any]:
    """Ghi đè các trường "thời gian" trong body của một request GraphQL.

    Hai trường này cùng nhau tạo nên "dấu vân tay thời gian" của một phiên trình duyệt:

    - `__req`: bộ đếm request tăng đơn điệu của tab. Bản bootstrap bắt được rồi phát lại
      y hệt là cùng một số lặp lại hàng trăm lần - dấu hiệu replay rõ nhất trong toàn bộ
      body. Ở đây tăng dần mỗi request (xem CometGraphQLClient._next_req_counter).
    - `jazoest`: checksum của `lsd` ("2" + tổng mã ord của từng ký tự). Lsd trong cache
      là của lúc bootstrap; nếu Facebook đã đổi lsd giữa chừng thì jazoest cũ không còn
      khớp, và một jazoest lệch là dấu hiệu body bị sửa tay. Tính lại từ lsd hiện hành.

    Cố ý KHÔNG đụng `__hs`/`__s`: hai thứ đó là HASH nội dung request (không phải bộ đếm),
    nên ghi đè bằng một con số sẽ sai và có thể làm Facebook từ chối request. Chúng cũng đã
    bị cache nguyên vẹn từ lúc bootstrap nên vẫn khớp với nội dung.

    Trả về body MỚI (không sửa tại chỗ) để không đụng vào cache đã lưu."""
    body = dict(body)
    if "__req" in body:
        body["__req"] = str(req_value)
    lsd = body.get("lsd")
    if lsd:
        body["jazoest"] = _jazoest(str(lsd))
    return body


def _jazoest(lsd: str) -> str:
    """Checksum của lsd mà Facebook dùng cho trường `jazoest` trong body form: "2" + tổng
    mã ord của từng ký tự của lsd. Trình duyệt thật luôn gửi jazoest khớp với lsd hiện
    hành."""
    return "2" + str(sum(ord(ch) for ch in lsd))


def _apply_variable_overrides(template: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Deep-copy một variables_template mà bootstrap.py đã cache và chỉ ghi đè các key đã cho
    (ở bất cứ đâu chúng xuất hiện trong cây) + sinh lại mọi trường *session_id - mọi giá trị
    khác (ví dụ các cờ __relay_internal__pv__...) giữ nguyên vì ta không biết toàn bộ schema
    hiện tại, thứ mà nền tảng thay đổi ở mỗi lần deploy. Dùng chung cho mọi loại query
    (search, comments, ...) để thêm loại mới không bao giờ cần logic vá biến riêng."""
    variables = copy.deepcopy(template)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in overrides:
                    node[key] = overrides[key]
                elif key.endswith("session_id") and isinstance(value, str):
                    node[key] = str(uuid.uuid4())
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(variables)
    return variables


def _iter_matching(node: Any, predicate: Callable[[dict], bool]):
    """Duyệt đệ quy một cây dict/list, yield mọi dict mà predicate(node) là true - phép duyệt
    cây duy nhất project này cần mỗi khi đường dẫn thật của một trường không chắc giữ ổn
    định qua các lần deploy của nền tảng."""
    if isinstance(node, dict):
        if predicate(node):
            yield node
        for value in node.values():
            yield from _iter_matching(value, predicate)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_matching(value, predicate)


def find_page_info(node: Any) -> dict[str, Any] | None:
    """Tìm trong một response GraphQL đã parse một dict `page_info` của Relay (có cả
    `has_next_page` lẫn `end_cursor`). Kết quả lồng sâu vài tầng và đường dẫn đó không chắc
    giữ ổn định qua các lần deploy, nên hàm này duyệt cả cây thay vì gán cứng. Dùng chung cho
    Facebook và Threads (đều dựa trên Relay/Comet) - đã xác nhận dạng giống hệt nhau trên cả
    hai."""
    for match in _iter_matching(node, lambda n: "has_next_page" in n and "end_cursor" in n):
        return match
    return None


def _parse_graphql_response(raw: str, *, strict: bool = False) -> dict[str, Any]:
    """Object JSON đầu tiên của một response GraphQL (có thể được stream), với mọi chunk giao
    dần về sau được trộn vào đó.

    Query Comet dùng @defer/@stream của Relay trả lời bằng nhiều object JSON, mỗi dòng một
    cái: cái đầu là payload ban đầu, mỗi cái sau mang {"label", "path", "data"} để trộn vào
    `path` bên trong `data` của cái đầu - đúng việc Relay làm trong trình duyệt. Trước đây
    chỉ đọc dòng đầu tiên; đã xác nhận 2026-09-28 rằng cách đó làm mất mọi comment của bài
    video/reel, vì query comment của chúng
    (FBUnifiedVideoFeedbackRightRailWithCommentPreloadingQuery) chỉ trả danh sách comment ở
    chunk thứ ba, được defer (khoảng 475KB trên body 485KB). Chunk không parse được hoặc
    không khớp thì bỏ qua - chúng chỉ bao giờ thêm dữ liệu vào object đầu, không bao giờ thay
    thế nó."""
    text = raw.strip()
    prefix = "for (;;);"
    text = text.removeprefix(prefix)
    lines = [line for line in text.splitlines() if line.strip()] or [text]
    try:
        first = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        if strict:
            raise GraphQLResponseError("Facebook returned a non-JSON or incomplete GraphQL response") from None
        raise SessionExpiredError(
            f"Could not parse GraphQL response (token may have expired): {exc}. Body: {text[:300]!r}"
        ) from exc
    if not isinstance(first, dict) or not isinstance(first.get("data"), dict):
        return first
    for line in lines[1:]:
        try:
            chunk = json.loads(line)
        except json.JSONDecodeError:
            logger.debug("graphql_stream_chunk_unparseable", chars=len(line))
            continue
        if isinstance(chunk, dict) and chunk.get("errors"):
            first.setdefault("errors", []).extend(chunk["errors"])
        if isinstance(chunk, dict) and isinstance(chunk.get("data"), dict):
            _merge_at_path(first["data"], chunk.get("path") or [], chunk["data"])
    return first


def _merge_at_path(target: dict[str, Any], path: list[Any], data: dict[str, Any]) -> None:
    """Deep-merge `data` vào `target` tại `path` (key dict / chỉ số list), tạo các tầng dict còn
    thiếu. Đường dẫn đâm vào một thứ không phải container thì bị bỏ thay vì ghi đè thứ đang
    có."""
    node: Any = target
    for step in path:
        if isinstance(node, dict):
            node = node.setdefault(step, {})
        elif isinstance(node, list) and isinstance(step, int) and 0 <= step < len(node):
            node = node[step]
        elif isinstance(node, list) and isinstance(step, int) and step == len(node):
            # @stream của Relay giao phần tử MỚI của danh sách (path ...edges, N với N = số phần tử đang có) - nối
            # vào thay vì bỏ. Trước 16/10/2026 bị bỏ: bảng tin Page chỉ đọc được bài đầu mỗi trang (1/2, 1/3).
            node.append({})
            node = node[step]
        else:
            return
    if isinstance(node, dict):
        _deep_merge(node, data)


def _deep_merge(target: dict[str, Any], data: dict[str, Any]) -> None:
    for key, value in data.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value
