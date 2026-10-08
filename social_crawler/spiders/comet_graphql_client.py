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
import json
import random
import time
import uuid
from typing import Any, Callable

from curl_cffi import requests as curl_requests

from social_crawler.clients.redis import RedisCache
from social_crawler.db.accounts import disable_account, get_accounts
from social_crawler.logger import get_logger
from social_crawler.services import pool

logger = get_logger(__name__)

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
    "CometGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]


class SessionExpiredError(RuntimeError):
    """Cache token thiếu/hết hạn hoặc nền tảng từ chối request (401/403) - chạy lại bootstrap.py."""


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
        if cached is None:
            raise SessionExpiredError(
                f"No token cache found in Redis (key={cache_key!r}, account={self._account!r}), or Redis "
                "is unreachable, or the cache expired. Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --query "test"'
            )
        self._cache = cached
        age = time.time() - self._cache["captured_at"]
        logger.info("loaded_token_cache", account=self._account, age_hours=round(age / 3600, 1))

        proxy = None
        try:
            self._proxy_cfg = pool.acquire_proxy_for_account(self.PLATFORM, self._account, required=True)
        except pool.ProxyPoolExhaustedError as exc:
            # required=True: đây là lưu lượng crawl thường ngày, không phải trình duyệt đăng nhập một
            # lần - không bao giờ quay về chạy không proxy (xem docstring của
            # ProxyPoolExhaustedError). Raise lại thành NetworkError để nó đi qua đúng phần xử lý thử
            # lại/cảnh báo Telegram mà mọi spider vốn đã có cho "proxy sập" (xem ví dụ
            # `except NetworkError` trong facebook/features/search/search.py).
            raise NetworkError(str(exc)) from exc
        self._proxy_outcome_recorded = False
        if self._proxy_cfg:
            proxy_url = pool.build_proxy_url(self._proxy_cfg)
            proxy = {"http": proxy_url, "https": proxy_url}
        logger.info(
            "graphql_session_ready", account=self._account, proxy=self._proxy_cfg["url"] if self._proxy_cfg else None
        )

        self._session = curl_requests.Session(impersonate="chrome", proxies=proxy)
        self._last_request_at: float | None = None

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

    def _throttle(self) -> None:
        """Giãn cách các request tới nền tảng - không có gì khác làm việc này, vì mọi spider ở đây
        gọi thẳng curl_cffi thay vì đi qua downloader của Scrapy."""
        base_interval = self._current_interval()
        if self._last_request_at is not None:
            target_gap = base_interval + random.uniform(0, self.REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.info(
                    "throttling", delay_seconds=round(remaining, 2), base_interval_seconds=round(base_interval, 2)
                )
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _headers(self, friendly_name: str, lsd: str) -> dict[str, str]:
        """Header chung cho cả hai nền tảng - Threads ghi đè để thêm các trường origin/x-csrftoken
        riêng qua super()._headers(...)."""
        headers = dict(self._cache["headers"])
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "referer": self.REFERER_URL,
                "x-fb-friendly-name": friendly_name,
                "x-fb-lsd": lsd,
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
    ) -> dict[str, Any]:
        if template is None:
            raise SessionExpiredError(
                f"Cache has no {template_source} (it was created by an older bootstrap.py). "
                "Re-run bootstrap.py to refresh the cache."
            )

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

        self._throttle()
        resp = self._post_with_retry(
            headers=self._headers(friendly_name, static.get("lsd", "")),
            cookies=self._cache["cookies"],
            body=body,
        )

        if resp.status_code in (401, 403):
            raise SessionExpiredError(
                f"{self.PLATFORM.capitalize()} rejected the request (status={resp.status_code}). Re-run bootstrap.py."
            )

        logger.info("received_response", status_code=resp.status_code, bytes=len(resp.text))
        parsed = _parse_graphql_response(resp.text)

        if isinstance(parsed, dict) and parsed.get("message") == "checkpoint_required":
            disabled = disable_account(
                self.PLATFORM, self._account, reason="checkpoint_required response during replay traffic"
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

        return parsed

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

    def _post_with_retry(self, headers: dict[str, str], cookies: dict[str, str], body: dict[str, Any]) -> Any:
        """POST có thử lại với backoff tăng dần khi bị giới hạn rate (429), lỗi server (5xx) và lỗi
        ở cấp mạng - các lỗi này là tạm thời và thường tự hồi phục, khác với token chết
        (401/403), thứ chỗ gọi xử lý riêng và không bao giờ thử lại ở đây."""
        last_exc: Exception | None = None
        resp = None
        # True ngay khi bất kỳ lần thử nào trong lời gọi này gặp 429/5xx/lỗi mạng - cấp cho
        # _adjust_interval để một request chỉ thành công sau khi thử lại vẫn được tính là căng
        # thẳng, không phải response sạch.
        stressed = False

        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self._session.post(self.GRAPHQL_URL, headers=headers, cookies=cookies, data=body, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                stressed = True
                logger.warning(
                    "request_failed",
                    platform=self.PLATFORM,
                    attempt=attempt,
                    max_retries=self.MAX_RETRIES,
                    error=str(exc),
                )
            else:
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
                    self._record_proxy_outcome_once(success=not stressed)
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
            raise RateLimitedError(
                f"{self.PLATFORM.capitalize()} rate-limited this request (status=429) even after "
                f"{self.MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        # resp ở đây vẫn là None - mọi lần thử đều raise RequestsError (lỗi ở cấp kết nối), chưa
        # hề tới được server của nền tảng, nên đây là vấn đề mạng/proxy, không phải session chết.
        raise NetworkError(f"Request failed after {self.MAX_RETRIES} attempts: {last_exc}") from last_exc


def _loggable(overrides: dict[str, Any]) -> dict[str, Any]:
    """Một số giá trị ghi đè (cursor phân trang Relay) là các khối mã hoá không đọc được dài
    hàng nghìn ký tự - cắt ngắn mọi thứ dài trước khi đưa vào log thay vì làm mọi request
    chìm trong nhiễu."""
    return {
        key: (f"{value[:40]}...({len(value)} chars)" if isinstance(value, str) and len(value) > 60 else value)
        for key, value in overrides.items()
    }


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


def _parse_graphql_response(raw: str) -> dict[str, Any]:
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
    if text.startswith(prefix):
        text = text[len(prefix) :]
    lines = [line for line in text.splitlines() if line.strip()] or [text]
    try:
        first = json.loads(lines[0])
    except json.JSONDecodeError as exc:
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
