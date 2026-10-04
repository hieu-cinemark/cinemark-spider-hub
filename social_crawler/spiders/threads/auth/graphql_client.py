"""
Client HTTP thường (không trình duyệt) gọi lại endpoint GraphQL của Threads bằng
token/doc_id mà bootstrap.py đã cache trong Redis. threads.com chạy trên cùng stack
GraphQL Comet/Barcelona với Facebook (đã xác nhận với một request
BarcelonaPostPageStrongIdTargetQuery thật bắt được), nên mọi thứ không riêng của Threads
(thiết lập session, bóp nhịp, thử lại/backoff, tạo biến theo mẫu, parse response) nằm
trong CometGraphQLClient (xem docstring module của spiders/comet_graphql_client.py) -
file này chỉ thêm những gì thực sự riêng của Threads: header x-csrftoken/origin thêm, query
tìm kiếm và lệnh GET reply REST xác thực bằng cookie (không có lọc ngày).

Lưu ý: tên key `variables` chính xác mà query kết quả tìm kiếm thật dùng (text query /
cursor / count) chỉ biết được khi bootstrap.py đã thực sự bắt được một cái -
_apply_variable_overrides chỉ ghi đè những key nào có mặt trong mẫu đã bắt, nên một phần
ghi đè không khớp gì chỉ âm thầm để nguyên phần đó của mẫu thay vì báo lỗi. Nếu kết quả
search() ngừng thay đổi với các tham số `query` khác nhau, hãy kiểm tra lại
variables_template thật đã bắt trong Redis so với các key ghi đè bên dưới.
"""

from __future__ import annotations

import random
import time
from typing import Any
from urllib.parse import urlencode

from curl_cffi import requests as curl_requests

from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    IG_APP_ID,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    REST_READ_UA,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    TEXT_FEED_REPLIES_URL,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.logger import get_logger
from social_crawler.spiders.comet_graphql_client import (
    CheckpointRequiredError,
    CometGraphQLClient,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    find_page_info,
)

__all__ = [
    "ThreadsGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]

logger = get_logger(__name__)


class ThreadsGraphQLClient(CometGraphQLClient):
    PLATFORM = "threads"
    REFERER_URL = "https://www.threads.com/"
    CACHE_REDIS_KEY_TMPL = CACHE_REDIS_KEY_TMPL
    ACTIVE_ACCOUNT_REDIS_KEY = ACTIVE_ACCOUNT_REDIS_KEY
    DEFAULT_ACCOUNT_KEY = DEFAULT_ACCOUNT_KEY
    GRAPHQL_URL = GRAPHQL_URL
    MAX_RETRIES = MAX_RETRIES
    MIN_REQUEST_INTERVAL_SECONDS = MIN_REQUEST_INTERVAL_SECONDS
    REQUEST_INTERVAL_JITTER_SECONDS = REQUEST_INTERVAL_JITTER_SECONDS
    RETRY_BACKOFF_BASE_SECONDS = RETRY_BACKOFF_BASE_SECONDS
    RETRY_BACKOFF_JITTER_SECONDS = RETRY_BACKOFF_JITTER_SECONDS
    THROTTLE_REDIS_KEY_TMPL = THROTTLE_REDIS_KEY_TMPL
    ADAPTIVE_INTERVAL_MAX_SECONDS = ADAPTIVE_INTERVAL_MAX_SECONDS
    COMMENTS_REDIS_KEY_TMPL = COMMENTS_REDIS_KEY_TMPL
    # Đã xác nhận với một request BarcelonaPostPageStrongIdDirectRepliesRefetchQuery thật bắt
    # được - tên biến Relay của Threads khác với query comment của Facebook ở mọi biến này (xem
    # mặc định của CometGraphQLClient, vốn là của Facebook).
    COMMENTS_ID_KEY = "postID"
    COMMENTS_CURSOR_KEY = "after"
    COMMENTS_COUNT_KEY = "first"

    def _comment_target_id(self, post_id: str) -> str:
        """Khác feedback id base64 của Facebook, Threads định địa chỉ reply của một bài bằng id bài
        dạng số thô, không mã hoá - đã xác nhận với cùng request thật bắt được như các tên biến ở
        trên."""
        return str(post_id)

    def _headers(self, friendly_name: str, lsd: str) -> dict[str, str]:
        headers = super()._headers(friendly_name, lsd)
        headers.update(
            {
                "origin": "https://www.threads.com",
                # /graphql/query (khác /api/graphql) bị 403 thẳng nếu thiếu cái này - đã xác nhận với một
                # request thật bắt được, nơi nó được đặt đúng bằng giá trị cookie csrftoken. Đọc từ cookie
                # lúc gửi request (không cache thành header tĩnh) vì nó phải luôn khớp với csrftoken hiện
                # hành của session.
                "x-csrftoken": self._cache["cookies"].get("csrftoken", ""),
            }
        )
        return headers

    def search(self, query: str, count: int = 10, cursor: str | None = None) -> dict[str, Any]:
        """Lấy một trang kết quả tìm kiếm (trang đầu khi cursor là None)."""
        return self._run(
            doc_id=self._cache["doc_id"],
            friendly_name=self._cache["fb_api_req_friendly_name"],
            template=self._cache.get("variables_template"),
            template_source="variables_template",
            overrides=_search_overrides(query, cursor, count),
        )

    def _rest_headers(self) -> dict[str, str]:
        """Header cho các lượt đọc /api/v1/text_feed/.... Không phải bộ của GraphQL: bề mặt đó muốn
        UA Chrome đã bắt; bề mặt này lại 403 với UA đó."""
        cookies = self._cache["cookies"]
        captured = self._cache.get("headers") or {}
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": captured.get("accept-language") or "en-US,en;q=0.9",
            "referer": self.REFERER_URL,
            "user-agent": REST_READ_UA,
            "x-asbd-id": captured.get("x-asbd-id") or "129477",
            "x-csrftoken": cookies.get("csrftoken", ""),
            "x-ig-app-id": captured.get("x-ig-app-id") or IG_APP_ID,
            "x-ig-www-claim": "0",
        }

    def get_text_feed_replies(self, post_id: str, count: int = 25, cursor: str | None = None) -> dict[str, Any]:
        """Một trang reply của `post_id` qua GET /api/v1/text_feed/.../replies/.

        Cùng session cookie mà search() vốn dùng - không cần bootstrap query comment riêng, không
        trình duyệt. `cursor` là paging_tokens.downward từ trang trước (None cho trang đầu; vài
        bản dump viết là downwards)."""
        params: dict[str, str] = {"count": str(count)}
        if cursor:
            params["paging_token"] = cursor
        url = f"{TEXT_FEED_REPLIES_URL.format(post_id=post_id)}?{urlencode(params)}"
        logger.info("sending_text_feed_replies_request", post_id=post_id, count=count, has_cursor=bool(cursor))
        self._throttle()
        resp = self._get_with_retry(url, self._rest_headers())

        if resp.status_code in (401, 403):
            raise SessionExpiredError(
                f"Threads rejected the text_feed replies request (status={resp.status_code}). Re-run bootstrap.py."
            )

        logger.info(
            "received_response", status_code=resp.status_code, bytes=len(resp.content), endpoint="text_feed_replies"
        )
        try:
            parsed = resp.json()
        except ValueError as exc:
            raise SessionExpiredError(
                f"Threads text_feed replies returned non-JSON (status={resp.status_code})."
            ) from exc

        if isinstance(parsed, dict) and parsed.get("message") == "checkpoint_required":
            from social_crawler.db.accounts import disable_account

            disabled = disable_account(
                self.PLATFORM, self._account, reason="checkpoint_required response during text_feed replies"
            )
            logger.error(
                "account_disabled_checkpoint_suspected" if disabled else "account_checkpoint_suspected",
                telegram=True,
                platform=self.PLATFORM,
                account=self._account,
                disabled=disabled,
                response=parsed,
            )
            raise CheckpointRequiredError(
                f"Threads returned checkpoint_required for account {self._account!r} - "
                "log in as this account through a real browser to resolve the checkpoint, then re-run bootstrap.py."
            )
        if isinstance(parsed, dict) and parsed.get("status") == "fail":
            raise SessionExpiredError(f"Threads text_feed replies failed: {parsed.get('message') or parsed}")
        if not isinstance(parsed, dict):
            raise SessionExpiredError("Threads text_feed replies returned a non-object JSON body.")
        return parsed

    def _get_with_retry(self, url: str, headers: dict[str, str]):
        """Bản GET sinh đôi của CometGraphQLClient._post_with_retry - cùng backoff cho
        429/5xx/mạng, khác method/URL (text_feed không phải GraphQL)."""
        last_exc: Exception | None = None
        resp = None
        stressed = False
        transient = {429, 500, 502, 503, 504}

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, headers=headers, cookies=self._cache["cookies"], timeout=15)
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
                if resp.status_code in transient:
                    stressed = True
                    logger.warning(
                        "threads_returned_error_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                    )
                else:
                    self._adjust_interval(stressed=stressed)
                    self._record_proxy_outcome_once(success=not stressed)
                    return resp

            if attempt < self.MAX_RETRIES:
                delay = self.RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(
                    0, self.RETRY_BACKOFF_JITTER_SECONDS
                )
                logger.info("retrying", delay_seconds=round(delay, 1))
                time.sleep(delay)

        self._adjust_interval(stressed=True)
        self._record_proxy_outcome_once(success=False)

        if resp is not None and resp.status_code == 429:
            raise RateLimitedError(
                f"Threads rate-limited this request (status=429) even after {self.MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        raise NetworkError(f"Request failed after {self.MAX_RETRIES} attempts: {last_exc}") from last_exc

    def search_next_page(self, query: str, cursor: str, count: int = 10) -> dict[str, Any]:
        """Lấy trang kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Cần bootstrap.py đã bắt được một request kết quả tìm kiếm có phân
        trang - nó tự làm việc này bằng cách cuộn trang kết quả."""
        pagination = self._cache.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no pagination info (no paginated search-results query was captured). "
                "Re-run bootstrap.py, which scrolls the results page to capture one."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="pagination.variables_template",
            overrides=_search_overrides(query, cursor, count),
        )


def _search_overrides(query: str, cursor: str | None, count: int | None) -> dict[str, Any]:
    """Tên trường đã xác nhận với một request BarcelonaSearchResultsRefetchableQuery thật bắt
    được: "after" kiểu Relay cho cursor (không phải "cursor") và "first" cho kích thước trang
    (không phải "count") - cả hai đều khác với thứ query tìm kiếm của Facebook dùng."""
    overrides: dict[str, Any] = {"query": query, "after": cursor}
    if count is not None:
        overrides["first"] = count
    return overrides
