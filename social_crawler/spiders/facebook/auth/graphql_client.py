"""
Client HTTP thường (không trình duyệt) gọi lại endpoint GraphQL của Facebook bằng
token/doc_id mà bootstrap.py đã cache trong Redis.

Dùng curl_cffi để giả dấu vân tay TLS/JA3 của Chrome thật - requests/httpx thường dễ bị
Facebook gắn cờ là bot qua bước bắt tay TLS.

Mọi thứ không riêng của Facebook (thiết lập session, bóp nhịp, thử lại/backoff, tạo biến
theo mẫu, parse response) nằm trong CometGraphQLClient (xem docstring module của
spiders/comet_graphql_client.py) - file này chỉ thêm những gì thực sự riêng của
Facebook: query tìm kiếm có lọc theo ngày và tính năng comment (Threads không có cả hai).
"""

from __future__ import annotations

import base64
import json
from datetime import date
from typing import Any

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REPLIES_REDIS_KEY_TMPL,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.spiders.comet_graphql_client import (
    CheckpointRequiredError,
    CometGraphQLClient,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    find_page_info,
)

__all__ = [
    "FacebookGraphQLClient",
    "SessionExpiredError",
    "RateLimitedError",
    "NetworkError",
    "CheckpointRequiredError",
    "find_page_info",
]


class FacebookGraphQLClient(CometGraphQLClient):
    PLATFORM = "facebook"
    REFERER_URL = "https://www.facebook.com/"
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
    REPLIES_REDIS_KEY_TMPL = REPLIES_REDIS_KEY_TMPL

    def search(
        self,
        query: str,
        count: int = 5,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Lấy trang kết quả tìm kiếm đầu tiên. Truyền start_date/end_date (phải đi cùng nhau) để
        dùng bộ lọc tìm kiếm "Ngày đăng" của Facebook và chỉ lấy bài tạo trong khoảng đó."""
        return self._run(
            doc_id=self._cache["doc_id"],
            friendly_name=self._cache["fb_api_req_friendly_name"],
            template=self._cache.get("variables_template"),
            template_source="variables_template",
            overrides=_search_overrides(query, None, count, start_date, end_date),
        )

    def search_next_page(
        self,
        query: str,
        cursor: str,
        count: int = 5,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Lấy trang kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Cần bootstrap.py đã bắt được một request
        SearchCometResultsPaginatedResultsQuery - nó tự làm việc này bằng cách cuộn trang kết
        quả. Truyền đúng start_date/end_date đã dùng ở trang đầu để bộ lọc ngày được giữ qua các
        trang."""
        pagination = self._cache.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no pagination info (no SearchCometResultsPaginatedResultsQuery "
                "was captured). Re-run bootstrap.py, which scrolls the results page to capture one."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="pagination.variables_template",
            overrides=_search_overrides(query, cursor, count, start_date, end_date),
        )

    def _comment_target_id(self, post_id: str) -> str:
        return _feedback_id(post_id)

    def _reply_target_id(self, legacy_comment_id: str) -> str:
        # Chưa được xác nhận độc lập với một request reply thật bắt được (xem docstring của
        # CometGraphQLClient._reply_target_id) - giả định giống hệt cách dựng feedback id của bài,
        # vì chuỗi reply của một comment tự nó là một object feedback trong đồ thị object của
        # Facebook. Sửa lại nếu một lần bắt thật bằng bootstrap --type replies cho thấy cách khác.
        return _feedback_id(legacy_comment_id)


def _search_overrides(
    query: str,
    cursor: str | None,
    count: int | None,
    start_date: date | None,
    end_date: date | None,
) -> dict[str, Any]:
    """Dùng chung cho search() và search_next_page() - khác biệt duy nhất giữa trang đầu và
    trang tiếp theo là cursor."""
    overrides: dict[str, Any] = {"text": query, "cursor": cursor}
    if count is not None:
        overrides["count"] = count
    if start_date and end_date:
        overrides["filters"] = _build_date_filters(start_date, end_date)
    return overrides


def _build_date_filters(start_date: date, end_date: date) -> list[str]:
    """Dựng phần ghi đè `filters` cho bộ lọc tìm kiếm "Ngày đăng" của Facebook, để chỉ lấy bài
    tạo giữa start_date và end_date (tính cả hai đầu). Định dạng đã xác nhận với một request
    thật bắt được (bấm chọn một năm trong bộ lọc ngày của giao diện tìm kiếm), không phải
    đoán: tháng/ngày là chuỗi không đệm số 0 "YYYY-M"/"YYYY-M-D", và cả bộ lọc được mã hoá
    JSON hai lần - Facebook lưu các tham số ngày bên trong dưới dạng *chuỗi* JSON, không phải
    object lồng, bên trong object bộ lọc ngoài, mà bản thân object đó cũng là một chuỗi JSON
    bên trong danh sách `filters` (không phải object)."""
    inner_args = {
        "start_year": str(start_date.year),
        "start_month": f"{start_date.year}-{start_date.month}",
        "start_day": f"{start_date.year}-{start_date.month}-{start_date.day}",
        "end_year": str(end_date.year),
        "end_month": f"{end_date.year}-{end_date.month}",
        "end_day": f"{end_date.year}-{end_date.month}-{end_date.day}",
    }
    filter_obj = {"name": "creation_time", "args": json.dumps(inner_args, separators=(",", ":"))}
    return [json.dumps(filter_obj, separators=(",", ":"))]


def _feedback_id(post_id: str) -> str:
    """Các query danh sách comment của Facebook định địa chỉ một bài bằng feedback id của nó,
    chính là base64("feedback:<post_id>") - đã xác nhận với một request thật bắt được chứ
    không phải giả định."""
    return base64.b64encode(f"feedback:{post_id}".encode()).decode()
