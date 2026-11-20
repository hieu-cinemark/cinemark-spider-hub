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
import uuid
from datetime import date
from typing import Any

from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    GRAPHQL_URL,
    LONG_PAUSE_CHANCE,
    LONG_PAUSE_MAX_SECONDS,
    LONG_PAUSE_MIN_SECONDS,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REPLIES_REDIS_KEY_TMPL,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.spiders.common.comet_errors import (
    CheckpointRequiredError,
    NetworkError,
    RateLimitedError,
    RequestBudgetExceeded,
    SessionExpiredError,
)
from social_crawler.spiders.common.comet_graphql_client import CometGraphQLClient
from social_crawler.spiders.common.comet_parse import find_page_info

__all__ = [
    "CheckpointRequiredError",
    "FacebookGraphQLClient",
    "NetworkError",
    "RateLimitedError",
    "RequestBudgetExceeded",
    "SessionExpiredError",
    "find_page_info",
]


# Cờ relay provider Facebook thêm sau khi thư viện facebook_gql_scraper chốt bản mẫu. So bản mẫu tìm kiếm với request thật của
# trình duyệt (SearchCometResultsInitialResultsQuery, bắt 19/11/2026) chỉ lệch đúng 3 cờ này -> Facebook trả lỗi 1675012
# `missing_required_variable_value` và mọi lượt facebook_search chết ở request đầu (mã thoát 91). Chỉ bổ sung biến còn thiếu,
# không đè giá trị đã có. Áp lúc gửi nên recipe đã lưu trong Redis cũng được vá. Khi lỗi này quay lại: bắt một request tìm
# kiếm thật (DevTools > Network > graphql, lấy `variables`) rồi so các khoá `__relay_internal__pv__*` với bản mẫu của thư viện.
RELAY_PROVIDER_ADDITIONS: dict[str, Any] = {
    "__relay_internal__pv__SearchCometRelatedSearchThumbnailDesignrelayprovider": True,
    "__relay_internal__pv__CometUFICommentActionLinkStableIDsrelayprovider": False,
    "__relay_internal__pv__GeminiUFISaveToKnowledgeHubGKrelayprovider": False,
}


def complete_relay_providers(template: dict[str, Any]) -> dict[str, Any]:
    """Bản mẫu kèm các cờ relay provider còn thiếu (không sửa bản gốc, không đè cờ đã có)."""
    return {**RELAY_PROVIDER_ADDITIONS, **template}


class FacebookGraphQLClient(CometGraphQLClient):
    """Client GraphQL Facebook không trình duyệt: phát lại request đã bootstrap qua curl_cffi với token/doc_id lưu
    trong Redis, kèm khoảng nghỉ ngẫu nhiên, thử lại có backoff, hạn mức request và phát hiện checkpoint/phiên hết
    hạn (kế thừa `CometGraphQLClient`). Các method nghiệp vụ: tìm kiếm, bình luận, reply, bảng tin Page, reels."""

    PLATFORM = "facebook"
    REFERER_URL = "https://www.facebook.com/"
    CACHE_REDIS_KEY_TMPL = CACHE_REDIS_KEY_TMPL
    ACTIVE_ACCOUNT_REDIS_KEY = ACTIVE_ACCOUNT_REDIS_KEY
    DEFAULT_ACCOUNT_KEY = DEFAULT_ACCOUNT_KEY
    GRAPHQL_URL = GRAPHQL_URL
    MAX_RETRIES = MAX_RETRIES
    MIN_REQUEST_INTERVAL_SECONDS = MIN_REQUEST_INTERVAL_SECONDS
    REQUEST_INTERVAL_JITTER_SECONDS = REQUEST_INTERVAL_JITTER_SECONDS
    LONG_PAUSE_CHANCE = LONG_PAUSE_CHANCE
    LONG_PAUSE_MIN_SECONDS = LONG_PAUSE_MIN_SECONDS
    LONG_PAUSE_MAX_SECONDS = LONG_PAUSE_MAX_SECONDS
    RETRY_BACKOFF_BASE_SECONDS = RETRY_BACKOFF_BASE_SECONDS
    RETRY_BACKOFF_JITTER_SECONDS = RETRY_BACKOFF_JITTER_SECONDS
    THROTTLE_REDIS_KEY_TMPL = THROTTLE_REDIS_KEY_TMPL
    ADAPTIVE_INTERVAL_MAX_SECONDS = ADAPTIVE_INTERVAL_MAX_SECONDS
    COMMENTS_REDIS_KEY_TMPL = COMMENTS_REDIS_KEY_TMPL
    REPLIES_REDIS_KEY_TMPL = REPLIES_REDIS_KEY_TMPL

    def _complete_template(self, template: dict[str, Any]) -> dict[str, Any]:
        return complete_relay_providers(template)

    def search(
        self,
        query: str,
        count: int = 5,
        start_date: date | None = None,
        end_date: date | None = None,
        recent_post: bool = False,
        experience: str | None = None,
    ) -> dict[str, Any]:
        """Lấy trang kết quả tìm kiếm đầu tiên. Truyền start_date/end_date (phải đi cùng nhau) để
        dùng bộ lọc tìm kiếm "Ngày đăng" của Facebook và chỉ lấy bài tạo trong khoảng đó."""
        self._require_search_recipe()
        # Mỗi lượt tìm một bsid (phiên tìm kiếm) mới như trình duyệt; các trang sau của lượt đó dùng lại. Trước 19/10/2026
        # mọi lượt tìm trong 6 giờ giữa hai lần bootstrap dùng chung đúng bsid đã bắt - một dấu hiệu bot dễ thấy.
        self._search_bsid = str(uuid.uuid4())
        return self._run(
            doc_id=self._cache["doc_id"],
            friendly_name=self._cache["fb_api_req_friendly_name"],
            template=self._cache.get("variables_template"),
            template_source="variables_template",
            overrides={
                **_search_overrides(query, None, count, start_date, end_date, recent_post),
                "bsid": self._search_bsid,
                **({"type": experience} if experience else {}),
            },
        )

    def search_next_page(
        self,
        query: str,
        cursor: str,
        count: int = 5,
        start_date: date | None = None,
        end_date: date | None = None,
        recent_post: bool = False,
        experience: str | None = None,
    ) -> dict[str, Any]:
        """Lấy trang kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Cần bootstrap.py đã bắt được một request
        SearchCometResultsPaginatedResultsQuery - nó tự làm việc này bằng cách cuộn trang kết
        quả. Truyền đúng start_date/end_date đã dùng ở trang đầu để bộ lọc ngày được giữ qua các
        trang."""
        pagination = self._cache.get("pagination")
        if pagination is None:
            from social_crawler.spiders.facebook.auth.sdk_fallback import ensure_recipe

            if ensure_recipe(self, "search"):
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
            overrides={
                **_search_overrides(query, cursor, count, start_date, end_date, recent_post),
                **({"bsid": self._search_bsid} if getattr(self, "_search_bsid", None) else {}),
                **({"type": experience} if experience else {}),
            },
        )

    def _html_headers(self) -> dict[str, str]:
        """GET trang HTML: cùng User-Agent / sec-ch với phiên đã bắt (24/10/2026 - trước đây không gửi header nào, lệch
        dấu vết với các request GraphQL của cùng phiên)."""
        captured = {k.lower(): v for k, v in (self._cache.get("headers") or {}).items()}
        headers = {
            k: v for k, v in captured.items() if k in ("user-agent", "accept-language") or k.startswith("sec-ch-ua")
        }
        # Trang chủ Facebook phải xin dạng HTML, không dùng Accept */* của GraphQL.
        # Thiếu nó thì transport requests có thể nhận HTTP 400 thay vì HTML chứa token.
        headers["accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        return headers

    def _comment_target_id(self, post_id: str) -> str:
        return _feedback_id(post_id)

    def _get_comments_cache(self):
        from social_crawler.spiders.facebook.auth.sdk_fallback import ensure_recipe

        if self._redis.get(COMMENTS_REDIS_KEY_TMPL.format(account=self._account)) is None:
            ensure_recipe(self, "comments")
        return super()._get_comments_cache()

    def _depth_replies(
        self,
        comment_id: str,
        *,
        post_id: str,
        expansion_token: str | None,
        cursor: str | None = None,
        count: int = -1,
        depth: int = 1,
    ):
        from social_crawler.spiders.common.comet_errors import GraphQLDocumentError
        from social_crawler.spiders.facebook.auth.http_refresh import PUBLIC_DOCS_KEY

        if depth not in (1, 2):
            raise ValueError("Reply depth must be 1 or 2")
        name = f"Depth{depth}CommentsListPaginationQuery"
        cached = self._redis.get(REPLIES_REDIS_KEY_TMPL.format(account=self._account)) or {}
        recipe = cached.get("reply_operations", {}).get(name) or cached.get("pagination") or cached
        public = self._redis.get(PUBLIC_DOCS_KEY) or {}
        doc_id = public.get("operations", {}).get(name)
        if recipe.get("fb_api_req_friendly_name") == name:
            doc_id = doc_id or recipe.get("doc_id")
        if not doc_id:
            from social_crawler.spiders.facebook.auth.sdk_fallback import discover_authenticated

            doc_id = discover_authenticated(self, {name}).get(name)
        if not doc_id:
            raise GraphQLDocumentError(f"No discovered {name}; do not use a guessed reply query")
        prefix = "replies" if depth == 1 else "subReplies"
        # Chuẩn hoá ID cũ dạng ghép trước khi nối với ID bài.
        comment_id = str(comment_id).removeprefix(f"{post_id}_")
        variables = {
            "clientKey": None,
            "expansionToken": expansion_token,
            "feedLocation": "POST_PERMALINK_DIALOG",
            "id": _feedback_id(f"{post_id}_{comment_id}"),
            "scale": (recipe.get("variables_template") or self._cache.get("variables_template") or {}).get("scale", 2),
            "useDefaultActor": False,
            f"{prefix}AfterCount": count if cursor is not None else None,
            f"{prefix}AfterCursor": cursor,
            f"{prefix}BeforeCount": None,
            f"{prefix}BeforeCursor": None,
            "__relay_internal__pv__CometUFICommentAutoTranslationTyperelayprovider": "ORIGINAL",
            "__relay_internal__pv__CometUFICommentAvatarStickerAnimatedImagerelayprovider": False,
            "__relay_internal__pv__CometUFICommentActionLinksRewriteEnabledrelayprovider": True,
            "__relay_internal__pv__IsWorkUserrelayprovider": False,
        }
        if recipe.get("fb_api_req_friendly_name") == name:
            dynamic = {
                key: variables[key]
                for key in (
                    "id",
                    "expansionToken",
                    f"{prefix}AfterCount",
                    f"{prefix}AfterCursor",
                    f"{prefix}BeforeCount",
                    f"{prefix}BeforeCursor",
                )
            }
            variables = {**variables, **(recipe.get("variables_template") or {}), **dynamic}
        if depth == 1:
            variables["focusCommentID"] = comment_id
        result = self._run(doc_id=doc_id, friendly_name=name, template=variables, template_source=name, overrides={})
        from social_crawler.spiders.common.comet_errors import GraphQLSchemaError
        from social_crawler.spiders.facebook.features.comments.extract import _replies_connection

        if "edges" not in _replies_connection(result):
            raise GraphQLSchemaError(f"Unexpected reply response structure for {name}")
        return result

    def get_replies(self, legacy_comment_id: str, *, post_id: str, expansion_token: str | None = None, depth: int = 1):
        return self._depth_replies(legacy_comment_id, post_id=post_id, expansion_token=expansion_token, depth=depth)

    def get_replies_next_page(
        self,
        legacy_comment_id: str,
        cursor: str,
        count: int = -1,
        *,
        post_id: str,
        expansion_token: str | None = None,
        depth: int = 1,
    ):
        return self._depth_replies(
            legacy_comment_id, post_id=post_id, expansion_token=expansion_token, cursor=cursor, count=count, depth=depth
        )


def _search_overrides(
    query: str,
    cursor: str | None,
    count: int | None,
    start_date: date | None,
    end_date: date | None,
    recent_post: bool = False,
) -> dict[str, Any]:
    """Dùng chung cho search() và search_next_page() - khác biệt duy nhất giữa trang đầu và
    trang tiếp theo là cursor."""
    overrides: dict[str, Any] = {"text": query, "cursor": cursor}
    if count is not None:
        overrides["count"] = count
    filters: list[str] = []
    if start_date and end_date:
        filters += _build_date_filters(start_date, end_date)
    if recent_post:
        # Bộ lọc "Bài viết gần đây" (chỉ có khi đã đăng nhập) - kết quả xếp mới nhất trước. Kết hợp
        # được với bộ lọc ngày (đã thử thực tế 2026-10-14: cả hai cùng lúc vẫn trả kết quả).
        filters.append(json.dumps({"name": "recent_posts", "args": ""}, separators=(",", ":")))
    overrides["filters"] = filters
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
