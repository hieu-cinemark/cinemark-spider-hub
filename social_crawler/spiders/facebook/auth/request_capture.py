"""
Bắt các request GraphQL bắn ra trong lúc một trigger Playwright chạy, và chọn ra request
"initial" / "paginated" trong số đó theo fb_api_req_friendly_name - Facebook đổi tên
chúng qua các lần deploy, nên ở đây khớp theo chuỗi con/từ khoá, không theo tên chính
xác.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Callable
from urllib.parse import parse_qsl

from patchright.sync_api import Request, Response

from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Facebook trả trang comment 2+ qua CommentsListComponentsPaginationQuery, thứ mà cuộn
# headless thường không bao giờ bắn (chỉ CommentListComponentsRootQuery gốc xuất hiện).
# doc_id đã lưu của nó vẫn nằm trong một chunk JS của Relay dưới dạng
# "...PaginationQuery_facebookRelayOperation":"<id>".
_COMMENTS_PAGINATION_DOC_ID_RE = re.compile(r"(CommentsListComponentsPaginationQuery\w*)[^0-9]{0,80}(\d{15,})")


def capture_graphql_requests(
    page,
    trigger,
    timeout_s: float = 25.0,
    on_response: Callable[[Response], None] | None = None,
) -> list[Request]:
    """Chạy `trigger(page)` và thu mọi request GraphQL (có doc_id) bắt được trong vòng
    `timeout_s` giây - không gắn với tên query cụ thể nào vì Facebook đổi tên chúng thường
    xuyên.

    on_response tuỳ chọn được gắn trong cùng khoảng thời gian (dùng bởi bootstrap comment để
    nhặt doc_id của PaginationQuery ra từ chunk JS của Relay khi Facebook không bao giờ thực
    sự bắn request GraphQL phân trang)."""
    captured: list[Request] = []

    def on_request(request: Request) -> None:
        if request.method != "POST" or "/api/graphql/" not in request.url:
            return
        if "doc_id=" in (request.post_data or ""):
            captured.append(request)

    page.on("request", on_request)
    if on_response is not None:
        page.on("response", on_response)
    try:
        trigger(page)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            page.wait_for_timeout(250)
    finally:
        page.remove_listener("request", on_request)
        if on_response is not None:
            page.remove_listener("response", on_response)
    return captured


def scrape_comments_pagination_doc_id(response: Response, into: dict[str, str]) -> None:
    """Nếu response này là một chunk JS định nghĩa doc_id đã lưu của
    CommentsListComponentsPaginationQuery, ghi nó vào `into` với key là tên operation Relay.
    Gọi từ handler page.on('response') là an toàn."""
    try:
        content_type = (response.headers.get("content-type") or "").lower()
        url = response.url
        if "javascript" not in content_type and not url.endswith(".js"):
            return
        if response.status != 200:
            return
        text = response.text()
    except Exception as exc:
        # Debug, không phải warning: đoạn này chạy ở mọi response chunk JS mà hook
        # page.on('response') này thấy, phần lớn hợp lệ không phải chunk đang tìm - nhưng nếu cứ
        # mãi không đọc được body ở đây thì doc_id này sẽ vĩnh viễn không được bắt mà không để lại
        # dấu vết nào, vì không có gì khác gọi đoạn này đủ phòng thủ để nhận ra.
        logger.debug("comments_pagination_scrape_failed", url=response.url, error=str(exc))
        return
    for match in _COMMENTS_PAGINATION_DOC_ID_RE.finditer(text):
        into[match.group(1)] = match.group(2)


def synthesize_comments_pagination(
    root_variables: dict[str, Any],
    *,
    doc_id: str,
    friendly_name: str = "CommentsListComponentsPaginationQuery",
) -> dict[str, Any]:
    """Dựng khối `pagination` trong Redis khi bootstrap chưa bao giờ bắt được một request
    GraphQL phân trang thật. Dạng khớp với body CommentsListComponentsPaginationQuery thật
    của Comet (đã xác nhận 2026-09-16): commentsAfterCount=-1 xin trang dày nhất mà Facebook
    chịu trả sau cursor (truyền 10/50 vẫn bị giới hạn khoảng 10)."""
    template: dict[str, Any] = {
        "commentsAfterCount": -1,
        "commentsAfterCursor": None,
        "commentsBeforeCount": None,
        "commentsBeforeCursor": None,
        # Comet thật thường gửi null ở đây kể cả khi query gốc dùng ý định REVERSE_CHRONOLOGICAL_*
        # - giữ null để pagination khớp với request của trình duyệt, không phải token sắp xếp của
        # mẫu gốc.
        "commentsIntentToken": None,
        "feedLocation": root_variables.get("feedLocation", "POST_PERMALINK_DIALOG"),
        "focusCommentID": root_variables.get("focusCommentID"),
        "scale": root_variables.get("scale", 2),
        "targetDialect": None,
        "useDefaultActor": root_variables.get("useDefaultActor", False),
        "id": root_variables.get("id"),
    }
    for key, value in root_variables.items():
        if key.startswith("__relay_internal__"):
            template[key] = value
    return {
        "doc_id": doc_id,
        "fb_api_req_friendly_name": friendly_name,
        "variables_template": template,
    }


def _variables(request: Request) -> dict:
    """`variables` GraphQL của một request bắt được ({} nếu không có hoặc không phải JSON)."""
    body = dict(parse_qsl(request.post_data or "", keep_blank_values=True))
    try:
        variables = json.loads(body.get("variables") or "{}")
    except ValueError:
        return {}
    return variables if isinstance(variables, dict) else {}


def name_requests(requests_seen: list[Request]) -> list[tuple[Request, str]]:
    named = []
    for request in requests_seen:
        body = dict(parse_qsl(request.post_data or "", keep_blank_values=True))
        named.append((request, body.get("fb_api_req_friendly_name", "")))
    return named


def pick_initial_request(named: list[tuple[Request, str]]) -> Request:
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while typing the search query. "
            "Facebook may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for request, name in named:
        lname = name.lower()
        if "initialresults" in lname and "parallelfetch" not in lname:
            return request

    for request, name in named:
        lname = name.lower()
        if "results" in lname and "parallelfetch" not in lname and "paginated" not in lname:
            logger.warning("falling_back_request_choice", reason="no_exact_initial_results_query", chosen=name)
            return request

    # Một số bản deploy Facebook hoàn toàn không có query kết quả "initial" riêng - query
    # "paginated" được dùng cho mọi trang, kể cả trang 1, chỉ là gọi với cursor=None
    # (client.search() vốn đã làm vậy qua phần ghi đè). Quay về dùng nó thay vì thất bại luôn.
    for request, name in named:
        lname = name.lower()
        if "results" in lname and "parallelfetch" not in lname and "paginated" in lname:
            logger.warning(
                "falling_back_request_choice",
                reason="only_paginated_results_query_captured",
                chosen=name,
                note="this Facebook deploy may use one query for every page - replaying it with cursor=None for page 1",
            )
            return request

    raise RuntimeError(
        "No search-results GraphQL request was captured (only saw: "
        f"{[name for _, name in named]}). Facebook may not have returned real "
        "results for this query, or its UI changed - try a more natural search "
        "phrase, or re-run with --show-browser to see what happened."
    )


def _pick_paginated(named: list[tuple[Request, str]], require: str | None = None) -> Request | None:
    """Tìm query dùng cho các trang tiếp theo (tên chứa "paginated" hoặc "pagination" - các bản
    deploy Facebook không nhất quán dùng cách viết nào), có thể yêu cầu thêm một từ khoá khác
    (ví dụ "comment") để phân biệt với query phân trang của tính năng khác."""
    for request, name in named:
        lname = name.lower()
        if ("paginated" in lname or "pagination" in lname) and (require is None or require in lname):
            return request
    return None


def pick_paginated_request(named: list[tuple[Request, str]]) -> Request | None:
    return _pick_paginated(named)


def pick_comments_request(named: list[tuple[Request, str]]) -> Request:
    """Cùng ý tưởng với pick_initial_request nhưng cho query "root" của danh sách comment -
    Facebook đặt tên có chữ 'Comment' (tên chính xác thay đổi theo bản deploy), và ta vẫn
    muốn tránh mọi biến thể ParallelFetch/làm nóng, và tránh query Pagination (đó là trang
    tiếp theo, không phải trang đầu)."""
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while opening the post. "
            "Facebook may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    # Chỉ query dùng key `id` (feedback id của bài) mới phát lại được cho bài KHÁC -
    # comet_graphql_client.get_comments ghi đè đúng biến đó (COMMENTS_ID_KEY). Chỉ có chữ
    # "comment" trong tên là chưa đủ: khi bài mở trong trình xem media, Facebook còn bắn
    # FBUnifiedVideoFeedbackRightRailWithCommentPreloadingQuery, dùng key initial_node_id cố
    # định thay vào. Bắt được ngày 2026-09-26, query đó khiến mọi job comment sau đó trả về
    # comment của chính bài dùng để bootstrap (hoặc không gì cả), bất kể hỏi post_id nào. Ưu
    # tiên query root riêng khi thấy cả hai.
    candidates = [
        (request, name)
        for request, name in named
        if "comment" in name.lower()
        and "parallelfetch" not in name.lower()
        and "pagination" not in name.lower()
        and "id" in _variables(request)
    ]
    for request, name in candidates:
        if "commentlistcomponentsroot" in name.lower() or "commentslistcomponentsroot" in name.lower():
            return request
    if candidates:
        return candidates[0][0]

    # Không bao giờ quay về một tên GraphQL không liên quan (ví dụ CSExperienceStateQuery /
    # CometLogoutHandlerQuery). Lưu cái đó làm cache comment khiến
    # _facebook_comments_cache_usable cứ là False (không có pagination) trong khi consumer vẫn
    # log saved_comments_query_cache — mọi job comment sau đó cứ bootstrap lại mãi. Lỗi rõ ràng
    # để người vận hành sửa session/proxy/giao diện ("Không thể tải đoạn chat") thay vì vậy.
    seen = [name for _, name in named]
    raise RuntimeError(
        "Did not capture a comments GraphQL query while opening the post "
        f"(saw {seen}). Facebook often shows 'Không thể tải đoạn chat' (\"Can't load chat\") when "
        "the comments panel fails under this proxy/session - refresh in a "
        "headed browser until comments load, then re-run bootstrap."
    )


def pick_paginated_comments_request(named: list[tuple[Request, str]]) -> Request | None:
    return _pick_paginated(named, require="comment")
