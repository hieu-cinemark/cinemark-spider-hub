"""
Bắt các request GraphQL bắn ra trong lúc một trigger Playwright chạy - giống
social_crawler.spiders.facebook.auth.request_capture.capture_graphql_requests, nhưng khớp
URL endpoint thật của threads.com thay vì của Facebook.

Không thể chỉ dùng lại nguyên capture_graphql_requests của Facebook: hàm đó đòi
"/api/graphql/" (có dấu gạch chéo cuối) trong URL, khớp endpoint của Facebook nhưng không
khớp của threads.com - đã xác nhận với lưu lượng thật bắt được rằng threads.com POST tới cả
"/api/graphql" (không có gạch chéo cuối) và một endpoint thứ hai, "/graphql/query", không
cái nào khớp phép kiểm tra gạch chéo cuối. Dùng nguyên bản của Facebook ở đây luôn âm thầm
bắt được 0 request, kể cả khi đăng nhập/tìm kiếm đã chạy hoàn toàn đúng - bug trông như
vấn đề xác thực nhưng không phải.

name_requests/pick_initial_request/pick_paginated_request thực sự chung (chúng chỉ nhìn
post_data của danh sách request đã lọc, không nhìn URL) và vẫn được import nguyên từ
facebook.auth.request_capture - không cần lặp lại ở đây.
"""

from __future__ import annotations

import time

from patchright.sync_api import Request

from social_crawler.logger import get_logger

logger = get_logger(__name__)

THREADS_GRAPHQL_URL_MARKERS = ("/api/graphql", "/graphql/query")

# Đã xác nhận với lưu lượng thật bắt được khi gõ một query tìm kiếm, nhấn Enter và cuộn:
# threads.com bắn vài query - "AccountSearch"/"KeywordSearch" ở mỗi phím gõ (chỉ là gợi ý
# trong dropdown - đã xác nhận bằng cách xem variables_template của một request
# KeywordSearch bắt được: chỉ có {"query", "has_communities", "has_favicons"}, hoàn toàn
# không có trường cursor/count, nên không phân trang được), và "SearchResultsRefetchableQuery"
# (quy ước đặt tên của Relay cho một connection phân trang/refetch được) sau khi Enter/cuộn -
# cái đó mới là feed kết quả đầy đủ thật và được ưu tiên ở đây. KeywordSearch chỉ được giữ
# làm phương án dự phòng phòng khi một bản deploy sau này ngừng bắn query Refetchable dưới
# đúng tên này.
_RESULTS_QUERY_NAME_MARKERS = ("searchresultsrefetchable", "keywordsearch")


def capture_graphql_requests(page, trigger, timeout_s: float = 25.0) -> list[Request]:
    """Chạy `trigger(page)` và thu mọi request GraphQL (có doc_id) bắt được trong vòng
    `timeout_s` giây - không gắn với tên query cụ thể nào vì Threads đổi tên chúng thường
    xuyên, và POST tới nhiều hơn một đường dẫn endpoint (xem docstring module)."""
    captured: list[Request] = []

    def on_request(request: Request) -> None:
        if request.method != "POST" or not any(marker in request.url for marker in THREADS_GRAPHQL_URL_MARKERS):
            return
        if "doc_id=" in (request.post_data or ""):
            captured.append(request)

    page.on("request", on_request)
    trigger(page)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        page.wait_for_timeout(250)
    page.remove_listener("request", on_request)
    return captured


def pick_initial_request(named: list[tuple[Request, str]]) -> Request:
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while typing the search query. "
            "Threads may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for marker in _RESULTS_QUERY_NAME_MARKERS:
        for request, name in named:
            if marker in name.lower():
                return request

    raise RuntimeError(
        "No search-results GraphQL request was captured (only saw: "
        f"{[name for _, name in named]}). Threads may not have returned real results for this "
        "query, or its UI changed - try a more natural search phrase, scroll further, or re-run "
        "with --show-browser to see what happened."
    )


def pick_paginated_request(named: list[tuple[Request, str]]) -> Request | None:
    """Cùng một query phục vụ cả trang đầu lẫn các trang sau (chỉ biến cursor thay đổi) - xem
    docstring của pick_initial_request."""
    for marker in _RESULTS_QUERY_NAME_MARKERS:
        for request, name in named:
            if marker in name.lower():
                return request
    return None


# Threads gọi comment của một bài là "replies" trong giao diện/thuật ngữ của nó, không phải
# "comments" như Facebook - khớp cả hai chuỗi con ở đây vì tên query GraphQL thật chỉ được
# xác nhận khi bootstrap.py --post-url đã thực sự bắt được một cái (xem docstring module này
# về việc bắt phải khớp thực tế, không được giả định).
_COMMENTS_QUERY_NAME_MARKERS = ("comment", "repl")


def pick_comments_request(named: list[tuple[Request, str]]) -> Request:
    """Cùng ý tưởng với pick_initial_request nhưng cho query "root" của danh sách reply - tránh
    mọi biến thể có phân trang (đó là trang tiếp theo, không phải trang đầu)."""
    if not named:
        raise RuntimeError(
            "Did not capture any GraphQL request while opening the post. "
            "Threads may have changed its UI, blocked the automation, or the account isn't actually logged in."
        )

    for request, name in named:
        lname = name.lower()
        if any(marker in lname for marker in _COMMENTS_QUERY_NAME_MARKERS) and "pagina" not in lname:
            return request

    # Không quay về một query bất kỳ: request cuối thường là feed đăng xuất, và lưu nó làm công
    # thức chỉ tạo ra một cache trông hợp lệ nhưng sai hoàn toàn.
    raise RuntimeError(
        "No Threads replies query was captured while opening the post (captured: "
        + ", ".join(name for _, name in named)
        + "). A cold permalink load is served the logged-out route - see browser_capture.py."
    )


def pick_paginated_comments_request(named: list[tuple[Request, str]]) -> Request | None:
    for request, name in named:
        lname = name.lower()
        if "pagina" in lname and any(marker in lname for marker in _COMMENTS_QUERY_NAME_MARKERS):
            return request

    # Threads không có biến thể phân trang đặt tên riêng cho query reply - đã xác nhận với một
    # lần bắt thật: "...DirectRepliesRefetchQuery" là query "refetchable" của Relay, vốn mang
    # biến cursor "after" riêng, và chính nó bắn lại (cùng tên) ở mỗi lần cuộn/trang tiếp theo.
    # Dùng lại nó cho phân trang thay vì báo "chưa bắt được phân trang" cho một thứ thực ra chạy
    # tốt với phần ghi đè cursor - cùng ý tưởng với phương án dự phòng
    # "only_paginated_results_query_captured" của pick_paginated_request, chỉ là tình huống
    # ngược lại (một query đảm nhận cả hai vai, tìm thấy dưới tên "initial" thay vì
    # "paginated").
    for request, name in named:
        lname = name.lower()
        if any(marker in lname for marker in _COMMENTS_QUERY_NAME_MARKERS):
            logger.warning(
                "falling_back_request_choice",
                reason="only_initial_replies_query_captured",
                chosen=name,
                note="this Threads deploy may use one refetchable query for every page - reusing it for pagination too",
            )
            return request
    return None
