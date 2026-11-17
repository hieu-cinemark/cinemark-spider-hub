"""Lỗi, mã lỗi Facebook/Threads và nhận diện checkpoint của client GraphQL Comet.

Tách từ comet_graphql_client.py (17/11/2026); file đó vẫn re-export mọi tên.
"""

from __future__ import annotations

from typing import Any

from curl_cffi import requests as curl_requests

from social_crawler.core.logger import get_logger

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
    """Biến truy vấn không còn khớp schema của server; cần sửa code."""


class GraphQLDocumentError(RuntimeError):
    """Persisted query không còn; dò lại công khai cũng không tìm được."""


class GraphQLResponseError(RuntimeError):
    """GraphQL báo lỗi; không bao giờ coi là một trang rỗng thành công."""


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
