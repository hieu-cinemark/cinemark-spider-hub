"""Hàm thuần của client GraphQL Comet: parse phản hồi (kể cả luồng), gộp dữ liệu trả dần, dựng biến theo mẫu, chuẩn hoá vân tay phiên.

Tách từ comet_graphql_client.py (17/11/2026); file đó vẫn re-export mọi tên.
"""

from __future__ import annotations

import copy
import json
import uuid
from collections.abc import Callable
from typing import Any

from social_crawler.core.fingerprint import (
    ACCEPT_LANGUAGE,
    resolve_profile,
)
from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

from social_crawler.spiders.common.comet_errors import GraphQLResponseError, SessionExpiredError


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
