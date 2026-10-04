"""
Biến một response GraphQL danh sách comment thô của Facebook thành các bản ghi phẳng, mỗi
comment một bản ghi. Đường dẫn trường được dịch ngược từ một response thật bắt được (xem
graphql_client.get_comments / bootstrap_comments). Cả comment cấp một lẫn reply của chúng
giờ đều phân trang bình thường (lần lượt là vòng start() và _fetch_replies của
comments.py) - mỗi loại cần query phân trang riêng bắt một lần qua bootstrap.py
(--post-url trơn cho comment, --post-url ... --type replies cho reply), vì Facebook trả
lần tải "initial" và "paginated" bằng hai query riêng ở đây (khác Threads, nơi một query
refetch đảm nhận cả hai - xem docstring get_comments trong comet_graphql_client.py).
"""

from __future__ import annotations

import base64
from collections import deque
from datetime import datetime, timezone
from typing import Any

from social_crawler.spiders.facebook.response_utils import get_path


def extract_comments(response: dict[str, Any]) -> list[dict[str, Any]]:
    edges = _comments_connection(response).get("edges") or []
    return [extract_comment(edge.get("node") or {}) for edge in edges]


def find_comments_page_info(response: dict[str, Any]) -> dict[str, Any] | None:
    """`page_info` riêng của connection comment (has_next_page/end_cursor) cho trang comment KẾ
    TIẾP. Cố ý không dùng tìm kiếm đệ quy chung như graphql_client.find_page_info: mỗi comment
    còn mang `feedback.replies_connection.page_info` riêng cho chuỗi reply của nó, và những
    cái đó bị gặp trước trong lượt duyệt theo chiều sâu (edges đứng trước page_info trong
    response) - luôn báo has_next_page=False vì không comment mẫu nào có reply, kể cả khi bản
    thân danh sách comment rõ ràng còn trang nữa."""
    return _comments_connection(response).get("page_info")


def _comments_connection(response: dict[str, Any]) -> dict[str, Any]:
    """Danh sách comment của bài. Query comment của bài thường đặt nó ở
    data.node.comment_rendering_instance_for_feed_location.comments; query của video/reel
    (FBUnifiedVideoFeedbackRightRailWithCommentPreloadingQuery, đã xác nhận 2026-09-28) lồng
    cùng object đó dưới
    data.video.creation_story.reels_feedback_renderer.story.feedback.comment_list_renderer.feedback
    - nên quay về comment_rendering_instance_for_feed_location nông nhất ở bất cứ đâu. Nông
    nhất, vì mỗi node comment mang comment_rendering_instance riêng (tên khác) cho reply của
    nó sâu hơn bên dưới."""
    direct = get_path(response, "data", "node", "comment_rendering_instance_for_feed_location", "comments")
    if direct:
        return direct
    found = _shallowest(response, "comment_rendering_instance_for_feed_location")
    return (found or {}).get("comments") or {} if isinstance(found, dict) else {}


def _shallowest(root: Any, key: str) -> Any:
    """Tìm theo chiều rộng giá trị dict đầu tiên lưu dưới `key`."""
    queue: deque[Any] = deque([root])
    while queue:
        node = queue.popleft()
        if isinstance(node, dict):
            if isinstance(node.get(key), dict):
                return node[key]
            queue.extend(node.values())
        elif isinstance(node, list):
            queue.extend(node)
    return None


def comment_post_id(comment_id: str | None) -> str | None:
    """Bài mà một comment thuộc về, giải mã từ id GraphQL của nó (base64 của
    "comment:<post id>_<comment id>"). None nếu không có dạng đó."""
    if not comment_id:
        return None
    try:
        decoded = base64.b64decode(comment_id + "=" * (-len(comment_id) % 4)).decode("utf-8")
    except ValueError, UnicodeDecodeError:
        return None
    prefix, sep, rest = decoded.partition(":")
    if prefix != "comment" or not sep or "_" not in rest:
        return None
    return rest.split("_", 1)[0]


def extract_replies(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Giống extract_comments, nhưng cho response của get_replies()/get_replies_next_page() -
    dùng lại nguyên extract_comment() vì node reply có dạng giống comment."""
    edges = _replies_connection(response).get("edges") or []
    return [extract_comment(edge.get("node") or {}) for edge in edges]


def find_replies_page_info(response: dict[str, Any]) -> dict[str, Any] | None:
    """`page_info` riêng của connection reply cho trang reply KẾ TIẾP của một comment. Xem
    docstring của find_comments_page_info ở trên để biết vì sao không thể chỉ dùng tìm kiếm
    đệ quy chung của graphql_client.find_page_info."""
    return _replies_connection(response).get("page_info")


def _replies_connection(response: dict[str, Any]) -> dict[str, Any]:
    # CHƯA được xác nhận với một response reply thật bắt được - phỏng đoán tốt nhất dựa trên
    # docstring của find_comments_page_info, vốn ghi rằng mỗi comment cấp một mang một
    # `feedback.replies_connection` cho chuỗi reply của nó. Một lời gọi get_replies() đặt lại
    # gốc query ở comment (qua _reply_target_id), nên thử cả dạng tương đương connection comment
    # lẫn một replies_connection cấp cao nhất trơn trước khi bỏ cuộc. Sửa lại khi một lần bắt
    # bootstrap `--type replies` thật cho thấy đường dẫn thật.
    return (
        get_path(response, "data", "node", "feedback", "replies_connection")
        or get_path(response, "data", "node", "replies_connection")
        or {}
    )


def extract_comment(node: dict[str, Any]) -> dict[str, Any]:
    author = node.get("author") or {}
    created_time = node.get("created_time")

    return {
        "comment_id": node.get("id"),
        "legacy_comment_id": node.get("legacy_fbid"),
        "message": get_path(node, "body", "text"),
        "date": _format_date(created_time),
        "timestamp": created_time,
        "author_name": author.get("name"),
        "author_id": author.get("id"),
        "author_url": author.get("url"),
        "author_gender": author.get("gender"),
        "author_profile_picture": get_path(author, "profile_picture_depth_0", "uri"),
        "replies_count": get_path(node, "feedback", "replies_fields", "total_count"),
        "reactions_count": _find_reactors_count(node),
        **_extract_attachment(node),
    }


def _format_date(timestamp: int | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _find_reactors_count(node: dict[str, Any]) -> int | None:
    """Số like nằm trên một node Feedback theo từng comment, chỉ tới được qua một trong các link
    hành động của comment (Like/Dislike/Reply/...) - tất cả đều tham chiếu cùng object
    feedback bên dưới, nên cái đầu tiên có nó là đủ."""
    for link in node.get("comment_action_links") or []:
        count = get_path(link, "comment", "feedback", "reactors", "count")
        if isinstance(count, int):
            return count
    return None


def _extract_attachment(node: dict[str, Any]) -> dict[str, Any]:
    """Một comment có thể mang tối đa một tệp đính kèm (sticker, chia sẻ GIF, ảnh, video...) dưới
    attachments[0].style_type_renderer.attachment.media. Dạng sticker và GIF bên dưới đã xác
    nhận với response thật; ảnh/video dùng cùng tên trường mà Facebook dùng cho tệp đính kèm
    của bài ở chỗ khác (Photo -> photo_image.uri, Video -> permalink_url) vì không có comment
    nào đính kèm những loại đó trong mẫu bắt được - nên kiểm tra lại với một cái thật nếu phần
    này bắt đầu trả về rỗng."""
    empty = {
        "is_sticker": False,
        "sticker_url": None,
        "is_gif": False,
        "gif": None,
        "image": None,
        "video": None,
    }

    attachments = node.get("attachments") or []
    if not attachments:
        return empty

    attachment = attachments[0]
    style_list = attachment.get("style_list") or []
    media = get_path(attachment, "style_type_renderer", "attachment", "media") or {}
    media_type = media.get("__typename")

    is_sticker = media_type == "Sticker"
    is_gif = "animated_image_share" in style_list or get_path(media, "animated_image", "uri") is not None

    return {
        "is_sticker": is_sticker,
        "sticker_url": get_path(media, "image", "uri") if is_sticker else None,
        "is_gif": is_gif,
        "gif": get_path(media, "animated_image", "uri") if is_gif else None,
        "image": get_path(media, "photo_image", "uri") if media_type == "Photo" else None,
        "video": media.get("permalink_url") if media_type == "Video" else None,
    }
