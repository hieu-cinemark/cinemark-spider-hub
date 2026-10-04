"""
Biến một response tìm kiếm GraphQL thô của Threads thành các bản ghi bài phẳng, dễ phân
tích.

Khác response tìm kiếm của Facebook, một node "post" (media) của Threads không có key
`__typename` cấp cao nhất riêng (đã xác nhận với hai response thật bắt được - một query chi
tiết một bài và một query chuỗi comment: *lớp bọc* quanh danh sách bài có
`__typename: "XDTTextAppThreadView"`, nhưng object bài/media lồng bên trong thì không). Nên
bài được nhận ra theo chữ ký cấu trúc thay vào - `pk` + `code` + `caption` + `user` +
`text_post_app_info` cùng có mặt - cùng cách bền vững như
facebook/response_utils.iter_matching, chỉ là với chữ ký khác.

Hàm này duyệt *toàn bộ* cây response, nghĩa là nó cũng sẽ nhặt mọi bản xem trước
reply/trích dẫn mà một dạng response sau này nhúng trong kết quả tìm kiếm, không chỉ kết
quả cấp cao nhất - hiện tại vô hại (một comment xem trước inline vẫn là bài thật đáng giữ),
nhưng nên biết nếu kết quả có lúc trông phình hơn so với giao diện hiển thị. Đường dẫn
trường được dịch ngược từ các response thật bắt được (xem auth/bootstrap.py) - Threads có
thể đổi dạng response ở bất kỳ lần deploy nào, nên cần kiểm tra lại với một response mới
nếu việc trích xuất bắt đầu trả về rỗng.
"""

from __future__ import annotations

from typing import Any, Iterator

from social_crawler.spiders.facebook.response_utils import get_path, iter_matching

POST_NODE_SIGNATURE = {"pk", "code", "caption", "user", "text_post_app_info"}


def iter_post_nodes(node: Any) -> Iterator[dict]:
    """Duyệt một response GraphQL đã parse, yield mọi dict trông như một node bài (media) của
    Threads."""
    return iter_matching(node, lambda n: POST_NODE_SIGNATURE <= n.keys())


def _image_url(media: dict[str, Any]) -> str | None:
    candidates = get_path(media, "image_versions2", "candidates") or []
    if candidates:
        return candidates[0].get("url")
    return None


def _video_url(media: dict[str, Any]) -> str | None:
    video_versions = media.get("video_versions") or []
    if video_versions:
        return video_versions[0].get("url")
    return None


def _media_url(media: dict[str, Any]) -> str | None:
    """Ưu tiên ảnh tĩnh (thumbnail cho dashboard) hơn mp4 phát được. Video vẫn có image_versions2
    trong mọi response Threads đã bắt."""
    return _image_url(media) or _video_url(media)


def _extract_quoted_post(media: dict[str, Any]) -> dict[str, Any] | None:
    """Trích dẫn/repost-kèm-text của Threads nằm ở text_post_app_info.share_info."""
    share_info = get_path(media, "text_post_app_info", "share_info") or {}
    nested = share_info.get("quoted_post") or share_info.get("reposted_post")
    if not isinstance(nested, dict) or not nested:
        return None
    user = nested.get("user") or {}
    username = user.get("username")
    code = nested.get("code")
    author = user.get("full_name") or username
    content = get_path(nested, "caption", "text")
    url = f"https://www.threads.com/@{username}/post/{code}" if username and code else None
    media_url = _image_url(nested) or _media_url(nested)
    if not any((author, content, url, media_url)):
        return None
    return {"author": author, "content": content, "url": url, "media_url": media_url}


def extract_post(media: dict[str, Any]) -> dict[str, Any]:
    user = media.get("user") or {}
    app_info = media.get("text_post_app_info") or {}
    tag_header = app_info.get("tag_header") or {}
    username = user.get("username")
    code = media.get("code")

    return {
        "post_id": media.get("pk"),
        "code": code,
        "url": f"https://www.threads.com/@{username}/post/{code}" if username and code else None,
        "message": get_path(media, "caption", "text"),
        "timestamp": media.get("taken_at"),
        "author_name": user.get("full_name"),
        "author_username": username,
        "author_id": user.get("pk"),
        "author_url": f"https://www.threads.com/@{username}" if username else None,
        "topic": tag_header.get("display_name"),
        "is_reply": app_info.get("is_reply"),
        "like_count": media.get("like_count"),
        "reply_count": app_info.get("direct_reply_count"),
        "repost_count": app_info.get("repost_count"),
        "quote_count": app_info.get("quote_count"),
        "media_type": media.get("media_type"),
        "media_url": _media_url(media),
        "cover_url": _image_url(media),
        "quoted": _extract_quoted_post(media),
    }


def extract_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Trích bài từ một response tìm kiếm thô, khử trùng theo id. Cùng một id có thể xuất hiện
    nhiều lần ở các độ sâu mở rộng khác nhau (một tham chiếu trơn cạnh một node đầy đủ) - giữ
    bản nào có nhiều trường nhất, cùng lý do như extract_response trong
    facebook/features/search/extract.py."""
    richest_by_id: dict[str, dict[str, Any]] = {}
    for node in iter_post_nodes(response):
        post_id = node.get("pk")
        if not post_id:
            continue
        if post_id not in richest_by_id or len(node) > len(richest_by_id[post_id]):
            richest_by_id[post_id] = node

    return [extract_post(node) for node in richest_by_id.values()]
