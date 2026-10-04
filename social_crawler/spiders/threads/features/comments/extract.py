"""
Trích reply Threads từ hai dạng response:

- GET /api/v1/text_feed/<id>/replies/ (extract_replies_from_text_feed) - thứ comments.py
  thực sự crawl.
- JSON DirectRepliesRefetchQuery bắt bằng trình duyệt (extract_replies_from_json) - phát
  lại GraphQL của query đó trả direct_replies: null; giữ lại để body bắt bằng trình duyệt
  vẫn parse được.
"""

from __future__ import annotations

from typing import Any


def _iter_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_dicts(item)


def find_direct_replies_in_json(obj: Any) -> dict[str, Any] | None:
    """Dạng lớp bọc chính xác của một response bắt được chưa được xác nhận độc lập với một lần bắt
    thật, nên hàm này duyệt cả cây tìm bất kỳ connection "direct_replies" nào thay vì giả định
    một đường dẫn `data.media.text_post_app_info` cố định."""
    for d in _iter_dicts(obj):
        candidate = d.get("direct_replies")
        if isinstance(candidate, dict) and isinstance(candidate.get("edges"), list):
            return candidate
    return None


def _extract_reply_thread(edge: dict[str, Any]) -> list[dict[str, Any]]:
    """Mỗi edge `direct_replies` bọc một connection `posts` - một chuỗi tự trả lời của reply cấp
    một cộng mọi reply lồng inline. Item 0 trả lời bài; cha của mỗi item sau là reply_id trước
    đó."""
    node = edge.get("node") or {}
    post_edges = (node.get("posts") or {}).get("edges") or []
    replies: list[dict[str, Any]] = []
    prev_id: str | None = None
    for post_edge in post_edges:
        extracted = _extract_post_as_reply((post_edge or {}).get("node") or {}, parent_reply_id=prev_id)
        if not extracted:
            continue
        replies.append(extracted)
        prev_id = extracted["reply_id"]
    return replies


def extract_replies_from_json(obj: Any) -> list[dict[str, Any]]:
    """Mọi reply trong một response DirectRepliesRefetchQuery bắt bằng trình duyệt - xem
    capture_reply_pages(), nơi gọi hàm này mỗi trang một lần."""
    direct_replies = find_direct_replies_in_json(obj)
    if not direct_replies:
        return []
    replies: list[dict[str, Any]] = []
    for edge in direct_replies.get("edges") or []:
        replies.extend(_extract_reply_thread(edge))
    return replies


def _caption_text(caption: Any) -> str | None:
    if isinstance(caption, dict):
        return caption.get("text")
    if isinstance(caption, str):
        return caption
    return None


def _extract_post_as_reply(post: dict[str, Any], *, parent_reply_id: str | None = None) -> dict[str, Any] | None:
    """Bản ghi reply phẳng từ một object media dạng Instagram (`post` của REST text_feed / node
    bài GraphQL). `parent_reply_id` là id nền tảng của comment mà reply này trả lời (None =
    trả lời trực tiếp bài)."""
    reply_id = post.get("pk") or post.get("id")
    if not reply_id:
        return None
    user = post.get("user") or {}
    app_info = post.get("text_post_app_info") or {}
    return {
        "reply_id": str(reply_id),
        "parent_reply_id": parent_reply_id,
        "message": _caption_text(post.get("caption")),
        "timestamp": post.get("taken_at"),
        "author_id": user.get("pk") or user.get("id"),
        "author_username": user.get("username"),
        "author_name": user.get("full_name"),
        "author_profile_picture": user.get("profile_pic_url"),
        "like_count": post.get("like_count"),
        "reply_count": app_info.get("direct_reply_count"),
        "code": post.get("code"),
    }


def _thread_item_post(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    post = item.get("post")
    if isinstance(post, dict):
        return post
    if "pk" in item or "code" in item:
        return item
    return None


def _root_post_id(response: dict[str, Any]) -> str | None:
    raw = response.get("target_post_id")
    if raw:
        return str(raw)
    containing = response.get("containing_thread") or {}
    items = containing.get("thread_items") or []
    if not items:
        return None
    post = _thread_item_post(items[-1])
    if not post:
        return None
    pk = post.get("pk") or post.get("id")
    return str(pk) if pk else None


def _declared_parent_id(item: dict[str, Any], post: dict[str, Any]) -> str | None:
    app_info = post.get("text_post_app_info") or {}
    for raw in (item.get("parent_reply_id"), post.get("parent_reply_id"), app_info.get("parent_reply_id")):
        if raw:
            return str(raw)
    return None


def extract_replies_from_text_feed(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Mọi reply trong một trang GET /api/v1/text_feed/<id>/replies/.

    Mỗi mục reply_threads[] là một chuỗi: item 0 là reply trực tiếp vào bài, các item sau là
    reply lồng đã được inline. parent_reply_id là item trước đó (hoặc parent_reply_id rõ ràng
    trên payload), không bao giờ là id bài gốc - giao diện nối comment với comment, không nối
    với bài."""
    root_id = _root_post_id(response)
    replies: list[dict[str, Any]] = []
    for thread in response.get("reply_threads") or []:
        if not isinstance(thread, dict):
            continue
        prev_id: str | None = None
        for item in thread.get("thread_items") or []:
            if not isinstance(item, dict):
                continue
            post = _thread_item_post(item)
            if not post:
                continue
            parent = _declared_parent_id(item, post) or prev_id
            if parent and root_id and parent == root_id:
                parent = None
            extracted = _extract_post_as_reply(post, parent_reply_id=parent)
            if not extracted:
                continue
            if extracted["reply_id"] == parent:
                extracted["parent_reply_id"] = None
            replies.append(extracted)
            prev_id = extracted["reply_id"]
    return replies


def apply_feed_parent(replies: list[dict[str, Any]], *, feed_id: str, root_post_id: str) -> list[dict[str, Any]]:
    """Khi feed là một reply lồng, REST coi reply đó là gốc chuỗi nên cha của item-0 trả về None.
    Trỏ chúng tới reply đã mở rộng thay vì để trông như thêm comment cấp một."""
    remapped: list[dict[str, Any]] = []
    for reply in replies:
        parent = reply.get("parent_reply_id")
        if feed_id != root_post_id and not parent:
            parent = feed_id
        if parent == root_post_id:
            parent = None
        remapped.append({**reply, "parent_reply_id": parent})
    return remapped


def replies_needing_expand(replies: list[dict[str, Any]], child_counts: dict[str, int]) -> list[str]:
    """Các id reply có direct_reply_count khai báo vẫn cao hơn số con ta đã thu - lấy
    text_feed/{id}/replies/ cho những cái đó, giống vòng reply theo từng comment của Facebook."""
    needed: list[str] = []
    seen: set[str] = set()
    for reply in replies:
        reply_id = reply.get("reply_id")
        if not reply_id or reply_id in seen:
            continue
        seen.add(reply_id)
        declared = int(reply.get("reply_count") or 0)
        if declared > child_counts.get(reply_id, 0):
            needed.append(reply_id)
    return needed


def find_text_feed_page_info(response: dict[str, Any]) -> dict[str, Any]:
    """Cursor cho lần GET ?paging_token= kế tiếp. Reply text_feed thật dùng
    paging_tokens.downwards (threads-go) / downward (junhoyeo). Payload token là
    `downward_other_replies` - trang reply anh em kế tiếp.

    `downwards_thread_will_continue` là một tín hiệu khác: *chuỗi* reply cuối còn item lồng để
    mở rộng không. Bài thật có 200 reply gửi cờ đó là false ở mọi trang trong khi vẫn trả token
    downwards không rỗng; coi cờ đó là dừng đã khiến chỉ lấy được trang 1. Dừng khi cursor
    rỗng."""
    tokens = response.get("paging_tokens") if isinstance(response.get("paging_tokens"), dict) else {}
    cursor = (
        tokens.get("downward")
        or tokens.get("downwards")
        or tokens.get("downward_cursor")
        or response.get("paging_token")
        or response.get("next_max_id")
    )
    if isinstance(cursor, str):
        cursor = cursor.strip() or None
    else:
        cursor = str(cursor) if cursor else None
    return {"has_next_page": bool(cursor), "end_cursor": cursor}
