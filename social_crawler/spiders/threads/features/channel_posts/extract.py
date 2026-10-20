"""Parse response tab "Threads" của trang hồ sơ (BarcelonaProfileThreadsTabRefetchableDirectQuery) - xem
ThreadsGraphQLClient.fetch_profile_posts. Dạng đã xác nhận với response thật bắt từ trình duyệt (20/10/2026):

    {"data": {"mediaData": {"edges": [{"node": {"thread_items": [{"post": {"pk": "...", "code": "...",
        "user": {"pk": "...", "username": "...", ...}, "caption": {"text": "..."}, "taken_at": 1751902713,
        "like_count": 7737, "text_post_app_info": {...}, ...}}], "thread_type": "thread", "id": "..."},
        "cursor": ""}, ...], "page_info": {"has_next_page": ..., "end_cursor": ...}}}}

Object "post" cùng dạng với node bài của tìm kiếm, nên dùng lại nguyên extract_post của features/search/extract.py - item
Kafka giống hệt bài tìm kiếm, ingest/khử trùng/liên quan/comment phía sau không cần biết bài đến từ đâu."""

from __future__ import annotations

from typing import Any

from social_crawler.spiders.comet_graphql_client import find_page_info
from social_crawler.spiders.threads.features.search.extract import extract_post


class ChannelPostsQueryError(RuntimeError):
    """Response không có data.mediaData.edges - thường là CHANNEL_POSTS_DOC_ID đã hết hạn (Threads trả errors) hoặc bị
    chặn. Khác "kênh không có bài": spider phải dừng với lỗi, không ghi nhận một lượt 0 bài."""


def extract_channel_page(
    response: Any, user_id: str | None = None
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """(bài của trang, page_info). Mỗi edge là một thread - lấy mọi post trong thread_items (bài gốc + phần tự trả lời
    nối tiếp của chính kênh). user_id: bỏ post của người khác lọt vào (vd. bài được trích dẫn/ repost hiện trong thread)."""
    data = response.get("data") if isinstance(response, dict) else None
    media_data = data.get("mediaData") if isinstance(data, dict) else None
    edges = media_data.get("edges") if isinstance(media_data, dict) else None
    if not isinstance(edges, list):
        errors = response.get("errors") if isinstance(response, dict) else None
        raise ChannelPostsQueryError(
            f"Threads profile posts response has no data.mediaData.edges: {str(errors or response)[:300]}"
        )

    posts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for edge in edges:
        node = edge.get("node") if isinstance(edge, dict) else None
        for item in (node or {}).get("thread_items") or []:
            post = item.get("post") if isinstance(item, dict) else None
            if not isinstance(post, dict) or not post.get("pk"):
                continue
            post_id = str(post["pk"])
            author_id = str((post.get("user") or {}).get("pk") or "")
            if post_id in seen or (user_id and author_id and author_id != str(user_id)):
                continue
            seen.add(post_id)
            posts.append(extract_post(post))
    return posts, media_data.get("page_info") or find_page_info(media_data)
