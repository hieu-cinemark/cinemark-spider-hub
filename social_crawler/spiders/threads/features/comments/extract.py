"""
Extracts Threads replies from two response shapes:

- GET /api/v1/text_feed/<id>/replies/ (extract_replies_from_text_feed) -
  what comments.py actually crawls.
- A browser-captured DirectRepliesRefetchQuery JSON
  (extract_replies_from_json) - GraphQL replay of that query returns
  direct_replies: null; kept so a captured browser body still parses.
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
    """A captured response's exact wrapper shape isn't independently
    confirmed against a real capture, so this walks the whole tree for any
    "direct_replies" connection instead of assuming a fixed
    `data.media.text_post_app_info` path."""
    for d in _iter_dicts(obj):
        candidate = d.get("direct_replies")
        if isinstance(candidate, dict) and isinstance(candidate.get("edges"), list):
            return candidate
    return None


def _extract_reply_thread(edge: dict[str, Any]) -> list[dict[str, Any]]:
    """Each `direct_replies` edge wraps a `posts` connection - a self-thread
    of the top-level reply plus any inline nested replies. Item 0 replies
    to the post; each later item's parent is the previous reply_id."""
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
    """Every reply in one browser-captured DirectRepliesRefetchQuery
    response - see capture_reply_pages(), which calls this once per page."""
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


def _extract_post_as_reply(
    post: dict[str, Any], *, parent_reply_id: str | None = None
) -> dict[str, Any] | None:
    """Flat reply record from an Instagram-shaped media object (text_feed
    REST `post` / GraphQL post node). `parent_reply_id` is the platform id
    of the comment this replies to (None = direct reply to the post)."""
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
    """Every reply in one GET /api/v1/text_feed/<id>/replies/ page.

    Each reply_threads[] entry is a chain: item 0 is a direct reply to the
    post, later items are nested replies already inlined. parent_reply_id
    is the previous item (or an explicit parent_reply_id on the payload),
    never the root post id - UI joins comments to comments, not to posts."""
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


def apply_feed_parent(
    replies: list[dict[str, Any]], *, feed_id: str, root_post_id: str
) -> list[dict[str, Any]]:
    """When the feed is a nested reply, REST treats that reply as the
    thread root so item-0 parents come back None. Point them at the
    expanded reply instead of looking like extra top-level comments."""
    remapped: list[dict[str, Any]] = []
    for reply in replies:
        parent = reply.get("parent_reply_id")
        if feed_id != root_post_id and not parent:
            parent = feed_id
        if parent == root_post_id:
            parent = None
        remapped.append({**reply, "parent_reply_id": parent})
    return remapped


def replies_needing_expand(
    replies: list[dict[str, Any]], child_counts: dict[str, int]
) -> list[str]:
    """Reply ids whose declared direct_reply_count is still higher than
    how many children we have already collected - fetch
    text_feed/{id}/replies/ for those, same as Facebook's per-comment
    replies loop."""
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
    """Cursor for the next GET ?paging_token=. Live text_feed replies use
    paging_tokens.downwards (threads-go) / downward (junhoyeo). The token
    payload is `downward_other_replies` - next sibling-reply page.

    `downwards_thread_will_continue` is a different signal: whether the
    last reply *chain* still has nested items to expand. Live 200-reply
    posts send that flag as false on every page while still returning a
    non-empty downwards token; treating the flag as stop caused page 1
    only. Stop when the cursor is empty."""
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
