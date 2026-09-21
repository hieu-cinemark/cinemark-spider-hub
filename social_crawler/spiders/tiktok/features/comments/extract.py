"""
Extracts TikTok comments / replies from /api/comment/list/ and
/api/comment/list/reply/ JSON (TikTokCommentClient). Field names confirmed
against real captured responses.
"""

from __future__ import annotations

from typing import Any


def _avatar_url(user: dict[str, Any]) -> str | None:
    urls = (user.get("avatar_thumb") or {}).get("url_list") or []
    return urls[0] if urls else None


def extract_comment(raw: dict[str, Any], *, parent_comment_id: str | None = None) -> dict[str, Any] | None:
    cid = raw.get("cid")
    if not cid:
        return None
    user = raw.get("user") or {}
    # Replies use reply_id for the parent; top-level uses reply_comment_total.
    parent = parent_comment_id or (str(raw["reply_id"]) if raw.get("reply_id") and str(raw.get("reply_id")) != "0" else None)
    return {
        "comment_id": str(cid),
        "message": raw.get("text"),
        "timestamp": raw.get("create_time"),
        "like_count": raw.get("digg_count") or 0,
        "reply_count": raw.get("reply_comment_total") or 0,
        "author_id": user.get("uid"),
        "author_username": user.get("unique_id"),
        "author_name": user.get("nickname"),
        "author_avatar_url": _avatar_url(user),
        "parent_comment_id": parent,
    }


def extract_comments(response: dict[str, Any], *, parent_comment_id: str | None = None) -> list[dict[str, Any]]:
    """Every comment (or reply) in one list / list/reply page."""
    comments = [
        extract_comment(c, parent_comment_id=parent_comment_id) for c in response.get("comments") or []
    ]
    return [c for c in comments if c is not None]
