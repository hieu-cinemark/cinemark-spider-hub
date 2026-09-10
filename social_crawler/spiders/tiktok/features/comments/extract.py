"""
Extracts TikTok comments from a real /api/comment/list/ JSON response -
captured directly off a live Patchright browser's own network traffic (see
comments.py), not replayed via curl_cffi. Field names confirmed against a
real captured response, not guessed.
"""

from __future__ import annotations

from typing import Any


def _avatar_url(user: dict[str, Any]) -> str | None:
    urls = (user.get("avatar_thumb") or {}).get("url_list") or []
    return urls[0] if urls else None


def extract_comment(raw: dict[str, Any]) -> dict[str, Any] | None:
    cid = raw.get("cid")
    if not cid:
        return None
    user = raw.get("user") or {}
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
    }


def extract_comments(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Every comment in one /api/comment/list/ page."""
    comments = [extract_comment(c) for c in response.get("comments") or []]
    return [c for c in comments if c is not None]
