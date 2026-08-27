"""
Turns a raw Threads GraphQL search response into flat, analysis-friendly
post records.

Unlike Facebook's search response, a Threads "post" (media) node carries no
top-level `__typename` key of its own (confirmed against two real captured
responses - a single-post detail query and a comment-thread query: the
*wrapper* around a list of posts has `__typename: "XDTTextAppThreadView"`,
but the post/media object nested inside it does not). So posts are
recognized by structural signature instead - `pk` + `code` + `caption` +
`user` + `text_post_app_info` together - the same resilience approach as
facebook/response_utils.iter_matching, just with a different signature.

This walks the *entire* response tree, which means it will also pick up any
reply/quote previews a future response shape embeds inside search results,
not just top-level results - harmless for now (a comment previewed inline is
still a real post worth keeping), but worth knowing if results ever look
inflated compared to what the UI shows. Field paths were reverse-engineered
from real captured responses (see auth/bootstrap.py) - Threads can change
its response shape on any deploy, so these should be re-checked against a
fresh response if extraction starts coming back empty.
"""

from __future__ import annotations

from typing import Any, Iterator

from social_crawler.spiders.facebook.response_utils import get_path, iter_matching

POST_NODE_SIGNATURE = {"pk", "code", "caption", "user", "text_post_app_info"}


def iter_post_nodes(node: Any) -> Iterator[dict]:
    """Walk a parsed GraphQL response, yielding every dict that looks like a
    Threads post (media) node."""
    return iter_matching(node, lambda n: POST_NODE_SIGNATURE <= n.keys())


def _media_url(media: dict[str, Any]) -> str | None:
    """Videos expose their playable url(s) directly via video_versions;
    photos via image_versions2.candidates. Picks the first entry of
    whichever is present - the first video_versions/candidates entry was the
    primary rendition in every real response checked so far."""
    video_versions = media.get("video_versions") or []
    if video_versions:
        return video_versions[0].get("url")

    candidates = get_path(media, "image_versions2", "candidates") or []
    if candidates:
        return candidates[0].get("url")

    return None


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
    }


def extract_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract posts from a raw search response, deduped by id. The same id
    can appear multiple times at different expansion depths (a bare
    reference next to a fully-populated node) - keep whichever instance has
    the most fields, same rationale as facebook/features/search/extract.py's
    extract_response."""
    richest_by_id: dict[str, dict[str, Any]] = {}
    for node in iter_post_nodes(response):
        post_id = node.get("pk")
        if not post_id:
            continue
        if post_id not in richest_by_id or len(node) > len(richest_by_id[post_id]):
            richest_by_id[post_id] = node

    return [extract_post(node) for node in richest_by_id.values()]
