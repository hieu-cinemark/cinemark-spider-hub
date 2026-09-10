"""
Extracts a Threads post's first page of replies from its permalink HTML -
not from a GraphQL AJAX call. Confirmed against real captured traffic: the
page embeds a Relay "RelayPrefetchedStreamCache" preload payload directly in
the HTML (server-side, present even for a logged-in fetch, not just a guest
one), and this is the *only* place the first page of replies is available -
the client-side "load more" query (BarcelonaPostPageStrongIdDirectRepliesRefetchQuery)
always comes back with direct_replies: null when replayed outside a live
browser session, even byte-for-byte identical to a real captured request
(tried: exact replay, fresh cursor from a real page_info, matching page
size, added sec-fetch-*/accept/priority headers - all still null). So this
project can only offer the SSR-embedded first page for Threads replies for
now; going further needs a real browser in the loop, not a curl_cffi replay.
"""

from __future__ import annotations

import json
import re
from typing import Any

_JSON_DECODER = json.JSONDecoder()


def _iter_bbox_results(html: str):
    """Every embedded Relay preload payload in the page looks like
    `"__bbox":{"complete":true,"result":{...}}` - walk to each `"__bbox":`
    occurrence and let the JSON decoder consume exactly one well-formed
    value from there, rather than trying to regex-match balanced braces
    across a document this large."""
    for match in re.finditer(r'"__bbox":', html):
        try:
            obj, _ = _JSON_DECODER.raw_decode(html, match.end())
        except (ValueError, json.JSONDecodeError):
            continue
        if isinstance(obj, dict):
            yield obj


def _find_direct_replies(html: str) -> dict[str, Any] | None:
    for bbox in _iter_bbox_results(html):
        result = bbox.get("result")
        data = result.get("data") if isinstance(result, dict) else None
        media = data.get("media") if isinstance(data, dict) else None
        app_info = media.get("text_post_app_info") if isinstance(media, dict) else None
        direct_replies = app_info.get("direct_replies") if isinstance(app_info, dict) else None
        if direct_replies:
            return direct_replies
    return None


def _extract_reply(edge: dict[str, Any]) -> dict[str, Any] | None:
    """Each `direct_replies` edge wraps its own little `posts` connection
    (a reply can be a multi-post "self-thread") - only the first post is
    taken here, same scope as Facebook's flat one-post-per-comment model."""
    node = edge.get("node") or {}
    post_edges = (node.get("posts") or {}).get("edges") or []
    if not post_edges:
        return None
    post = post_edges[0].get("node") or {}
    user = post.get("user") or {}
    caption = post.get("caption") or {}
    app_info = post.get("text_post_app_info") or {}

    reply_id = post.get("pk")
    if not reply_id:
        return None

    return {
        "reply_id": str(reply_id),
        "message": caption.get("text"),
        "timestamp": post.get("taken_at"),
        "author_id": user.get("pk"),
        "author_username": user.get("username"),
        "author_name": user.get("full_name"),
        "author_profile_picture": user.get("profile_pic_url"),
        "like_count": post.get("like_count"),
        "reply_count": app_info.get("direct_reply_count"),
        "code": post.get("code"),
    }


def extract_first_page_replies(html: str) -> list[dict[str, Any]]:
    """Every reply on the SSR-embedded first page of a post's permalink -
    see module docstring for why there's no pagination beyond this."""
    direct_replies = _find_direct_replies(html)
    if not direct_replies:
        return []
    replies = [_extract_reply(edge) for edge in direct_replies.get("edges") or []]
    return [r for r in replies if r is not None]
