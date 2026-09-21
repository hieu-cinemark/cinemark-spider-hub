"""
Turns a raw Facebook comments-list GraphQL response into flat records, one
per comment. Field paths were reverse-engineered from a real captured
response (see graphql_client.get_comments / bootstrap_comments). Both
top-level comments and their replies page normally now (comments.py's
start() loop and _fetch_replies respectively) - each needs its own
paginated query captured once via bootstrap.py (plain --post-url for
comments, --post-url ... --type replies for replies), since Facebook serves
the "initial" and "paginated" fetch as two separate queries here (unlike
Threads, where one refetchable query serves both - see get_comments' own
docstring in comet_graphql_client.py).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from social_crawler.spiders.facebook.response_utils import get_path


def extract_comments(response: dict[str, Any]) -> list[dict[str, Any]]:
    edges = _comments_connection(response).get("edges") or []
    return [extract_comment(edge.get("node") or {}) for edge in edges]


def find_comments_page_info(response: dict[str, Any]) -> dict[str, Any] | None:
    """The comments connection's own `page_info` (has_next_page/end_cursor)
    for the NEXT page of comments. Deliberately not a generic recursive
    search like graphql_client.find_page_info: each comment also carries its
    own `feedback.replies_connection.page_info` for its replies thread, and
    those get hit first in a depth-first walk (edges comes before page_info
    in the response) - always reporting has_next_page=False since none of
    the sample comments had replies, even when the comment list itself very
    much has another page."""
    return _comments_connection(response).get("page_info")


def _comments_connection(response: dict[str, Any]) -> dict[str, Any]:
    return (
        get_path(
            response,
            "data",
            "node",
            "comment_rendering_instance_for_feed_location",
            "comments",
        )
        or {}
    )


def extract_replies(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Like extract_comments, but for a get_replies()/get_replies_next_page()
    response - reuses extract_comment() as-is since a reply node is
    comment-shaped."""
    edges = _replies_connection(response).get("edges") or []
    return [extract_comment(edge.get("node") or {}) for edge in edges]


def find_replies_page_info(response: dict[str, Any]) -> dict[str, Any] | None:
    """The replies connection's own `page_info` for the NEXT page of replies
    to one comment. See find_comments_page_info's docstring above for why
    this can't just be graphql_client.find_page_info's generic recursive
    search."""
    return _replies_connection(response).get("page_info")


def _replies_connection(response: dict[str, Any]) -> dict[str, Any]:
    # NOT yet confirmed against a real captured replies response - best
    # guess based on find_comments_page_info's own docstring, which notes
    # each top-level comment carries a `feedback.replies_connection` for its
    # replies thread. A get_replies() call re-roots the query at the
    # comment (via _reply_target_id), so try both the equivalent-to-
    # comments-connection shape and a bare top-level replies_connection
    # before giving up. Correct this once a real `--type replies` bootstrap
    # capture shows the actual path.
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
    """Like count lives on a per-comment Feedback node that's only reachable
    through one of the comment's action links (Like/Dislike/Reply/...) -
    they all reference the same underlying feedback object, so the first
    one that has it is enough."""
    for link in node.get("comment_action_links") or []:
        count = get_path(link, "comment", "feedback", "reactors", "count")
        if isinstance(count, int):
            return count
    return None


def _extract_attachment(node: dict[str, Any]) -> dict[str, Any]:
    """A comment can carry at most one attachment (sticker, GIF share, photo,
    video...) under attachments[0].style_type_renderer.attachment.media.
    Sticker and GIF shapes below are confirmed against a real response;
    image/video use the same field names Facebook uses for post attachments
    elsewhere (Photo -> photo_image.uri, Video -> permalink_url) since no
    comment with those attached showed up in the captured sample - worth
    double-checking against a real one if this starts coming back empty."""
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
