"""Convert already-fetched SDK snapshots to the normal Kafka/ingest contract.

Never access lazy SDK properties here: serialization must not cause extra requests.
"""

from __future__ import annotations


def post_payload(data: dict, query: str) -> dict:
    author = data.get("author") or {}
    attachments = [a for a in (data.get("attachments") or []) if isinstance(a, dict)]
    media = attachments[0] if attachments else {}
    reactions = data.get("reactions") or {}
    return {
        "platform": "facebook",
        "query": query,
        "post_id": str(data["id"]),
        "url": data.get("url") or f"https://www.facebook.com/{data['id']}",
        "message": data.get("text") or data.get("title"),
        "timestamp": data.get("creation_time"),
        "author_name": author.get("name"),
        "author_id": author.get("id"),
        "author_url": author.get("url"),
        "comments_count": data.get("comments_count"),
        "reactions_count": reactions.get("total"),
        "reactions": {str(r.get("name") or r.get("id")): r.get("count") for r in reactions.get("breakdown", [])},
        "shares_count": data.get("share_count"),
        "media_type": {"photo": "Photo", "video": "Video"}.get(media.get("type"), media.get("type")),
        "media_url": media.get("video_url") or media.get("image_url") or media.get("url"),
        "cover_url": media.get("thumbnail_image_url") or media.get("image_url") or media.get("fallback_image_url"),
        "duration_seconds": media.get("length_in_second"),
        "attachments": attachments,
        "crawl_method": "facebook_gql_sdk",
    }


def comment_payload(data: dict, post_id: str) -> dict:
    author = data.get("author") or {}
    return {
        "platform": "facebook",
        "post_id": str(post_id),
        "comment_id": str(data["id"]),
        "message": data.get("text"),
        "timestamp": data.get("created_time"),
        "author_name": author.get("name"),
        "author_id": author.get("id"),
        "author_url": author.get("url"),
        "author_gender": author.get("gender"),
        "author_profile_picture": author.get("profile_picture_url"),
        "reactions_count": data.get("reaction_count"),
        "replies_count": data.get("reply_count"),
        "attachments": data.get("attachments") or [],
        "crawl_method": "facebook_gql_sdk",
    }
