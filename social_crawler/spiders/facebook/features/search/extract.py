"""
Turns a raw Facebook GraphQL search response (deeply nested Relay JSON) into
flat, analysis-friendly records: one dict per post and one per other entity
(page, group, hashtag, video...).

Field paths here were reverse-engineered from a real captured response (see
bootstrap.py / graphql_client.py) - Facebook can change its response shape
on any deploy, so these should be re-checked against a fresh response if
extraction starts coming back empty.
"""

from __future__ import annotations

from typing import Any, Iterator

from social_crawler.constants.facebook import REACTION_ID_TO_NAME, FacebookEntityType
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.response_utils import find_first, get_path, iter_matching

logger = get_logger(__name__)

# Entities of these types are folded into their parent post instead of being
# emitted on their own - a Feedback node is just the reactions/comments data
# for a Story, not something worth reporting standalone.
FOLDED_TYPES = {FacebookEntityType.FEEDBACK}


def iter_entities(node: Any) -> Iterator[dict]:
    """Walk a parsed GraphQL response, yielding every dict that looks like a
    Facebook entity (has both __typename and id)."""
    return iter_matching(node, lambda n: "__typename" in n and "id" in n)


def extract_post(story: dict[str, Any], feedback_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Build a flat post record from a Story entity. Reaction/comment counts
    live on a separate Feedback entity that the Story only references by id
    (`story["feedback"]["id"]`) - `feedback_by_id` resolves that reference to
    the fully-expanded Feedback entity collected elsewhere in the same
    response, which is where the actual counts are."""
    actor = get_path(story, "actors", 0) or {}
    feedback_ref = story.get("feedback") or {}
    feedback = feedback_by_id.get(feedback_ref.get("id"), feedback_ref)

    reactions: dict[str, int] = {}
    for edge in get_path(feedback, "comet_ufi_summary_and_actions_renderer", "feedback", "top_reactions", "edges") or []:
        reaction_id = get_path(edge, "node", "id")
        localized_name = get_path(edge, "node", "localized_name")
        name = REACTION_ID_TO_NAME.get(reaction_id)
        if name is None and localized_name:
            logger.warning("unknown_reaction_id", reaction_id=reaction_id, localized_name=localized_name)
            name = localized_name
        count = edge.get("reaction_count")
        if name and isinstance(count, int):
            reactions[name] = count

    media_type, media_url, duration_seconds = _extract_media(story)

    return {
        "post_id": story.get("post_id"),
        "url": story.get("permalink_url"),
        "message": get_path(story, "comet_sections", "content", "story", "message", "text"),
        "timestamp": story.get("creation_time"),
        "author_name": actor.get("name"),
        "author_id": actor.get("id"),
        "author_url": actor.get("url"),
        "comments_count": get_path(feedback, "comment_rendering_instance", "comments", "total_count"),
        "reactions_count": sum(reactions.values()) if reactions else None,
        "reactions": reactions or None,
        "shares_count": _find_nested_count(feedback, "share_count"),
        "hashtags": _extract_hashtags(story) or None,
        "media_type": media_type,
        "media_url": media_url,
        "duration_seconds": duration_seconds,
    }


def _extract_media(story: dict[str, Any]) -> tuple[str | None, str | None, float | None]:
    """The top-level `attachments[0]["media"]` is often just a stub
    ({__typename, id}) - the fully-populated media node with real URLs lives
    one level deeper, under `attachments[0]["styles"]["attachment"]["media"]`.
    Photos only expose a direct file URL via `photo_image.uri`; videos don't
    expose a raw file URL here, so we fall back to their Facebook permalink.
    Facebook's search response doesn't include video view/play counts at
    all (checked against a real captured response) - only duration is
    available here, via `length_in_second`.

    Note: multi-photo albums (`StoryAttachmentAlbumStyleRenderer`) don't
    have a single `media` node at this path at all - their photos live under
    `styles.attachment.all_subattachments.nodes[]` instead, which isn't
    handled here yet."""
    attachment = get_path(story, "attachments", 0) or {}
    media = get_path(attachment, "styles", "attachment", "media") or attachment.get("media") or {}
    media_type = media.get("__typename")

    if media_type == FacebookEntityType.PHOTO:
        media_url = get_path(media, "photo_image", "uri")
    else:
        media_url = media.get("permalink_url") or media.get("url")

    duration_seconds = media.get("length_in_second") if media_type == FacebookEntityType.VIDEO else None

    return media_type, media_url, duration_seconds


def _find_nested_count(node: Any, key: str) -> int | None:
    """Search for the first `{key: {"count": N}}` pattern. Facebook nests
    share_count (and similarly reaction_count) inside a list of UFI action
    renderers at an index that isn't guaranteed to stay stable across post
    types, so this searches instead of hardcoding a path."""
    match = find_first(node, lambda n: isinstance(n.get(key), dict) and isinstance(n[key].get("count"), int))
    return match[key]["count"] if match else None


def _extract_hashtags(story: dict[str, Any]) -> list[str]:
    """Hashtags mentioned in the post text are Hashtag entities inside
    `message.ranges[]`, referenced by URL rather than by name - the slug is
    taken from the URL since the entity itself carries no plain name field."""
    ranges = (
        get_path(
            story,
            "comet_sections", "content", "story",
            "comet_sections", "message", "story", "message", "ranges",
        )
        or []
    )
    hashtags = []
    for r in ranges:
        entity = r.get("entity") or {}
        if entity.get("__typename") != FacebookEntityType.HASHTAG:
            continue
        url = entity.get("url") or ""
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        if slug:
            hashtags.append(slug)
    return hashtags


def extract_entity_summary(entity: dict[str, Any]) -> dict[str, Any]:
    """Build a flat record for a non-post entity (Page/User, Group, Hashtag,
    Video, Photo...) - just the fields useful for identifying and linking to it."""
    return {
        "type": entity.get("__typename"),
        "id": entity.get("id"),
        "name": entity.get("name") or entity.get("short_name"),
        "url": entity.get("url") or entity.get("profile_url"),
    }


def extract_response(response: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract (posts, other_entities) from a raw search response, deduped by
    id. Other entities with neither a name nor a url are dropped - they're
    bare references (e.g. {__typename, id}) that carry no information of
    their own and are just noise in the output."""
    entities = list(iter_entities(response))

    # The same id can appear multiple times at different expansion depths
    # (e.g. a bare {__typename, id} stub next to a fully-populated node with
    # the real fields) - keep whichever instance has the most fields instead
    # of letting iteration order decide, otherwise a stub encountered first
    # would win the dedup and silently discard the richer version.
    richest_by_id: dict[str, dict[str, Any]] = {}
    for entity in entities:
        entity_id = entity.get("id")
        if not entity_id:
            continue
        if entity_id not in richest_by_id or len(entity) > len(richest_by_id[entity_id]):
            richest_by_id[entity_id] = entity

    feedback_by_id = {
        entity_id: entity
        for entity_id, entity in richest_by_id.items()
        if entity.get("__typename") == FacebookEntityType.FEEDBACK
    }

    posts: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []

    for entity in richest_by_id.values():
        typename = entity.get("__typename")
        if typename in FOLDED_TYPES:
            continue

        if typename == FacebookEntityType.STORY:
            posts.append(extract_post(entity, feedback_by_id))
        else:
            summary = extract_entity_summary(entity)
            if summary["name"] or summary["url"]:
                others.append(summary)

    return posts, others
