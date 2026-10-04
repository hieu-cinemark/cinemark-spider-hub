"""
Biến một response tìm kiếm GraphQL thô của Facebook (JSON Relay lồng sâu) thành các bản
ghi phẳng, dễ phân tích: mỗi bài một dict và mỗi thực thể khác một dict (page, group,
hashtag, video...).

Đường dẫn trường ở đây được dịch ngược từ một response thật bắt được (xem bootstrap.py /
graphql_client.py) - Facebook có thể đổi dạng response ở bất kỳ lần deploy nào, nên cần
kiểm tra lại với một response mới nếu việc trích xuất bắt đầu trả về rỗng.
"""

from __future__ import annotations

from typing import Any, Iterator

from social_crawler.constants.facebook import REACTION_ID_TO_NAME, FacebookEntityType
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.response_utils import find_first, get_path, iter_matching

logger = get_logger(__name__)

# Thực thể thuộc các loại này được gộp vào bài cha thay vì tự phát ra riêng - một node
# Feedback chỉ là dữ liệu reaction/comment của một Story, không đáng báo cáo riêng.
FOLDED_TYPES = {FacebookEntityType.FEEDBACK}


def iter_entities(node: Any) -> Iterator[dict]:
    """Duyệt một response GraphQL đã parse, yield mọi dict trông như một thực thể Facebook (có cả
    __typename lẫn id)."""
    return iter_matching(node, lambda n: "__typename" in n and "id" in n)


def extract_post(story: dict[str, Any], feedback_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Dựng một bản ghi bài phẳng từ một thực thể Story. Số reaction/comment nằm trên một thực
    thể Feedback riêng mà Story chỉ tham chiếu theo id (`story["feedback"]["id"]`) -
    `feedback_by_id` phân giải tham chiếu đó thành thực thể Feedback đã mở rộng đầy đủ được
    thu thập ở chỗ khác trong cùng response, nơi có số liệu thật."""
    actor = get_path(story, "actors", 0) or {}
    feedback_ref = story.get("feedback") or {}
    feedback = feedback_by_id.get(feedback_ref.get("id"), feedback_ref)

    reactions: dict[str, int] = {}
    for edge in (
        get_path(feedback, "comet_ufi_summary_and_actions_renderer", "feedback", "top_reactions", "edges") or []
    ):
        reaction_id = get_path(edge, "node", "id")
        localized_name = get_path(edge, "node", "localized_name")
        name = REACTION_ID_TO_NAME.get(reaction_id)
        if name is None and localized_name:
            logger.warning("unknown_reaction_id", reaction_id=reaction_id, localized_name=localized_name)
            name = localized_name
        count = edge.get("reaction_count")
        if name and isinstance(count, int):
            reactions[name] = count

    media_type, media_url, duration_seconds, cover_url = _extract_media(story)
    quoted = _extract_quoted_post(story)

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
        "cover_url": cover_url,
        "duration_seconds": duration_seconds,
        "quoted": quoted,
    }


def _extract_media(story: dict[str, Any]) -> tuple[str | None, str | None, float | None, str | None]:
    """`attachments[0]["media"]` cấp cao nhất thường chỉ là một stub ({__typename, id}) - node
    media đầy đủ có URL thật nằm sâu hơn một tầng, dưới
    `attachments[0]["styles"]["attachment"]["media"]`. Ảnh chỉ lộ URL file trực tiếp qua
    `photo_image.uri`; video không lộ URL file thô ở đây, nên `media_url` quay về permalink
    Facebook của chúng và `cover_url` lấy `preferred_thumbnail.image.uri` khi có. Response tìm
    kiếm của Facebook hoàn toàn không có số lượt xem/phát video (đã kiểm tra với một response
    thật bắt được) - ở đây chỉ có thời lượng, qua `length_in_second`.

    Lưu ý: album nhiều ảnh (`StoryAttachmentAlbumStyleRenderer`) hoàn toàn không có node
    `media` duy nhất ở đường dẫn này - ảnh của chúng nằm dưới
    `styles.attachment.all_subattachments.nodes[]`, phần này chưa được xử lý ở đây."""
    attachment = get_path(story, "attachments", 0) or {}
    media = get_path(attachment, "styles", "attachment", "media") or attachment.get("media") or {}
    media_type = media.get("__typename")
    cover_url = get_path(media, "preferred_thumbnail", "image", "uri")

    if media_type == FacebookEntityType.PHOTO:
        media_url = get_path(media, "photo_image", "uri") or cover_url
        if not cover_url:
            cover_url = media_url
    else:
        media_url = media.get("permalink_url") or media.get("url")

    duration_seconds = media.get("length_in_second") if media_type == FacebookEntityType.VIDEO else None

    return media_type, media_url, duration_seconds, cover_url


def _extract_quoted_post(story: dict[str, Any]) -> dict[str, Any] | None:
    """Một lần chia sẻ kèm bình luận (hoặc trích dẫn) lưu bài gốc ở `attached_story`. Chỉ sâu 1
    tầng - ta không làm phẳng chia sẻ lồng của chia sẻ."""
    attached = story.get("attached_story") or get_path(
        story, "comet_sections", "content", "story", "comet_sections", "attached_story"
    )
    if not isinstance(attached, dict) or not attached:
        return None
    inner = attached.get("story") if isinstance(attached.get("story"), dict) else attached
    actor = get_path(inner, "actors", 0) or inner.get("actor") or {}
    message = (
        get_path(inner, "message", "text")
        or get_path(inner, "comet_sections", "content", "story", "message", "text")
        or get_path(inner, "comet_sections", "message", "story", "message", "text")
    )
    url = inner.get("permalink_url") or inner.get("url")
    _media_type, media_url, _duration, cover_url = _extract_media(inner)
    author = actor.get("name") or actor.get("username")
    if not any((author, message, url, cover_url, media_url)):
        return None
    return {
        "author": author,
        "content": message,
        "url": url,
        "media_url": cover_url or media_url,
    }


def _find_nested_count(node: Any, key: str) -> int | None:
    """Tìm mẫu `{key: {"count": N}}` đầu tiên. Facebook lồng share_count (và tương tự
    reaction_count) bên trong một danh sách renderer hành động UFI ở một chỉ số không chắc ổn
    định giữa các loại bài, nên hàm này tìm thay vì gán cứng đường dẫn."""
    match = find_first(node, lambda n: isinstance(n.get(key), dict) and isinstance(n[key].get("count"), int))
    return match[key]["count"] if match else None


def _extract_hashtags(story: dict[str, Any]) -> list[str]:
    """Hashtag nhắc trong nội dung bài là các thực thể Hashtag bên trong `message.ranges[]`, tham
    chiếu bằng URL thay vì tên - slug được lấy từ URL vì bản thân thực thể không có trường tên
    trơn nào."""
    ranges = (
        get_path(
            story,
            "comet_sections",
            "content",
            "story",
            "comet_sections",
            "message",
            "story",
            "message",
            "ranges",
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
    """Dựng một bản ghi phẳng cho thực thể không phải bài (Page/User, Group, Hashtag, Video,
    Photo...) - chỉ các trường hữu ích để nhận diện và liên kết tới nó."""
    return {
        "type": entity.get("__typename"),
        "id": entity.get("id"),
        "name": entity.get("name") or entity.get("short_name"),
        "url": entity.get("url") or entity.get("profile_url"),
    }


def extract_response(response: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Trích xuất (posts, other_entities) từ một response tìm kiếm thô, khử trùng theo id. Thực
    thể khác không có cả tên lẫn url bị bỏ - chúng là tham chiếu trơn (ví dụ
    {__typename, id}) không mang thông tin riêng nào và chỉ là nhiễu trong output."""
    entities = list(iter_entities(response))

    # Cùng một id có thể xuất hiện nhiều lần ở các độ sâu mở rộng khác nhau (ví dụ một stub
    # {__typename, id} trơn cạnh một node đầy đủ có trường thật) - giữ bản nào có nhiều trường
    # nhất thay vì để thứ tự duyệt quyết định, nếu không một stub gặp trước sẽ thắng khi khử
    # trùng và âm thầm bỏ mất bản đầy đủ hơn.
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
