"""
Trích reel từ response tab Reels của một Page (ProfileCometAppCollectionReelsRendererPaginationQuery, 2026-10-21):
data.node.aggregated_fb_shorts.edges[].profile_reel_node.node là một Story - post_id (khớp external_id bài Facebook
đã lưu từ tìm kiếm / bảng tin), creation_time, message.text, và attachments[0].media (Video) mang
play_count_reduced = lượt xem dạng rút gọn ("6,5K", "953", "1,2 Tr"). Tìm kiếm và bảng tin không có lượt xem; trang reel
khi chưa đăng nhập cũng không - đây là chỗ duy nhất có.
"""

from __future__ import annotations

import re
from typing import Any

from social_crawler.spiders.facebook.response_utils import find_first, get_path, iter_matching

# Hậu tố rút gọn của Facebook (tiếng Việt và tiếng Anh). "N" = nghìn, "Tr" = triệu, "T" = tỷ.
_UNITS = {
    "k": 1_000,
    "n": 1_000,
    "nghìn": 1_000,
    "m": 1_000_000,
    "tr": 1_000_000,
    "triệu": 1_000_000,
    "b": 1_000_000_000,
    "t": 1_000_000_000,
    "tỷ": 1_000_000_000,
}
_REDUCED = re.compile(r"^\s*([\d.,]+)\s*([^\d\s.,]*)")


def parse_reduced_count(text: Any) -> int | None:
    """"6,5K" -> 6500, "1,2 Tr" -> 1200000, "953" -> 953, "12.345" -> 12345 (không đơn vị thì dấu chấm/phẩy là phân
    cách nghìn). None khi không đọc được."""
    if isinstance(text, int):
        return text
    match = _REDUCED.match(str(text or ""))
    if not match:
        return None
    number, unit = match.group(1), match.group(2).lower().rstrip(".")
    if not unit:
        digits = re.sub(r"[.,]", "", number)
        return int(digits) if digits.isdigit() else None
    multiplier = _UNITS.get(unit)
    if multiplier is None:
        return None
    try:
        return round(float(number.replace(",", ".")) * multiplier)
    except ValueError:
        return None


def extract_reels(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Mỗi reel: post_id, video_id, url, timestamp, message, author_name, view_count (đã đổi ra số), play_count_reduced
    (chuỗi gốc). Reel không có post_id hoặc không đọc được lượt xem thì bỏ."""
    reels: list[dict[str, Any]] = []
    for edge in iter_matching(response, lambda n: isinstance(n.get("profile_reel_node"), dict)):
        story = get_path(edge, "profile_reel_node", "node") or {}
        post_id = story.get("post_id")
        if not post_id:
            continue
        media = get_path(story, "attachments", 0, "media") or {}
        holder = find_first(story, lambda n: "play_count_reduced" in n) or {}
        reduced = holder.get("play_count_reduced")
        views = parse_reduced_count(reduced)
        if views is None:
            continue
        actor = get_path(story, "actors", 0) or {}
        reels.append(
            {
                "post_id": str(post_id),
                "video_id": media.get("id"),
                "url": media.get("permalink_url") or media.get("shareable_url"),
                "timestamp": story.get("creation_time") or media.get("created_time"),
                "message": get_path(story, "message", "text"),
                "author_name": actor.get("name"),
                "view_count": views,
                "play_count_reduced": reduced,
            }
        )
    return reels
