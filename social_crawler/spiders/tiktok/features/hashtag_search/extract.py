"""
Biến một response /api/challenge/item_list/ thô của TikTok thành các bản ghi video phẳng,
dễ phân tích. Đường dẫn trường đã xác nhận với một response thật bắt được (xem client.py)
- TikTok có thể đổi dạng response ở bất kỳ lần deploy nào, nên kiểm tra lại với một
response mới nếu việc trích xuất bắt đầu trả về rỗng.
"""

from __future__ import annotations

from collections import Counter
from typing import Any


def _hashtag_names(item: dict[str, Any]) -> list[str]:
    """Mọi #hashtag nhắc trong caption của video, lấy từ contents[].textExtra thay vì parse từ
    desc - textExtra vốn đã có cách tách riêng của TikTok, không cần đoán."""
    names: list[str] = []
    for content in item.get("contents") or []:
        for extra in content.get("textExtra") or []:
            name = extra.get("hashtagName")
            if name:
                names.append(name)
    return names


def _http_url(value: Any) -> str | None:
    """Các trường cover/play của TikTok khi thì là URL dạng chuỗi, khi thì `{url_list: [...]}` /
    `{url: ...}`. Duyệt các dạng hiển nhiên và trả về URL http(s) đầu tiên."""
    if isinstance(value, str) and value.startswith("http"):
        return value
    if isinstance(value, dict):
        for key in ("url_list", "url", "uri"):
            found = _http_url(value.get(key))
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = _http_url(item)
            if found:
                return found
    return None


def extract_video(item: dict[str, Any]) -> dict[str, Any]:
    author = item.get("author") or {}
    stats = item.get("stats") or {}
    video = item.get("video") or {}
    music = item.get("music") or {}
    video_id = item.get("id")
    username = author.get("uniqueId")

    return {
        "video_id": video_id,
        "url": f"https://www.tiktok.com/@{username}/video/{video_id}" if username and video_id else None,
        "desc": item.get("desc"),
        # Phỏng đoán ngôn ngữ caption của chính TikTok ("vi", "es", "un" = không rõ) và quốc gia nơi
        # video được đăng. Giúp các quy tắc ingest của cinemark-api (app/services/relevance_rules.py)
        # phát hiện video nước ngoài dùng chung một hashtag không dấu (#memin -> nội dung "Memín" của
        # Mexico) kể cả khi caption chỉ có hashtag. Không có -> None.
        "text_language": item.get("textLanguage"),
        "location_created": item.get("locationCreated"),
        "create_time": item.get("createTime"),
        "author_id": author.get("id"),
        "author_username": username,
        "author_name": author.get("nickname"),
        "author_avatar_url": author.get("avatarThumb"),
        "duration": video.get("duration"),
        "cover_url": (
            _http_url(video.get("originCover")) or _http_url(video.get("cover")) or _http_url(video.get("dynamicCover"))
        ),
        "play_url": _http_url(video.get("playAddr")),
        "music_title": music.get("title"),
        "hashtags": _hashtag_names(item),
        "play_count": stats.get("playCount"),
        "like_count": stats.get("diggCount"),
        "comment_count": stats.get("commentCount"),
        "share_count": stats.get("shareCount"),
        "collect_count": stats.get("collectCount"),
    }


def extract_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Trích video từ một trang response tìm kiếm hashtag, khử trùng theo id (itemList không nên
    lặp trong một trang, nhưng vẫn giữ phòng thủ cho cùng trường hợp biên mà các extractor của
    Facebook/Threads đề phòng)."""
    richest_by_id: dict[str, dict[str, Any]] = {}
    for item in response.get("itemList") or []:
        video_id = item.get("id")
        if not video_id:
            continue
        if video_id not in richest_by_id or len(item) > len(richest_by_id[video_id]):
            richest_by_id[video_id] = item

    return [extract_video(item) for item in richest_by_id.values()]


def update_related_hashtag_counts(
    response: dict[str, Any], counts: Counter[tuple[str, str]], exclude_ids: set[str]
) -> None:
    """Đếm mọi hashtag KHÁC xuất hiện cùng với video trong trang vừa crawl - trường
    `challenges[]` của mỗi item liệt kê mọi tag nó mang (id + title), không chỉ tag đã query,
    nên có được miễn phí từ response ta vốn đang lấy. Gọi một lần mỗi trang, truyền cùng một
    Counter `counts` suốt lượt crawl, rồi lọc bằng top_related_hashtags() một lần ở cuối - đếm
    dồn thay vì giữ mọi trang thô trong bộ nhớ để tính sau."""
    for item in response.get("itemList") or []:
        for challenge in item.get("challenges") or []:
            cid, title = challenge.get("id"), challenge.get("title")
            if not cid or not title or cid in exclude_ids:
                continue
            counts[(cid, title)] += 1


def top_related_hashtags(
    counts: Counter[tuple[str, str]], min_occurrences: int = 2, limit: int = 10
) -> list[dict[str, Any]]:
    """Các hashtag liên quan đáng để người xem, phổ biến nhất trước. min_occurrences lọc nhiễu -
    một tag chỉ xuất hiện trên một video thường là hashtag không liên quan riêng của creator đó,
    không phải tín hiệu thật rằng nó liên quan tới thứ đã tìm. `id` ở đây vốn đã là
    challenge_id dạng số mà search_hashtag() cần - không cần thêm lượt resolve_hashtag() để
    crawl một trong số này tiếp."""
    return [
        {"id": cid, "title": title, "count": count}
        for (cid, title), count in counts.most_common(limit)
        if count >= min_occurrences
    ]
