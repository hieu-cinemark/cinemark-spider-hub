"""
Turns a raw TikTok /api/challenge/item_list/ response into flat,
analysis-friendly video records. Field paths were confirmed against a real
captured response (see client.py) - TikTok can change its response shape on
any deploy, so re-check against a fresh response if extraction starts
coming back empty.
"""

from __future__ import annotations

from typing import Any


def _hashtag_names(item: dict[str, Any]) -> list[str]:
    """Every #hashtag mentioned in the video's caption, pulled from
    contents[].textExtra rather than parsed out of desc - textExtra already
    has TikTok's own segmentation, no guessing needed."""
    names: list[str] = []
    for content in item.get("contents") or []:
        for extra in content.get("textExtra") or []:
            name = extra.get("hashtagName")
            if name:
                names.append(name)
    return names


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
        "create_time": item.get("createTime"),
        "author_id": author.get("id"),
        "author_username": username,
        "author_name": author.get("nickname"),
        "author_avatar_url": author.get("avatarThumb"),
        "duration": video.get("duration"),
        "cover_url": video.get("cover"),
        "play_url": video.get("playAddr"),
        "music_title": music.get("title"),
        "hashtags": _hashtag_names(item),
        "play_count": stats.get("playCount"),
        "like_count": stats.get("diggCount"),
        "comment_count": stats.get("commentCount"),
        "share_count": stats.get("shareCount"),
        "collect_count": stats.get("collectCount"),
    }


def extract_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract videos from one page of a hashtag search response, deduped
    by id (itemList shouldn't repeat within a single page, but this stays
    defensive against the same edge case Facebook/Threads' extractors
    guard)."""
    richest_by_id: dict[str, dict[str, Any]] = {}
    for item in response.get("itemList") or []:
        video_id = item.get("id")
        if not video_id:
            continue
        if video_id not in richest_by_id or len(item) > len(richest_by_id[video_id]):
            richest_by_id[video_id] = item

    return [extract_video(item) for item in richest_by_id.values()]
