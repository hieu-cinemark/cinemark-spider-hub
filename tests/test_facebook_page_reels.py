"""Tab Reels của Page (ProfileCometAppCollectionReelsRendererPaginationQuery): trích reel + lượt xem rút gọn."""

from __future__ import annotations

import pytest

from social_crawler.spiders.facebook.auth.request_capture import is_page_reels
from social_crawler.spiders.facebook.features.page_reels.extract import extract_reels, parse_reduced_count


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("6,5K", 6500),
        ("953", 953),
        ("12K", 12000),
        ("1,2 Tr", 1200000),
        ("1.2M", 1200000),
        ("3,4 N", 3400),
        ("12.345", 12345),
        ("2 tỷ", 2000000000),
        (None, None),
        ("abc", None),
        ("5 lượt", None),
    ],
)
def test_parse_reduced_count(text, expected) -> None:
    assert parse_reduced_count(text) == expected


def _edge(post_id: str, video_id: str, reduced: str | None, created: int = 1774270977) -> dict:
    media = {
        "__typename": "Video",
        "id": video_id,
        "permalink_url": f"https://www.facebook.com/reel/{video_id}/",
        "created_time": created,
    }
    if reduced is not None:
        media["play_count_reduced"] = reduced
    return {
        "profile_reel_node": {
            "node": {
                "__typename": "Story",
                "post_id": post_id,
                "creation_time": created + 8,
                "message": {"text": "Reel về phim"},
                "actors": [{"__typename": "User", "name": "Fan Page"}],
                "attachments": [{"media": media}],
            }
        },
        "cursor": "c1",
        "node": {"__typename": "TimelineAppCollectionItem", "id": "x"},
    }


def test_extract_reels_reads_views_and_post_id() -> None:
    response = {
        "data": {
            "node": {
                "__typename": "TimelineAppCollection",
                "aggregated_fb_shorts": {
                    "edges": [_edge("1374490074715604", "1491747119129500", "6,5K"), _edge("2", "3", None)],
                    "page_info": {"has_next_page": True, "end_cursor": "next"},
                },
            }
        }
    }
    reels = extract_reels(response)
    assert len(reels) == 1  # reel không có lượt xem bị bỏ
    reel = reels[0]
    assert reel["post_id"] == "1374490074715604"
    assert reel["video_id"] == "1491747119129500"
    assert reel["url"] == "https://www.facebook.com/reel/1491747119129500/"
    assert reel["view_count"] == 6500
    assert reel["play_count_reduced"] == "6,5K"
    assert reel["timestamp"] == 1774270985
    assert reel["author_name"] == "Fan Page"


def test_is_page_reels() -> None:
    assert is_page_reels("ProfileCometAppCollectionReelsRendererPaginationQuery")
    assert not is_page_reels("ProfileCometTimelineFeedRefetchQuery")
    assert not is_page_reels("FBReelsRootWithEntrypointQuery")
