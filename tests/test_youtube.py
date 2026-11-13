"""YouTube (logic chép từ data-miner): đọc số, thời gian tương đối, Shorts, token trang, định tuyến job."""

import asyncio

import pytest

from social_crawler.consumer import dispatch
from social_crawler.consumer.jobs import youtube as job
from social_crawler.spiders.youtube.search import search
from social_crawler.spiders.youtube.utils import parse_relative_time, parse_view_count


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("426.197 lượt xem", 426197),
        ("1,7 N lượt xem", 1700),
        ("1,2 Tr lượt xem", 1_200_000),
        ("2,042", 2042),
        ("1.7K views", 1700),
        ("2 tỷ", 2_000_000_000),
        ("Không có lượt xem", 0),
        (None, 0),
        (2042, 2042),
    ],
)
def test_parse_view_count(text, expected):
    assert parse_view_count(text) == expected


def test_parse_relative_time():
    now = 1_000_000_000
    assert parse_relative_time("17 giờ trước", now) == now - 17 * 3600
    assert parse_relative_time("2 days ago", now) == now - 2 * 86400
    assert parse_relative_time("1 tháng trước (đã chỉnh sửa)", now) == now - 30 * 86400
    assert parse_relative_time("", now) is None


def _short(video_id):
    return {
        "shortsLockupViewModel": {
            "onTap": {
                "innertubeCommand": {"reelWatchEndpoint": {"videoId": video_id, "thumbnail": {"thumbnails": []}}}
            },
            "overlayMetadata": {"primaryText": {"content": "Short"}, "secondaryText": {"content": "1,2 Tr lượt xem"}},
        }
    }


def test_search_page_has_videos_and_shorts_and_main_token():
    data = {
        "contents": {
            "twoColumnSearchResultsRenderer": {
                "primaryContents": {
                    "sectionListRenderer": {
                        "contents": [
                            {
                                "itemSectionRenderer": {
                                    "contents": [
                                        {"videoRenderer": {"videoId": "v1", "title": {"runs": [{"text": "Phim"}]}}},
                                        {
                                            "gridShelfViewModel": {
                                                "contents": [_short("s1")],
                                                # Token của kệ con - không được lấy làm trang kế.
                                                "continuationCommand": {"token": "SHELF"},
                                            }
                                        },
                                    ]
                                }
                            },
                            {
                                "continuationItemRenderer": {
                                    "continuationEndpoint": {"continuationCommand": {"token": "MAIN"}}
                                }
                            },
                        ]
                    }
                }
            }
        }
    }
    videos = search._page_videos(data)
    assert [(v["video_id"], v["is_short"]) for v in videos] == [("v1", False), ("s1", True)]
    assert videos[1]["view_count"] == 1_200_000 and videos[1]["url"].endswith("/shorts/s1")
    assert search._next_token(data) == "MAIN"


def test_post_value_prefers_player_meta():
    value = job._post_value(
        {"video_id": "v1", "title": "T", "timestamp": 123, "like_count": 5, "view_count": 9, "channel_handle": "@kenh"},
        {"video_id": "v1", "published_time": "2 ngày trước", "is_short": True},
        "kw1",
        search_mode="shorts",
    )
    assert value["platform"] == "youtube" and value["keyword_id"] == "kw1"
    assert value["create_time"] == 123 and value["like_count"] == 5
    assert value["url"] == "https://www.youtube.com/shorts/v1"
    assert value["channel_url"] == "https://www.youtube.com/@kenh" and value["search_mode"] == "shorts"


@pytest.mark.parametrize(
    ("kind", "target"),
    [(None, "run_youtube_search"), ("comments", "run_youtube_comments"), ("channel_videos", "run_youtube_channel")],
)
def test_dispatch_routes_youtube(monkeypatch, kind, target):
    called = []
    for name in ("run_youtube_search", "run_youtube_comments", "run_youtube_channel"):
        monkeypatch.setattr(dispatch, name, lambda request, _n=name: called.append(_n) or asyncio.sleep(0))
    monkeypatch.setattr(dispatch, "_already_done", lambda run_id: False)
    monkeypatch.setattr(dispatch, "_published_before_stop", lambda request, platform: False)
    monkeypatch.setattr(dispatch, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(dispatch, "start_task", lambda request: None)
    monkeypatch.setattr(dispatch, "finish_task", lambda request, status, **_: None)
    monkeypatch.setattr(dispatch, "_mark_done", lambda run_id: None)
    request = {"platform": "youtube", "keyword": "Mưa Đỏ"}
    if kind:
        request["type"] = kind
    asyncio.run(dispatch._handle_request(request))
    assert called == [target]
