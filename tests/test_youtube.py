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
    request = {"platform": "youtube", "keyword": "Mưa Đỏ", "_lane": 0}
    if kind:
        request["type"] = kind
    asyncio.run(dispatch._handle_request(request))
    assert called == [target]


def test_kafka_loop_forwards_youtube_to_lane_queues(monkeypatch):
    pushed = []

    class Cache:
        def lpush_json(self, key, value):
            pushed.append((key, value["id"]))

    monkeypatch.setattr(dispatch, "RedisCache", Cache)
    assert asyncio.run(dispatch._handle_request({"platform": "youtube", "keyword": "x", "id": 1})) is True
    assert (
        asyncio.run(dispatch._handle_request({"platform": "youtube", "type": "comments", "post_id": "v", "id": 2}))
        is True
    )
    assert pushed == [(job.SEARCH_QUEUE_KEY, 1), (job.COMMENTS_QUEUE_KEY, 2)]


def test_lane_proxy_falls_back_when_marked_bad(monkeypatch):
    monkeypatch.setattr(job, "_proxy_urls", lambda: ["http://a", "http://b"])
    monkeypatch.setattr(job, "worker_lane", lambda: 3)
    job._bad_proxies.clear()
    assert job._proxy() == "http://b"
    job.mark_proxy_bad("http://b")
    assert job._proxy() == "http://a"
    job.mark_proxy_bad("http://a")
    assert job._proxy() is None  # mọi proxy nghỉ -> IP máy
    job._bad_proxies.clear()


def test_account_page_feed_is_skipped_by_default(monkeypatch):
    from social_crawler.consumer.common import CrawlJobSkipped
    from social_crawler.consumer.jobs import page_feed

    monkeypatch.delenv("FACEBOOK_ACCOUNT_PAGE_FEED", raising=False)
    with pytest.raises(CrawlJobSkipped):
        asyncio.run(page_feed._run_page_feed_spider({"platform": "facebook", "page_url": "https://www.facebook.com/x"}))


def test_lane_proxy_skips_a_dead_proxy(monkeypatch):
    monkeypatch.setattr(job.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(job, "_proxy_urls", lambda: ["http://dead:1"])
    monkeypatch.setattr(job, "worker_lane", lambda: 0)
    job._bad_proxies.clear()
    job._proxy_checked.clear()

    class Dead:
        def __init__(self, **_):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def get(self, url):
            raise job.httpx.ConnectError("refused")

    monkeypatch.setattr(job.httpx, "AsyncClient", Dead)
    assert asyncio.run(job.lane_proxy()) is None  # proxy chết -> IP máy, không đứng chờ
    assert "http://dead:1" in job._bad_proxies
    job._bad_proxies.clear()


def test_proxy_urls_loads_on_startup_and_refreshes_after_ttl(monkeypatch):
    now = 0.0
    calls = []
    monkeypatch.setattr(job.time, "monotonic", lambda: now)
    monkeypatch.setattr(job, "_proxy_cache", {"at": None, "urls": []})
    monkeypatch.setattr(job, "list_proxies", lambda platform: calls.append(platform) or [{"platform": "youtube"}])
    monkeypatch.setattr(job, "build_proxy_url", lambda row: "http://proxy:1")

    assert job._proxy_urls() == ["http://proxy:1"]
    now = job.PROXY_REFRESH_SECONDS - 1
    assert job._proxy_urls() == ["http://proxy:1"]
    assert calls == ["youtube"]
    now = job.PROXY_REFRESH_SECONDS
    assert job._proxy_urls() == ["http://proxy:1"]
    assert calls == ["youtube", "youtube"]


def test_lane_proxy_checks_at_startup_and_rechecks_after_ttl(monkeypatch):
    now = 0.0
    requests = []
    monkeypatch.setattr(job.time, "monotonic", lambda: now)
    monkeypatch.setattr(job, "_proxy_urls", lambda: ["http://proxy:1"])
    monkeypatch.setattr(job, "worker_lane", lambda: 0)
    monkeypatch.setattr(job, "_bad_proxies", {})
    monkeypatch.setattr(job, "_proxy_checked", {})

    class Healthy:
        def __init__(self, **_):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def get(self, url):
            requests.append(url)
            return job.httpx.Response(204)

    monkeypatch.setattr(job.httpx, "AsyncClient", Healthy)
    assert asyncio.run(job.lane_proxy()) == "http://proxy:1"
    assert len(requests) == 1
    now = job.PROXY_CHECK_SECONDS - 1
    assert asyncio.run(job.lane_proxy()) == "http://proxy:1"
    assert len(requests) == 1
    now = job.PROXY_CHECK_SECONDS
    assert asyncio.run(job.lane_proxy()) == "http://proxy:1"
    assert len(requests) == 2
