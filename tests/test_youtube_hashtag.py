import asyncio

import pytest

from social_crawler.consumer.jobs import youtube as job
from social_crawler.spiders.youtube.hashtag import hashtag


@pytest.mark.parametrize(
    ("keyword", "tab", "expected"),
    [
        # Đúng chuỗi trình duyệt gửi cho youtube.com/hashtag/phimle.
        ("#PhimLe", "all", "6gUKCgZwaGltbGUYAQ=="),
        ("phimle", "all", "6gUKCgZwaGltbGUYAQ=="),
        ("#phimle", "shorts", "6gUKCgZwaGltbGUYAw=="),
    ],
)
def test_params_match_what_the_browser_sends(keyword, tab, expected):
    assert hashtag.hashtag_params(keyword, tab) == expected


def test_params_encode_long_names_with_a_multi_byte_length():
    import base64

    raw = base64.b64decode(hashtag.hashtag_params("#" + "a" * 200, "all"))
    assert raw[:2] == b"\xea\x05" and raw[2:4] == b"\xcd\x01" and raw[4:7] == b"\x0a\xc8\x01"


def _short(video_id):
    return {
        "shortsLockupViewModel": {
            "onTap": {"innertubeCommand": {"reelWatchEndpoint": {"videoId": video_id}}},
            "overlayMetadata": {
                "primaryText": {"content": "short " + video_id},
                "secondaryText": {"content": "3,8 N lượt xem"},
            },
        }
    }


def _video(video_id):
    return {
        "videoRenderer": {
            "videoId": video_id,
            "title": {"runs": [{"text": "video " + video_id}]},
            "publishedTimeText": {"simpleText": "6 ngày trước"},
        }
    }


def _continuation(token):
    return {"continuationItemRenderer": {"continuationEndpoint": {"continuationCommand": {"token": token}}}}


def test_page_videos_reads_both_renderers_once():
    data = {"contents": [_video("v1"), _short("s1"), _video("v1"), _short("s1")]}
    videos = hashtag.page_videos(data)
    assert [(v["video_id"], v["is_short"]) for v in videos] == [("v1", False), ("s1", True)]
    assert videos[1]["view_count"] == 3800 and videos[0]["published_time"] == "6 ngày trước"


def test_hashtag_pages_follow_continuations_until_the_end(monkeypatch):
    pages = [
        {"contents": [_short("a"), _short("b"), _continuation("t2")]},
        {"onResponseReceivedActions": [{"items": [_short("b"), _short("c"), _continuation("t3")]}]},
        {"onResponseReceivedActions": [{"items": [_short("d")]}]},
    ]
    bodies = []

    class Response:
        def __init__(self, data):
            self.data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self.data

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json):
            bodies.append(json)
            return Response(pages[len(bodies) - 1])

    async def api_key(proxy=None):
        return "k"

    monkeypatch.setattr(hashtag, "get_youtube_api_key", api_key)
    monkeypatch.setattr(hashtag, "create_httpx_client", lambda **kwargs: Client())
    monkeypatch.setattr(hashtag, "get_context", lambda url: {"url": url})

    async def collect():
        pages_seen = hashtag.hashtag_pages("#X", tab="shorts", max_pages=0)
        return [[v["video_id"] for v in videos] async for _, videos in pages_seen]

    got = asyncio.run(collect())
    assert got == [["a", "b"], ["c"], ["d"]]
    assert bodies[0]["browseId"] == "FEhashtag" and bodies[1]["continuation"] == "t2" and "browseId" not in bodies[1]


def test_hash_keywords_use_the_hashtag_tabs_and_others_use_search(monkeypatch):
    used = []
    monkeypatch.setattr(job, "hashtag_pages", lambda keyword, **kw: used.append(("tag", keyword, kw["tab"])))
    monkeypatch.setattr(job, "search_pages", lambda keyword, **kw: used.append(("search", keyword)))
    tags = job.search_sources("#BoSuaBay", ["relevance"], 5, None)
    assert list(tags) == ["hashtag_all", "hashtag_shorts"]
    for factory in tags.values():
        factory()
    assert used == [("tag", "#BoSuaBay", "all"), ("tag", "#BoSuaBay", "shorts")]
    assert list(job.search_sources("Bò Sữa Bay", ["relevance", "khong_co"], 5, None)) == ["relevance"]
