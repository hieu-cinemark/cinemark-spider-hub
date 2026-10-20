"""features/channel_posts của Threads: parse response tab hồ sơ, bộ khớp phim, spider (client/Kafka/Redis giả, không gọi
Threads thật) và handler type=channel_posts của consumer."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from social_crawler import crawl_request_consumer as consumer
from social_crawler.spiders.keyword_filter import ChannelMovieMatcher
from social_crawler.spiders.threads.auth.graphql_client import ThreadsGraphQLClient
from social_crawler.spiders.threads.features.channel_posts import channel_posts
from social_crawler.spiders.threads.features.channel_posts.extract import ChannelPostsQueryError, extract_channel_page

USER = {"pk": "63150926090", "username": "minnnsb", "full_name": "Quang Minh", "is_verified": False}


def _post(pk: str, code: str, text: str, taken_at: int, user: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "pk": pk,
        "id": f"{pk}_{USER['pk']}",
        "code": code,
        "user": user or USER,
        "caption": {"text": text},
        "like_count": 7737,
        "taken_at": taken_at,
        "media_type": 2,
        "image_versions2": {"candidates": [{"url": "https://cdn/img.jpg"}]},
        "video_versions": [{"url": "https://cdn/v.mp4"}],
        "text_post_app_info": {
            "direct_reply_count": 33,
            "repost_count": 779,
            "quote_count": 0,
            "reshare_count": 304,
            "is_reply": False,
            "text_fragments": {"fragments": []},
        },
    }


def _response(posts: list[list[dict[str, Any]]], *, has_next: bool = False, cursor: str | None = None) -> dict:
    return {
        "data": {
            "mediaData": {
                "edges": [
                    {
                        "node": {"thread_items": [{"post": p} for p in thread], "thread_type": "thread", "id": "x"},
                        "cursor": "",
                    }
                    for thread in posts
                ],
                "page_info": {"has_next_page": has_next, "end_cursor": cursor},
            }
        },
        "extensions": {"is_final": True},
    }


NOW = int(time.time())
SAMPLE = _response(
    [
        [_post("3671548032039695522", "DLz9XuBgrSi", "Đi xem Trại Buôn Người cùng hội bạn", NOW - 3600)],
        [
            _post("1", "A1", "chuyện hôm nay", NOW - 7200),
            _post("2", "A2", "phần tiếp", NOW - 7100),
        ],
        [_post("9", "Q9", "bài của người khác", NOW - 100, user={"pk": "42", "username": "other"})],
    ],
    has_next=True,
    cursor="CURSOR1",
)


def test_extract_channel_page_sample() -> None:
    posts, page_info = extract_channel_page(SAMPLE, "63150926090")
    assert [p["post_id"] for p in posts] == ["3671548032039695522", "1", "2"]  # bài của người khác bị bỏ
    first = posts[0]
    assert first["url"] == "https://www.threads.com/@minnnsb/post/DLz9XuBgrSi"
    assert first["author_username"] == "minnnsb" and first["author_id"] == "63150926090"
    assert first["like_count"] == 7737 and first["reply_count"] == 33 and first["repost_count"] == 779
    assert first["timestamp"] == NOW - 3600 and first["media_url"] == "https://cdn/img.jpg"
    assert page_info == {"has_next_page": True, "end_cursor": "CURSOR1"}


@pytest.mark.parametrize(
    "response", [{"errors": [{"message": "x"}], "data": None}, {"data": {}}, "nope", {"data": {"mediaData": {}}}]
)
def test_extract_channel_page_unexpected_shape_raises(response: Any) -> None:
    with pytest.raises(ChannelPostsQueryError):
        extract_channel_page(response)


def test_fetch_profile_posts_sends_profile_tab_query() -> None:
    client = ThreadsGraphQLClient.__new__(ThreadsGraphQLClient)
    captured: dict[str, Any] = {}

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return SAMPLE

    client._run = fake_run  # type: ignore[method-assign]
    assert client.fetch_profile_posts("63150926090", "CUR") is SAMPLE
    assert captured["doc_id"] == "29049009161361820"
    assert captured["friendly_name"] == "BarcelonaProfileThreadsTabRefetchableDirectQuery"
    assert captured["overrides"] == {"userID": "63150926090", "after": "CUR", "first": 10}
    assert captured["extra_headers"] == {"x-root-field-name": "xdt_api__v1__text_feed__user_id__profile__connection"}
    template = captured["template"]
    assert template["userID"] == "" and template["after"] is None and template["first"] == 10
    assert template["__relay_internal__pv__BarcelonaIsLoggedInrelayprovider"] is True


def test_matcher_hashtag_title_keyword() -> None:
    matcher = ChannelMovieMatcher("Trại Buôn Người", '["TRẠI BUÔN NGƯỜI: Phần 2"]', '["#traibuonnguoi", "tbn"]')
    assert matcher.keep("Review #TraiBuonNguoi nè")  # hashtag trùng
    assert matcher.keep("hello", topic="TBN")  # chủ đề Threads trùng hashtag
    assert matcher.keep("đi xem trại buôn người thôi")  # từ khoá
    assert not matcher.keep("#tbnfan cuối tuần")  # hashtag phải trùng hẳn, không phải chuỗi con
    assert not matcher.keep(None)
    loose = ChannelMovieMatcher(None, ["Mai"], ["maiphim"])
    assert not loose.keep("Mai đi học")  # tên quá ngắn không so chuỗi con
    assert ChannelMovieMatcher().keep("bất kỳ")  # không có điều kiện -> giữ hết


class FakeKafka:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def publish(self, *, topic: str, key: str, value: dict[str, Any]) -> bool:
        self.messages.append({"topic": topic, "key": key, "value": value})
        return True


class FakeClient:
    def __init__(self, pages: list[dict[str, Any]], profile: dict[str, Any] | None = None) -> None:
        self.pages = pages
        self.profile = profile if profile is not None else {"pk": USER["pk"], "username": USER["username"]}
        self.cursors: list[str | None] = []

    def fetch_profile(self, username: str) -> dict[str, Any] | None:
        return self.profile

    def fetch_profile_posts(self, user_id: str, after: str | None = None) -> dict[str, Any]:
        self.cursors.append(after)
        return self.pages[len(self.cursors) - 1]


def _run_spider(monkeypatch: pytest.MonkeyPatch, client: FakeClient, **kwargs: Any) -> tuple[Any, list[Any]]:
    kafka = FakeKafka()
    monkeypatch.setattr(channel_posts, "KafkaPublisher", lambda: kafka)
    monkeypatch.setattr(channel_posts, "ThreadsGraphQLClient", lambda redis_cache=None: client)
    spider = channel_posts.ThreadsChannelPostsSpider(username="@minnnsb", keyword_id="kw1", dedupe="false", **kwargs)

    async def collect() -> list[Any]:
        return [item async for item in spider.start()]

    items = asyncio.run(collect())
    return kafka, items


def test_spider_publishes_only_matching_posts_in_search_format(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient([SAMPLE, _response([], has_next=False)])
    kafka, items = _run_spider(monkeypatch, client, keyword="Trại Buôn Người", max_pages=3)
    assert len(kafka.messages) == 1 and len(items) == 1
    message = kafka.messages[0]
    assert message["key"] == "threads:3671548032039695522"
    value = message["value"]
    assert value["platform"] == "threads" and value["keyword_id"] == "kw1" and value["source"] == "threads_channel"
    assert value["post_id"] == "3671548032039695522" and value["message"].startswith("Đi xem")
    assert client.cursors == [None, "CURSOR1"]  # trang 2 rỗng, has_next_page=false -> dừng


def test_spider_stops_at_max_pages_and_day_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    old = NOW - 90 * 86_400
    old_page = _response([[_post("5", "B5", "Trại Buôn Người", old)]], has_next=True, cursor="C2")
    client = FakeClient([SAMPLE, old_page, SAMPLE])
    kafka, _ = _run_spider(monkeypatch, client, keyword="Trại Buôn Người", max_pages=5, days=60)
    assert client.cursors == [None, "CURSOR1"]  # cả trang 2 cũ hơn 60 ngày -> không lật tiếp
    assert [m["value"]["post_id"] for m in kafka.messages] == ["3671548032039695522"]

    client = FakeClient([SAMPLE, SAMPLE, SAMPLE])
    _run_spider(monkeypatch, client, keyword="Trại Buôn Người", max_pages=2)
    assert client.cursors == [None, "CURSOR1"]


def test_spider_channel_not_found_finishes_without_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient([], profile={})
    kafka, items = _run_spider(monkeypatch, client, keyword="x")
    assert not kafka.messages and not items and client.cursors == []


def test_spider_unexpected_response_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient([{"errors": [{"message": "doc_id"}]}])
    with pytest.raises(SystemExit) as exc:
        _run_spider(monkeypatch, client, keyword="x")
    assert exc.value.code == 1


def test_consumer_builds_channel_posts_args(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    class Cache:
        def set(self, *a: Any, **k: Any) -> None:
            pass

        def delete(self, *a: Any) -> None:
            pass

    async def fake_session(**_k: Any) -> bool:
        return True

    async def fake_subprocess(args: list[str], **_k: Any) -> int:
        calls.append(args)
        return 0

    monkeypatch.setattr(consumer, "RedisCache", Cache)
    monkeypatch.setattr(consumer, "_stopped_for", lambda **_k: False)
    monkeypatch.setattr(consumer, "_ensure_threads_session", fake_session)
    monkeypatch.setattr(consumer, "_run_subprocess", fake_subprocess)
    request = {
        "type": "channel_posts",
        "platform": "threads",
        "username": "@minnnsb",
        "keyword_id": "kw1",
        "keyword": "Trại Buôn Người",
        "match_terms": ["Trại Buôn Người"],
        "match_hashtags": ["traibuonnguoi"],
        "max_pages": 3,
        "days": 60,
        "run_id": "r1",
    }
    asyncio.run(consumer._run_threads_channel_posts(request))
    args = calls[0]
    assert args[1:3] == ["crawl", "threads_channel_posts"]
    assert "username=minnnsb" in args and "keyword_id=kw1" in args
    assert 'match_terms=["Trại Buôn Người"]' in args and 'match_hashtags=["traibuonnguoi"]' in args
    assert "max_pages=3" in args and "days=60" in args

    with pytest.raises(consumer.CrawlJobFailed):
        asyncio.run(consumer._run_threads_channel_posts({**request, "keyword_id": None}))
