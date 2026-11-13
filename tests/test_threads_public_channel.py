"""Kênh Threads không đăng nhập: client gửi đủ cờ provider, lật trang dừng đúng mốc, job gửi bài source=page."""

import asyncio

import pytest

from social_crawler.consumer.jobs import threads_channel as job
from social_crawler.spiders.threads.public import channel
from social_crawler.spiders.threads.public.client import QUERIES, ThreadsPublicBlocked, variables_for


def _node(pk, ts, author="111", username="kenh"):
    return {
        "pk": pk,
        "code": f"C{pk}",
        "caption": {"text": f"bài {pk}"},
        "taken_at": ts,
        "like_count": 1,
        "user": {"pk": author, "username": username},
        "text_post_app_info": {"direct_reply_count": 0},
    }


class FakeClient:
    def __init__(self, pages, user=None, blocked_at=None):
        self.pages = pages
        self.user = user if user is not None else {"pk": "111", "username": "kenh", "follower_count": 9}
        self.blocked_at = blocked_at
        self.calls = 0

    def user_by_username(self, handle):
        return self.user or None

    def profile_threads_page(self, user_id, cursor=None):
        if self.blocked_at is not None and self.calls == self.blocked_at:
            raise ThreadsPublicBlocked("HTTP 429")
        nodes = self.pages[self.calls]
        self.calls += 1
        nxt = f"c{self.calls}" if self.calls < len(self.pages) else None
        return {"mediaData": {"edges": [{"node": {"thread_items": [{"post": n}]}} for n in nodes]}}, nxt

    def close(self):
        pass


def test_handle_from_url():
    assert channel.handle_from_url("https://www.threads.com/@Kenh.Phim_1") == "kenh.phim_1"
    assert channel.handle_from_url("https://threads.net/@abc/post/XYZ") == "abc"
    assert channel.handle_from_url("@abc") == "abc"
    assert channel.handle_from_url("https://facebook.com/abc") is None


def test_every_declared_provider_is_sent():
    for name, query in QUERIES.items():
        v = variables_for(name, x=1)
        assert v["x"] == 1
        assert all(f"__relay_internal__pv__{p}relayprovider" in v for p in query["providers"])
    assert (
        variables_for("BarcelonaUsernameHovercardImplDirectQuery")[
            "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider"
        ]
        is False
    )


def test_stops_when_whole_page_is_older_than_since(monkeypatch):
    monkeypatch.setattr(channel.time, "sleep", lambda s: None)
    pages = [[_node("1", 300), _node("2", 250)], [_node("3", 150), _node("4", 120)], [_node("5", 50)]]
    batches = []
    r = channel.fetch_channel(
        "kenh", since_ts=200, client=FakeClient(pages), on_batch=lambda b: batches.append(b) or True
    )
    assert r.status == "ok" and r.stop_reason == "since" and r.pages == 2
    assert [p["post_id"] for b in batches for p in b] == ["1", "2"]
    assert r.followers == 9 and r.oldest_ts == 120


def test_quoted_posts_of_other_authors_are_dropped(monkeypatch):
    monkeypatch.setattr(channel.time, "sleep", lambda s: None)
    pages = [[_node("1", 300), _node("9", 300, author="999", username="khac")]]
    batches = []
    r = channel.fetch_channel("kenh", client=FakeClient(pages), on_batch=lambda b: batches.append(b) or True)
    assert r.stop_reason == "exhausted"
    assert [p["post_id"] for p in batches[0]] == ["1"]


def test_not_found_and_blocked(monkeypatch):
    monkeypatch.setattr(channel.time, "sleep", lambda s: None)
    assert channel.fetch_channel("x", client=FakeClient([], user={})).status == "not_found"
    r = channel.fetch_channel("kenh", client=FakeClient([[_node("1", 9)]], blocked_at=0))
    assert r.status == "blocked" and r.pages == 0


class FakeCache:
    def __init__(self):
        self.store = {}

    def add_if_new(self, key, ttl):
        if key in self.store:
            return False
        self.store[key] = 1
        return True

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.store[key] = value

    def delete(self, key):
        self.store.pop(key, None)


class FakeKafka:
    def __init__(self):
        self.sent = []

    async def publish(self, topic, key, value):
        self.sent.append((key, value))
        return True


def test_publish_marks_source_page_and_dedupes():
    cache, kafka = FakeCache(), FakeKafka()
    request = {"page_url": "https://www.threads.com/@kenh", "keyword_id": None}
    posts = [{"post_id": "1", "message": "a"}, {"post_id": "2", "message": "b"}]
    assert asyncio.run(job._publish(cache, kafka, request, posts)) == 2
    assert asyncio.run(job._publish(cache, kafka, request, posts)) == 0
    key, value = kafka.sent[0]
    assert key == "threads:1"
    assert value["platform"] == "threads" and value["source"] == "page"
    assert value["source_page"] == "https://www.threads.com/@kenh"


def test_pick_exit_skips_cooled_down(monkeypatch):
    cache = FakeCache()
    monkeypatch.setattr(job, "_exits", lambda: [("proxy1", "http://a"), ("proxy2", "http://b")])
    cache.set(job.COOLDOWN_KEY_TMPL.format(exit="proxy1"), 1)
    assert {job.pick_exit(cache)[0] for _ in range(4)} == {"proxy2"}
    cache.set(job.COOLDOWN_KEY_TMPL.format(exit="proxy2"), 1)
    assert job.pick_exit(cache) is None


def test_invalid_handle_fails():
    with pytest.raises(job.CrawlJobFailed):
        asyncio.run(job.run_threads_channel({"page_url": "https://facebook.com/x"}))
