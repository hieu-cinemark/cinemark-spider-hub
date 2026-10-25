"""Job post_metrics: đọc đúng số của bài (kể cả trang reel lẫn reel gợi ý), phân loại bị chặn, cooldown proxy và rơi về
page_feed cho bài không lấy được số."""

import asyncio
import json
from typing import ClassVar

from social_crawler.consumer.jobs import post_metrics as job
from social_crawler.spiders.facebook.features.post_metrics import post_metrics as pm
from social_crawler.spiders.facebook.features.post_metrics.post_metrics import FetchResult

PAGE_POST_HTML = (
    '{"feedback":{"reaction_count":{"count":1397},"share_count":{"count":16},'
    '"comment_rendering_instance":{"comments":{"total_count":32}}}}'
    '{"other":{"reaction_count":{"count":2}}}'
)
# Reel gợi ý xuất hiện TRƯỚC reel của bài: phải lấy khối ngay sau id 988965410142997.
REEL_HTML = (
    '{"video_id":"111","unified_reactors":{"count":640},"total_comment_count":67}'
    + "x" * 30_000
    + '{"video_id":"988965410142997","unified_reactors":{"count":3},"total_comment_count":1}'
)
REEL_URL = "https://www.facebook.com/reel/988965410142997/"


def test_parse_page_post_takes_first_feedback_block():
    assert pm.parse_metrics(PAGE_POST_HTML, "https://www.facebook.com/a23/posts/pfbid0abc") == {
        "like": 1397,
        "comment": 32,
        "share": 16,
    }


def test_parse_reel_reads_block_after_its_own_id():
    assert pm.parse_metrics(REEL_HTML, REEL_URL) == {"like": 3, "comment": 1, "share": None}


def test_parse_without_metrics_is_empty():
    assert pm.parse_metrics("<html>Log in to Facebook</html>", "https://www.facebook.com/x/posts/1") == {}


def test_classify():
    assert pm.classify(200, "https://www.facebook.com/login/?next=x", "", {}) == "login_redirect"
    assert pm.classify(404, "u", "", {}) == "unavailable"
    assert pm.classify(400, "u", "", {}) == "http_error"
    assert pm.classify(200, "u", "This content isn't available right now", {}) == "unavailable"
    assert pm.classify(200, "u", "<html></html>", {}) == "no_metrics"
    assert pm.classify(200, "u", "", {"like": 1}) == "ok"


class FakeCache:
    def __init__(self):
        self.values = {}
        self.lists = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.values[key] = value

    def delete(self, key):
        self.values.pop(key, None)

    def lpush_json(self, key, value):
        self.lists.setdefault(key, []).insert(0, json.dumps(value))


class FakePublisher:
    sent: ClassVar[list] = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def publish(self, topic, key, value):
        FakePublisher.sent.append((topic, key, value))
        return True


def _setup(monkeypatch, results: dict[str, FetchResult], proxies=(10, 11)):
    cache = FakeCache()
    FakePublisher.sent = []
    calls = []

    def fake_fetch(url, proxy):
        calls.append((url, proxy))
        return results[url]

    monkeypatch.setattr(job, "RedisCache", lambda: cache)
    monkeypatch.setattr(job, "KafkaPublisher", FakePublisher)
    monkeypatch.setattr(job, "fetch_post_metrics", fake_fetch)
    monkeypatch.setattr(job, "_stopped_for", lambda **_: False)
    monkeypatch.setattr(job, "build_proxy_url", lambda p: f"http://proxy{p['id']}")
    monkeypatch.setattr(job, "DELAY_SECONDS", 0.0)
    monkeypatch.setattr(job, "JITTER_SECONDS", 0.0)
    monkeypatch.setattr(job, "ENABLED", True)
    monkeypatch.setattr(job, "get_setting", lambda key: True)
    monkeypatch.setattr(job, "_usable_proxies", lambda: [{"id": i, "url": f"h{i}:1"} for i in proxies])
    return cache, calls


def _ok(like, comment=None, share=None):
    return FetchResult("ok", 200, "u", "", {"like": like, "comment": comment, "share": share})


def _blocked():
    return FetchResult("login_redirect", 200, "https://www.facebook.com/login", "", {})


def _request(urls, pages=None):
    pages = pages or {}
    return {
        "platform": "facebook",
        "type": "post_metrics",
        "posts": [{"external_id": f"e{i}", "url": u, "page_url": pages.get(u)} for i, u in enumerate(urls)],
        "fallback_page_feeds": {p: {"type": "page_feed", "page_url": p} for p in set(pages.values()) if p},
    }


def _priority(cache):
    return [json.loads(x) for x in cache.lists.get("crawl_priority:facebook", [])]


def test_ok_posts_publish_metrics_and_no_fallback(monkeypatch):
    cache, calls = _setup(monkeypatch, {"u1": _ok(10, 2, 1), "u2": _ok(5, None, None)})
    asyncio.run(job._run_post_metrics(_request(["u1", "u2"], {"u1": "P1", "u2": "P1"})))
    messages = [value for _, _, value in FakePublisher.sent]
    assert messages[0] == {
        "platform": "facebook",
        "kind": "metrics",
        "source": "logged_out",
        "post_id": "e0",
        "url": "u1",
        "reactions_count": 10,
        "comments_count": 2,
        "shares_count": 1,
    }
    assert "comments_count" not in messages[1] and "shares_count" not in messages[1]
    assert [proxy for _, proxy in calls] == ["http://proxy10", "http://proxy11"]  # xoay vòng
    assert _priority(cache) == []


def test_blocked_proxy_cools_down_and_failed_posts_fall_back(monkeypatch):
    monkeypatch.setattr(job, "BLOCK_STRIKES", 2)
    results = {f"u{i}": _blocked() for i in range(5)}
    cache, calls = _setup(monkeypatch, results, proxies=(10,))
    pages = {"u0": "P1", "u1": "P1", "u2": "P2", "u3": "P2", "u4": None}
    asyncio.run(job._run_post_metrics(_request(list(results), pages)))
    assert len(calls) == 2  # proxy duy nhất nghỉ sau 2 lần bị chặn, không gọi tiếp
    assert "fb_logged_out:proxy_cooldown:10" in cache.values
    assert sorted(r["page_url"] for r in _priority(cache)) == ["P1", "P2"]
    assert FakePublisher.sent == []


def test_cooled_proxy_is_skipped_and_disabled_means_fallback(monkeypatch):
    cache, calls = _setup(monkeypatch, {"u1": _ok(1)}, proxies=(10,))
    cache.values["fb_logged_out:proxy_cooldown:10"] = {"status": "login_redirect"}
    asyncio.run(job._run_post_metrics(_request(["u1"], {"u1": "P1"})))
    assert calls == []
    assert [r["page_url"] for r in _priority(cache)] == ["P1"]

    cache, calls = _setup(monkeypatch, {"u1": _ok(1)})
    monkeypatch.setattr(job, "ENABLED", False)
    asyncio.run(job._run_post_metrics(_request(["u1"], {"u1": "P1"})))
    assert calls == [] and [r["page_url"] for r in _priority(cache)] == ["P1"]


def test_dashboard_switch_off_means_fallback(monkeypatch):
    cache, calls = _setup(monkeypatch, {"u1": _ok(1)})
    monkeypatch.setattr(job, "get_setting", lambda key: False)
    asyncio.run(job._run_post_metrics(_request(["u1"], {"u1": "P1"})))
    assert calls == [] and [r["page_url"] for r in _priority(cache)] == ["P1"]
