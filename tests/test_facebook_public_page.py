"""Đọc page không đăng nhập: phân loại, không dùng proxy gán tài khoản, chặn thì nghỉ proxy, chỉ gửi bài mới."""

import asyncio
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from social_crawler.consumer.common import CrawlJobRetryable, CrawlJobSkipped
from social_crawler.consumer.jobs import public_page as job
from social_crawler.spiders.facebook.features.public_page import public_page as page


class Cache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)

    def add_if_new(self, key, ttl_seconds):
        if key in self.data:
            return False
        self.data[key] = 1
        return True


HTML = (
    '<meta property="og:title" content="Tr&#x1ea1;i Bu&#xf4;n Ng&#x1b0;&#x1edd;i Movie" />'
    '<meta property="og:description" content="Trang. 42.854 người theo dõi · 4.030 người đang nói về điều này." />'
    '"userID":"61574279384043","userVanity":"traibuon","category_name":"Movie","is_verified":true'
)


def test_parse_info_reads_counts_and_identity():
    info = page.parse_info(HTML)
    assert info["name"] == "Trại Buôn Người Movie"
    assert info["followers"] == 42854 and info["talking_about"] == 4030
    assert info["page_id"] == "61574279384043" and info["category"] == "Movie" and info["verified"] is True


def test_parse_posts_skips_bad_json_and_dedupes(monkeypatch):
    monkeypatch.setattr(page, "extract_response", lambda blob: ([{"post_id": "1"}, {"post_id": "1"}], []))
    html = (
        f'<script type="application/json">{json.dumps({"a": 1})}</script>'
        '<script type="application/json">{broken</script>'
    )
    assert page.parse_posts(html) == [{"post_id": "1"}]


@pytest.mark.parametrize(
    "status,final,body,expected",
    [
        (200, "https://www.facebook.com/login/?next=x", "", "login_redirect"),
        (404, "https://www.facebook.com/x", "", "unavailable"),
        (500, "https://www.facebook.com/x", "", "http_error"),
        (200, "https://www.facebook.com/x", "<html></html>", "no_data"),
        (200, "https://www.facebook.com/x", HTML, "ok"),
    ],
)
def test_fetch_classifies(monkeypatch, status, final, body, expected):
    response = SimpleNamespace(status_code=status, url=final, text=body, headers={})
    _session(monkeypatch, response)
    assert page.fetch_public_page("https://www.facebook.com/x", "http://p").status == expected


def test_pick_proxy_skips_a_proxy_at_its_parallel_limit(monkeypatch):
    monkeypatch.setattr(job, "PROXY_IDS", [15, 10])
    monkeypatch.setattr(job, "MAX_PER_PROXY", 1)
    monkeypatch.setattr(job, "list_proxies_without_meta_accounts", lambda: [{"id": 15}, {"id": 10}])
    monkeypatch.setitem(job._active, 15, 1)
    assert job._pick_proxy(Cache())["id"] == 10
    monkeypatch.setitem(job._active, 10, 1)
    assert job._pick_proxy(Cache()) is None


def test_pick_proxy_respects_order_cooldown_and_assignment(monkeypatch):
    monkeypatch.setattr(job, "PROXY_IDS", [17, 10])
    monkeypatch.setattr(job, "list_proxies_without_meta_accounts", lambda: [{"id": 10}, {"id": 17}, {"id": 18}])
    cache = Cache()
    assert job._pick_proxy(cache)["id"] == 17
    cache.set(job.COOLDOWN_KEY_TMPL.format(proxy_id=17), "login_redirect")
    assert job._pick_proxy(cache)["id"] == 10
    cache.set(job.COOLDOWN_KEY_TMPL.format(proxy_id=10), "login_redirect")
    assert job._pick_proxy(cache) is None
    monkeypatch.setattr(job, "list_proxies_without_meta_accounts", lambda: [{"id": 18}])
    assert job._pick_proxy(Cache()) is None  # #18 chở tài khoản khác không có trong danh sách cho phép


def test_meta_filter_counts_disabled_accounts(monkeypatch):
    from social_crawler.db import proxies

    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(proxies, "connect", lambda: conn)
    proxies.list_proxies_without_meta_accounts()
    assert "pa.enabled" not in conn.execute.call_args.args[0]


def test_three_blocks_rest_the_proxy_and_success_resets():
    cache = Cache()
    proxy = {"id": 17}
    job._report(cache, proxy, "login_redirect", 200)
    job._report(cache, proxy, "ok", 200)
    job._report(cache, proxy, "login_redirect", 200)
    job._report(cache, proxy, "timeout", 0)
    assert not cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=17))
    job._report(cache, proxy, "login_redirect", 200)
    assert cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=17)) == "login_redirect"


def _setup(monkeypatch, result, sent=True):
    cache = Cache()
    monkeypatch.setattr(job, "RedisCache", lambda: cache)
    monkeypatch.setattr(job, "_pick_proxy", lambda c: {"id": 17, "url": "h:1"})
    monkeypatch.setattr(job, "build_proxy_url", lambda p: "http://p")
    monkeypatch.setattr(job, "fetch_public_page", lambda u, p, **kw: result)
    kafka = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), publish=AsyncMock(return_value=sent))
    monkeypatch.setattr(job, "KafkaPublisher", lambda: kafka)
    return cache, kafka


REQUEST = {"type": "public_page", "platform": "facebook", "page_url": "https://www.facebook.com/x", "keyword_id": "k"}


def test_new_post_keeps_page_feed_contract_and_seen_post_is_not_resent(monkeypatch):
    result = page.PublicPage("ok", 200, {"page_id": "9", "followers": 5}, [{"post_id": "1", "message": "m"}])
    cache, kafka = _setup(monkeypatch, result)
    asyncio.run(job._run_public_page(REQUEST))
    value = kafka.publish.await_args.kwargs["value"]
    assert value["source"] == "page" and value["keyword_id"] == "k" and value["source_page"] == REQUEST["page_url"]
    assert cache.get(f"page_feed_last:{REQUEST['page_url']}")["new"] == 1
    assert cache.get(job.INFO_KEY_TMPL.format(url=REQUEST["page_url"]))["followers"] == 5
    asyncio.run(job._run_public_page(REQUEST))
    assert kafka.publish.await_count == 1
    assert cache.get(f"page_feed_last:{REQUEST['page_url']}")["new"] == 0


def test_undelivered_post_stays_unseen_and_retries(monkeypatch):
    cache, _ = _setup(monkeypatch, page.PublicPage("ok", 200, {"page_id": "9"}, [{"post_id": "1"}]), sent=False)
    with pytest.raises(CrawlJobRetryable):
        asyncio.run(job._run_public_page(REQUEST))
    assert not [k for k in cache.data if k.endswith(":1")]


def test_blocked_read_writes_nothing_and_no_proxy_pauses_scheduler(monkeypatch):
    cache, kafka = _setup(monkeypatch, page.PublicPage("login_redirect", 200))
    with pytest.raises(CrawlJobSkipped):
        asyncio.run(job._run_public_page(REQUEST))
    kafka.publish.assert_not_awaited()
    assert f"page_feed_last:{REQUEST['page_url']}" not in cache.data
    monkeypatch.setattr(job, "_pick_proxy", lambda c: None)
    with pytest.raises(CrawlJobSkipped):
        asyncio.run(job._run_public_page(REQUEST))
    assert cache.get(job.PAUSED_KEY) == 1


class WorkerCache(Cache):
    def __init__(self, queue=()):
        super().__init__()
        self.queue = list(queue)
        self.acked = []

    def claim_json(self, key, processing):
        return (self.queue.pop(0), json.loads(self.queue_raw)) if self.queue else (None, None)

    def ack(self, processing, raw):
        self.acked.append(raw)


def _worker_env(monkeypatch, raw=None, proxy=True, draining=False):
    from social_crawler.consumer import loop

    cache = WorkerCache([raw] if raw else [])
    cache.queue_raw = raw
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: draining)
    monkeypatch.setattr(loop.public_page_job, "_pick_proxy", lambda c: {"id": 15} if proxy else None)
    handle = AsyncMock(return_value=False)
    monkeypatch.setattr(loop, "_handle_request", handle)
    return loop, cache, handle


def test_public_page_is_not_on_the_idle_queue():
    from social_crawler.consumer import loop

    assert "public_page" not in loop.IDLE_JOB_TYPES


def test_worker_runs_job_and_acks(monkeypatch):
    raw = json.dumps(REQUEST)
    loop, cache, handle = _worker_env(monkeypatch, raw)
    assert asyncio.run(loop._run_one_public_page(cache)) is True
    assert handle.await_args.args[0]["page_url"] == REQUEST["page_url"]
    assert cache.acked == [raw]


def test_worker_failed_job_is_acked_and_does_not_escape(monkeypatch):
    raw = json.dumps(REQUEST)
    loop, cache, handle = _worker_env(monkeypatch, raw)
    handle.side_effect = RuntimeError("boom")
    assert asyncio.run(loop._run_one_public_page(cache)) is True
    assert cache.acked == [raw]


def test_worker_leaves_queue_alone_without_proxy_and_pauses_filler(monkeypatch):
    raw = json.dumps(REQUEST)
    loop, cache, handle = _worker_env(monkeypatch, raw, proxy=False)
    assert asyncio.run(loop._run_one_public_page(cache)) is False
    handle.assert_not_awaited()
    assert cache.queue == [raw] and cache.get(job.PAUSED_KEY) == 1


def test_busy_proxies_do_not_pause_filler(monkeypatch):
    loop, cache, _ = _worker_env(monkeypatch, json.dumps(REQUEST), proxy=False)
    monkeypatch.setitem(job._active, 15, 1)
    assert asyncio.run(loop._run_one_public_page(cache)) is False
    assert cache.get(job.PAUSED_KEY) is None


def test_worker_waits_during_stop(monkeypatch):
    raw = json.dumps(REQUEST)
    loop, cache, _ = _worker_env(monkeypatch, raw, draining=True)
    assert asyncio.run(loop._run_one_public_page(cache)) is False
    assert cache.queue == [raw]


def test_parallel_jobs_never_share_a_proxy_slot(monkeypatch):
    monkeypatch.setattr(job, "PROXY_IDS", [15, 10])
    monkeypatch.setattr(job, "MAX_PER_PROXY", 1)
    monkeypatch.setattr(job, "list_proxies_without_meta_accounts", lambda: [{"id": 15}, {"id": 10}])
    cache = Cache()
    monkeypatch.setattr(job, "RedisCache", lambda: cache)
    monkeypatch.setattr(job, "build_proxy_url", lambda p: f"http://{p['id']}")
    used = []
    both_running = threading.Barrier(2, timeout=5)

    def fetch(url, proxy, **kw):
        used.append(proxy)
        both_running.wait()
        return page.PublicPage("no_data", 200)

    monkeypatch.setattr(job, "fetch_public_page", fetch)

    async def both():
        return await asyncio.gather(
            job._run_public_page(REQUEST), job._run_public_page(REQUEST), return_exceptions=True
        )

    asyncio.run(both())
    assert sorted(used) == ["http://10", "http://15"]
    assert not any(job._active.values())


def _session(monkeypatch, initial, *responses):
    session = Mock()
    session.__enter__ = Mock(return_value=session)
    session.__exit__ = Mock(return_value=False)
    session.get.return_value = initial
    session.post.side_effect = responses
    factory = Mock(return_value=session)
    monkeypatch.setattr(page.cffi_requests, "Session", factory)
    monkeypatch.setattr(page.time, "sleep", lambda seconds: None)
    return session, factory


def _story(identifier):
    return {
        "__typename": "Story",
        "id": f"story:{identifier}",
        "post_id": identifier,
        "creation_time": int(page.time.time()) - 100,
        "actors": [{"id": "61574279384043", "name": "Movie"}],
        "comet_sections": {"content": {"story": {"message": {"text": identifier}}}},
    }


def _html():
    payload = {
        "preload": {
            "queryName": "ProfileCometTimelineFeedQuery",
            "actorID": "0",
            "variables": {"userID": "61574279384043", "count": 1, "runWithContinuationKey": False, "provider": True},
        },
        "data": {"user": {"timeline_list_feed_units": {"edges": [{"node": _story("1")}]}}},
        "unrelated": {"page_info": {"has_next_page": False}},
        "deferred": {
            "path": ["user", "timeline_list_feed_units"],
            "data": {"page_info": {"has_next_page": True, "end_cursor": "cursor-1"}},
        },
    }
    body = HTML + '<script type="application/json">' + json.dumps(payload) + "</script>"
    body += '<script type="application/json">["LSD",[],{"token":"guest-lsd"}]</script>'
    body += '"server_revision":123'
    return SimpleNamespace(status_code=200, url=REQUEST["page_url"], headers={}, text=body)


def _graphql(ids, cursor, more, *, errors=None):
    initial = {
        "data": {"node": {"id": "61574279384043", "timeline_list_feed_units": {"edges": []}}},
        "errors": errors or [],
    }
    chunks = [initial]
    for index, identifier in enumerate(ids):
        chunks.append(
            {"path": ["node", "timeline_list_feed_units", "edges", index], "data": {"node": _story(identifier)}}
        )
    chunks.append(
        {
            "path": ["node", "timeline_list_feed_units"],
            "data": {"page_info": {"has_next_page": more, "end_cursor": cursor}},
        }
    )
    text = "for(;;);" + "\n".join(json.dumps(c) for c in chunks)
    return SimpleNamespace(status_code=200, url="https://www.facebook.com/api/graphql/", headers={}, text=text)


def _doc_cache():
    cache = Cache()
    cache.set("facebook:public_pages:feed_doc:123", "28781619204843417")
    return cache


def test_guest_feed_follows_deferred_cursor_and_streamed_posts_without_accounts(monkeypatch):
    optional_error = {"path": ["node", "save_info", "save_flow"], "message": "field_exception", "severity": "ERROR"}
    session, factory = _session(
        monkeypatch,
        _html(),
        _graphql(["1", "2", "3"], "cursor-2", True, errors=[optional_error]),
        _graphql(["4"], "cursor-3", False),
    )
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", max_pages=0, cache=_doc_cache())
    assert [p["post_id"] for p in result.posts] == ["1", "2", "3", "4"]
    assert result.pages_fetched == 3 and result.stop_reason == "exhausted"
    assert result.pagination_status is None
    assert session.get.call_count == 1
    assert factory.call_args.kwargs["trust_env"] is False
    bodies = [call.kwargs["data"] for call in session.post.call_args_list]
    assert [json.loads(b["variables"])["cursor"] for b in bodies] == [None, "cursor-2"]
    assert all(b["av"] == b["__user"] == "0" and b["fb_dtsg"] == "" and b["lsd"] == "guest-lsd" for b in bodies)
    assert all(b["doc_id"] == "28781619204843417" for b in bodies)
    assert all("cookies" not in call.kwargs for call in session.post.call_args_list)


def test_stop_ends_a_long_job_and_keeps_fetched_posts(monkeypatch):
    session, _ = _session(monkeypatch, _html(), _graphql(["1"], "cursor-2", True), _graphql(["2"], "cursor-3", False))
    checks = iter([False, True])
    result = page.fetch_public_page(
        REQUEST["page_url"], "http://p", cache=_doc_cache(), should_stop=lambda: next(checks)
    )
    assert [p["post_id"] for p in result.posts] == ["1"]
    assert result.stop_reason == "cancelled" and result.pagination_status is None
    assert session.post.call_count == 1


@pytest.mark.parametrize("max_pages,expected_posts,reason", [(1, ["1"], "max_pages"), (2, ["1", "2"], "max_pages")])
def test_guest_feed_respects_page_limit(monkeypatch, max_pages, expected_posts, reason):
    session, _ = _session(monkeypatch, _html(), _graphql(["2"], "cursor-2", True))
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", max_pages=max_pages, cache=_doc_cache())
    assert [p["post_id"] for p in result.posts] == expected_posts
    assert result.stop_reason == reason and session.post.call_count == max_pages - 1


def test_login_redirect_during_pagination_preserves_posts_and_reports_block(monkeypatch):
    redirect = SimpleNamespace(
        status_code=302, url="https://www.facebook.com/api/graphql/", headers={"location": "/login/"}
    )
    session, _ = _session(monkeypatch, _html(), _graphql(["2"], "cursor-2", True), redirect)
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=_doc_cache())
    assert [p["post_id"] for p in result.posts] == ["1", "2"]
    assert result.pagination_status == result.stop_reason == "login_redirect"
    assert session.post.call_count == 2


def test_repeated_cursor_stops_instead_of_refetching_forever(monkeypatch):
    session, _ = _session(monkeypatch, _html(), _graphql(["2"], "cursor-2", True), _graphql(["3"], "cursor-2", True))
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", max_pages=0, cache=_doc_cache())
    assert result.stop_reason == "missing_or_repeated_cursor" and session.post.call_count == 2


def test_feed_does_not_confuse_missing_feed_with_exhaustion(monkeypatch):
    body = json.dumps({"data": {"node": {"id": "61574279384043", "comments": {"page_info": {"has_next_page": False}}}}})
    response = SimpleNamespace(status_code=200, url=REQUEST["page_url"], headers={}, text=body)
    _session(monkeypatch, _html(), response)
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=_doc_cache())
    assert result.stop_reason == "schema_changed" and result.pages_fetched == 1


def test_stale_doc_id_invalidates_only_public_doc_cache(monkeypatch):
    response = SimpleNamespace(
        status_code=200,
        url=REQUEST["page_url"],
        headers={},
        text=json.dumps(
            {
                "errors": [{"message": "GraphQL document with id 28781619204843417 was not found"}],
            }
        ),
    )
    session, _ = _session(monkeypatch, _html(), response)
    cache = _doc_cache()
    cache.set("facebook:page_feed_query:account", {"doc_id": "private"})
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=cache)
    assert result.pagination_status == "doc_id_stale" and session.post.call_count == 1
    assert cache.get("facebook:public_pages:feed_doc:123") is None
    assert cache.get("facebook:page_feed_query:account") == {"doc_id": "private"}


def test_discovers_public_operation_from_allowed_assets_and_caches_by_revision(monkeypatch):
    session = Mock()
    js = '__d("ProfileCometTimelineFeedRefetchQuery_facebookRelayOperation",[],function(){a.exports="12345678901234"})'
    session.get.return_value = SimpleNamespace(status_code=200, text=js, content=js.encode())
    html = '<script src="https://evil.test/private.js"></script><script src="https://static.xx.fbcdn.net/feed.js"></script>'
    cache = Cache()
    monkeypatch.delenv("FB_PUBLIC_PAGES_DOC_ID", raising=False)
    assert page._feed_doc_id(session, html, {"__rev": "9"}, cache) == "12345678901234"
    assert session.get.call_args.args[0] == "https://static.xx.fbcdn.net/feed.js"
    assert page._feed_doc_id(session, html, {"__rev": "9"}, cache) == "12345678901234"
    assert session.get.call_count == 1


def test_job_passes_pagination_limit_and_keeps_partial_posts_on_block(monkeypatch):
    result = page.PublicPage("ok", 302, {"page_id": "9"}, [{"post_id": "1"}], 2, "login_redirect", "login_redirect")
    cache, kafka = _setup(monkeypatch, result)
    fetch = Mock(return_value=result)
    monkeypatch.setattr(job, "fetch_public_page", fetch)
    asyncio.run(job._run_public_page({**REQUEST, "max_pages": 7}))
    kwargs = fetch.call_args.kwargs
    assert callable(kwargs.pop("should_stop"))
    assert kwargs == {"max_pages": 7, "since_days": 60, "cache": cache}
    assert cache.get(job.STRIKES_KEY_TMPL.format(proxy_id=17)) == 1
    assert kafka.publish.await_count == 1
    assert cache.get(f"page_feed_last:{REQUEST['page_url']}")["pagination_status"] == "login_redirect"


def test_kafka_start_or_publish_failure_does_not_lose_unsent_posts(monkeypatch):
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [{"post_id": "1"}, {"post_id": "2"}])
    cache, kafka = _setup(monkeypatch, result)
    kafka.start.side_effect = RuntimeError("broker unavailable")
    with pytest.raises(RuntimeError):
        asyncio.run(job._run_public_page(REQUEST))
    assert not any(k.startswith(job.SEEN_POSTS_KEY) for k in cache.data)
    kafka.start.side_effect = None
    kafka.publish.side_effect = RuntimeError("send interrupted")
    with pytest.raises(RuntimeError):
        asyncio.run(job._run_public_page(REQUEST))
    assert not any(k.startswith(job.SEEN_POSTS_KEY) for k in cache.data)


def _with_timestamps(response, timestamps):
    for identifier, timestamp in timestamps.items():
        response.text = response.text.replace(
            json.dumps(_story(identifier)), json.dumps({**_story(identifier), "creation_time": timestamp})
        )
    return response


def test_sixty_day_window_filters_html_and_api_posts_and_restarts_cursor(monkeypatch):
    now = 1800000000
    cutoff = now - 60 * 86400
    monkeypatch.setattr(page.time, "time", lambda: now)
    initial = _with_timestamps(_html(), {"1": cutoff - 1})
    batch = _with_timestamps(
        _graphql(["boundary", "recent", "old", "undated", "future"], "cursor-next", False),
        {"boundary": cutoff, "recent": now, "old": cutoff - 1, "undated": None, "future": now + 1},
    )
    session, _ = _session(monkeypatch, initial, batch)
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=_doc_cache())
    assert [p["post_id"] for p in result.posts] == ["boundary", "recent"]
    assert result.since_ts == cutoff and result.until_ts == now
    variables = json.loads(session.post.call_args.kwargs["data"]["variables"])
    assert variables["afterTime"] == cutoff - 1 and variables["beforeTime"] == now + 1
    assert variables["cursor"] is None and variables["omitPinnedPost"] is True


def test_default_window_can_fetch_more_than_ten_pages(monkeypatch):
    batches = [_graphql([str(i)], f"cursor-{i}", i < 12) for i in range(2, 13)]
    session, _ = _session(monkeypatch, _html(), *batches)
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=_doc_cache())
    assert result.pages_fetched == 12 and len(result.posts) == 12
    assert result.stop_reason == "exhausted" and session.post.call_count == 11


def test_all_old_page_stops_at_date_boundary_without_publishing_old_posts(monkeypatch):
    now = 1800000000
    monkeypatch.setattr(page.time, "time", lambda: now)
    old = _with_timestamps(_graphql(["old"], "cursor-old", True), {"old": now - 61 * 86400})
    session, _ = _session(monkeypatch, _html(), old)
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=_doc_cache())
    assert [p["post_id"] for p in result.posts] == ["1"]
    assert result.stop_reason == "since_days" and session.post.call_count == 1
