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

    def hincrby(self, key, field, amount=1, ttl_seconds=None):
        bucket = self.data.setdefault(key, {})
        bucket[field] = bucket.get(field, 0) + amount

    def set_nx(self, key, value, ttl_seconds):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def add_if_new(self, key, ttl_seconds):
        if key in self.data:
            return False
        self.data[key] = 1
        return True


@pytest.fixture(autouse=True)
def _no_real_ip_check(monkeypatch):
    monkeypatch.setattr(job, "_exit_ip", lambda proxy: None)
    monkeypatch.setenv("FB_PUBLIC_PAGES_WINDOW_DAYS", "0")
    monkeypatch.setattr(job, "_cancel_requested", lambda **kw: False)


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
        (429, "https://www.facebook.com/x", "", "rate_limited"),
        (200, "https://www.facebook.com/x", "<html></html>", "no_data"),
        (200, "https://www.facebook.com/x", HTML, "ok"),
    ],
)
def test_fetch_classifies(monkeypatch, status, final, body, expected):
    response = SimpleNamespace(status_code=status, url=final, text=body, headers={})
    _session(monkeypatch, response)
    assert page.fetch_public_page("https://www.facebook.com/x", "http://p").status == expected


def _cooled(monkeypatch, *ips):
    monkeypatch.setattr(job, "per_proxy", lambda: 1)
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [{"id": 15}])
    readings = iter(ips)
    monkeypatch.setattr(job, "_exit_ip", lambda proxy: next(readings))
    cache = Cache()
    for _ in range(3):
        job._report(cache, {"id": 15}, "login_redirect", 200)
    assert cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=15))
    cache.delete(job.IP_CHECK_KEY_TMPL.format(proxy_id=15))  # hết thời gian nghỉ tối thiểu
    return cache


def _next_check(cache):
    cache.delete(job.IP_CHECK_KEY_TMPL.format(proxy_id=15))


def test_minimum_rest_blocks_ip_check_and_grows_with_pace(monkeypatch):
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [{"id": 15}])
    measured = Mock(return_value="2.2.2.2")
    monkeypatch.setattr(job, "_exit_ip", measured)
    cache = Cache()
    ttls = {}
    original = cache.set

    def spy(key, value, ttl_seconds=None):
        ttls[key] = ttl_seconds
        original(key, value, ttl_seconds)

    monkeypatch.setattr(cache, "set", spy)
    job._report(cache, {"id": 15}, "rate_limited", 429)
    measured.reset_mock()
    assert job._pick_proxy(cache) is None
    measured.assert_not_called()
    assert ttls[job.IP_CHECK_KEY_TMPL.format(proxy_id=15)] == job.MIN_REST_SECONDS * 2


def test_blocked_signal_doubles_pace_to_limit_and_clean_job_halves(monkeypatch):
    monkeypatch.setattr(job, "MAX_PACE", 8)
    cache = Cache()
    proxy = {"id": 15}
    for expected in (2, 4, 8, 8):
        job._report(cache, proxy, "timeout", 0)
        assert job._pace(cache, 15) == expected
    job._report(cache, proxy, "ok", 200)
    assert job._pace(cache, 15) == 4


def test_new_ip_keeps_the_slower_pace(monkeypatch):
    cache = _cooled(monkeypatch, "1.1.1.1", "2.2.2.2")
    pace = job._pace(cache, 15)
    assert pace > 1 and job._pick_proxy(cache)["id"] == 15
    assert job._pace(cache, 15) == pace


def test_new_ip_ends_cooldown_at_once(monkeypatch):
    cache = _cooled(monkeypatch, "1.1.1.1", "2.2.2.2")
    assert job._pick_proxy(cache)["id"] == 15
    assert not cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=15))


def test_same_ip_keeps_cooldown_and_checks_are_throttled(monkeypatch):
    cache = _cooled(monkeypatch, "1.1.1.1", "1.1.1.1")
    assert job._pick_proxy(cache) is None
    assert job._pick_proxy(cache) is None  # lần đọc thứ hai sẽ ném StopIteration nếu bị đo lại
    assert cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=15))


def test_rotating_back_to_a_blocked_ip_does_not_release(monkeypatch):
    cache = _cooled(monkeypatch, "1.1.1.1", "3.3.3.3")
    cache.set(job.BLOCKED_IP_KEY_TMPL.format(ip="3.3.3.3"), 1)
    assert job._pick_proxy(cache) is None


def test_unknown_ip_at_block_time_uses_first_reading_as_baseline(monkeypatch):
    cache = _cooled(monkeypatch, None, "1.1.1.1", "1.1.1.1", "2.2.2.2")
    assert job._pick_proxy(cache) is None
    _next_check(cache)
    assert job._pick_proxy(cache) is None
    _next_check(cache)
    assert job._pick_proxy(cache)["id"] == 15


def test_failed_ip_reading_keeps_cooldown(monkeypatch):
    cache = _cooled(monkeypatch, "1.1.1.1", None)
    assert job._pick_proxy(cache) is None


def test_pick_proxy_skips_a_proxy_at_its_parallel_limit(monkeypatch):
    monkeypatch.setattr(job, "per_proxy", lambda: 1)
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [{"id": 15}, {"id": 10}])
    monkeypatch.setitem(job._active, 15, 1)
    assert job._pick_proxy(Cache())["id"] == 10
    monkeypatch.setitem(job._active, 10, 1)
    assert job._pick_proxy(Cache()) is None


def test_pick_proxy_uses_every_public_proxy_in_id_order_and_skips_cooldown(monkeypatch):
    # 13/11/2026: không còn danh sách ID trong env - mọi proxy nền tảng public không gán tài khoản đều được dùng.
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [{"id": 17}, {"id": 10}, {"id": 18}])
    cache = Cache()
    assert job._pick_proxy(cache)["id"] == 10
    cache.set(job.COOLDOWN_KEY_TMPL.format(proxy_id=10), "login_redirect")
    assert job._pick_proxy(cache)["id"] == 17
    cache.set(job.COOLDOWN_KEY_TMPL.format(proxy_id=17), "login_redirect")
    assert job._pick_proxy(cache)["id"] == 18
    cache.set(job.COOLDOWN_KEY_TMPL.format(proxy_id=18), "login_redirect")
    assert job._pick_proxy(cache) is None
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [])
    assert job._pick_proxy(Cache()) is None  # proxy chở tài khoản không bao giờ có trong danh sách public


def test_meta_filter_counts_disabled_accounts(monkeypatch):
    from social_crawler.db import proxies

    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(proxies, "connect", lambda: conn)
    proxies.list_proxies_without_meta_accounts()
    assert "pa.enabled" not in conn.execute.call_args.args[0]


def test_public_page_pool_is_every_enabled_public_proxy(monkeypatch):
    from social_crawler.db import proxies

    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(proxies, "connect", lambda: conn)
    assert proxies.list_public_page_proxies() == []
    query = conn.execute.call_args.args[0]
    # 13/11/2026: không còn ghim tài khoản - pool public là mọi proxy nền tảng public đang bật, không cooldown.
    assert "platform = 'public'" in query and "enabled = true" in query and "cooldown_until" in query
    assert "platform_accounts" not in query


def test_public_page_pool_db_failure_does_not_fall_back_to_account_proxies(monkeypatch):
    from social_crawler.db import proxies

    monkeypatch.setattr(proxies, "connect", Mock(side_effect=proxies.psycopg.OperationalError("db offline")))
    assert proxies.list_public_page_proxies() == []


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


@pytest.mark.parametrize("status", ["rate_limited", "access_restricted"])
def test_explicit_rate_limit_rests_proxy_immediately(status):
    cache = Cache()
    job._report(cache, {"id": 15}, status, 200)
    assert cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=15)) == status
    assert cache.get(job.STRIKES_KEY_TMPL.format(proxy_id=15)) is None


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
    with pytest.raises(job.PublicPageProxyUnavailable):
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

    def requeue_claim(self, processing, key, raw):
        self.queue.append(raw)
        return True


def _worker_env(monkeypatch, raw=None, proxy=True, draining=False):
    from social_crawler.consumer import loop

    cache = WorkerCache([raw] if raw else [])
    cache.queue_raw = raw
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: draining)
    monkeypatch.setattr(loop, "_facebook_search_waiting", lambda: False)
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


def test_worker_requeues_a_proxy_slot_race_without_acknowledging_or_losing_the_job(monkeypatch):
    raw = json.dumps(REQUEST)
    loop, cache, handle = _worker_env(monkeypatch, raw)
    handle.side_effect = job.PublicPageProxyUnavailable("proxy became busy")
    assert asyncio.run(loop._run_one_public_page(cache)) is False
    assert cache.queue == [raw] and cache.acked == []


def test_worker_clears_resource_pause_when_a_proxy_recovers(monkeypatch):
    loop, cache, _ = _worker_env(monkeypatch)
    cache.set(job.PAUSED_KEY, 1)
    assert asyncio.run(loop._run_one_public_page(cache)) is False
    assert cache.get(job.PAUSED_KEY) is None


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
    monkeypatch.setattr(job, "per_proxy", lambda: 1)
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [{"id": 15}, {"id": 10}])
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


def test_rate_limit_graphql_error_preserves_posts_and_does_not_invalidate_doc_id(monkeypatch):
    response = SimpleNamespace(
        status_code=200,
        url=REQUEST["page_url"],
        headers={},
        text=json.dumps({"errors": [{"code": 1675004, "message": "Rate limit exceeded", "severity": "CRITICAL"}]}),
    )
    session, _ = _session(monkeypatch, _html(), response)
    cache = _doc_cache()
    result = page.fetch_public_page(REQUEST["page_url"], "http://p", cache=cache)
    assert result.pagination_status == result.stop_reason == "access_restricted"
    assert result.pages_fetched == 1 and [p["post_id"] for p in result.posts] == ["1"]
    assert session.post.call_count == 1
    assert cache.get("facebook:public_pages:feed_doc:123") == "28781619204843417"


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
    kwargs = dict(fetch.call_args.kwargs)
    assert callable(kwargs.pop("should_stop")) and callable(kwargs.pop("on_batch"))
    assert kwargs.pop("request_interval") == job.BASE_INTERVAL_SECONDS
    assert kwargs == {"max_pages": 7, "since_days": 60, "cache": cache}
    assert cache.get(job.FEED_STRIKES_KEY_TMPL.format(proxy_id=17)) == 1
    assert not cache.get(job.STRIKES_KEY_TMPL.format(proxy_id=17))
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


DAY = 86400


def test_plan_windows_are_stable_newest_first_and_skip_finished():
    until = 100 * DAY + 3600
    windows = page.plan_windows(until - 21 * DAY, until, 7, {})
    assert [w[2] for w in windows] == [str(98 * DAY), str(91 * DAY), str(84 * DAY), None]
    assert windows[0] == (98 * DAY, until, str(98 * DAY))
    assert windows[-1][0] == until - 21 * DAY  # cửa sổ cũ nhất bị cắt, không bao giờ được ghi
    finished = {str(91 * DAY): 98 * DAY + DAY}
    assert str(91 * DAY) not in [w[2] for w in page.plan_windows(until - 21 * DAY, until, 7, finished)]


def test_plan_windows_reads_only_after_an_early_read_and_one_window_when_disabled():
    until = 100 * DAY + 3600
    early = {str(98 * DAY): 99.5 * DAY}
    newest = page.plan_windows(until - 14 * DAY, until, 7, early)[0]
    assert newest[0] == int(99.5 * DAY) - page.WINDOW_LATE_SECONDS
    assert page.plan_windows(10, 20, 0, {}) == [(10, 20, None)]


def _windowed(monkeypatch, responses):
    monkeypatch.setenv("FB_PUBLIC_PAGES_WINDOW_DAYS", "7")
    monkeypatch.setattr(page.time, "time", lambda: 100 * DAY + 3600)
    monkeypatch.setattr(page.time, "sleep", lambda s: None)
    return _session(monkeypatch, _html(), *responses)


def test_block_saves_finished_window_and_next_job_resumes_at_the_missing_one(monkeypatch):
    cache = _doc_cache()
    empty = _graphql([], "x", False)
    blocked = SimpleNamespace(status_code=429, url=REQUEST["page_url"], headers={}, text="")
    session, _ = _windowed(monkeypatch, [empty, blocked])
    first = page.fetch_public_page(REQUEST["page_url"], "http://p", since_days=14, cache=cache)
    assert first.pagination_status == "rate_limited" and first.windows_done == 1
    assert session.post.call_count == 2
    saved = cache.get(page._progress_key(REQUEST["page_url"]))
    assert list(saved) == [str(98 * DAY)]
    session, _ = _windowed(monkeypatch, [empty, empty, empty])
    second = page.fetch_public_page(REQUEST["page_url"], "http://p", since_days=14, cache=cache)
    assert second.stop_reason == "exhausted" and second.pagination_status is None
    after_times = [json.loads(c.kwargs["data"]["variables"])["afterTime"] for c in session.post.call_args_list]
    assert after_times == sorted(after_times, reverse=True) and len(after_times) == 3


def test_failed_batch_stops_and_does_not_record_the_window(monkeypatch):
    cache = _doc_cache()
    monkeypatch.setattr(page.time, "time", lambda: 100 * DAY + 3600)
    _windowed(monkeypatch, [_graphql(["1"], "x", False)])
    result = page.fetch_public_page(
        REQUEST["page_url"], "http://p", since_days=14, cache=cache, on_batch=lambda posts: False
    )
    assert result.pagination_status == "publish_failed" and result.windows_done == 0
    assert not cache.get(page._progress_key(REQUEST["page_url"]))


def test_job_publishes_each_batch_before_the_next_and_records_stats(monkeypatch):
    seen_at_batch = []
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [{"post_id": "1"}, {"post_id": "2"}], 3, "exhausted")
    cache, kafka = _setup(monkeypatch, result)

    def fetch(url, proxy, **kw):
        assert kw["on_batch"]([{"post_id": "1"}]) is True
        seen_at_batch.append(kafka.publish.await_count)
        assert kw["on_batch"]([{"post_id": "2"}]) is True
        seen_at_batch.append(kafka.publish.await_count)
        return result

    monkeypatch.setattr(job, "fetch_public_page", fetch)
    asyncio.run(job._run_public_page(REQUEST))
    assert seen_at_batch == [1, 2] and kafka.publish.await_count == 2
    assert cache.get(f"page_feed_last:{REQUEST['page_url']}")["new"] == 2
    stats = next(v for k, v in cache.data.items() if ":stats:" in k)
    assert stats == {"proxy17:html_ok": 1, "proxy17:feed_ok": 2, "proxy17:new_posts": 2}
    kafka.stop.assert_awaited_once()


def test_job_interval_follows_pace(monkeypatch):
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [])
    cache, _ = _setup(monkeypatch, result)
    cache.set(job.PACE_KEY_TMPL.format(proxy_id=17), 4)
    fetch = Mock(return_value=result)
    monkeypatch.setattr(job, "fetch_public_page", fetch)
    asyncio.run(job._run_public_page(REQUEST))
    assert fetch.call_args.kwargs["request_interval"] == job.BASE_INTERVAL_SECONDS * 4


def test_worker_waits_while_facebook_search_is_queued(monkeypatch):
    raw = json.dumps(REQUEST)
    loop, cache, handle = _worker_env(monkeypatch, raw)
    monkeypatch.setattr(loop, "_facebook_search_waiting", lambda: True)
    assert asyncio.run(loop._run_one_public_page(cache)) is False
    handle.assert_not_awaited()
    assert cache.queue == [raw]


def test_search_waiting_ignores_other_types_and_stale_entries(monkeypatch):
    from social_crawler.consumer import loop

    now = 1_000_000.0
    monkeypatch.setattr(loop.time, "time", lambda: now)
    monkeypatch.setattr(loop, "PUBLIC_YIELD_TO_SEARCH", True)
    monkeypatch.setattr(loop, "FACEBOOK_STALE_JOB_HOURS", 6)
    pending = []
    monkeypatch.setattr(loop, "RedisCache", lambda: SimpleNamespace(lrange_json=lambda key: pending))
    pending[:] = [{"type": "comments", "queued_at": now}, {"type": "public_page", "queued_at": now}]
    assert loop._facebook_search_waiting() is False
    pending[:] = [{"type": "search", "queued_at": now - 7 * 3600}]
    assert loop._facebook_search_waiting() is False
    pending[:] = [{"type": "search", "queued_at": now - 60}]
    assert loop._facebook_search_waiting() is True
    monkeypatch.setattr(loop, "PUBLIC_YIELD_TO_SEARCH", False)
    assert loop._facebook_search_waiting() is False


def test_running_job_shows_on_queue_page_and_is_cleared(monkeypatch):
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [])
    cache, _ = _setup(monkeypatch, result)
    seen = {}

    def fetch(url, proxy, **kw):
        seen.update(cache.get(job.RUNNING_KEY_TMPL.format(proxy_id=17)))
        return result

    monkeypatch.setattr(job, "fetch_public_page", fetch)
    asyncio.run(job._run_public_page({**REQUEST, "run_id": "r1"}))
    assert seen["run_id"] == "r1" and seen["type"] == "public_page" and seen["keyword"] == REQUEST["page_url"]
    assert cache.get(job.RUNNING_KEY_TMPL.format(proxy_id=17)) is None


def test_row_stop_ends_the_running_job(monkeypatch):
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [])
    _setup(monkeypatch, result)
    fetch = Mock(return_value=result)
    monkeypatch.setattr(job, "fetch_public_page", fetch)
    monkeypatch.setattr(job, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(job, "_cancel_requested", lambda run_id=None, platform=None: run_id == "r1")
    asyncio.run(job._run_public_page({**REQUEST, "run_id": "r1"}))
    assert fetch.call_args.kwargs["should_stop"]() is True


def test_publish_failure_is_not_a_clean_job_for_pace(monkeypatch):
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [], pagination_status="publish_failed")
    cache, _ = _setup(monkeypatch, result)
    cache.set(job.PACE_KEY_TMPL.format(proxy_id=17), 4)
    asyncio.run(job._run_public_page(REQUEST))
    assert job._pace(cache, 17) == 4


def test_feed_restriction_rests_only_pagination_and_next_job_reads_html(monkeypatch):
    restricted = page.PublicPage(
        "ok", 200, {"page_id": "9"}, [{"post_id": "1"}], 1, "access_restricted", "access_restricted"
    )
    cache, _ = _setup(monkeypatch, restricted)
    asyncio.run(job._run_public_page(REQUEST))
    assert cache.get(job.FEED_COOLDOWN_KEY_TMPL.format(proxy_id=17)) == "access_restricted"
    assert not cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=17))
    html_only = page.PublicPage("ok", 200, {"page_id": "9"}, [{"post_id": "2"}], 1, "max_pages")
    fetch = Mock(return_value=html_only)
    monkeypatch.setattr(job, "fetch_public_page", fetch)
    asyncio.run(job._run_public_page(REQUEST))
    assert fetch.call_args.kwargs["max_pages"] == 1
    assert cache.get(job.FEED_COOLDOWN_KEY_TMPL.format(proxy_id=17))


def test_html_block_still_rests_the_whole_proxy(monkeypatch):
    cache, _ = _setup(monkeypatch, page.PublicPage("rate_limited", 429))
    with pytest.raises(CrawlJobSkipped):
        asyncio.run(job._run_public_page(REQUEST))
    assert cache.get(job.COOLDOWN_KEY_TMPL.format(proxy_id=17)) == "rate_limited"
    assert not cache.get(job.FEED_COOLDOWN_KEY_TMPL.format(proxy_id=17))


def test_new_ip_does_not_end_feed_cooldown(monkeypatch):
    cache = _cooled(monkeypatch, "1.1.1.1", "2.2.2.2")
    cache.set(job.FEED_COOLDOWN_KEY_TMPL.format(proxy_id=15), "access_restricted")
    assert job._pick_proxy(cache)["id"] == 15
    assert cache.get(job.FEED_COOLDOWN_KEY_TMPL.format(proxy_id=15))


def test_page_opens_keep_a_gap_scaled_by_pace(monkeypatch):
    monkeypatch.setattr(job, "per_proxy", lambda: 1)
    monkeypatch.setattr(job, "list_public_page_proxies", lambda: [{"id": 15}])
    monkeypatch.setattr(job, "html_gap_seconds", lambda: 120)
    monkeypatch.setattr(job, "build_proxy_url", lambda p: "http://p")
    result = page.PublicPage("ok", 200, {"page_id": "9"}, [])
    monkeypatch.setattr(job, "fetch_public_page", lambda u, p, **kw: result)
    monkeypatch.setattr(job, "KafkaPublisher", lambda: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()))
    cache = Cache()
    monkeypatch.setattr(job, "RedisCache", lambda: cache)
    cache.set(job.PACE_KEY_TMPL.format(proxy_id=15), 2)
    ttls = {}
    original = cache.set
    monkeypatch.setattr(
        cache, "set", lambda k, v, ttl_seconds=None: (ttls.__setitem__(k, ttl_seconds), original(k, v))[1]
    )
    asyncio.run(job._run_public_page(REQUEST))
    gate = job.HTML_GATE_KEY_TMPL.format(proxy_id=15)
    assert ttls[gate] == 240
    assert job._pick_proxy(cache) is None
    assert job.should_pause_filler(cache) is False  # sắp rảnh, không phải hết proxy
    cache.delete(gate)
    assert job._pick_proxy(cache)["id"] == 15


def test_dedupe_false_resends_seen_posts_for_metric_refresh():
    """Quét lại page phim đang chiếu (dedupe=false): bài đã thấy vẫn được gửi lại để ingest cập nhật số tương tác."""
    cache = Cache()
    cache.set(f"{job.SEEN_POSTS_KEY}:p1", 1)
    kafka = SimpleNamespace(publish=AsyncMock(return_value=True))
    posts = [{"post_id": "p1", "content": "cũ"}, {"post_id": "p2", "content": "mới"}]
    request = {"page_url": "https://www.facebook.com/phim", "keyword_id": "kw", "dedupe": "false"}
    assert asyncio.run(job._publish_new_posts(cache, request, posts, kafka)) == 2
    assert cache.get(f"{job.SEEN_POSTS_KEY}:p2")
    # Mặc định vẫn chỉ gửi bài mới.
    kafka.publish.reset_mock()
    assert asyncio.run(job._publish_new_posts(cache, {**request, "dedupe": "true"}, posts, kafka)) == 0
    kafka.publish.assert_not_awaited()
