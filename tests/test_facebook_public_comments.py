"""Anonymous pipeline protocol, pagination, transport isolation and consumer routing.

All network, Kafka, Redis and account calls are mocked; live samples are documented separately.
"""

import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from social_crawler.spiders.facebook.auth import public_comments as public
from social_crawler.spiders.facebook.features.comments import public_comments as spider_module


class Cache:
    def __init__(self, data=None):
        self.data = data or {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, **kwargs):
        self.data[key] = value

    def set_nx(self, key, value, ttl_seconds):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def next_counter(self, key, floor, ttl):
        self.data[key] = max(self.data.get(key, 0), floor) + 1
        return self.data[key]

    def delete_if_equals(self, key, value):
        if self.data.get(key) == value:
            del self.data[key]
            return True
        return False

    def delete(self, key):
        self.data.pop(key, None)


def response(nodes=None, cursor=None, more=False, kind="comments"):
    conn = {"edges": [{"node": n} for n in nodes or []], "page_info": {"end_cursor": cursor, "has_next_page": more}}
    node = (
        {"comment_rendering_instance_for_feed_location": {"comments": conn}}
        if kind == "comments"
        else {"replies_connection": conn}
    )
    return json.dumps({"data": {"node": node}})


def node(identifier="1", post="123", replies=0):
    return {
        "id": base64.b64encode(f"comment:{post}_{identifier}".encode()).decode(),
        "legacy_fbid": identifier,
        "body": {"text": "sample"},
        "feedback": {"id": "server-feedback", "replies_fields": {"total_count": replies}},
    }


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(public, "public_proxy", lambda proxy_id: "http://proxy.example:1234")
    monkeypatch.setattr(public.time, "sleep", lambda delay: None)
    session = SimpleNamespace(trust_env=True, proxies={}, cookies={}, close=Mock(), post=Mock())
    monkeypatch.setattr(public.requests, "Session", lambda: session)
    value = public.PublicCommentsClient(cache=Cache(), proxy_id=10)
    yield value
    value.close()


def test_payload_is_explicitly_anonymous_and_uses_permalink_policy():
    data = public.payload("comments", "feedback", "12345678901234", "cursor")
    assert data["av"] == data["__user"] == "0"
    assert data["fb_dtsg"] == ""
    variables = json.loads(data["variables"])
    assert variables["commentsAfterCursor"] == "cursor"
    assert variables["feedLocation"] == "POST_PERMALINK_DIALOG"
    assert variables["commentsIntentToken"] == "REVERSE_CHRONOLOGICAL_UNFILTERED_INTENT_V1"
    assert "__req" not in data


def test_transport_clears_cookies_disables_environment_and_never_follows_redirects(client):
    def post(url, **kwargs):
        assert url == public.GRAPHQL_URL
        assert not client.session.cookies
        assert "cookies" not in kwargs and "Authorization" not in kwargs["headers"]
        assert kwargs["allow_redirects"] is False
        assert kwargs["data"]["__user"] == "0"
        client.session.cookies["guest"] = "guest-only"
        return SimpleNamespace(status_code=200, text=response([node()]))

    client.session.post.side_effect = post
    client.session.cookies["c_user"] = "must-be-cleared"
    client.comments("123")
    client.comments("123", "next")
    assert client.requests == 2
    assert client.session.trust_env is False
    assert client.session.proxies["https"] == "http://proxy.example:1234"


@pytest.mark.parametrize(
    "message,error",
    [
        ("GraphQL document with ID 1 was not found", public.PublicQueryStale),
        ("missing_required_variable_value scale", public.PublicSchemaChanged),
    ],
)
def test_query_errors_are_never_empty_success(client, message, error):
    client.session.post.return_value = SimpleNamespace(
        status_code=200, text=json.dumps({"errors": [{"message": message}]})
    )
    with pytest.raises(error):
        client.comments("123")
    assert client.requests == 1


def test_1675012_retries_same_cursor_with_bounded_budget(client):
    bad = SimpleNamespace(status_code=200, text=json.dumps({"errors": [{"code": 1675012}]}))
    client.session.post.side_effect = [bad, bad, SimpleNamespace(status_code=200, text=response([node()]))]
    assert client.comments("123", "cursor")[0]
    assert client.requests == 3
    assert {
        json.loads(c.kwargs["data"]["variables"])["commentsAfterCursor"] for c in client.session.post.call_args_list
    } == {"cursor"}


def test_1675012_exhaustion_reports_incomplete_and_sets_cooldown(client):
    client.session.post.return_value = SimpleNamespace(
        status_code=200, text=json.dumps({"errors": [{"code": 1675012}]})
    )
    with pytest.raises(public.PublicCommentsError, match="incomplete"):
        client.comments("123")
    assert client.requests == 3
    assert client.cache.get("facebook:public_comments:proxy:10:cooldown") == 1


@pytest.mark.parametrize("code", [302, 403, 429])
def test_redirect_and_block_have_no_account_fallback(client, code):
    client.session.post.return_value = SimpleNamespace(status_code=code, text="")
    with pytest.raises(public.PublicAccessUnavailable):
        client.comments("123")
    assert client.requests == 1


def test_server_failure_retries_are_bounded_and_counted(client):
    client.session.post.side_effect = [
        SimpleNamespace(status_code=503),
        SimpleNamespace(status_code=200, text=response()),
    ]
    assert client.comments("123")[0] == []
    assert client.requests == 2


def test_account_assigned_to_public_proxy_mid_job_stops_before_request(client, monkeypatch):
    monkeypatch.setattr(public, "public_proxy", Mock(side_effect=RuntimeError("assigned")))
    with pytest.raises(RuntimeError):
        client.comments("123")
    client.session.post.assert_not_called()


def test_job_budget_never_sends_an_extra_request(client):
    client.max_requests = 1
    client.session.post.return_value = SimpleNamespace(status_code=200, text=response())
    client.comments("123")
    with pytest.raises(public.PublicCommentsError, match="budget"):
        client.comments("123")
    assert client.session.post.call_count == 1


def make_spider(monkeypatch, pages, **kwargs):
    spider = spider_module.FacebookPublicCommentsSpider(post_id=kwargs.pop("post_id", "123"), **kwargs)
    fake = SimpleNamespace(comments=Mock(side_effect=pages), replies=Mock(), close=Mock())
    monkeypatch.setattr(spider_module, "PublicCommentsClient", lambda **kw: fake)
    spider._kafka = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), publish=AsyncMock(return_value=True))
    return spider, fake


def collect(spider):
    async def run():
        return [item async for item in spider.start()]

    return asyncio.run(run())


def test_spider_paginates_and_keeps_existing_kafka_contract(monkeypatch):
    spider, client = make_spider(
        monkeypatch,
        [([node("1")], {"has_next_page": True, "end_cursor": "next"}), ([node("2")], {"has_next_page": False})],
        include_replies=False,
    )
    items = collect(spider)
    assert len(items) == 2
    assert client.comments.call_args.args == ("123", "next")
    for call in spider._kafka.publish.call_args_list:
        value = call.kwargs["value"]
        assert value["platform"] == "facebook" and value["post_id"] == "123"
        assert "reply_expansion_token" not in value
    client.close.assert_called_once()
    spider._kafka.stop.assert_awaited_once()


def test_reply_pagination_uses_server_feedback_id_not_a_guessed_id(monkeypatch):
    parent = node("1", replies=2)
    parent["feedback"]["expansion_info"] = {"expansion_token": "token"}
    spider, client = make_spider(monkeypatch, [([parent], {"has_next_page": False})])
    client.replies.side_effect = [
        ([node("2")], {"has_next_page": True, "end_cursor": "reply-next"}),
        ([node("3")], {"has_next_page": False}),
    ]
    items = collect(spider)
    assert len(items) == 3
    assert client.replies.call_args.args == ("server-feedback", "token", "reply-next")
    assert items[-1].parent_comment_id == parent["id"]


def test_composite_database_post_id_is_preserved_for_ingest(monkeypatch):
    spider, client = make_spider(
        monkeypatch, [([node()], {"has_next_page": False})], post_id="99_123", include_replies=False
    )
    assert collect(spider)[0].post_id == "99_123"
    assert client.comments.call_args.args[0] == "123"


@pytest.mark.parametrize("failure", ["cursor", "kafka"])
def test_partial_or_failed_pagination_never_reports_success(monkeypatch, failure):
    page = ([node()], {"has_next_page": True, "end_cursor": "same"})
    spider, client = make_spider(monkeypatch, [page, page], include_replies=False, max_pages=0)
    if failure == "kafka":
        spider._kafka.publish.return_value = False
    with pytest.raises(public.PublicCommentsError):
        collect(spider)
    client.close.assert_called_once()
    spider._kafka.stop.assert_awaited_once()


def test_page_limit_ends_the_job_normally_and_keeps_later_comments(monkeypatch):
    parent = node("1", replies=9)
    parent["feedback"]["expansion_info"] = {"expansion_token": "token"}
    spider, client = make_spider(
        monkeypatch, [([parent, node("2")], {"has_next_page": True, "end_cursor": "next"})], max_pages=1
    )
    client.replies.return_value = ([node("3")], {"has_next_page": True, "end_cursor": "reply-next"})
    assert len(collect(spider)) == 3
    assert client.comments.call_count == 1 and client.replies.call_count == 1


def test_stale_query_cools_the_queue_down(client):
    client.session.post.return_value = SimpleNamespace(
        status_code=200, text=json.dumps({"errors": [{"message": "GraphQL document with ID 1 was not found"}]})
    )
    with pytest.raises(public.PublicQueryStale):
        client.comments("123")
    assert public.queue_hold_reason(client.cache, 10) == "cooldown"


def test_queue_hold_reason(monkeypatch):
    monkeypatch.setattr(public, "public_proxy", lambda proxy_id: "http://proxy.example:1234")
    monkeypatch.setenv("FACEBOOK_PUBLIC_COMMENTS_DAILY_CAP", "2")
    scope = "facebook:public_comments:proxy:10"
    day = public.datetime.now(public.UTC).date().isoformat()
    assert public.queue_hold_reason(Cache(), 10) is None
    assert public.queue_hold_reason(Cache({f"{scope}:requests:{day}": 1}), 10) is None
    assert public.queue_hold_reason(Cache({f"{scope}:requests:{day}": 2}), 10) == "daily_budget"
    assert public.queue_hold_reason(Cache({f"{scope}:cooldown": 1}), 10) == "cooldown"
    monkeypatch.setattr(public, "public_proxy", Mock(side_effect=RuntimeError("assigned")))
    assert public.queue_hold_reason(Cache(), 10) == "proxy_unavailable"


def test_public_hold_keeps_the_queue_and_rewinds_anything_fetched(monkeypatch):
    from social_crawler.consumer import loop

    partition = object()
    consumer = SimpleNamespace(
        assignment=lambda: {partition},
        pause=Mock(),
        resume=Mock(),
        seek=Mock(),
        getmany=AsyncMock(return_value={partition: [SimpleNamespace(offset=7), SimpleNamespace(offset=8)]}),
    )
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(loop, "queue_hold_reason", Mock(side_effect=["cooldown", None]))
    asyncio.run(loop._wait_out_public_hold(consumer, "facebook_public"))
    consumer.seek.assert_called_once_with(partition, 7)
    consumer.resume.assert_called_once_with(partition)

    reason = Mock(side_effect=AssertionError("Account platforms have no public hold"))
    monkeypatch.setattr(loop, "queue_hold_reason", reason)
    asyncio.run(loop._wait_out_public_hold(None, "facebook"))


def test_stop_releases_the_public_hold_so_jobs_can_be_skipped(monkeypatch):
    from social_crawler.consumer import loop

    consumer = SimpleNamespace(assignment=lambda: set(), pause=Mock(), resume=Mock(), getmany=AsyncMock())
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: platform == "facebook")
    monkeypatch.setattr(loop, "queue_hold_reason", lambda: "daily_budget")
    asyncio.run(loop._wait_out_public_hold(consumer, "facebook_public"))
    consumer.getmany.assert_not_awaited()
    consumer.resume.assert_called_once_with()


def test_anonymous_job_routing_and_preflight_never_touch_account_state(monkeypatch):
    from social_crawler.consumer import common, dispatch
    from social_crawler.consumer.jobs import comments

    request = {
        "platform": "facebook",
        "type": "comments",
        "post_id": "123",
        "post_url": "https://www.facebook.com/x/posts/123",
        "access_mode": "anonymous",
        "run_id": "run-public",
        "max_pages": 0,
    }
    assert dispatch._request_platform(request) == "facebook_public"
    assert "facebook_public" in common.PLATFORM_CONSUMER_GROUPS
    assert dispatch._request_platform({**request, "access_mode": "authenticated"}) == "facebook"
    assert dispatch._request_platform({k: v for k, v in request.items() if k != "access_mode"}) == "facebook"
    monkeypatch.setattr(comments, "RedisCache", Cache)
    monkeypatch.setattr(comments, "_stopped_for", lambda **kw: False)
    for name in ("_ensure_facebook_session", "_ensure_comments_cache", "_ensure_replies_cache"):
        monkeypatch.setattr(comments, name, AsyncMock(side_effect=AssertionError("Account must not be touched")))
    monkeypatch.setattr(comments.lanes, "account_for_job", Mock(side_effect=AssertionError("No account allocation")))
    run = AsyncMock(return_value=0)
    monkeypatch.setattr(comments, "_run_subprocess", run)
    asyncio.run(comments._run_comments_spider(request))
    assert "facebook_public_comments" in run.call_args.args[0]
    assert run.call_args.kwargs["platform"] == "facebook"  # Existing Stop/cancel namespace.


def test_doctor_failure_is_not_reported_healthy(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["doctor", "--post-id", "123"])
    client = SimpleNamespace(comments=Mock(side_effect=public.PublicQueryStale("stale")), close=Mock())
    monkeypatch.setattr(public, "PublicCommentsClient", lambda **kw: client)
    assert public.main() == 1
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    client.close.assert_called_once()


def test_public_consumer_subscribes_only_its_topic_and_ignores_account_gates(monkeypatch):
    from social_crawler.consumer import loop

    captured = []

    def stop_at_constructor(*args, **kwargs):
        captured.append(args[0])
        raise RuntimeError("constructor probe")

    monkeypatch.setattr(loop, "AIOKafkaConsumer", stop_at_constructor)
    with pytest.raises(RuntimeError, match="constructor probe"):
        asyncio.run(loop._run_platform_consumer_once("facebook_public", "group-public"))
    assert captured == ["crawl_requests.facebook_public_comments"]
    monkeypatch.setattr(loop.lanes, "lane_account", Mock(side_effect=AssertionError("No account for public worker")))
    asyncio.run(loop._yield_partitions_while_no_account(None, "facebook_public"))
    asyncio.run(loop._wait_out_hourly_cap(None, "facebook_public"))
