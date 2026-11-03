"""HTTP recovery never opens a browser or replays an explicitly rejected session."""

import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from social_crawler.constants import facebook as fb
from social_crawler.spiders.common import comet_graphql_client as cgc
from social_crawler.spiders.facebook.auth import http_refresh as hr
from social_crawler.spiders.facebook.auth.cookies import save_browser_state
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient


class Cache:
    def __init__(self, data=None):
        self.data = data or {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, **kwargs):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


def client():
    c = object.__new__(FacebookGraphQLClient)
    c._account = "test"
    c._cache = {"body_static": {"fb_dtsg": "old", "lsd": "lsd"}, "cookies": {"c_user": "123"}, "headers": {}}
    c._redis = Cache({fb.CACHE_REDIS_KEY_TMPL.format(account="test"): c._cache})
    c._profile = {}
    c._req_counter = 1
    c._throttle = Mock(side_effect=c._ensure_lease)
    c._session = Mock()
    return c


def response(payload, status=200, headers=None):
    return SimpleNamespace(text=json.dumps(payload), status_code=status, headers=headers or {})


@pytest.mark.parametrize(
    "headers",
    [
        {"Set-Cookie": "datr=a; Path=/, c_user=deleted; Max-Age=0"},
        {"set-cookie": 'c_user="deleted"; Path=/'},
        {"SET-LOGIN": "logged-out"},
    ],
)
def test_logout_headers(headers):
    assert hr.session_logged_out(headers)
    assert not hr.session_logged_out({"set-cookie": "datr=deleted; c_user=123"})


def test_header_stops_transient_retry_and_future_requests(monkeypatch):
    c = client()
    c._proxy_cfg = None
    monkeypatch.setattr(cgc.ip_switch, "touch", lambda *a: None)
    c._session.post.return_value = response({}, 503, {"set-login": "logged-out"})

    def drop(reason):
        c._session_dead = True

    c._drop_rejected_session = Mock(side_effect=drop)
    with pytest.raises(cgc.SessionExpiredError):
        c._post_with_retry({}, {}, {})
    with pytest.raises(cgc.SessionExpiredError):
        c._post_with_retry({}, {}, {})
    assert c._session.post.call_count == 1
    assert c._drop_rejected_session.call_count == 1


def test_http_refresh_updates_all_recipes():
    c = client()
    key = fb.COMMENTS_REDIS_KEY_TMPL.format(account="test")
    c._redis.data[key] = {"doc_id": "123", "body_static": {"fb_dtsg": "old"}}
    c._session.get.return_value = SimpleNamespace(
        status_code=200,
        headers={},
        text='["DTSGInitialData",[],{"token":"new"}]["LSD",[],{"token":"new-lsd"}]{"USER_ID":"123"}',
    )
    assert hr.refresh(c)
    assert c._cache["body_static"]["fb_dtsg"] == "new"
    assert c._redis.get(key)["doc_id"] == "123"
    assert c._redis.get(key)["body_static"]["lsd"] == "new-lsd"
    assert c._throttle.call_count == 1
    assert c._session.get.call_args.kwargs["allow_redirects"] is False


def _cookie(name, value):
    return {
        "name": name,
        "value": value,
        "domain": ".facebook.com",
        "path": "/",
        "expires": 1830000000,
        "httpOnly": True,
        "secure": True,
        "sameSite": "None",
    }


def test_http_cookies_reach_browser_state():
    c = client()
    key = fb.STATE_REDIS_KEY_TMPL.format(account="test")
    origins = [{"origin": "https://www.facebook.com", "localStorage": [{"name": "a", "value": "b"}]}]
    other = {**_cookie("xs", "unrelated"), "domain": ".example.com"}
    c._redis.data[key] = {
        "cookies": [_cookie("c_user", "123"), _cookie("xs", "old"), _cookie("fr", "x"), other],
        "origins": origins,
    }
    c._session.cookies.get_dict.return_value = {"c_user": "123", "xs": "new", "fr": "deleted", "wd": "1"}
    hr.save_tokens(c, {"fb_dtsg": "new"})
    state = c._redis.get(key)
    cookies = {(item["name"], item["domain"]): item for item in state["cookies"]}
    assert cookies["xs", ".facebook.com"] == _cookie("xs", "new")
    assert cookies["xs", ".example.com"]["value"] == "unrelated"
    assert ("fr", ".facebook.com") not in cookies
    assert cookies["wd", ".facebook.com"]["value"] == "1"
    assert state["origins"] == origins
    assert "fr" not in c._cache["cookies"]
    assert c._redis.get(fb.CACHE_REDIS_KEY_TMPL.format(account="test"))["cookies"]["xs"] == "new"


def test_http_cookies_never_overwrite_another_identity_in_browser_state():
    c = client()
    key = fb.STATE_REDIS_KEY_TMPL.format(account="test")
    state = {"cookies": [_cookie("c_user", "999"), _cookie("xs", "theirs")], "origins": []}
    c._redis.data[key] = state
    c._session.cookies.get_dict.return_value = {"c_user": "123", "xs": "new"}
    hr.save_tokens(c, {})
    assert c._redis.get(key) == state


def test_save_tokens_keeps_the_client_normalized_fingerprint():
    c = client()
    c._cache = {**c._cache, "headers": {"user-agent": "normalized"}}
    c._redis.data[fb.CACHE_REDIS_KEY_TMPL.format(account="test")] = {
        "body_static": {},
        "cookies": {"c_user": "123"},
        "headers": {"user-agent": "as-captured"},
    }
    hr.save_tokens(c, {"fb_dtsg": "new"})
    assert c._cache["headers"]["user-agent"] == "normalized"
    assert c._cache["body_static"]["fb_dtsg"] == "new"


def test_browser_state_reaches_every_recipe():
    cache = Cache()
    search = fb.CACHE_REDIS_KEY_TMPL.format(account="test")
    comments = fb.COMMENTS_REDIS_KEY_TMPL.format(account="test")
    cache.data[search] = {"doc_id": "1", "cookies": {"c_user": "123", "xs": "old"}}
    cache.data[comments] = {"doc_id": "2", "cookies": {"c_user": "123", "xs": "old"}}
    state = {"cookies": [_cookie("c_user", "123"), _cookie("xs", "new")], "origins": []}
    save_browser_state(cache, "test", state)
    assert cache.get(fb.STATE_REDIS_KEY_TMPL.format(account="test")) == state
    assert cache.get(search) == {"doc_id": "1", "cookies": {"c_user": "123", "xs": "new"}}
    assert cache.get(comments)["cookies"]["xs"] == "new"


@pytest.mark.parametrize("cookies", [[_cookie("datr", "x")], [_cookie("c_user", "999"), _cookie("xs", "theirs")]])
def test_logged_out_or_foreign_browser_state_keeps_recipe_cookies(cookies):
    cache = Cache()
    search = fb.CACHE_REDIS_KEY_TMPL.format(account="test")
    recipe = {"doc_id": "1", "cookies": {"c_user": "123", "xs": "good"}}
    cache.data[search] = recipe
    save_browser_state(cache, "test", {"cookies": cookies, "origins": []})
    assert cache.get(search) == recipe


def test_client_rereads_cache_after_acquiring_the_lease(monkeypatch):
    key = fb.CACHE_REDIS_KEY_TMPL.format(account="test")
    from social_crawler.spiders.facebook.auth import identity

    monkeypatch.setattr(identity, "runtime_system", lambda: "Linux")
    recipe = {
        "captured_at": 1,
        "body_static": {},
        "cookies": {"xs": "old"},
        "headers": {"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151.0.0.0 Safari/537.36"},
    }
    cache = Cache({key: recipe})

    def previous_holder_saved(*args):
        cache.data[key] = {**recipe, "cookies": {"xs": "new"}}

    monkeypatch.setattr(cgc.account_lease, "acquire", previous_holder_saved)
    monkeypatch.setattr(cgc.pool, "acquire_proxy_for_account", lambda *a, **kw: None)
    assert FacebookGraphQLClient(redis_cache=cache, account="test")._cache["cookies"]["xs"] == "new"


@pytest.fixture
def rested(monkeypatch):
    from social_crawler.accounts import account_warning
    from social_crawler.runtime import lanes

    calls = Mock(return_value=True)
    monkeypatch.setattr(account_warning, "rest_warned_account", calls)
    monkeypatch.setattr(lanes, "release_account", Mock())
    return calls


def restricted_client():
    c = client()
    c._redis.data[fb.ACTIVE_ACCOUNT_REDIS_KEY] = "test"
    return c


@pytest.mark.parametrize(
    "payload,signal",
    [
        ({"error": 368, "errorSummary": "Blocked"}, "error_368"),
        ({"errors": [{"message": "x", "code": 1675004}]}, "error_1675004"),
        ({"errors": [{"message": "Rate limit exceeded"}]}, "rate limit exceeded"),
        ({"error": 1, "errorSummary": "You're Temporarily Blocked"}, "you're temporarily blocked"),
    ],
)
def test_explicit_restriction_rests_account_and_stops_client(rested, payload, signal):
    c = restricted_client()
    token_cache = c._redis.get(fb.CACHE_REDIS_KEY_TMPL.format(account="test"))
    c._post_with_retry = Mock(return_value=response(payload))
    with pytest.raises(cgc.AccountRestrictedError):
        run(c)
    assert rested.call_args.args[:3] == ("facebook", "test", signal)
    assert c._redis.get(fb.ACTIVE_ACCOUNT_REDIS_KEY) is None
    assert c._redis.get(fb.CACHE_REDIS_KEY_TMPL.format(account="test")) == token_cache
    assert c._post_with_retry.call_count == 1
    with pytest.raises(cgc.SessionExpiredError):
        c._ensure_lease()


@pytest.mark.parametrize("status", [302, 403])
def test_denied_or_redirected_graphql_rests_account(rested, status):
    c = restricted_client()
    c._post_with_retry = Mock(return_value=response({}, status, {"location": "https://www.facebook.com/login"}))
    with pytest.raises(cgc.AccountRestrictedError):
        run(c)
    assert rested.call_args.args[2] == f"http_{status}"


def test_sustained_429_rests_account(rested, monkeypatch):
    c = restricted_client()
    c._proxy_cfg = None
    c.RETRY_BACKOFF_BASE_SECONDS = c.RETRY_BACKOFF_JITTER_SECONDS = 0
    c._adjust_interval = Mock()
    c._record_proxy_outcome_once = Mock()
    monkeypatch.setattr(cgc.ip_switch, "touch", lambda *a: None)
    c._session.post.return_value = response({}, 429)
    with pytest.raises(cgc.RateLimitedError):
        c._post_with_retry({}, {}, {})
    assert rested.call_args.args[2] == "http_429"


def test_checkpoint_redirect_is_a_checkpoint(monkeypatch):
    c = restricted_client()
    disable = Mock(return_value=True)
    monkeypatch.setattr(cgc, "disable_account", disable)
    with pytest.raises(cgc.CheckpointRequiredError) as caught:
        c._check_session_headers(response({}, 302, {"Location": "https://www.facebook.com/checkpoint/123/"}))
    assert not isinstance(caught.value, cgc.AccountRestrictedError)
    disable.assert_called_once()


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"text": "rate limit exceeded, you're temporarily blocked"}},
        {"data": {"text": "error 368"}, "extensions": {"code": 368}},
    ],
)
def test_post_content_is_not_a_restriction_signal(rested, payload):
    c = restricted_client()
    c._post_with_retry = Mock(return_value=response(payload))
    assert run(c) == payload
    rested.assert_not_called()


def test_unclassified_error_fails_the_job_without_resting(rested):
    c = restricted_client()
    c._post_with_retry = Mock(return_value=response({"errors": [{"message": "Something went wrong", "code": 1}]}))
    with pytest.raises(cgc.GraphQLResponseError):
        run(c)
    rested.assert_not_called()


def run(c):
    return c._run(
        doc_id="old-id",
        friendly_name="SearchQuery",
        template={"text": "old"},
        template_source="test",
        overrides={"text": "movie"},
    )


def test_schema_error_never_rediscover_or_retry(monkeypatch):
    c = client()
    c._post_with_retry = Mock(
        return_value=response({"errors": [{"message": "missing_required_variable_value: scale"}]})
    )
    discover = Mock()
    monkeypatch.setattr(hr, "discover_public", discover)
    with pytest.raises(cgc.GraphQLSchemaError):
        run(c)
    discover.assert_not_called()
    assert c._post_with_retry.call_count == 1


def test_doc_rotation_retries_once(monkeypatch):
    c = client()
    c._post_with_retry = Mock(
        side_effect=[
            response({"errors": [{"message": "GraphQL document with ID old was not found"}]}),
            response({"data": {"ok": True}}),
        ]
    )
    monkeypatch.setattr(hr, "discover_public", lambda cache: {"SearchQuery": "new-id"})
    assert run(c)["data"]["ok"]
    assert c._post_with_retry.call_count == 2
    assert c._post_with_retry.call_args.kwargs["body"]["doc_id"] == "new-id"


def test_error_token_recovered_once():
    c = client()
    c._post_with_retry = Mock(
        side_effect=[response({"error": 1, "dtsgToken": "new"}), response({"data": {"ok": True}})]
    )
    assert run(c)["data"]["ok"]
    assert c._post_with_retry.call_args.kwargs["body"]["fb_dtsg"] == "new"


@pytest.mark.parametrize("depth,prefix", [(1, "replies"), (2, "subReplies")])
def test_reply_correct_id_token_and_cursor(depth, prefix):
    c = client()
    name = f"Depth{depth}CommentsListPaginationQuery"
    c._redis.data[hr.PUBLIC_DOCS_KEY] = {"operations": {name: "123456789012345"}}
    c._run = Mock(return_value={"data": {"node": {"replies_connection": {"edges": []}}}})
    c.get_replies_next_page("456", "cursor", post_id="123", expansion_token="token", depth=depth)
    variables = c._run.call_args.kwargs["template"]
    assert base64.b64decode(variables["id"]).decode() == "feedback:123_456"
    assert variables["expansionToken"] == "token"
    assert variables[f"{prefix}AfterCursor"] == "cursor"
    assert variables[f"{prefix}AfterCount"] == -1
    assert "commentsAfterCursor" not in variables
    if depth == 1:
        assert variables["focusCommentID"] == "456"


def test_static_discovery_never_takes_id_from_adjacent_module():
    text = '__d("SearchQuery_facebookRelayOperation",[],function(a,b,c,d,e,f){e.exports="123456789012345"});__d("Other",[],function(){a="999999999999999"});'
    assert hr.operations_in_script(text) == {"SearchQuery": "123456789012345"}
    assert not hr.operations_in_script(
        '__d("Missing_facebookRelayOperation",[],function(){});__d("Other",[],function(){a="999999999999999"});'
    )


def test_missing_cache_never_launches_browser(monkeypatch):
    from social_crawler.consumer import sessions

    monkeypatch.setattr(sessions, "RedisCache", Cache)
    monkeypatch.setattr(sessions, "_facebook_session_is_cached", lambda account: False)
    monkeypatch.setattr(sessions, "is_account_usable", lambda *args: True)
    process = Mock(side_effect=AssertionError("Browser must not run"))
    monkeypatch.setattr(sessions, "_run_subprocess", process)
    assert not asyncio.run(sessions._ensure_facebook_session(account="test"))
    process.assert_not_called()


def test_public_discovery_uses_empty_cookie_jar_for_every_fetch(monkeypatch):
    c = Cache()
    c.set_nx = lambda *a, **kw: True
    monkeypatch.setattr(hr, "public_proxy", lambda proxy_id: "http://proxy:1234")
    session = Mock()
    session.__enter__ = Mock(return_value=session)
    session.__exit__ = Mock(return_value=False)
    session.cookies = {}
    calls = []

    def get(url, **kwargs):
        assert not session.cookies
        assert "cookies" not in kwargs and "headers" not in kwargs
        assert kwargs["allow_redirects"] is False
        calls.append(url)
        session.cookies["guest_cookie"] = "anonymous"
        text = (
            '<script src="https://static.xx.fbcdn.net/a.js"></script>'
            if len(calls) == 1
            else '__d("SearchQuery_facebookRelayOperation",[],function(a,b,c,d,e,f){e.exports="123456789012345"});'
        )
        return SimpleNamespace(
            status_code=200, headers={}, text=text, content=text.encode(), raise_for_status=lambda: None
        )

    session.get.side_effect = get
    factory = Mock(return_value=session)
    monkeypatch.setattr(hr.requests, "Session", factory)
    assert hr.discover_public(c, pages=["https://www.facebook.com/"]) == {"SearchQuery": "123456789012345"}
    assert len(calls) == 2
    assert factory.call_args.kwargs["trust_env"] is False


def test_refresh_refuses_anonymous_token():
    c = client()
    c._session.get.return_value = SimpleNamespace(
        status_code=200, headers={}, text='["DTSGInitialData",[],{"token":"anonymous"}]{"USER_ID":"0"}'
    )
    assert not hr.refresh(c)
    assert c._cache["body_static"]["fb_dtsg"] == "old"


def test_reply_structure_change_is_an_error_not_zero_replies():
    c = client()
    c._redis.data[hr.PUBLIC_DOCS_KEY] = {"operations": {"Depth1CommentsListPaginationQuery": "123456789012345"}}
    c._run = Mock(return_value={"data": {"different": {}}})
    with pytest.raises(cgc.GraphQLSchemaError):
        c.get_replies("456", post_id="123")


def test_manual_reply_capture_selects_depth1_not_parent_comments():
    from social_crawler.spiders.facebook.auth.request_capture import pick_replies_request

    root, reply = object(), object()
    assert (
        pick_replies_request([(root, "CommentListComponentsRootQuery"), (reply, "Depth1CommentsListPaginationQuery")])
        is reply
    )
    with pytest.raises(RuntimeError):
        pick_replies_request([(root, "CommentsListComponentsPaginationQuery")])


def test_streamed_schema_errors_are_not_discarded():
    parsed = cgc._parse_graphql_response(
        '{"data": {"node": {}}}\n{"errors": [{"message":"missing_required_variable_value"}]}'
    )
    assert hr.classify_error(parsed) == "schema"


def test_reply_spider_fetches_nested_replies_without_publishing_expansion_tokens(monkeypatch):
    from unittest.mock import AsyncMock

    from social_crawler.spiders.facebook.features.comments import comments as module

    spider = module.FacebookCommentsSpider(post_id="123", include_replies="true")
    spider._kafka = SimpleNamespace(publish=AsyncMock(return_value=True))
    c = SimpleNamespace(get_replies=Mock(side_effect=[{"level": 1}, {"level": 2}]))

    def extract(data):
        if data["level"] == 1:
            return [
                {
                    "comment_id": "reply",
                    "legacy_comment_id": "456",
                    "replies_count": 1,
                    "reply_expansion_token": "secret-expansion",
                }
            ]
        return [
            {"comment_id": "subreply", "legacy_comment_id": "789", "replies_count": 0, "reply_expansion_token": None}
        ]

    monkeypatch.setattr(module, "extract_replies", extract)
    monkeypatch.setattr(module, "find_replies_page_info", lambda data: None)

    async def collect():
        return [item async for item in spider._fetch_replies(c, "parent", "001", "parent-expansion")]

    items = asyncio.run(collect())
    assert [item.parent_comment_id for item in items] == ["parent", "reply"]
    assert c.get_replies.call_args.kwargs["depth"] == 2
    assert c.get_replies.call_args.kwargs["expansion_token"] == "secret-expansion"
    for call in spider._kafka.publish.call_args_list:
        assert "reply_expansion_token" not in call.kwargs["value"]
