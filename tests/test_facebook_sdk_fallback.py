"""Exercise the real pinned templates and V8 runtime without Facebook/Redis/DB."""

import base64
import copy
import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests
from py_mini_racer import JSTimeoutException

from social_crawler.constants import facebook as fb
from social_crawler.db import accounts
from social_crawler.spiders.common import comet_graphql_client as core
from social_crawler.spiders.facebook.auth import http_refresh
from social_crawler.spiders.facebook.auth import operation_fallback as ops
from social_crawler.spiders.facebook.auth import sdk_fallback as sdk
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient
from social_crawler.spiders.facebook.auth.identity import IDENTITY_KEY

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/152.0.0.0 Safari/537.36"
NAME = "SearchCometResultsPaginatedResultsQuery"
ID = "123456789012345"


class Cache:
    def __init__(self):
        self.data = {
            fb.STATE_REDIS_KEY_TMPL.format(account="test"): {
                "cookies": [
                    {"name": name, "value": value, "domain": ".facebook.com", "expires": -1}
                    for name, value in {"c_user": "123", "xs": "test-session", "datr": "device"}.items()
                ],
                "origins": [],
            },
            IDENTITY_KEY.format(account="test"): {"user_agent": UA},
        }

    def get(self, key):
        return copy.deepcopy(self.data.get(key))

    def set(self, key, value, **kwargs):
        self.data[key] = copy.deepcopy(value)

    def set_nx(self, key, value, **kwargs):
        if key in self.data:
            return False
        self.set(key, value)
        return True

    def delete(self, key):
        self.data.pop(key, None)


class Raw(io.BytesIO):
    def read1(self, size=-1, decode_content=False):
        return super().read1(size)


def response(text, status=200, headers=None):
    return SimpleNamespace(
        text=text,
        content=text.encode(),
        status_code=status,
        headers=headers or {},
        raw=Raw(text.encode()),
        close=lambda: None,
    )


def bundle(name=NAME):
    # Static regex cannot see this computed ID; the module factory must execute.
    return (
        '__d("'
        + name
        + '_facebookRelayOperation",[],function(a,b,c,d,e,f){e.exports=[12345,67890,12345].join("")},98);'
    )


@pytest.fixture
def environment(monkeypatch):
    monkeypatch.setenv("FACEBOOK_GQL_FALLBACK_ENABLED", "1")
    monkeypatch.setattr("social_crawler.spiders.facebook.auth.identity.runtime_system", lambda: "macOS")
    monkeypatch.setattr(accounts, "is_account_usable", lambda *a: True)
    monkeypatch.setattr(accounts, "get_account_by_key", Mock(return_value={"id": "123"}))
    monkeypatch.setattr(accounts, "session_setup_blocked", Mock(return_value=False))
    monkeypatch.setattr(core.account_lease, "acquire", Mock())
    monkeypatch.setattr(core.account_lease, "release", Mock())
    monkeypatch.setattr(core.account_lease, "lost", lambda *a: False)
    monkeypatch.setattr(core.ip_switch, "touch", lambda *a: None)
    monkeypatch.setattr(core.pool, "acquire_proxy_for_account", lambda *a, **k: {"url": "http://proxy.invalid:8159"})
    throttle = Mock()
    monkeypatch.setattr(FacebookGraphQLClient, "_throttle", throttle)
    monkeypatch.setattr(FacebookGraphQLClient, "_throttle_asset", throttle)
    monkeypatch.setattr(FacebookGraphQLClient, "_mark_request_finished", Mock())
    calls = []

    class Session:
        def __init__(self, **options):
            self.options = options
            self.cookies = requests.cookies.RequestsCookieJar()
            self.headers = {}
            self.verify = True
            self.closed = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def close(self):
            self.closed = True

        def get(self, url, **kwargs):
            calls.append((self, url, kwargs))
            if url == "https://www.facebook.com/":
                assert "text/html" in kwargs["headers"].get("accept", "")
                return response('["DTSGInitialData", [], {"token": "test-token"}]{"USER_ID":"123"}')
            if "fbcdn.net" in url:
                return response(
                    "\n".join(bundle(c.OPERATION_NAME.removesuffix(ops.SUFFIX)) for c in sdk.templates().values())
                )
            return response('<script src="https://static.xx.fbcdn.net/rsrc.php/test"></script>')

    monkeypatch.setattr(sdk.requests, "Session", Session)
    return SimpleNamespace(cache=Cache(), calls=calls, throttle=throttle, Session=Session)


def test_real_v8_resolves_computed_ids_and_graphql_parameters():
    runtime = ops.V8Operations()
    try:
        assert not http_refresh.operations_in_script(bundle())
        assert runtime.add(bundle())[NAME] == ID
        source = (
            '__d("Comments.graphql",[],function(a,b,c,d,e,f){e.exports={params:{id:[12345,67890,12345].join("")}}});'
        )
        assert runtime.add(source)["Comments"] == ID
    finally:
        runtime.close()


def test_v8_infinite_loop_is_interrupted(monkeypatch):
    monkeypatch.setattr(ops, "V8_SECONDS", 0.03)
    runtime = ops.V8Operations()
    try:
        with pytest.raises(JSTimeoutException):
            runtime.add("while (true) {}")
    finally:
        runtime.close()


def test_browserless_setup_then_all_recipe_types(environment):
    c = FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert sdk.is_http_recipe(c._cache)
    assert c._profile == {"target": "chrome152_macos", "user_agent": UA}
    assert c._session.options["default_headers"] is True and c._session.options["trust_env"] is False
    assert c._session.headers["user-agent"] == UA
    assert c._cache["body_static"]["fb_dtsg"] == "test-token"
    assert c._cache["doc_id"] == ID
    assert c._cache["pagination"]["variables_template"]["args"]["experience"]["type"] == "GLOBAL_SEARCH"
    assert "__req" not in c._cache["body_static"]
    assert c.GRAPHQL_URL == "https://web.facebook.com/api/graphql/"
    for feature in ("comments", "replies", "page_feed", "page_reels"):
        assert sdk.ensure_recipe(c, feature)
        cached = environment.cache.get(sdk.FEATURE_KEYS[feature].format(account="test"))
        assert cached["pagination"]["doc_id"] == ID
    assert "Depth2CommentsListPaginationQuery" in cached_operations(environment.cache)
    assert environment.throttle.call_count == len(environment.calls)
    for session, url, kwargs in environment.calls:
        assert session.verify is True
        assert kwargs["allow_redirects"] is False
        assert kwargs["headers"]["user-agent"] == UA
        if "fbcdn.net" in url:
            assert kwargs["headers"]["accept"] == "*/*"
            assert "cookies" not in kwargs
            assert not session.cookies
        else:
            assert "cookies" not in kwargs
            assert session.cookies.get("c_user") == "123"
    assert environment.cache.get(http_refresh.PUBLIC_DOCS_KEY) is None


def cached_operations(cache):
    return cache.get(fb.REPLIES_REDIS_KEY_TMPL.format(account="test"))["reply_operations"]


def test_imported_cookie_without_origin_ua_does_not_reuse_db_identity(environment):
    environment.cache.delete(IDENTITY_KEY.format(account="test"))
    with pytest.raises(core.SessionExpiredError):
        FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert not environment.calls
    accounts.get_account_by_key.assert_not_called()
    core.account_lease.release.assert_called_once()


@pytest.mark.parametrize("status", [403, 429])
def test_discovery_restriction_stops_after_first_request(environment, monkeypatch, status):
    monkeypatch.setattr(environment.Session, "get", Mock(return_value=response("", status)))
    rest = Mock()
    monkeypatch.setattr(FacebookGraphQLClient, "_rest_restricted_account", rest)
    with pytest.raises((core.AccountRestrictedError, core.RateLimitedError)):
        FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert environment.Session.get.call_count == 1
    rest.assert_called_once()
    assert environment.cache.get(fb.CACHE_REDIS_KEY_TMPL.format(account="test")) is None
    core.account_lease.release.assert_called_once()


def test_budget_exhaustion_during_discovery_never_fetches_next_asset(environment):
    environment.throttle.side_effect = [None, None, core.RequestBudgetExceeded("test budget")]
    with pytest.raises(core.RequestBudgetExceeded):
        FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert len(environment.calls) == 2
    core.account_lease.release.assert_called_once()


def test_existing_recipes_are_not_replaced(environment):
    c = FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    key = fb.COMMENTS_REDIS_KEY_TMPL.format(account="test")
    environment.cache.set(
        key,
        {"doc_id": "captured", "variables_template": {"custom": True}, "pagination": {"doc_id": "captured-pagination"}},
    )
    before = len(environment.calls)
    assert sdk.ensure_recipe(c, "comments")
    assert environment.cache.get(key)["doc_id"] == "captured"
    assert len(environment.calls) == before


def test_stale_document_falls_back_once_without_swapping_variables(environment, monkeypatch):
    c = FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    error = response(json.dumps({"errors": [{"message": "GraphQL document with ID old was not found"}]}))
    c._post_with_retry = Mock(side_effect=[error, response('{"data":{"ok":true}}')])
    monkeypatch.setattr(http_refresh, "discover_public", lambda cache: {})
    discover = Mock(return_value={NAME: "999999999999999"})
    monkeypatch.setattr(sdk, "discover_authenticated", discover)
    assert c.search("film")["data"]["ok"]
    bodies = [call.kwargs["body"] for call in c._post_with_retry.call_args_list]
    assert bodies[0]["doc_id"] != bodies[1]["doc_id"]
    assert bodies[0]["variables"] == bodies[1]["variables"]
    discover.assert_called_once_with(c, {NAME}, force=True)


@pytest.mark.parametrize(
    "url",
    [
        "http://static.xx.fbcdn.net/a",
        "https://fbcdn.net.evil.com/a",
        "https://user@www.facebook.com/",
        "https://www.facebook.com:8080/",
        "https://127.0.0.1/",
    ],
)
def test_discovery_rejects_external_or_credential_urls(url):
    assert not ops.allowed_url(url)
    assert not ops.allowed_url(url, asset=True)


def test_extensionless_bootloader_assets_are_discovered():
    assert ops.script_urls(
        '{"rsrcMap":{"a":{"src":"https:\\/\\/static.xx.fbcdn.net\\/rsrc.php\\/v4abc","type":"js"}}}',
        "https://www.facebook.com/",
    ) == ["https://static.xx.fbcdn.net/rsrc.php/v4abc"]


def test_disable_fallback_sends_no_requests(environment, monkeypatch):
    monkeypatch.setenv("FACEBOOK_GQL_FALLBACK_ENABLED", "0")
    with pytest.raises(core.SessionExpiredError):
        FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert not environment.calls


def test_cookie_from_another_database_account_is_rejected(environment):
    accounts.get_account_by_key.return_value = {"id": "999"}
    with pytest.raises(core.SessionExpiredError):
        FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert not environment.calls


def test_db_cookie_metadata_survives_recipe_expiry(environment):
    environment.cache.data.clear()
    encoded_ua = base64.b64encode(UA.encode()).decode()
    accounts.get_account_by_key.return_value = {
        "id": "123",
        "cookie": f"c_user=123; xs=test-session; datr=device; useragent={encoded_ua}",
    }
    c = FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert c._cache["headers"]["user-agent"] == UA
    assert environment.cache.get(IDENTITY_KEY.format(account="test"))["user_agent"] == UA
    environment.cache.delete(fb.CACHE_REDIS_KEY_TMPL.format(account="test"))
    assert sdk.seed(environment.cache, "test")["headers"]["user-agent"] == UA


def test_missing_pagination_preserves_captured_query_and_renewed_cookie(environment):
    c = FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    key = fb.COMMENTS_REDIS_KEY_TMPL.format(account="test")
    environment.cache.set(
        key, {**c._cache, "doc_id": "original", "variables_template": {"captured": True}, "pagination": None}
    )
    c._session.cookies.set("xs", "renewed", domain=".facebook.com")
    assert sdk.ensure_recipe(c, "comments")
    saved = environment.cache.get(key)
    assert saved["doc_id"] == "original"
    assert saved["variables_template"] == {"captured": True}
    assert saved["pagination"]["fb_api_req_friendly_name"] == "CommentsListComponentsPaginationQuery"
    assert saved["cookies"]["xs"] == "renewed"


def test_structured_restriction_in_http_200_stops_discovery(environment, monkeypatch):
    monkeypatch.setattr(environment.Session, "get", Mock(return_value=response('{"error":1675004}')))
    rest = Mock()
    monkeypatch.setattr(FacebookGraphQLClient, "_rest_restricted_account", rest)
    with pytest.raises(core.AccountRestrictedError):
        FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    assert environment.Session.get.call_count == 1
    rest.assert_called_once_with("error_1675004")


def test_search_pagination_is_filled_without_replacing_initial_query(environment):
    c = FacebookGraphQLClient(redis_cache=environment.cache, account="test")
    key = fb.CACHE_REDIS_KEY_TMPL.format(account="test")
    recipe = {**c._cache, "doc_id": "original-first-page", "pagination": None}
    environment.cache.set(key, recipe)
    c._cache = recipe
    c._post_with_retry = Mock(return_value=response('{"data":{"ok":true}}'))
    assert c.search_next_page("film", "next-cursor")["data"]["ok"]
    assert c._cache["doc_id"] == "original-first-page"
    variables = json.loads(c._post_with_retry.call_args.kwargs["body"]["variables"])
    assert variables["cursor"] == "next-cursor"
    assert variables["args"]["text"] == "film"
