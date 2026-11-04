"""Native Facebook identity, passive health checks and fail-closed request control.

No account, Facebook request, real browser or real Redis is used here.
"""

import base64
import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import redis

from social_crawler.constants import facebook as fb
from social_crawler.spiders.common import comet_graphql_client as cgc
from social_crawler.spiders.facebook.auth import identity
from social_crawler.spiders.facebook.auth.cookies import extract_facebook_user_agent, extract_user_agent
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"


class Cache:
    def __init__(self, data=None):
        self.data = copy.deepcopy(data or {})

    def get(self, key):
        return copy.deepcopy(self.data.get(key))

    def set(self, key, value, **kwargs):
        self.data[key] = copy.deepcopy(value)

    def delete(self, key):
        self.data.pop(key, None)

    def next_counter(self, key, floor, ttl):
        value = max(self.data.get(key, 0), floor) + 1
        self.data[key] = value
        return value


class Context:
    def __init__(self, state=None):
        self.state = state or {"cookies": [], "origins": []}
        self.closed = False

    def new_page(self):
        return SimpleNamespace(evaluate=lambda js: UA)

    def close(self):
        self.closed = True

    def storage_state(self):
        return copy.deepcopy(self.state)


class Browser:
    def __init__(self):
        self.calls = []
        self.contexts = []

    def new_context(self, **kwargs):
        self.calls.append(kwargs)
        ctx = Context(kwargs.get("storage_state"))
        self.contexts.append(ctx)
        return ctx


@pytest.fixture(autouse=True)
def linux_runtime(monkeypatch):
    monkeypatch.setattr(identity, "runtime_system", lambda: "Linux")


def test_native_browser_has_no_ua_cdp_or_device_overrides():
    browser = Browser()
    cache = Cache()
    context = identity.new_facebook_context(browser, "test", redis_cache=cache)
    assert browser.calls == [{}, {"no_viewport": True}]
    assert browser.contexts[0].closed
    assert context._facebook_native_identity["user_agent"] == UA
    assert not cache.data  # Opening a context is not a successful authenticated login.


@pytest.mark.parametrize(
    "expected", [None, UA.replace("151.", "146."), UA.replace("X11; Linux x86_64", "Macintosh; Intel Mac OS X 10_15_7")]
)
def test_restore_mismatch_fails_before_cookies_are_loaded(expected):
    browser = Browser()
    with pytest.raises(identity.BrowserIdentityError):
        identity.new_facebook_context(
            browser, "test", user_agent=expected, storage_state={"cookies": [{"name": "c_user", "value": "1"}]}
        )
    assert browser.calls == [{}]
    assert browser.contexts[0].closed


def test_restore_matching_identity_uses_native_browser_not_override():
    cache = Cache({identity.IDENTITY_KEY.format(account="test"): {"user_agent": UA}})
    browser = Browser()
    identity.new_facebook_context(
        browser, "test", redis_cache=cache, storage_state={"cookies": [{"name": "c_user", "value": "1"}]}
    )
    assert "user_agent" not in browser.calls[-1]
    assert "extra_http_headers" not in browser.calls[-1]


def test_imported_ua_is_preserved_for_fb_and_legacy_threads_behavior_remains():
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0.0.0"
    cookies = {"useragent": base64.b64encode(ua.encode()).decode()}
    assert extract_facebook_user_agent(cookies) == ua
    assert "Chrome/151." in extract_user_agent(cookies)


def test_fb_preserves_headers_and_rejects_foreign_os():
    c = object.__new__(FacebookGraphQLClient)
    c._account = "test"
    cache = {"headers": {"user-agent": UA, "sec-ch-ua-platform": '"Linux"', "accept-language": "en-US"}}
    assert c._prepare_cache(cache) == cache
    assert c._prepare_cache(cache) is not cache
    with pytest.raises(cgc.SessionExpiredError):
        c._prepare_cache(
            {"headers": {"user-agent": UA.replace("X11; Linux x86_64", "Macintosh; Intel Mac OS X 10_15_7")}}
        )


def test_new_login_invalidates_old_recipes_once_and_saves_real_identity():
    key = fb.CACHE_REDIS_KEY_TMPL.format(account="test")
    cache = Cache({key: {"headers": {"user-agent": UA}, "cookies": {"c_user": "1"}}})
    context = identity.new_facebook_context(Browser(), "test", redis_cache=cache)
    context.state = {"cookies": [{"name": "c_user", "value": "1", "domain": ".facebook.com"}], "origins": []}
    identity.save_native_state(cache, "test", context)
    assert cache.get(key) is None
    assert cache.get(identity.IDENTITY_KEY.format(account="test"))["user_agent"] == UA
    cache.set(key, {"headers": {"user-agent": UA}, "cookies": {"c_user": "1"}})
    identity.save_native_state(cache, "test", context)
    assert cache.get(key) is not None
    header = identity.native_cookie_header("c_user=1; useragent=old", context)
    assert base64.b64encode(UA.encode()).decode() in header


def make_client(cache, capture="capture-1"):
    c = object.__new__(FacebookGraphQLClient)
    c._account = "test"
    c._cache = {"captured_at": 1, "request_context_id": capture, "body_static": {"__req": "7", "__s": "captured"}}
    c._redis = cache
    c._req_counter = c._initial_req_counter()
    return c


def test_request_sequence_survives_clients_and_new_capture_has_own_scope():
    cache = Cache()
    assert make_client(cache)._next_req_counter() == 8
    assert make_client(cache)._next_req_counter() == 9
    assert make_client(cache, "capture-2")._next_req_counter() == 8


def test_counter_error_or_unknown_format_never_resets_to_seed():
    c = make_client(Cache())
    c._redis.next_counter = Mock(side_effect=redis.RedisError("unavailable"))
    with pytest.raises(cgc.NetworkError):
        c._next_req_counter()
    c._cache["body_static"]["__req"] = "unsupported!"
    with pytest.raises(cgc.GraphQLSchemaError):
        c._initial_req_counter()


def test_each_physical_retry_reserves_its_own_sequence(monkeypatch):
    c = make_client(Cache())
    c._proxy_cfg = None
    c._throttle = Mock()
    c._adjust_interval = Mock()
    c._record_proxy_outcome_once = Mock()
    monkeypatch.setattr(cgc.ip_switch, "touch", lambda *a: None)
    monkeypatch.setattr(cgc.time, "sleep", lambda *a: None)
    c._session = SimpleNamespace(
        post=Mock(
            side_effect=[SimpleNamespace(status_code=503, headers={}), SimpleNamespace(status_code=200, headers={})]
        )
    )
    c._post_with_retry({}, {}, {"__req": "7"})
    assert [call.kwargs["data"]["__req"] for call in c._session.post.call_args_list] == ["8", "9"]


def test_budget_failure_stops_before_post(monkeypatch):
    from social_crawler.runtime import search_budget

    c = make_client(Cache())
    c._proxy_cfg = None
    c._session = SimpleNamespace(post=Mock())
    c._throttle = c._count_request
    monkeypatch.setattr(cgc.ip_switch, "touch", lambda *a: None)
    monkeypatch.setattr(search_budget, "count_request", Mock(side_effect=redis.RedisError("unavailable")))
    with pytest.raises(cgc.NetworkError):
        c._post_with_retry({}, {}, {})
    c._session.post.assert_not_called()


def test_hourly_budget_still_applies_when_daily_cap_is_disabled(monkeypatch):
    from social_crawler.runtime import search_budget as budget

    c = make_client(Cache())
    monkeypatch.setattr(budget, "DAILY_CAP", 0)
    monkeypatch.setattr(budget, "HOURLY_CAP", 60)
    monkeypatch.setattr(budget, "count_request", lambda *a: 100)
    monkeypatch.setattr(budget, "used_this_hour", lambda *a: 100)
    monkeypatch.setattr(budget, "ramp_factor", lambda *a: 1)
    monkeypatch.setattr(budget, "new_account_cap", lambda *a: 0)
    with pytest.raises(cgc.RequestBudgetExceeded):
        c._count_request()


@pytest.mark.parametrize("operation", ["get", "set"])
def test_shared_throttle_failure_stops_request(monkeypatch, operation):
    c = make_client(Cache())
    c._count_request = Mock()
    c._current_interval = Mock(return_value=1)
    c._last_request_at = None
    setattr(c._redis, operation, Mock(side_effect=redis.RedisError("unavailable")))
    with pytest.raises(cgc.NetworkError):
        c._throttle()


def test_uninspected_state_path_cannot_bypass_restore_validation():
    browser = Browser()
    with pytest.raises(identity.BrowserIdentityError):
        identity.new_facebook_context(browser, storage_state="state.json")
    assert not browser.calls


def test_imported_cookie_invalidates_old_identity_instead_of_reusing_it(monkeypatch):
    from contextlib import nullcontext

    from social_crawler.runtime import account_lease
    from social_crawler.spiders.facebook.auth import cookies

    key = fb.CACHE_REDIS_KEY_TMPL.format(account="test")
    cache = Cache(
        {key: {"headers": {"user-agent": UA}}, identity.IDENTITY_KEY.format(account="test"): {"user_agent": UA}}
    )
    monkeypatch.setattr(cookies, "RedisCache", lambda: cache)
    monkeypatch.setattr(account_lease, "hold", lambda *args: nullcontext())
    cookies.import_cookies({"c_user": "1", "xs": "new", "datr": "device"}, account="test")
    assert cache.get(key) is None
    assert cache.get(identity.IDENTITY_KEY.format(account="test")) is None
    assert cache.get(fb.STATE_REDIS_KEY_TMPL.format(account="test"))["cookies"]


def test_removed_facebook_nurture_does_not_open_browser_or_penalize_proxy(monkeypatch):
    from social_crawler.accounts import nurture

    browser = SimpleNamespace(close=Mock())
    monkeypatch.setattr(nurture, "_load_state", lambda *a: {"cookies": [{"name": "c_user", "value": "1"}]})
    monkeypatch.setattr(nurture, "_proxy_for", lambda *a: ({"server": "http://proxy"}, {"id": 1, "url": "proxy"}))
    monkeypatch.setattr(nurture, "launch_chromium", lambda *a, **kw: browser)
    monkeypatch.setattr(
        nurture, "new_facebook_context", Mock(side_effect=identity.BrowserIdentityError("different runtime"))
    )
    release = Mock()
    monkeypatch.setattr(nurture.pool, "release_proxy", release)
    monkeypatch.setattr(nurture.ip_switch, "release", Mock())
    result = nurture.nurture_one(
        None,
        platform="facebook",
        account={"email": "test", "id": "1"},
        redis_cache=Cache(),
        headless=True,
        min_scrolls=1,
        max_scrolls=1,
        visits=0,
    )
    assert result == "skipped"
    release.assert_not_called()
    browser.close.assert_not_called()
