"""Cổng cookie, khai báo client, chính sách IP và dòng thời gian (04/11/2026). Không gọi Facebook, Redis hay DB thật."""

import time
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

from scripts import measure_proxy_ip, open_account_browser
from social_crawler.accounts import session_client, session_quality
from social_crawler.db import account_events, accounts, proxies
from social_crawler.runtime import pool
from social_crawler.spiders.common import comet_graphql_client as core
from social_crawler.spiders.facebook.auth import http_refresh, sdk_fallback
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient

NOW = 1_800_000_000
MAC_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/156.0.0.0 Safari/537.36"
real_policy = pytest.mark.real_session_policy


def good_cookies(age_days=5):
    return {"c_user": "123", "xs": f"33:abc:2:{NOW - age_days * 86400}:-1:1", "datr": "d", "sb": "s", "fr": "f"}


# --- cookie ---------------------------------------------------------------------------------------


def test_cookie_from_a_real_browser_passes():
    result = session_quality.assess(good_cookies(), MAC_UA, "123", now=NOW)
    assert result.usable
    assert result.facts["xs_age_days"] == 5
    assert result.facts["ua_system"] == "mac"


@pytest.mark.parametrize(
    "ua",
    [
        # Đúng các UA đi kèm tài khoản mua sẵn đo được ngày 04/11/2026.
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/137.0.1697.129 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/137.0.2670.267 Safari/537.36",
    ],
)
def test_impossible_chrome_build_is_refused(ua):
    assert session_quality.assess(good_cookies(), ua, "123", now=NOW).problems == ["user_agent_impossible_chrome_build"]


@pytest.mark.parametrize(
    "ua",
    [
        MAC_UA,
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 Chrome/137.0.7151.119 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; rv:130.0) Gecko/20100101 Firefox/130.0",
    ],
)
def test_real_user_agents_pass(ua):
    assert session_quality.assess(good_cookies(), ua, "123", now=NOW).usable


def test_session_from_the_lost_test_account_is_refused():
    assert "xs_too_old" in session_quality.assess(good_cookies(376), MAC_UA, "123", now=NOW).problems


def test_old_session_inside_the_limit_passes():
    assert session_quality.assess(good_cookies(142), MAC_UA, "123", now=NOW).usable


def test_missing_parts_and_wrong_owner_are_refused():
    assert session_quality.assess({}, None, now=NOW).problems == [
        "missing_c_user",
        "missing_xs",
        "missing_datr",
        "missing_user_agent",
    ]
    assert "c_user_mismatch" in session_quality.assess(good_cookies(), MAC_UA, "999", now=NOW).problems


def test_unreadable_xs_is_unknown_not_refused():
    cookies = {**good_cookies(), "xs": "not-a-known-format"}
    result = session_quality.assess(cookies, MAC_UA, "123", now=NOW)
    assert result.usable and result.facts["xs_age_days"] is None


def test_facts_never_contain_cookie_values():
    cookies = good_cookies()
    facts = str(session_quality.assess(cookies, MAC_UA, "123", now=NOW).facts)
    assert cookies["xs"] not in facts and "abc" not in facts


def test_url_encoded_xs_is_read():
    cookies = {**good_cookies(), "xs": f"33%3Aabc%3A2%3A{NOW - 10 * 86400}%3A-1%3A1"}
    assert session_quality.assess(cookies, MAC_UA, "123", now=NOW).facts["xs_age_days"] == 10


# --- một phiên một client ----------------------------------------------------------------------------


@real_policy
@pytest.mark.parametrize("declared", [None, "manual", "browser"])
def test_http_client_is_refused_unless_declared(monkeypatch, declared):
    monkeypatch.setattr(session_client, "session_client_of", lambda *a: declared)
    events = Mock()
    monkeypatch.setattr(account_events, "record", events)
    with pytest.raises(session_client.SessionClientMismatch):
        session_client.require("test", session_client.HTTP)
    assert events.call_args.args[2] == "session_client_refused"


@real_policy
def test_declared_client_and_manual_default_slot_pass(monkeypatch):
    monkeypatch.setattr(session_client, "session_client_of", lambda *a: "http")
    monkeypatch.setattr(account_events, "record", Mock())
    session_client.require("test", session_client.HTTP)
    monkeypatch.setattr(session_client, "session_client_of", Mock(side_effect=AssertionError("no row for default")))
    session_client.require("default", session_client.BROWSER)


@real_policy
def test_client_constructor_sends_nothing_when_session_belongs_to_a_person(monkeypatch):
    monkeypatch.setattr(session_client, "session_client_of", lambda *a: "manual")
    monkeypatch.setattr(account_events, "record", Mock())
    monkeypatch.setattr(core.account_lease, "acquire", Mock())
    release = Mock()
    monkeypatch.setattr(core.account_lease, "release", release)
    acquire_proxy = Mock(side_effect=AssertionError("must stop before choosing a proxy"))
    monkeypatch.setattr(core.pool, "acquire_proxy_for_account", acquire_proxy)
    cache = SimpleNamespace(get=lambda key: None)
    with pytest.raises(core.SessionExpiredError):
        FacebookGraphQLClient(redis_cache=cache, account="test")
    release.assert_called_once()


@real_policy
def test_nurture_skips_a_session_it_does_not_own(monkeypatch):
    from social_crawler.accounts import nurture

    monkeypatch.setattr(session_client, "session_client_of", lambda *a: "http")
    monkeypatch.setattr(account_events, "record", Mock())
    launch = Mock(side_effect=AssertionError("must not open a browser"))
    monkeypatch.setattr(nurture, "launch_chromium", launch)
    outcome = nurture.nurture_one(
        None,
        platform="facebook",
        account={"id": "1", "cookie": ""},
        redis_cache=None,
        headless=True,
        min_scrolls=0,
        max_scrolls=0,
        visits=0,
    )
    assert outcome == "skipped"


def test_manual_chrome_does_not_open_on_a_session_the_system_holds():
    row = {"id": 128, "platform": "facebook", "session_client": "http", "ip_type": "static"}
    assert "http" in open_account_browser.policy_refusal(row)
    assert open_account_browser.policy_refusal({**row, "session_client": "manual"}) is None
    assert open_account_browser.policy_refusal({**row, "session_client": "manual", "ip_type": "rotating"})
    assert open_account_browser.policy_refusal({"id": 6, "platform": "threads", "session_client": None}) is None
    assert "for Testing" in open_account_browser.chrome_app_for("facebook", None)
    assert open_account_browser.chrome_app_for("threads", None) == "Google Chrome"


def test_manual_days_count_toward_warmup(monkeypatch):
    from social_crawler.accounts import warmup

    monkeypatch.setattr(warmup, "nurture_only_accounts", lambda platform: {})
    monkeypatch.setattr(warmup, "_required_days", lambda platform: {"a": 2, "b": 2})
    monkeypatch.setattr(warmup, "nurture_days", lambda *a: 0)
    monkeypatch.setattr(warmup, "manual_nurture_days", lambda platform: {"a": 3})
    assert warmup.unwarmed_account_ids("facebook", SimpleNamespace()) == {"b"}


# --- chính sách IP ------------------------------------------------------------------------------------


def proxy(**fields):
    return {"id": 18, "url": "1.2.3.4:80", "platform": "facebook", "ip_type": "static", **fields}


class Conn:
    def __init__(self, n=0, error=None):
        self.n, self.error = n, error

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *args):
        if self.error:
            raise self.error
        return SimpleNamespace(fetchone=lambda: {"n": self.n})


@real_policy
@pytest.mark.parametrize(
    "fields,reason",
    [
        ({"ip_type": "rotating"}, "rotating_ip"),
        ({"ip_type": "unknown"}, "ip_type_not_static"),
        ({"platform": "public"}, "public_proxy"),
    ],
)
def test_facebook_refuses_proxies_outside_policy(monkeypatch, fields, reason):
    monkeypatch.setattr(proxies, "connect", Mock(side_effect=AssertionError("label alone decides")))
    assert proxies.meta_proxy_violation("facebook", proxy(**fields), "a") == reason


@real_policy
def test_facebook_needs_its_own_ip(monkeypatch):
    monkeypatch.setattr(proxies, "connect", lambda: Conn(n=1))
    assert proxies.meta_proxy_violation("facebook", proxy(), "a") == "shared_ip"
    monkeypatch.setattr(proxies, "connect", lambda: Conn(n=0))
    assert proxies.meta_proxy_violation("facebook", proxy(), "a") is None


@real_policy
def test_unreadable_policy_stops_facebook(monkeypatch):
    monkeypatch.setattr(proxies, "connect", lambda: Conn(error=psycopg.OperationalError("down")))
    assert proxies.meta_proxy_violation("facebook", proxy(), "a") == "policy_unreadable"


@real_policy
def test_threads_only_refuses_rotating(monkeypatch):
    monkeypatch.setattr(proxies, "connect", Mock(side_effect=AssertionError("no db")))
    assert proxies.meta_proxy_violation("threads", proxy(ip_type="unknown"), "a") is None
    assert proxies.meta_proxy_violation("threads", proxy(ip_type="rotating"), "a") == "rotating_ip"


@real_policy
def test_pool_refuses_before_claiming_the_port(monkeypatch):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", lambda *a: (1, 18))
    monkeypatch.setattr(pool, "get_proxy_by_id", lambda _: proxy(ip_type="rotating"))
    monkeypatch.setattr(pool.ip_switch, "claim", Mock(side_effect=AssertionError("must not claim")))
    events = Mock()
    monkeypatch.setattr(account_events, "record", events)
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.acquire_proxy_for_account("facebook", "a", required=True)
    assert events.call_args.kwargs["violation"] == "rotating_ip"


@real_policy
@pytest.mark.parametrize("path", ["first_pin", "repin"])
def test_every_way_to_get_a_proxy_passes_the_policy(monkeypatch, path):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", lambda *a: (1, None if path == "first_pin" else 18))
    monkeypatch.setattr(pool, "get_proxy_by_id", lambda _: None)
    monkeypatch.setattr(pool, "get_proxy_raw_status", lambda _: {"enabled": False, "consecutive_failures": 0})
    monkeypatch.setattr(pool, "_meta_auto_repin", lambda: True)
    monkeypatch.setattr(pool, "pin_account_to_least_loaded_proxy", lambda *a: proxy(ip_type="unknown"))
    monkeypatch.setattr(pool.ip_switch, "claim", Mock(side_effect=AssertionError("must not claim")))
    monkeypatch.setattr(account_events, "record", Mock())
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.acquire_proxy_for_account("facebook", "a", required=True)


@real_policy
def test_first_pin_also_watches_the_exit_ip(monkeypatch):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", lambda *a: (1, None))
    monkeypatch.setattr(pool, "pin_account_to_least_loaded_proxy", lambda *a: proxy())
    monkeypatch.setattr(pool, "meta_proxy_violation", lambda *a: None)
    monkeypatch.setattr(pool.ip_switch, "claim", lambda *a: True)
    watch = Mock(return_value=False)
    monkeypatch.setattr(pool, "watch_exit_ip", watch)
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.acquire_proxy_for_account("facebook", "a", required=True)
    watch.assert_called_once()


class Sql:
    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.statements.append(sql)
        return SimpleNamespace(fetchone=lambda: None)


def test_automatic_pin_counts_load_by_endpoint_not_row_id(monkeypatch):
    sql = Sql()
    monkeypatch.setattr(proxies, "connect", lambda: sql)
    assert proxies.pin_account_to_least_loaded_proxy("facebook", 1) is None
    assert "same.proxy_url = pp.proxy_url" in sql.statements[0]
    assert "pa.assigned_proxy_id = pp.id" not in sql.statements[0]


class RecipeCache:
    def __init__(self, recipe):
        from social_crawler.constants import facebook as fb

        self.key = fb.CACHE_REDIS_KEY_TMPL.format(account="a")
        self.deleted = []
        self.recipe = recipe

    def delete(self, key):
        self.deleted.append(key)


HTTP_RECIPE = {"recipe_source": sdk_fallback.SOURCE, "transport": "curl_cffi"}
OLD_REQUESTS_RECIPE = {"recipe_source": sdk_fallback.SOURCE, "transport": "requests"}
BROWSER_RECIPE = {"doc_id": "1"}


@real_policy
@pytest.mark.parametrize(
    "declared,recipe,kept",
    [
        ("http", BROWSER_RECIPE, False),
        ("browser", HTTP_RECIPE, False),
        ("browser", OLD_REQUESTS_RECIPE, False),
        ("http", HTTP_RECIPE, True),
        ("http", OLD_REQUESTS_RECIPE, True),
        ("browser", BROWSER_RECIPE, True),
        ("manual", BROWSER_RECIPE, True),
        (None, HTTP_RECIPE, True),
    ],
)
def test_changing_client_discards_only_the_old_clients_recipes(monkeypatch, declared, recipe, kept):
    monkeypatch.setattr(session_client, "session_client_of", lambda *a: declared)
    monkeypatch.setattr(account_events, "record", Mock())
    cache = RecipeCache(recipe)
    result = session_client.discard_recipes_of_other_client(cache, "a", recipe)
    assert result is (recipe if kept else None)
    assert bool(cache.deleted) is (not kept)
    if not kept:
        assert cache.key in cache.deleted and len(cache.deleted) == 5


@real_policy
def test_client_rebuilds_after_switching_from_browser_to_http(monkeypatch):
    monkeypatch.setattr(session_client, "session_client_of", lambda *a: "http")
    monkeypatch.setattr(account_events, "record", Mock())
    monkeypatch.setattr(core.account_lease, "acquire", Mock())
    monkeypatch.setattr(core.account_lease, "release", Mock())
    seed = Mock(return_value=None)
    monkeypatch.setattr(sdk_fallback, "seed", seed)
    store = {}
    cache = SimpleNamespace(
        get=lambda key: store.get(key, BROWSER_RECIPE | {"captured_at": 1}),
        delete=lambda key: store.__setitem__(key, None),
    )
    with pytest.raises(core.SessionExpiredError, match="HTTP setup requires"):
        FacebookGraphQLClient(redis_cache=cache, account="a")
    seed.assert_called_once()


class WatchCache:
    def __init__(self):
        self.data = {}

    def set_nx(self, key, value, ttl_seconds=None):
        return True

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value


@real_policy
def test_mislabelled_proxy_is_caught_by_watching_its_exit_ip(monkeypatch):
    ips = iter(["1.1.1.1", "2.2.2.2", "3.3.3.3"])
    monkeypatch.setattr(pool.curl_requests, "get", lambda *a, **k: SimpleNamespace(status_code=200, text=next(ips)))
    monkeypatch.setattr(pool, "build_proxy_url", lambda p: "http://x")
    mark = Mock()
    monkeypatch.setattr(pool, "mark_proxy_rotating", mark)
    events = Mock()
    monkeypatch.setattr(account_events, "record", events)
    cache = WatchCache()
    assert pool.watch_exit_ip(proxy(), "a", cache) is True
    assert pool.watch_exit_ip(proxy(), "a", cache) is True
    mark.assert_not_called()
    assert pool.watch_exit_ip(proxy(), "a", cache) is False
    mark.assert_called_once_with(18)
    assert events.call_args.args[2] == "proxy_detected_rotating"


@real_policy
def test_stable_or_unmeasurable_exit_ip_passes(monkeypatch):
    monkeypatch.setattr(pool.curl_requests, "get", lambda *a, **k: SimpleNamespace(status_code=200, text="1.1.1.1"))
    monkeypatch.setattr(pool, "build_proxy_url", lambda p: "http://x")
    monkeypatch.setattr(pool, "mark_proxy_rotating", Mock(side_effect=AssertionError("stable")))
    monkeypatch.setattr(account_events, "record", Mock())
    cache = WatchCache()
    for _ in range(5):
        assert pool.watch_exit_ip(proxy(), "a", cache) is True
    monkeypatch.setattr(pool.curl_requests, "get", lambda *a, **k: SimpleNamespace(status_code=503, text=""))
    assert pool.watch_exit_ip(proxy(), "a", cache) is True


def test_old_sightings_leave_the_window(monkeypatch):
    monkeypatch.setattr(pool.curl_requests, "get", lambda *a, **k: SimpleNamespace(status_code=200, text="3.3.3.3"))
    monkeypatch.setattr(pool, "build_proxy_url", lambda p: "http://x")
    old = int(time.time()) - 2 * 3600
    cache = WatchCache()
    cache.data["proxy_ip:seen:18"] = [["1.1.1.1", old], ["2.2.2.2", old]]
    assert pool.watch_exit_ip(proxy(), "a", cache) is True


@pytest.mark.parametrize(
    "ips,label",
    [
        (["1.1.1.1"] * 7, "static"),
        (["1.1.1.1", "2.2.2.2"], "rotating"),
        (["1.1.1.1", None, "1.1.1.1"], None),
        ([None, None], None),
        (["1.1.1.1", None, "2.2.2.2"], "rotating"),
    ],
)
def test_measure_only_concludes_from_complete_data(ips, label):
    assert measure_proxy_ip.classify(ips) == label


# --- dòng thời gian ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "headers,signal",
    [
        ({"Set-Cookie": "c_user=deleted; Path=/"}, "set_cookie_c_user_deleted"),
        ({"set-login": "logged-out"}, "set_login_logged_out"),
        ({"set-cookie": "datr=deleted"}, None),
    ],
)
def test_logout_signal_names_the_header(headers, signal):
    assert http_refresh.logout_signal(headers) == signal


@real_policy
def test_failed_event_write_never_breaks_the_caller(monkeypatch):
    monkeypatch.setattr(account_events, "connect", Mock(side_effect=psycopg.OperationalError("down")))
    account_events.record("facebook", "a", "anything", detail=1)


@real_policy
def test_event_without_an_account_is_dropped(monkeypatch):
    connect = Mock(side_effect=AssertionError("no write"))
    monkeypatch.setattr(account_events, "connect", connect)
    account_events.record("facebook", "", "anything")


@real_policy
def test_logout_event_records_signal_without_query_string(monkeypatch):
    events = Mock()
    monkeypatch.setattr(account_events, "record", events)
    client = object.__new__(FacebookGraphQLClient)
    client._account = "a"
    client._session = SimpleNamespace(_wrapped=SimpleNamespace())
    client._drop_rejected_session = Mock()
    response = SimpleNamespace(
        status_code=302,
        url="https://www.facebook.com/?token=secret",
        headers={"set-login": "logged-out", "location": "https://www.facebook.com/login?next=secret"},
    )
    with pytest.raises(core.SessionExpiredError):
        client._check_session_headers(response)
    details = events.call_args.kwargs
    assert details["signal"] == "set_login_logged_out" and details["status"] == 302
    assert "secret" not in str(details)
    assert "set_login_logged_out" in client._drop_rejected_session.call_args.args[0]


@real_policy
def test_dead_check_writes_an_event(monkeypatch):
    monkeypatch.setattr(accounts, "connect", lambda: Conn())
    events = Mock()
    monkeypatch.setattr(account_events, "record", events)
    accounts.record_cookie_check("facebook", "a", status="alive", note=None)
    events.assert_not_called()
    accounts.record_cookie_check("facebook", "a", status="dead", note="x")
    assert events.call_args.args[2] == "session_marked_dead"


# --- không còn đăng nhập tự động -------------------------------------------------------------------


@pytest.mark.parametrize("module", ["facebook", "threads"])
def test_account_without_session_never_opens_a_browser(monkeypatch, module):
    import importlib

    bootstrap = importlib.import_module(f"social_crawler.spiders.{module}.auth.bootstrap")
    account = {"id": "1", "email": "a@x.com", "password": "secret", "cookie": ""}
    monkeypatch.setattr(bootstrap, "get_account_by_key", lambda *a: account)
    monkeypatch.setattr(bootstrap, "launch_chromium", Mock(side_effect=AssertionError("must not open a browser")))
    monkeypatch.setattr(bootstrap.pool, "acquire_proxy_for_account", Mock(side_effect=AssertionError("no proxy")))
    monkeypatch.setattr(bootstrap, "account_lease", SimpleNamespace(acquire=Mock(), release=Mock()), raising=False)
    cache = SimpleNamespace(get=lambda key: None, set=Mock(), delete=Mock())
    with pytest.raises(RuntimeError, match="import cookies"):
        bootstrap._get_authenticated_context(object(), cache, None, False, prefer_account="a@x.com")


def test_old_cookie_check_message_is_skipped_not_run_as_a_search(monkeypatch):
    import asyncio

    from social_crawler.consumer import dispatch

    monkeypatch.setattr(dispatch, "_already_done", lambda run_id: False)
    monkeypatch.setattr(dispatch, "_cancel_requested", lambda **k: False)
    monkeypatch.setattr(dispatch, "_published_before_stop", lambda *a, **k: False)
    monkeypatch.setattr(dispatch, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(dispatch, "start_task", lambda request: None)
    monkeypatch.setattr(dispatch, "finish_task", Mock())
    monkeypatch.setattr(dispatch, "_mark_done", lambda run_id: None)
    spider = Mock(side_effect=AssertionError("must not run as a search"))
    monkeypatch.setattr(dispatch, "_run_spider", spider)
    assert (
        asyncio.run(dispatch._handle_request({"type": "cookie_check", "platform": "facebook", "run_id": "r"})) is True
    )
    assert dispatch.finish_task.call_args.args[1] == "skipped"
