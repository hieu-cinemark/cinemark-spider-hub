"""Cổng cookie, chính sách IP và dòng thời gian (04/11/2026). Không gọi Facebook, Redis hay DB thật."""

from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

from scripts import measure_proxy_ip, open_account_browser
from social_crawler.accounts import session_quality
from social_crawler.db import account_events, accounts
from social_crawler.spiders.common import comet_graphql_client as core
from social_crawler.spiders.facebook.auth import http_refresh
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
def test_facebook_nurture_never_opens_a_browser(monkeypatch):
    from social_crawler.accounts import nurture

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
    assert open_account_browser.policy_refusal(row) is None
    assert open_account_browser.policy_refusal({**row, "session_client": "manual"}) is None
    assert open_account_browser.policy_refusal({**row, "session_client": "manual", "ip_type": "rotating"})
    assert open_account_browser.policy_refusal({"id": 6, "platform": "threads", "session_client": None}) is None
    assert "for Testing" in open_account_browser.chrome_app_for("facebook", None)
    assert open_account_browser.chrome_app_for("threads", None) == "Google Chrome"


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
@real_policy
@real_policy
@real_policy
@real_policy
@real_policy
@real_policy
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
@real_policy
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
