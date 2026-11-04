"""Review findings of 04/11/2026: wire headers and cookies, CDN isolation, partial discovery and setup errors."""

import asyncio
import copy
import http.server
import socket
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests
from curl_cffi import CurlOpt
from curl_cffi import requests as cr
from py_mini_racer import JSOOMException, JSTimeoutException

from social_crawler.consumer import sessions
from social_crawler.consumer.common import CrawlJobSkipped
from social_crawler.runtime import pool
from social_crawler.runtime.facebook_pacing import PacedSession
from social_crawler.spiders.common import comet_graphql_client as core
from social_crawler.spiders.facebook.auth import http_refresh, identity, operation_fallback, sdk_fallback
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient

UA_152 = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
V8_STOPS = [JSTimeoutException(), JSOOMException()]


class Handler(http.server.BaseHTTPRequestHandler):
    seen = []

    def log_message(self, *args):
        pass

    def do_GET(self):
        Handler.seen.append(self.headers)
        self.send_response(200)
        cookie = {
            "/set-cookie": "fr=new; Path=/; Domain=.facebook.com",
            "/delete-xs": "xs=deleted; Path=/; Domain=.facebook.com; Max-Age=0",
            "/delete-xs-expired": "xs=deleted; Path=/; Domain=.facebook.com; Expires=Thu, 01 Jan 1970 00:00:01 GMT",
        }.get(self.path)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")


@pytest.fixture(scope="module")
def server():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def wire_session(server=None, finished=None):
    # www.facebook.com trỏ về server local: không có gì ra mạng, nhưng cookie đi đúng domain như thật.
    options = {CurlOpt.RESOLVE: [f"www.facebook.com:{server.rsplit(':', 1)[1]}:127.0.0.1"]} if server else {}
    session = cr.Session(impersonate="chrome152_macos", default_headers=True, trust_env=False, curl_options=options)
    session.headers.update({"user-agent": UA_152})
    return PacedSession(session, finished or Mock())


def fb_url(server, path):
    return f"http://www.facebook.com:{server.rsplit(':', 1)[1]}{path}"


def sent_cookies():
    headers = Handler.seen[-1].get_all("Cookie") or []
    assert len(headers) <= 1
    pairs = [part.split("=") for part in headers[0].split("; ")] if headers else []
    assert len({name for name, _ in pairs}) == len(pairs)
    return dict(pairs)


def test_wire_sends_each_cookie_once_and_the_newest_value_wins(server):
    Handler.seen.clear()
    session = wire_session(server)
    cached = {"c_user": "123", "fr": "old"}
    session.get(fb_url(server, "/set-cookie"), cookies=cached, timeout=5)
    assert sent_cookies() == cached
    session.get(fb_url(server, "/plain"), cookies=cached, timeout=5)
    assert sent_cookies() == {"c_user": "123", "fr": "new"}


@pytest.mark.parametrize("path", ["/delete-xs", "/delete-xs-expired"])
def test_wire_cookie_deleted_by_the_server_is_not_sent_again(server, path):
    Handler.seen.clear()
    session = wire_session(server)
    cached = {"c_user": "123", "xs": "old"}
    session.get(fb_url(server, path), cookies=cached, timeout=5)
    session.get(fb_url(server, "/plain"), cookies=cached, timeout=5)
    assert sent_cookies() == {"c_user": "123"}
    assert session.removed_cookies() == {"xs"}


def test_deleted_cookie_does_not_survive_in_the_saved_session(server):
    session = wire_session(server)
    session.get(fb_url(server, "/delete-xs"), cookies={"c_user": "123", "xs": "old"}, timeout=5)
    saved = {}
    client = SimpleNamespace(
        _account="test",
        _cache={"cookies": {"c_user": "123", "xs": "old"}, "body_static": {}},
        _session=session,
        _redis=SimpleNamespace(get=lambda key: None, set=lambda key, value, **kw: saved.__setitem__(key, value)),
    )
    http_refresh.save_tokens(client, {})
    assert "xs" not in client._cache["cookies"]
    assert not [c for state in saved.values() for c in state.get("cookies", []) if c["name"] == "xs"]


def test_wire_hints_match_the_user_agent_version(server):
    Handler.seen.clear()
    wire_session().get(f"{server}/plain", timeout=5)
    headers = Handler.seen[-1]
    assert headers["user-agent"] == UA_152
    assert 'v="152"' in headers["sec-ch-ua"]
    assert headers["sec-ch-ua-platform"] == '"macOS"'
    assert headers["sec-fetch-mode"] == "navigate"


def test_wire_sends_the_login_browser_brands_for_chrome_152(server):
    Handler.seen.clear()
    session = cr.Session(impersonate="chrome152_macos", default_headers=True, trust_env=False)
    session.headers.update(sdk_fallback.session_headers({"target": "chrome152_macos", "user_agent": UA_152}))
    session.get(f"{server}/plain", timeout=5)
    assert Handler.seen[-1].get_all("sec-ch-ua") == ['"Not?A_Brand";v="24", "Chromium";v="152"']


def test_other_targets_keep_their_own_brands():
    assert sdk_fallback.session_headers({"target": "chrome151_macos", "user_agent": "UA"}) == {"user-agent": "UA"}


def test_wire_bundle_request_is_not_a_navigation(server):
    Handler.seen.clear()
    wire_session().get(f"{server}/asset", headers=dict(sdk_fallback.ASSET_HEADERS), timeout=5)
    headers = Handler.seen[-1]
    assert headers["sec-fetch-dest"] == "script"
    assert "upgrade-insecure-requests" not in headers and "sec-fetch-user" not in headers


def test_network_failure_still_records_finish():
    finished = Mock()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with pytest.raises(cr.RequestsError):
        wire_session(finished=finished).get(f"http://127.0.0.1:{port}/", timeout=1)
    finished.assert_called_once()


@pytest.mark.parametrize(
    "ua,target",
    [
        (UA_152, "chrome152_macos"),
        (UA_152.replace("152.0", "156.0"), None),
        (UA_152.replace("Macintosh; Intel Mac OS X 10_15_7", "Windows NT 10.0; Win64; x64"), None),
    ],
)
def test_transport_requires_an_exact_chrome_and_system_match(monkeypatch, ua, target):
    monkeypatch.setattr(identity, "runtime_system", lambda: "macOS")
    if target:
        assert identity.transport_profile({"user-agent": ua})["target"] == target
        assert sdk_fallback._transport_problems(ua) == []
    else:
        assert sdk_fallback._transport_problems(ua) == ["no_matching_tls_transport"]


# --- discovery ---------------------------------------------------------------------------------


def discovery_client(monkeypatch, cdn_get):
    class Cdn:
        cookies = requests.cookies.RequestsCookieJar()

        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            return cdn_get(url)

    monkeypatch.setattr(sdk_fallback.requests, "Session", Cdn)
    monkeypatch.setenv("FACEBOOK_GQL_FALLBACK_ENABLED", "1")
    stored = {}
    redis = SimpleNamespace(
        get=lambda key: stored.get(key),
        set=lambda key, value, **kw: stored.__setitem__(key, value),
        set_nx=lambda *a, **kw: True,
    )
    page = '<script src="https://static.xx.fbcdn.net/rsrc.php/a"></script><script src="https://static.xx.fbcdn.net/rsrc.php/b"></script>'
    client = SimpleNamespace(
        _account="test",
        _redis=redis,
        _profile={"target": "chrome152_macos", "user_agent": UA_152},
        _proxy_cfg=None,
        _cache={"cookies": {"c_user": "123"}},
        _session=SimpleNamespace(
            get=lambda url, **kw: SimpleNamespace(status_code=200, headers={}, text=page, content=page.encode())
        ),
        _ensure_lease=Mock(),
        _throttle=Mock(),
        _throttle_asset=Mock(),
        _mark_request_finished=Mock(),
        _html_headers=lambda: {"accept": "text/html"},
        _check_session_headers=Mock(),
        _rest_restricted_account=Mock(),
        _drop_rejected_session=Mock(),
        STALL_SECONDS=15,
        STALL_MIN_BYTES_PER_SECOND=100,
    )
    return client


def cdn_response(body, status=200):
    from io import BytesIO

    class Raw(BytesIO):
        def read1(self, size=-1, decode_content=False):
            return super().read1(size)

    return SimpleNamespace(
        status_code=status, headers={}, text=body, content=body.encode(), raw=Raw(body.encode()), close=lambda: None
    )


NAME = "SearchCometResultsPaginatedResultsQuery"
BUNDLE = f'"{NAME}_facebookRelayOperation":"123456789012345"'


@pytest.mark.parametrize("status", [401, 403, 302, 429, 404])
def test_cdn_status_never_touches_the_account_and_other_bundles_still_count(monkeypatch, status):
    bodies = iter([cdn_response("", status), cdn_response(BUNDLE)])
    client = discovery_client(monkeypatch, lambda url: next(bodies))
    found = sdk_fallback.discover_authenticated(client, {NAME})
    assert found[NAME] == "123456789012345"
    client._rest_restricted_account.assert_not_called()
    client._drop_rejected_session.assert_not_called()


def test_cdn_network_error_skips_only_that_bundle(monkeypatch):
    def get(url):
        if url.endswith("/a"):
            raise cr.RequestsError("proxy")
        return cdn_response(BUNDLE)

    client = discovery_client(monkeypatch, get)
    assert sdk_fallback.discover_authenticated(client, {NAME})[NAME] == "123456789012345"


def test_deadline_during_throttle_keeps_ids_already_found(monkeypatch):
    other = '"Depth2CommentsListPaginationQuery_facebookRelayOperation":"999999999999999"'
    client = discovery_client(monkeypatch, lambda url: cdn_response(other))
    real_monotonic = time.monotonic
    calls = {"n": 0}

    def throttle():
        calls["n"] += 1
        if calls["n"] == 2:
            monkeypatch.setattr(sdk_fallback.time, "monotonic", lambda: real_monotonic() + 1000)

    client._throttle_asset = throttle
    found = sdk_fallback.discover_authenticated(client, {NAME})
    assert found == {"Depth2CommentsListPaginationQuery": "999999999999999"}
    saved = client._redis.get(sdk_fallback.DOCS_KEY.format(account="test"))
    assert saved["operations"] == found


@pytest.mark.parametrize("error", V8_STOPS)
def test_interrupted_v8_stops_discovery_and_keeps_ids(monkeypatch, error):
    other = '"Depth2CommentsListPaginationQuery_facebookRelayOperation":"999999999999999"'
    page = '<script src="https://static.xx.fbcdn.net/a"></script><script src="https://static.xx.fbcdn.net/b"></script>'
    fetched = []

    def fetch(url, asset, deadline):
        fetched.append(url)
        return other if asset else page

    runtime = object.__new__(operation_fallback.V8Operations)
    runtime.add = Mock(side_effect=error)
    runtime.close = Mock()
    monkeypatch.setattr(operation_fallback, "V8Operations", lambda: runtime)
    found = operation_fallback.discover(fetch, ["https://www.facebook.com/x"], {NAME})
    assert found == {"Depth2CommentsListPaginationQuery": "999999999999999"}
    assert len(fetched) == 2  # the isolate never received a second bundle
    runtime.close.assert_called_once()


def test_collect_stops_on_a_real_v8_timeout(monkeypatch):
    monkeypatch.setattr(operation_fallback, "V8_SECONDS", 0.03)
    runtime = operation_fallback.V8Operations()
    try:
        assert runtime.collect("while (true) {}") is None
    finally:
        runtime.close()


@pytest.mark.parametrize("error", V8_STOPS)
def test_collect_reports_interrupted_isolate(error):
    runtime = object.__new__(operation_fallback.V8Operations)
    runtime.add = Mock(side_effect=error)
    assert runtime.collect("source") is None


def test_collect_skips_only_an_oversized_bundle():
    runtime = object.__new__(operation_fallback.V8Operations)
    runtime.add = Mock(side_effect=ValueError("too large"))
    assert runtime.collect("source") == {}


@pytest.mark.parametrize("error", V8_STOPS)
def test_public_discovery_saves_ids_when_v8_stops(monkeypatch, error):
    stored = {}
    cache = SimpleNamespace(
        set_nx=lambda *a, **kw: True,
        get=lambda key: stored.get(key),
        set=lambda key, value, **kw: stored.__setitem__(key, value),
    )
    monkeypatch.setattr(http_refresh, "public_proxy", lambda proxy_id: "http://proxy:1")
    monkeypatch.setenv("FACEBOOK_GQL_FALLBACK_ENABLED", "1")
    static = '"Static_facebookRelayOperation":"123456789012345"'
    urls = []

    class Session:
        cookies = Mock()

        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            urls.append(url)
            text = (
                '<script src="https://static.xx.fbcdn.net/a"></script><script src="https://static.xx.fbcdn.net/b"></script>'
                if "fbcdn" not in url
                else static
            )
            return SimpleNamespace(
                status_code=200, headers={}, text=text, content=text.encode(), raise_for_status=lambda: None
            )

    monkeypatch.setattr(http_refresh.requests, "Session", Session)
    runtime = object.__new__(operation_fallback.V8Operations)
    runtime.add = Mock(side_effect=error)
    runtime.close = Mock()
    monkeypatch.setattr(operation_fallback, "V8Operations", lambda: runtime)
    found = http_refresh.discover_public(cache, pages=["https://www.facebook.com/x"])
    assert found == {"Static": "123456789012345"}
    assert stored[http_refresh.PUBLIC_DOCS_KEY]["operations"] == found
    assert runtime.add.call_count == 1
    assert sum("fbcdn" in url for url in urls) == 1


# --- redirects, refresh and setup errors ---------------------------------------------------------


def refresh_client(result):
    get = Mock(side_effect=result) if isinstance(result, Exception) else Mock(return_value=result)
    return SimpleNamespace(
        _cache={"cookies": {"c_user": "123"}},
        _session=SimpleNamespace(get=get),
        _throttle=Mock(),
        _html_headers=dict,
        _check_session_headers=Mock(),
        _rest_restricted_account=Mock(),
        _proxy_cfg={"url": "http://proxy:1"},
        REQUEST_TIMEOUT=(1, 1),
    )


def test_refresh_redirect_fails_without_resting_the_account():
    client = refresh_client(
        SimpleNamespace(status_code=302, headers={"location": "https://web.facebook.com/"}, text="")
    )
    assert http_refresh.refresh(client) is False
    client._rest_restricted_account.assert_not_called()
    client._check_session_headers.assert_called_once()


def test_refresh_403_still_rests_the_account():
    client = refresh_client(SimpleNamespace(status_code=403, headers={}, text="", content=b""))
    with pytest.raises(core.AccountRestrictedError):
        http_refresh.refresh(client)
    client._rest_restricted_account.assert_called_once()


def test_discovery_page_redirect_is_skipped_without_resting(monkeypatch):
    client = discovery_client(monkeypatch, lambda url: cdn_response(BUNDLE))
    client._session = SimpleNamespace(
        get=lambda url, **kw: SimpleNamespace(status_code=302, headers={}, text="", content=b"")
    )
    assert sdk_fallback.discover_authenticated(client, {NAME}) == {}
    client._rest_restricted_account.assert_not_called()
    client._check_session_headers.assert_called()


def test_refresh_network_error_requeues_through_the_proxy_path():
    client = refresh_client(cr.RequestsError("secret-proxy-password"))
    with pytest.raises(core.NetworkError) as raised:
        http_refresh.refresh(client)
    assert isinstance(raised.value.__cause__, pool.ProxyPoolExhaustedError)
    assert "secret-proxy-password" not in str(raised.value)


def test_seed_refuses_blocked_session(monkeypatch):
    from social_crawler.constants import facebook as fb
    from social_crawler.db import accounts
    from social_crawler.spiders.facebook.auth.identity import IDENTITY_KEY

    monkeypatch.setenv("FACEBOOK_GQL_FALLBACK_ENABLED", "1")
    monkeypatch.setattr(identity, "runtime_system", lambda: "macOS")
    monkeypatch.setattr(accounts, "is_account_usable", lambda *a: True)
    monkeypatch.setattr(accounts, "get_account_by_key", lambda *a: {"id": "123"})
    blocked = Mock(return_value=True)
    monkeypatch.setattr(accounts, "session_setup_blocked", blocked)
    state = {
        "cookies": [
            {"name": name, "value": value, "domain": ".facebook.com", "expires": -1}
            for name, value in {"c_user": "123", "xs": "rejected", "datr": "device"}.items()
        ]
    }
    data = {
        fb.STATE_REDIS_KEY_TMPL.format(account="test"): state,
        IDENTITY_KEY.format(account="test"): {"user_agent": UA_152},
    }
    cache = SimpleNamespace(get=data.get)
    assert sdk_fallback.seed(cache, "test") is None
    blocked.return_value = False
    assert sdk_fallback.seed(cache, "test")["cookies"]["xs"] == "rejected"


def test_session_setup_blocked_fails_closed_on_database_error(monkeypatch):
    import psycopg

    from social_crawler.db import accounts

    monkeypatch.setattr(accounts, "connect", Mock(side_effect=psycopg.OperationalError("down")))
    assert accounts.session_setup_blocked("facebook", "test") is True


def session_setup(monkeypatch, constructor):
    stored = {}

    class Cache:
        def get(self, key):
            return stored.get(key)

        def set(self, key, value, **kwargs):
            stored[key] = value

    monkeypatch.setattr(sessions, "RedisCache", Cache)
    monkeypatch.setattr(sessions, "_facebook_session_is_cached", lambda account: False)
    monkeypatch.setattr(sessions, "is_account_usable", lambda *a: True)
    monkeypatch.setattr(FacebookGraphQLClient, "__init__", constructor)
    return stored


def test_busy_lease_or_proxy_does_not_lock_the_account_out_of_refresh(monkeypatch):
    def constructor(self, **kwargs):
        raise core.NetworkError("busy") from pool.ProxyPoolExhaustedError("busy")

    stored = session_setup(monkeypatch, constructor)
    with pytest.raises(pool.ProxyPoolExhaustedError):
        asyncio.run(sessions._ensure_facebook_session(account="test"))
    assert "facebook:http_refresh_failed:test" not in stored


def test_real_setup_failure_still_locks_refresh(monkeypatch):
    def constructor(self, **kwargs):
        raise core.SessionExpiredError("rejected")

    stored = session_setup(monkeypatch, constructor)
    assert not asyncio.run(sessions._ensure_facebook_session(account="test"))
    assert stored["facebook:http_refresh_failed:test"] == 1


def test_budget_during_session_setup_skips_the_job(monkeypatch):
    def constructor(self, **kwargs):
        raise core.RequestBudgetExceeded("today")

    stored = session_setup(monkeypatch, constructor)
    with pytest.raises(CrawlJobSkipped):
        asyncio.run(sessions._ensure_facebook_session(account="test"))
    assert "facebook:http_refresh_failed:test" not in stored


def test_budget_while_preparing_a_recipe_skips_the_job(monkeypatch):
    def constructor(self, **kwargs):
        raise core.RequestBudgetExceeded("today")

    session_setup(monkeypatch, constructor)
    with pytest.raises(CrawlJobSkipped):
        sessions._ensure_facebook_recipe("test", "page_feed")


def test_asset_throttle_keeps_lease_and_budget_without_account_rest(monkeypatch):
    client = object.__new__(FacebookGraphQLClient)
    client._ensure_lease = Mock()
    client._count_request = Mock(side_effect=core.RequestBudgetExceeded("today"))
    sleep = Mock()
    monkeypatch.setattr(core.time, "sleep", sleep)
    with pytest.raises(core.RequestBudgetExceeded):
        client._throttle_asset()
    client._ensure_lease.assert_called_once()
    sleep.assert_not_called()
    client._count_request = Mock()
    client._throttle_asset()
    assert sleep.call_args.args[0] <= max(client.ASSET_GAP_SECONDS)


# --- cookie bị xoá giữa job không được sống lại ở job sau -------------------------------------------


class JobHandler(http.server.BaseHTTPRequestHandler):
    set_cookie = None
    seen = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        JobHandler.seen.append(self.headers.get("Cookie") or "")
        body = b'{"data":{"ok":true}}'
        self.send_response(200)
        if JobHandler.set_cookie:
            self.send_header("Set-Cookie", JobHandler.set_cookie)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def job(monkeypatch):
    from social_crawler.constants import facebook as fb
    from social_crawler.db import accounts
    from social_crawler.runtime import lanes

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), JobHandler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    port = httpd.server_address[1]
    JobHandler.seen, JobHandler.set_cookie = [], None
    real_session = core.curl_requests.Session

    def local_session(**options):
        options["proxies"] = None
        options["curl_options"] = {
            **options.get("curl_options", {}),
            CurlOpt.RESOLVE: [f"web.facebook.com:{port}:127.0.0.1"],
        }
        return real_session(**options)

    monkeypatch.setattr(core.curl_requests, "Session", local_session)
    monkeypatch.setattr(core.account_lease, "acquire", Mock())
    monkeypatch.setattr(core.account_lease, "release", Mock())
    monkeypatch.setattr(core.account_lease, "lost", lambda *a: False)
    monkeypatch.setattr(core.ip_switch, "touch", lambda *a: None)
    monkeypatch.setattr(core.pool, "acquire_proxy_for_account", lambda *a, **k: None)
    monkeypatch.setattr(identity, "runtime_system", lambda: "macOS")
    monkeypatch.setattr(FacebookGraphQLClient, "_throttle", Mock())
    monkeypatch.setattr(accounts, "rest_account_until", Mock(return_value=True))
    monkeypatch.setattr(accounts, "get_account_by_key", lambda *a: {"id": "123"})
    monkeypatch.setattr(accounts, "record_cookie_check", Mock())
    monkeypatch.setattr(accounts, "is_account_usable", lambda *a: True)
    monkeypatch.setattr(accounts, "session_setup_blocked", lambda *a: True)
    monkeypatch.setattr(lanes, "release_account", Mock())
    cookies = {"c_user": "123", "xs": "33:a:2:1800000000:-1:1", "datr": "d", "fr": "old"}
    recipe = {
        "recipe_source": sdk_fallback.SOURCE,
        "transport": "curl_cffi",
        "captured_at": time.time(),
        "doc_id": "1",
        "fb_api_req_friendly_name": "Q",
        "variables_template": {},
        "headers": {"user-agent": UA_152},
        "cookies": cookies,
        "body_static": {"__a": "1", "lsd": "l"},
    }
    store = {
        fb.CACHE_REDIS_KEY_TMPL.format(account="a"): recipe,
        fb.COMMENTS_REDIS_KEY_TMPL.format(account="a"): {**recipe},
        fb.STATE_REDIS_KEY_TMPL.format(account="a"): {
            "cookies": [{"name": k, "value": v, "domain": ".facebook.com"} for k, v in cookies.items()],
            "origins": [],
        },
    }
    cache = SimpleNamespace(
        store=store,
        get=lambda key: copy.deepcopy(store.get(key)),
        set=lambda key, value, **kw: store.__setitem__(key, copy.deepcopy(value)),
        delete=lambda key: store.pop(key, None),
    )

    def run_one_job():
        client = FacebookGraphQLClient(redis_cache=cache, account="a")
        client.GRAPHQL_URL = f"http://web.facebook.com:{port}/api/graphql/"
        return client._run(doc_id="1", friendly_name="Q", template={}, template_source="test", overrides={})

    yield SimpleNamespace(run=run_one_job, cache=cache, fb=fb)
    httpd.shutdown()


def test_cookie_deleted_in_one_job_is_not_sent_by_the_next(job):
    JobHandler.set_cookie = "fr=deleted; Path=/; Domain=.facebook.com; Max-Age=0"
    job.run()
    assert "fr=old" in JobHandler.seen[0]
    JobHandler.set_cookie = None
    job.run()
    assert "fr=" not in JobHandler.seen[1] and "xs=" in JobHandler.seen[1]
    for key in (job.fb.CACHE_REDIS_KEY_TMPL, job.fb.COMMENTS_REDIS_KEY_TMPL):
        assert "fr" not in job.cache.store[key.format(account="a")]["cookies"]
    state = job.cache.store[job.fb.STATE_REDIS_KEY_TMPL.format(account="a")]
    assert "fr" not in {c["name"] for c in state["cookies"]}


@pytest.mark.parametrize("name", ["xs", "c_user"])
def test_session_cookie_deleted_stops_the_job_and_nothing_is_sent_again(job, name):
    JobHandler.set_cookie = f"{name}=gone; Path=/; Domain=.facebook.com; Max-Age=0"
    with pytest.raises(core.SessionExpiredError):
        job.run()
    assert job.fb.CACHE_REDIS_KEY_TMPL.format(account="a") not in job.cache.store
    JobHandler.set_cookie = None
    with pytest.raises(core.SessionExpiredError):
        job.run()
    assert len(JobHandler.seen) == 1
